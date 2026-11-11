package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
)

const taskUploadFile uint32 = 1

const (
	stateQueued     = "queued"
	stateProcessing = "processing"
	stateDone       = "done"
	stateFailed     = "failed"
)

// taskMessage is the payload that travels Redis -> proxy -> worker -> back.
type taskMessage struct {
	JobID  string `json:"job_id"`
	TaskID uint32 `json:"task_id"`
	S3Path string `json:"s3_path"`
}

type jobServer struct {
	rdb    *redis.Client
	mc     *minio.Client
	bucket string
	queue  *RedisStore
}

func jobKey(id string) string { return "job:" + id }

func newJobID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%x", b[:])
}

func (s *jobServer) setJobState(ctx context.Context, jobID string, fields map[string]any) {
	if err := s.rdb.HSet(ctx, jobKey(jobID), fields).Err(); err != nil {
		log.Printf("job %s: failed to update state: %v", jobID, err)
	}
}

// POST /jobs
//
//	multipart "file"      -> streams the file to MinIO, enqueues task
//	{"s3_path": "key"}    -> enqueues a task for an existing object
func (s *jobServer) createJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := newJobID()

	var s3path string
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		f, hdr, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "multipart field \"file\" required", http.StatusBadRequest)
			return
		}
		defer f.Close()
		s3path = fmt.Sprintf("jobs/%s-%s", jobID, hdr.Filename)
		// size -1 => chunked streaming upload, file never buffered in memory
		if _, err := s.mc.PutObject(ctx, s.bucket, s3path, f, -1, minio.PutObjectOptions{}); err != nil {
			http.Error(w, "s3 upload failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		var body struct {
			S3Path string `json:"s3_path"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.S3Path == "" {
			http.Error(w, `expected multipart "file" or {"s3_path": "..."}`, http.StatusBadRequest)
			return
		}
		s3path = body.S3Path
	}

	msg, _ := json.Marshal(taskMessage{JobID: jobID, TaskID: taskUploadFile, S3Path: s3path})
	if err := s.queue.PushMessage(ctx, msg); err != nil {
		http.Error(w, "enqueue failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.setJobState(ctx, jobID, map[string]any{
		"state":   stateQueued,
		"task_id": taskUploadFile,
		"s3_path": s3path,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "s3_path": s3path})
}

// GET /jobs/{id}?wait=<seconds>
//
// Long-polls the job record until it leaves queued/processing or wait
// elapses (default 30s), then reports state and the streamed length.
func (s *jobServer) getJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := r.PathValue("id")
	waitSec := 30
	if v := r.URL.Query().Get("wait"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			waitSec = n
		}
	}
	deadline := time.Now().Add(time.Duration(waitSec) * time.Second)

	respond := func(fields map[string]string) {
		if len(fields) == 0 {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		length, _ := strconv.ParseInt(fields["length"], 10, 64)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"job_id":  jobID,
			"state":   fields["state"],
			"task_id": fields["task_id"],
			"s3_path": fields["s3_path"],
			"length":  length,
		})
	}

	for {
		fields, err := s.rdb.HGetAll(ctx, jobKey(jobID)).Result()
		if err != nil {
			http.Error(w, "redis error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if len(fields) == 0 {
			respond(fields)
			return
		}
		st := fields["state"]
		if st == stateDone || st == stateFailed || time.Now().After(deadline) {
			respond(fields)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}
