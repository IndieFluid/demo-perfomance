package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"time"

	fluidclient "github.com/IndieFluid/fluid-client-go"
	"github.com/minio/minio-go/v7"
)

// dispatcherLoop pops job messages from the Redis queue and pushes
// them to the fluid-proxy worker pool over TCP. It polls once a
// second when the queue is empty.
func dispatcherLoop(ctx context.Context, queue *RedisStore, fc *fluidclient.Client) {
	for {
		msg, err := queue.PopMessage(ctx)
		switch {
		case errors.Is(err, errNoMessage):
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		case err != nil:
			log.Printf("dispatcher: pop failed: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if err := fc.PushTask(ctx, msg); err != nil {
			log.Printf("dispatcher: push to proxy failed: %v", err)
		}
	}
}

type taskHandler func(ctx context.Context, t taskMessage)

// serveLoop receives tasks dispatched back by the proxy's workers and
// runs the handler mapped by task_id.
func serveLoop(ctx context.Context, fc *fluidclient.Client, s *jobServer, handlers map[uint32]taskHandler) {
	for {
		err := fc.Serve(ctx, func(msg []byte) {
			var t taskMessage
			if err := json.Unmarshal(msg, &t); err != nil {
				log.Printf("executor: bad task message: %v", err)
				return
			}
			h, ok := handlers[t.TaskID]
			if !ok {
				log.Printf("executor: no handler for task_id %d", t.TaskID)
				return
			}
			h(ctx, t)
		})
		if ctx.Err() != nil {
			return
		}
		log.Printf("executor: receive loop ended: %v; retrying", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// uploadFileHandler (task_id = 1) streams the object from MinIO and
// records its byte length — io.Copy to Discard never buffers the
// whole object in memory.
func uploadFileHandler(s *jobServer) taskHandler {
	return func(ctx context.Context, t taskMessage) {
		s.setJobState(ctx, t.JobID, map[string]any{"state": stateProcessing})

		obj, err := s.mc.GetObject(ctx, s.bucket, t.S3Path, minio.GetObjectOptions{})
		if err != nil {
			s.setJobState(ctx, t.JobID, map[string]any{"state": stateFailed, "error": err.Error()})
			return
		}
		n, err := io.Copy(io.Discard, obj)
		obj.Close()
		if err != nil {
			s.setJobState(ctx, t.JobID, map[string]any{"state": stateFailed, "error": err.Error()})
			return
		}
		s.setJobState(ctx, t.JobID, map[string]any{"state": stateDone, "length": n})
	}
}
