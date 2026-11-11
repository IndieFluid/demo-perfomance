package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	fluidclient "github.com/IndieFluid/fluid-client-go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// retryUntil loops fn until it succeeds or ctx is cancelled — the app
// tolerates dependencies (proxy/redis/minio) still starting up.
func retryUntil(ctx context.Context, what string, fn func() error) error {
	for {
		if err := fn(); err != nil {
			log.Printf("%s not ready: %v — retrying", what, err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}
		return nil
	}
}

func main() {
	var (
		httpAddr   = env("HTTP_ADDR", ":8081")
		proxyAddr  = env("PROXY_ADDR", "localhost:8080")
		redisAddr  = env("REDIS_ADDR", "localhost:6379")
		s3Endpoint = env("S3_ENDPOINT", "localhost:9000")
		s3User     = env("S3_ACCESS_KEY", "minioadmin")
		s3Pass     = env("S3_SECRET_KEY", "minioadmin")
		bucket     = env("S3_BUCKET", "jobs")
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	if err := retryUntil(ctx, "redis", func() error { return rdb.Ping(ctx).Err() }); err != nil {
		log.Fatal(err)
	}

	mc, err := minio.New(s3Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3User, s3Pass, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("minio client: %v", err)
	}
	if err := retryUntil(ctx, "minio", func() error {
		ok, err := mc.BucketExists(ctx, bucket)
		if err != nil {
			return err
		}
		if !ok {
			return mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	var fc *fluidclient.Client
	if err := retryUntil(ctx, "proxy", func() error {
		c, err := fluidclient.Dial(ctx, proxyAddr)
		if err != nil {
			return err
		}
		fc = c
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	defer fc.Close()

	// bootstrap the worker pool; the gradient autoscaler takes over
	// from here
	if err := fc.AddWorkers(ctx, 2); err != nil {
		log.Printf("add workers failed: %v", err)
	}

	queue := NewRedisStore(rdb, "jobqueue")
	srv := &jobServer{rdb: rdb, mc: mc, bucket: bucket, queue: queue}
	handlers := map[uint32]taskHandler{
		taskUploadFile: uploadFileHandler(srv),
	}

	go dispatcherLoop(ctx, queue, fc)
	go serveLoop(ctx, fc, srv, handlers)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", srv.createJob)
	mux.HandleFunc("GET /jobs/{id}", srv.getJob)
	httpSrv := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		httpSrv.Shutdown(shutdownCtx)
	}()

	log.Printf("demo app listening on %s (proxy %s)", httpAddr, proxyAddr)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
