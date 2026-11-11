package main

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

var errNoMessage = errors.New("no message")

// RedisStore implements the same PushMessage/PopMessage contract as
// fluid-core's store.Store, backed by a Redis list (FIFO via
// LPUSH/RPOP). Swap in KAN-14's implementation once it lands.
type RedisStore struct {
	rdb *redis.Client
	key string
}

func NewRedisStore(rdb *redis.Client, key string) *RedisStore {
	return &RedisStore{rdb: rdb, key: key}
}

func (s *RedisStore) PushMessage(ctx context.Context, message []byte) error {
	return s.rdb.LPush(ctx, s.key, message).Err()
}

func (s *RedisStore) PopMessage(ctx context.Context) ([]byte, error) {
	msg, err := s.rdb.RPop(ctx, s.key).Result()
	if errors.Is(err, redis.Nil) {
		return nil, errNoMessage
	}
	if err != nil {
		return nil, err
	}
	return []byte(msg), nil
}
