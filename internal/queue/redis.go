package queue

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	c *redis.Client
}

func NewRedis(c *redis.Client) *Redis {
	return &Redis{c: c}
}

func (r *Redis) Ready(ctx context.Context) error {
	return r.c.Ping(ctx).Err()
}

func (r *Redis) Publish(ctx context.Context, channel string, message any) error {
	raw, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return r.c.Publish(ctx, channel, raw).Err()
}

func (r *Redis) Subscribe(ctx context.Context, channel string, cb func(string)) error {
	sub := r.c.Subscribe(ctx, channel)
	go func() {
		for msg := range sub.Channel() {
			cb(msg.Payload)
		}
	}()
	return nil
}

func (r *Redis) ScanQueues(ctx context.Context) ([]string, error) {
	var cursor uint64
	out := []string{}
	for {
		keys, next, err := r.c.Scan(ctx, cursor, "queue:*", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if strings.Count(key, "_") == 1 {
				out = append(out, key)
			}
		}
		if next == 0 {
			return out, nil
		}
		cursor = next
	}
}

func (r *Redis) RoomCount(ctx context.Context, room string) (int, error) {
	value, err := r.c.Get(ctx, "room:"+room+":count").Int()
	if err == redis.Nil {
		return 0, nil
	}
	return value, err
}

func (r *Redis) IssueToken(ctx context.Context, token, room string) error {
	if err := r.c.SAdd(ctx, "issued_tokens:"+room, token).Err(); err != nil {
		return err
	}
	return r.c.Set(ctx, "token:"+token, "issued", tokenTTL).Err()
}

func (r *Redis) TokenTTL(ctx context.Context, token string) (time.Duration, error) {
	return r.c.TTL(ctx, "token:"+token).Result()
}
