package queue

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func (r *Redis) EnsureGroup(ctx context.Context) error {
	err := r.c.XGroupCreateMkStream(ctx, streamKey, consumerGroup, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (r *Redis) AddStream(ctx context.Context, eventID, eventDateID string) error {
	return r.c.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey, Values: map[string]any{"eventId": eventID, "eventDateId": eventDateID},
	}).Err()
}

func (r *Redis) ReadStream(ctx context.Context, consumer string) ([]StreamMessage, error) {
	streams, err := r.c.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: consumerGroup, Consumer: consumer, Streams: []string{streamKey, ">"},
		Count: 10, Block: time.Second,
	}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := []StreamMessage{}
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			out = append(out, StreamMessage{
				ID:          msg.ID,
				EventID:     toString(msg.Values["eventId"]),
				EventDateID: toString(msg.Values["eventDateId"]),
			})
		}
	}
	return out, nil
}

func (r *Redis) Ack(ctx context.Context, id string) error {
	return r.c.XAck(ctx, streamKey, consumerGroup, id).Err()
}

func toString(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}
