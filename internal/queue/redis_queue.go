package queue

import (
	"context"
	"encoding/json"
)

func (r *Redis) AddClient(ctx context.Context, name string, item QueueItem) error {
	raw, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return r.c.RPush(ctx, name, raw).Err()
}

func (r *Redis) RemoveClient(ctx context.Context, name string, item QueueItem) (int64, error) {
	raw, err := json.Marshal(item)
	if err != nil {
		return 0, err
	}
	return r.c.LRem(ctx, name, 0, raw).Result()
}

func (r *Redis) Queue(ctx context.Context, name string) ([]QueueItem, error) {
	raw, err := r.c.LRange(ctx, name, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	out := []QueueItem{}
	for _, value := range raw {
		var item QueueItem
		if json.Unmarshal([]byte(value), &item) == nil {
			out = append(out, item)
		}
	}
	return out, nil
}

func (r *Redis) FindIndex(ctx context.Context, name string, item QueueItem) (int64, error) {
	queue, err := r.Queue(ctx, name)
	if err != nil {
		return -1, err
	}
	for i, next := range queue {
		if next == item {
			return int64(i), nil
		}
	}
	return -1, nil
}

func (r *Redis) PopIfPresent(ctx context.Context, name string) (int64, QueueItem, bool, error) {
	length, err := r.c.LLen(ctx, name).Result()
	if err != nil || length == 0 {
		return length, QueueItem{}, false, err
	}
	raw, err := r.c.LPop(ctx, name).Result()
	if err != nil {
		return length, QueueItem{}, false, err
	}
	var item QueueItem
	if err := json.Unmarshal([]byte(raw), &item); err != nil {
		return length, QueueItem{}, false, err
	}
	return length, item, true, nil
}
