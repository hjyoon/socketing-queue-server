package queue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func integrationRedis(t *testing.T) *Redis {
	t.Helper()
	addr := os.Getenv("SOCKETING_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set SOCKETING_TEST_REDIS_ADDR to an isolated Redis instance for integration tests")
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect to integration Redis: %v", err)
	}
	return NewRedis(client)
}

func TestRedisConsumerGroupIncludesExistingRequests(t *testing.T) {
	cache := integrationRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exists, err := cache.c.Exists(ctx, streamKey).Result()
	if err != nil || exists != 0 {
		t.Fatalf("integration Redis DB 15 must have no existing %s stream (exists=%d, error=%v)", streamKey, exists, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := cache.c.Del(cleanupCtx, streamKey).Err(); err != nil {
			t.Errorf("remove test stream: %v", err)
		}
	})
	// This request arrived before the worker could initialize its consumer group.
	if err := cache.AddStream(ctx, "event", "before-startup"); err != nil {
		t.Fatal(err)
	}
	if err := cache.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := cache.ReadStream(ctx, "test-consumer")
	if err != nil || len(before) != 1 || before[0].EventDateID != "before-startup" {
		t.Fatalf("existing request was skipped: messages=%v, error=%v", before, err)
	}
	if err := cache.Ack(ctx, before[0].ID); err != nil {
		t.Fatal(err)
	}
	// Ensuring an existing group must preserve its position rather than replaying it.
	if err := cache.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cache.AddStream(ctx, "event", "after-startup"); err != nil {
		t.Fatal(err)
	}
	after, err := cache.ReadStream(ctx, "test-consumer")
	if err != nil || len(after) != 1 || after[0].EventDateID != "after-startup" {
		t.Fatalf("existing group position was not preserved: messages=%v, error=%v", after, err)
	}
}

func TestRedisSubscriptionReceivesBroadcastAndCloses(t *testing.T) {
	cache := integrationRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	channel := "socketing:test:" + t.Name()
	messages := make(chan string, 1)
	if err := cache.Subscribe(ctx, channel, func(raw string) { messages <- raw }); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := cache.Publish(ctx, channel, map[string]string{"type": "tokenIssued"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case raw := <-messages:
		if raw != `{"type":"tokenIssued"}` {
			t.Fatalf("unexpected broadcast: %s", raw)
		}
	case <-ctx.Done():
		t.Fatal("subscription did not receive the broadcast")
	}
	cancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		counts, err := cache.c.PubSubNumSub(closeCtx, channel).Result()
		if err != nil {
			t.Fatalf("check subscriber cleanup: %v", err)
		}
		if counts[channel] == 0 {
			return
		}
		select {
		case <-closeCtx.Done():
			t.Fatal("subscription remained open after cancellation")
		case <-ticker.C:
		}
	}
}

func TestRedisSubscriptionReturnsConnectionFailure(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	_ = client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := NewRedis(client).Subscribe(ctx, "test", func(string) {}); err == nil {
		t.Fatal("subscription reported success on a closed Redis client")
	}
}
