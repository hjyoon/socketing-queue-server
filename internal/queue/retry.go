package queue

import (
	"context"
	"log"
	"time"
)

const (
	initialRetryDelay = time.Second
	maxRetryDelay     = 30 * time.Second
)

func (s *Service) ensureConsumerGroup(ctx context.Context) bool {
	return retry(ctx, "initialize queue consumer group", func() error {
		return s.cache.EnsureGroup(ctx)
	})
}

func retry(ctx context.Context, operation string, attempt func() error) bool {
	delay := initialRetryDelay
	for ctx.Err() == nil {
		if err := attempt(); err == nil {
			return ctx.Err() == nil
		} else if ctx.Err() == nil {
			log.Printf("%s failed: %v; retrying in %s", operation, err, delay)
		}
		if !waitForRetry(ctx, delay) {
			return false
		}
		delay = nextRetryDelay(delay)
	}
	return false
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

func nextRetryDelay(delay time.Duration) time.Duration {
	return min(delay*2, maxRetryDelay)
}
