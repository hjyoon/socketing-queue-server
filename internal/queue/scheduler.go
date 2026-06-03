package queue

import (
	"context"
	"net/http"
	"time"

	"github.com/hjyoon/socketing-queue-server/internal/auth"
)

func (s *Service) notifyScheduling(ctx context.Context, eventID, dateID string) error {
	token, err := auth.Sign(map[string]any{
		"sub": "scheduling", "eventId": eventID, "eventDateId": dateID,
	}, s.cfg.JWTSecret, 10*time.Minute)
	if err != nil {
		return err
	}
	for _, path := range []string{
		"scheduling/reservation/status", "scheduling/queue/status",
	} {
		if err := s.postScheduling(ctx, path, token); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) postScheduling(ctx context.Context, path, token string) error {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, serviceURL(s.cfg.SchedulingURL, path), nil,
	)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := s.client.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	return err
}
