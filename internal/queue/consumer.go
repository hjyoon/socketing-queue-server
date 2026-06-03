package queue

import (
	"context"
	"strings"
	"time"

	"github.com/hjyoon/socketing-queue-server/internal/auth"
)

func (s *Service) consume(ctx context.Context, consumer string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		messages, err := s.cache.ReadStream(ctx, consumer)
		if err != nil {
			if strings.Contains(err.Error(), "NOGROUP") {
				_ = s.cache.EnsureGroup(ctx)
			}
			continue
		}
		for _, msg := range messages {
			_ = s.processStream(ctx, msg)
			_ = s.cache.Ack(ctx, msg.ID)
		}
	}
}

func (s *Service) processStream(ctx context.Context, msg StreamMessage) error {
	if msg.EventID == "" || msg.EventDateID == "" {
		return nil
	}
	room := roomName(msg.EventID, msg.EventDateID)
	if err := s.cleanupExpiredTokens(ctx, room); err != nil {
		return err
	}
	issued, err := s.cache.IssuedCount(ctx, room)
	if err != nil {
		return err
	}
	connected, err := s.cache.RoomCount(ctx, room)
	if err != nil {
		return err
	}
	if issued+connected >= s.cfg.MaxRoomConnections {
		return nil
	}
	_, first, ok, err := s.cache.PopIfPresent(ctx, "queue:"+room)
	if err != nil || !ok {
		return err
	}
	token, err := s.issueToken(ctx, first, msg.EventID, msg.EventDateID)
	if err != nil {
		return err
	}
	return s.cache.Publish(ctx, broadcastChannel, map[string]any{
		"socketId": first.SocketID, "type": "tokenIssued",
		"payload": map[string]string{"token": token}, "disconnect": true,
	})
}

func (s *Service) issueToken(ctx context.Context, item QueueItem, eventID, dateID string) (string, error) {
	token, err := auth.Sign(map[string]any{
		"sub": item.UserID, "eventId": eventID, "eventDateId": dateID,
	}, s.cfg.EntranceSecret, entranceTTL)
	if err != nil {
		return "", err
	}
	return token, s.cache.IssueToken(ctx, token, roomName(eventID, dateID))
}

func (s *Service) cleanupExpiredTokens(ctx context.Context, room string) error {
	tokens, err := s.cache.IssuedTokens(ctx, room)
	if err != nil {
		return err
	}
	for _, token := range tokens {
		ttl, err := s.cache.TokenTTL(ctx, token)
		if err != nil {
			return err
		}
		if ttl == -2*time.Second {
			_ = s.cache.RemoveIssuedToken(ctx, room, token)
		}
	}
	return nil
}
