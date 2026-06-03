package queue

import (
	"context"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	broadcastChannel = "socketing:queue:broadcast"
	streamKey        = "queue-messages"
	consumerGroup    = "queue-group"
	tokenTTL         = 10 * time.Second
	entranceTTL      = 10 * time.Minute
)

type Config struct {
	JWTSecret          string
	EntranceSecret     string
	SchedulingURL      string
	MaxRoomConnections int
}

type QueueItem struct {
	SocketID string `json:"socketId"`
	UserID   string `json:"userId"`
}

type StreamMessage struct {
	ID          string
	EventID     string
	EventDateID string
}

type Cache interface {
	Ready(context.Context) error
	Publish(context.Context, string, any) error
	Subscribe(context.Context, string, func(string)) error
	AddClient(context.Context, string, QueueItem) error
	RemoveClient(context.Context, string, QueueItem) (int64, error)
	Queue(context.Context, string) ([]QueueItem, error)
	FindIndex(context.Context, string, QueueItem) (int64, error)
	PopIfPresent(context.Context, string) (int64, QueueItem, bool, error)
	ScanQueues(context.Context) ([]string, error)
	RoomCount(context.Context, string) (int, error)
	EnsureGroup(context.Context) error
	AddStream(context.Context, string, string) error
	ReadStream(context.Context, string) ([]StreamMessage, error)
	Ack(context.Context, string) error
	IssuedTokens(context.Context, string) ([]string, error)
	TokenTTL(context.Context, string) (time.Duration, error)
	RemoveIssuedToken(context.Context, string, string) error
	IssuedCount(context.Context, string) (int, error)
	IssueToken(context.Context, string, string) error
}

type Store interface {
	Ready(context.Context) error
}

type userClaims = jwt.MapClaims
