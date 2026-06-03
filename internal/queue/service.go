package queue

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hjyoon/socketing-queue-server/internal/auth"
)

type Service struct {
	cfg    Config
	cache  Cache
	store  Store
	hub    *Hub
	done   context.CancelFunc
	client *http.Client
}

func NewService(cfg Config, cache Cache, store Store) *Service {
	return &Service{
		cfg: cfg, cache: cache, store: store, hub: NewHub(),
		client: &http.Client{Timeout: 2 * time.Second},
	}
}

func (s *Service) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	s.done = cancel
	_ = s.cache.Subscribe(runCtx, broadcastChannel, s.handleBroadcast)
	go func() {
		if err := s.cache.EnsureGroup(runCtx); err != nil {
			return
		}
		s.consume(runCtx, "consumer-"+auth.UUID())
	}()
}

func (s *Service) Stop() {
	if s.done != nil {
		s.done()
	}
}

func (s *Service) Ready(c *gin.Context) error {
	if err := s.cache.Ready(c.Request.Context()); err != nil {
		return err
	}
	return s.store.Ready(c.Request.Context())
}

func (s *Service) handleBroadcast(raw string) {
	var msg struct {
		SocketID   string          `json:"socketId"`
		Room       string          `json:"room"`
		Type       string          `json:"type"`
		Payload    json.RawMessage `json:"payload"`
		Disconnect bool            `json:"disconnect"`
	}
	if json.Unmarshal([]byte(raw), &msg) != nil || msg.Type == "" {
		return
	}
	var payload any
	_ = json.Unmarshal(msg.Payload, &payload)
	if msg.SocketID != "" {
		s.sendSocket(msg.SocketID, msg.Type, payload, msg.Disconnect)
		return
	}
	if msg.Room == "" {
		return
	}
	if msg.Type == "queueStatus" {
		_ = s.broadcastQueueUpdate(context.Background(), msg.Room)
		return
	}
	for _, client := range s.hub.Clients(msg.Room) {
		client.Send(msg.Type, payload)
	}
}

func (s *Service) sendSocket(id, t string, payload any, disconnect bool) {
	client := s.hub.Get(id)
	if client == nil {
		return
	}
	client.Send(t, payload)
	if disconnect {
		time.AfterFunc(50*time.Millisecond, func() { client.Close(1000, "Token issued") })
	}
}

var errStopConsumer = errors.New("queue consumer stopped")
