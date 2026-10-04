package queue

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hjyoon/socketing-queue-server/internal/auth"
)

type streamResult struct {
	messages []StreamMessage
	err      error
}

type workerCache struct {
	Cache
	stream            chan streamResult
	subscribeFailures int
	groupFailures     int
	groupErr          error
	readyErr          error
	subscribeCalls    int
	groupCalls        int
	readCalls         int
	broadcast         func(string)
	popped            bool
	issuedToken       string
	acknowledged      []string
}

func (c *workerCache) Ready(context.Context) error { return c.readyErr }

func (c *workerCache) Subscribe(_ context.Context, _ string, cb func(string)) error {
	c.subscribeCalls++
	if c.subscribeCalls <= c.subscribeFailures {
		return errors.New("Redis subscription unavailable")
	}
	c.broadcast = cb
	return nil
}

func (c *workerCache) EnsureGroup(context.Context) error {
	c.groupCalls++
	if c.groupErr != nil {
		return c.groupErr
	}
	if c.groupCalls <= c.groupFailures {
		return errors.New("Redis unavailable")
	}
	return nil
}

func (c *workerCache) ReadStream(ctx context.Context, _ string) ([]StreamMessage, error) {
	c.readCalls++
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-c.stream:
		return result.messages, result.err
	}
}

func (c *workerCache) IssuedTokens(context.Context, string) ([]string, error) {
	return nil, nil
}

func (c *workerCache) IssuedCount(context.Context, string) (int, error) { return 0, nil }
func (c *workerCache) RoomCount(context.Context, string) (int, error)   { return 0, nil }

func (c *workerCache) PopIfPresent(context.Context, string) (int64, QueueItem, bool, error) {
	if c.popped {
		return 0, QueueItem{}, false, nil
	}
	c.popped = true
	return 1, QueueItem{SocketID: "socket", UserID: "user"}, true, nil
}

func (c *workerCache) IssueToken(_ context.Context, token, _ string) error {
	c.issuedToken = token
	return nil
}

func (c *workerCache) Publish(_ context.Context, _ string, message any) error {
	raw, err := json.Marshal(message)
	if err == nil {
		c.broadcast(string(raw))
	}
	return err
}

func (c *workerCache) Ack(_ context.Context, id string) error {
	c.acknowledged = append(c.acknowledged, id)
	return nil
}

type workerStore struct{ readyErr error }

func (s *workerStore) Ready(context.Context) error { return s.readyErr }

type sentMessage struct {
	typeName string
	payload  any
}

func newWorkerTest() (*Service, *workerCache, *workerStore, chan sentMessage) {
	cache := &workerCache{stream: make(chan streamResult, 4)}
	store := &workerStore{}
	service := NewService(Config{EntranceSecret: "test-entrance-secret", MaxRoomConnections: 1}, cache, store)
	sent := make(chan sentMessage, 1)
	client := NewTestClient("socket", "user")
	client.send = func(kind string, payload any) { sent <- sentMessage{kind, payload} }
	service.hub.Add(client)
	return service, cache, store, sent
}

func enqueueAdmission(cache *workerCache) {
	cache.stream <- streamResult{messages: []StreamMessage{{ID: "1-0", EventID: "event", EventDateID: "date"}}}
}

func readyContext() *gin.Context {
	return &gin.Context{Request: httptest.NewRequest(http.MethodGet, "/readiness", nil)}
}

func assertAdmission(t *testing.T, cache *workerCache, sent <-chan sentMessage) {
	t.Helper()
	select {
	case message := <-sent:
		if message.typeName != "tokenIssued" {
			t.Fatalf("message type = %q, want tokenIssued", message.typeName)
		}
		payload, ok := message.payload.(map[string]any)
		if !ok {
			t.Fatalf("unexpected token payload: %#v", message.payload)
		}
		token, _ := payload["token"].(string)
		if token == "" || token != cache.issuedToken {
			t.Fatal("broadcast token does not match the stored entrance token")
		}
		claims, err := auth.Verify(token, "test-entrance-secret")
		if err != nil {
			t.Fatalf("verify entrance token: %v", err)
		}
		if claims["sub"] != "user" || claims["eventId"] != "event" || claims["eventDateId"] != "date" {
			t.Fatalf("unexpected entrance claims: %v", claims)
		}
	default:
		t.Fatal("waiting client did not receive an entrance token")
	}
	if len(cache.acknowledged) != 1 || cache.acknowledged[0] != "1-0" {
		t.Fatalf("acknowledged messages = %v, want [1-0]", cache.acknowledged)
	}
}

func TestWorkerRecoversFromStartupFailure(t *testing.T) {
	for _, stage := range []string{"subscription", "consumer group"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				service, cache, _, sent := newWorkerTest()
				if stage == "subscription" {
					cache.subscribeFailures = 1
				} else {
					cache.groupFailures = 2
				}
				enqueueAdmission(cache)
				service.Start(context.Background())
				defer service.Stop()
				synctest.Wait()
				if err := service.Ready(readyContext()); err == nil {
					t.Fatal("worker reported ready while initialization was failing")
				}
				time.Sleep(5 * time.Second)
				synctest.Wait()
				assertAdmission(t, cache, sent)
				if err := service.Ready(readyContext()); err != nil {
					t.Fatalf("recovered worker is not ready: %v", err)
				}
			})
		})
	}
}

func TestWorkerStopCancelsInitializationRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service, cache, _, _ := newWorkerTest()
		cache.groupErr = errors.New("Redis unavailable")
		service.Start(context.Background())
		defer service.Stop()
		synctest.Wait()
		attempts := cache.groupCalls
		service.Stop()
		synctest.Wait()
		time.Sleep(time.Hour)
		synctest.Wait()
		if cache.groupCalls != attempts || cache.readCalls != 0 {
			t.Fatalf("stopped worker continued running: group calls %d -> %d, reads %d", attempts, cache.groupCalls, cache.readCalls)
		}
		if err := service.Ready(readyContext()); err == nil {
			t.Fatal("stopped worker reported ready")
		}
	})
}

func TestWorkerRecoversFromReadFailure(t *testing.T) {
	for _, failure := range []string{"connection reset", "NOGROUP No such consumer group"} {
		t.Run(failure, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				service, cache, _, sent := newWorkerTest()
				cache.stream <- streamResult{}
				service.Start(context.Background())
				defer service.Stop()
				synctest.Wait()
				if err := service.Ready(readyContext()); err != nil {
					t.Fatalf("worker is not ready after successful read: %v", err)
				}
				if failure != "connection reset" {
					// Recreating a lost group must also survive transient Redis failures.
					cache.groupFailures = cache.groupCalls + 2
				}
				cache.stream <- streamResult{err: errors.New(failure)}
				synctest.Wait()
				if err := service.Ready(readyContext()); err == nil {
					t.Fatal("worker reported ready during a stream failure")
				}
				enqueueAdmission(cache)
				time.Sleep(10 * time.Second)
				synctest.Wait()
				assertAdmission(t, cache, sent)
				if err := service.Ready(readyContext()); err != nil {
					t.Fatalf("recovered worker is not ready: %v", err)
				}
				if failure != "connection reset" && cache.groupCalls != 4 {
					t.Fatalf("consumer group initialization calls = %d, want 4", cache.groupCalls)
				}
				service.Stop()
				synctest.Wait()
				if err := service.Ready(readyContext()); err == nil {
					t.Fatal("stopped consumer still reported ready")
				}
			})
		})
	}
}

func TestWorkerReadinessIncludesDependencies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service, cache, store, _ := newWorkerTest()
		cache.stream <- streamResult{}
		service.Start(context.Background())
		defer service.Stop()
		synctest.Wait()
		cache.readyErr = errors.New("Redis is down")
		if err := service.Ready(readyContext()); !errors.Is(err, cache.readyErr) {
			t.Fatalf("readiness error = %v, want Redis error", err)
		}
		cache.readyErr = nil
		store.readyErr = errors.New("PostgreSQL is down")
		if err := service.Ready(readyContext()); !errors.Is(err, store.readyErr) {
			t.Fatalf("readiness error = %v, want PostgreSQL error", err)
		}
	})
}
