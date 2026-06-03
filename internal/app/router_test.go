package app

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

type fakeService struct {
	err error
}

func (f fakeService) Ready(*gin.Context) error { return f.err }
func (f fakeService) HandleHTTP(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"ws": true})
}

func TestRouterHealth(t *testing.T) {
	router := NewRouter(Config{CORSOrigins: []string{"*"}}, fakeService{})
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/liveness", nil)
	router.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected liveness status: %d", resp.Code)
	}
}

func TestRouterReadiness(t *testing.T) {
	router := NewRouter(Config{CORSOrigins: []string{"*"}}, fakeService{})
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/readiness", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected readiness status: %d", resp.Code)
	}
}

func TestRouterReadinessError(t *testing.T) {
	router := NewRouter(Config{CORSOrigins: []string{"*"}}, fakeService{err: errors.New("down")})
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/readiness", nil))
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected readiness status: %d", resp.Code)
	}
}
