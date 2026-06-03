package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"

	"github.com/hjyoon/socketing-queue-server/internal/app"
	"github.com/hjyoon/socketing-queue-server/internal/queue"
)

func main() {
	cfg := app.LoadConfig()
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		healthcheck(cfg.Port)
		return
	}
	db, err := sql.Open("postgres", cfg.DBURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	rc := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rc.Close()
	cache := queue.NewRedis(rc)
	service := queue.NewService(queue.Config{
		JWTSecret: cfg.JWTSecret, EntranceSecret: cfg.EntranceSecret,
		SchedulingURL: cfg.SchedulingURL, MaxRoomConnections: cfg.MaxRoomConnections,
	}, cache, queue.NewPostgres(db))
	ctx := context.Background()
	service.Start(ctx)
	defer service.Stop()
	if err := app.NewRouter(cfg, service).Run(":" + cfg.Port); err != nil {
		log.Fatal(err)
	}
}

func healthcheck(port string) {
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://127.0.0.1:" + port + "/liveness")
	if err != nil {
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusInternalServerError {
		os.Exit(1)
	}
}
