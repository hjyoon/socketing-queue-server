package app

import (
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port               string
	JWTSecret          string
	EntranceSecret     string
	RedisAddr          string
	DBURL              string
	MaxRoomConnections int
	SchedulingURL      string
	CORSOrigins        []string
}

func LoadConfig() Config {
	return Config{
		Port:               env("PORT", "3000"),
		JWTSecret:          env("JWT_SECRET", "my-jwt-secret"),
		EntranceSecret:     env("JWT_SECRET_FOR_ENTRANCE", "my-jwt-secret"),
		RedisAddr:          redisAddr(),
		DBURL:              dbURL(),
		MaxRoomConnections: envInt("MAX_ROOM_CONNECTIONS", 1000),
		SchedulingURL:      env("SCHEDULING_SERVER_URL", "http://localhost:3001/"),
		CORSOrigins:        splitList(env("CORS_ALLOWED_ORIGINS", "*")),
	}
}

func dbURL() string {
	raw := env("DB_URL", "postgres://postgres:password@localhost:5432/socketing")
	if strings.Contains(raw, "sslmode=") {
		return raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" {
		return raw
	}
	query := parsed.Query()
	query.Set("sslmode", "disable")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(env(key, ""))
	if err != nil {
		return fallback
	}
	return value
}

func redisAddr() string {
	return env("CACHE_HOST", "localhost") + ":" + env("CACHE_PORT", "6379")
}

func splitList(value string) []string {
	parts := strings.Split(value, ",")
	out := []string{}
	for _, part := range parts {
		if item := strings.TrimSpace(part); item != "" {
			out = append(out, item)
		}
	}
	if len(out) == 0 {
		return []string{"*"}
	}
	return out
}
