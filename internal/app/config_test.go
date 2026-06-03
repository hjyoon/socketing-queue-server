package app

import "testing"

func TestLoadConfig(t *testing.T) {
	t.Setenv("PORT", "3010")
	t.Setenv("JWT_SECRET", "jwt")
	t.Setenv("JWT_SECRET_FOR_ENTRANCE", "entrance")
	t.Setenv("CACHE_HOST", "redis")
	t.Setenv("CACHE_PORT", "6380")
	t.Setenv("DB_URL", "postgres://u:p@db:5432/app")
	t.Setenv("MAX_ROOM_CONNECTIONS", "120")
	t.Setenv("SCHEDULING_SERVER_URL", "http://scheduling/")
	t.Setenv("CORS_ALLOWED_ORIGINS", "http://a, http://b")
	cfg := LoadConfig()
	if cfg.Port != "3010" || cfg.JWTSecret != "jwt" || cfg.EntranceSecret != "entrance" {
		t.Fatalf("unexpected secrets or port: %#v", cfg)
	}
	if cfg.RedisAddr != "redis:6380" || cfg.MaxRoomConnections != 120 {
		t.Fatalf("unexpected redis or max: %#v", cfg)
	}
	if cfg.DBURL != "postgres://u:p@db:5432/app?sslmode=disable" {
		t.Fatalf("unexpected db url: %s", cfg.DBURL)
	}
	if len(cfg.CORSOrigins) != 2 || cfg.CORSOrigins[1] != "http://b" {
		t.Fatalf("unexpected cors origins: %#v", cfg.CORSOrigins)
	}
}

func TestConfigFallbacks(t *testing.T) {
	t.Setenv("MAX_ROOM_CONNECTIONS", "bad")
	t.Setenv("DB_URL", "not a url")
	t.Setenv("CORS_ALLOWED_ORIGINS", " , ")
	cfg := LoadConfig()
	if cfg.MaxRoomConnections != 1000 || cfg.DBURL != "not a url" {
		t.Fatalf("unexpected fallback config: %#v", cfg)
	}
	if len(cfg.CORSOrigins) != 1 || cfg.CORSOrigins[0] != "*" {
		t.Fatalf("unexpected cors fallback: %#v", cfg.CORSOrigins)
	}
}

func TestDBURLKeepsSSLMode(t *testing.T) {
	t.Setenv("DB_URL", "postgres://u:p@db/app?sslmode=require")
	if got := dbURL(); got != "postgres://u:p@db/app?sslmode=require" {
		t.Fatalf("unexpected db url: %s", got)
	}
}
