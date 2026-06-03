package auth

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSignVerify(t *testing.T) {
	token, err := Sign(map[string]any{"sub": "user"}, "secret", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := Verify(token, "secret")
	if err != nil {
		t.Fatal(err)
	}
	if claims["sub"] != "user" || claims["jti"] == "" {
		t.Fatalf("unexpected claims: %#v", claims)
	}
}

func TestVerifyRejectsInvalidToken(t *testing.T) {
	if _, err := Verify("bad", "secret"); err == nil {
		t.Fatal("expected invalid token error")
	}
}

func TestUUID(t *testing.T) {
	id := UUID()
	if len(id) != 36 || strings.Count(id, "-") != 4 {
		t.Fatalf("unexpected uuid: %s", id)
	}
}

func TestUUIDFallback(t *testing.T) {
	orig := randomBytes
	randomBytes = func([]byte) (int, error) { return 0, errors.New("no entropy") }
	defer func() { randomBytes = orig }()
	if id := UUID(); len(id) != 32 {
		t.Fatalf("unexpected fallback uuid: %s", id)
	}
}
