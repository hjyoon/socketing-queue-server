package auth

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var randomBytes = rand.Read

func Sign(claims map[string]any, secret string, ttl time.Duration) (string, error) {
	now := time.Now()
	c := jwt.MapClaims{"iat": now.Unix(), "exp": now.Add(ttl).Unix(), "jti": UUID()}
	for k, v := range claims {
		c[k] = v
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString([]byte(secret))
}

func Verify(token, secret string) (jwt.MapClaims, error) {
	parsed, err := jwt.Parse(token, func(t *jwt.Token) (any, error) {
		return []byte(secret), nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil {
		return nil, err
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok || !parsed.Valid {
		return nil, jwt.ErrTokenInvalidClaims
	}
	return claims, nil
}

func UUID() string {
	var b [16]byte
	if _, err := randomBytes(b[:]); err != nil {
		raw := hex.EncodeToString([]byte(time.Now().String()))
		return raw[:32]
	}
	b[6], b[8] = (b[6]&0x0f)|0x40, (b[8]&0x3f)|0x80
	return hex.EncodeToString(b[:4]) + "-" + hex.EncodeToString(b[4:6]) +
		"-" + hex.EncodeToString(b[6:8]) + "-" + hex.EncodeToString(b[8:10]) +
		"-" + hex.EncodeToString(b[10:])
}
