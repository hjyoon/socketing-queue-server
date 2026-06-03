package queue

import "context"

func (r *Redis) IssuedTokens(ctx context.Context, room string) ([]string, error) {
	return r.c.SMembers(ctx, "issued_tokens:"+room).Result()
}

func (r *Redis) RemoveIssuedToken(ctx context.Context, room, token string) error {
	return r.c.SRem(ctx, "issued_tokens:"+room, token).Err()
}

func (r *Redis) IssuedCount(ctx context.Context, room string) (int, error) {
	count, err := r.c.SCard(ctx, "issued_tokens:"+room).Result()
	return int(count), err
}
