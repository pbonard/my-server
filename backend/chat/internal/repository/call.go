package repository

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
)

// SetCallServerIP keeps the ip of the chat server holding a member's call websocket, a member is in one call at a time
func (r *repository) SetCallServerIP(ctx context.Context, key, ip string) error {
	result := r.client.Do(ctx, r.client.B().Set().Key(key).Value(ip).Build())
	if result.Error() != nil {
		slog.Error("fail to save call server ip", "err", result.Error(), "key", key)
		return result.Error()
	}
	return nil
}

func (r *repository) RemoveCallServerIP(ctx context.Context, key string) error {
	result := r.client.Do(ctx, r.client.B().Del().Key(key).Build())
	if result.Error() != nil {
		slog.Error("fail to remove call server ip", "err", result.Error(), "key", key)
		return result.Error()
	}
	return nil
}

// FindCallParticipantIds are the members in the call, as the uuid strings AddCallParticipantId keeps
func (r *repository) FindCallParticipantIds(ctx context.Context, key string) ([]string, error) {
	result := r.client.Do(ctx, r.client.B().Smembers().Key(key).Build())
	if result.Error() != nil {
		slog.Error("fail to find call participants", "err", result.Error(), "key", key)
		return nil, result.Error()
	}
	ids, err := result.AsStrSlice()
	if err != nil {
		slog.Error("fail to read call participants", "err", err, "key", key)
		return nil, err
	}
	return ids, nil
}

func (r *repository) AddCallParticipantId(ctx context.Context, key string, memberId uuid.UUID) error {
	result := r.client.Do(ctx, r.client.B().Sadd().Key(key).Member(memberId.String()).Build())
	if result.Error() != nil {
		slog.Error("fail to add call participant", "err", result.Error(), "key", key, "memberId", memberId)
		return result.Error()
	}
	return nil
}

func (r *repository) RemoveCallParticipantId(ctx context.Context, key string, memberId uuid.UUID) error {
	result := r.client.Do(ctx, r.client.B().Srem().Key(key).Member(memberId.String()).Build())
	if result.Error() != nil {
		slog.Error("fail to remove call participant", "err", result.Error(), "key", key, "memberId", memberId)
		return result.Error()
	}
	return nil
}
