package repository

import (
	"context"
	"log/slog"
)

func (r *repository) GetServerIPs(ctx context.Context, key string) ([]string, error) {
	result := r.client.Do(ctx, r.client.B().Smembers().Key(key).Build())
	if result.Error() != nil {
		slog.Error("fail to get member ip", "err", result.Error())
		return nil, result.Error()
	}
	value, err := result.AsStrSlice()
	if err != nil {
		slog.Error("fail to get ips value string slice", "err", err)
	}
	return value, nil
}

func (r *repository) RemoveServerIP(ctx context.Context, key, ip string) error {
	result := r.client.Do(ctx, r.client.B().Srem().Key(key).Member(ip).Build())
	if result.Error() != nil {
		slog.Error("fail to remove member ip", "err", result.Error())
		return result.Error()
	}
	return nil
}
