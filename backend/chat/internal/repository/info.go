package repository

import (
	"context"
	"fmt"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func (r *repository) FindChatRoomInfoById(ctx context.Context, app string, id gocql.UUID) (name string, roomType string, err error) {
	chatRoom, err := r.apps.Table(app, "chat_room_by_id")
	if err != nil {
		return "", "", err
	}
	err = r.session.Query(fmt.Sprintf("SELECT name, room_type FROM %s WHERE id = ?", chatRoom), id).ScanContext(ctx, &name, &roomType)
	if err != nil {
		slog.Error("fail to find chat room info by id",
			"err", err,
			"app", app,
			"id", id.String())
		return "", "", nil
	}
	return name, roomType, nil
}

func (r *repository) FindProfileById(ctx context.Context, app string, id gocql.UUID) (name string, err error) {
	profile, err := r.apps.Table(app, "profile_by_id")
	if err != nil {
		return "", err
	}
	err = r.session.Query(fmt.Sprintf(`SELECT name FROM %s WHERE id = ?`, profile),
		id).ScanContext(ctx, &name)
	if err != nil {
		slog.Info("fail to find profile by id",
			"err", err,
			"app", app,
			"id", id.String())
		return "", err
	}
	return name, nil
}

func (r *repository) SaveNameById(ctx context.Context, app string, id gocql.UUID, name string) error {
	profile, err := r.apps.Table(app, "profile_by_id")
	if err != nil {
		return err
	}
	chatRoom, err := r.apps.Table(app, "chat_room_by_id")
	if err != nil {
		return err
	}
	err = r.session.Batch(gocql.LoggedBatch).
		Query(fmt.Sprintf("UPDATE %s SET name = ? WHERE id = ?", profile), name, id).
		Query(fmt.Sprintf("INSERT INTO %s (id, name, room_type) VALUES (?, ?, ?)", chatRoom), id, name, "personal").
		ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save profile name by id",
			"err", err,
			"app", app,
			"id", id.String(),
			"name", name)
		return err
	}
	return nil
}
