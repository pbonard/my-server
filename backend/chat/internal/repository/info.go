package repository

import (
	"context"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func (r *repository) FindChatRoomInfoById(ctx context.Context, id gocql.UUID) (name string, roomType string, err error) {
	err = r.session.Query("SELECT name, room_type FROM chat_room_by_id WHERE id = ?", id).ScanContext(ctx, &name, &roomType)
	if err != nil {
		slog.Error("fail to find chat room info by id",
			"err", err,
			"id", id.String())
		return "", "", nil
	}
	return name, roomType, nil
}

func (r *repository) FindProfileById(ctx context.Context, id gocql.UUID) (name string, err error) {
	err = r.session.Query(`SELECT name FROM profile_by_id WHERE id = ?`,
		id).ScanContext(ctx, &name)
	if err != nil {
		slog.Info("fail to find profile by id",
			"err", err,
			"id", id.String())
		return "", err
	}
	return name, nil
}

func (r *repository) SaveNameById(ctx context.Context, id gocql.UUID, name string) error {
	err := r.session.Batch(gocql.LoggedBatch).
		Query("UPDATE profile_by_id SET name = ? WHERE id = ?", name, id).
		Query("INSERT INTO chat_room_by_id (id, name, room_type) VALUES (?, ?, ?)", id, name, "personal").
		ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save profile name by id",
			"err", err,
			"id", id.String(),
			"name", name)
		return err
	}
	return nil
}
