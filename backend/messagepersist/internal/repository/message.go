package repository

import (
	"context"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// SaveMessage keeps a row of the room in toId's inbox. roomId is the room as toId sees it
func (r *repository) SaveMessage(ctx context.Context, toId, roomId gocql.UUID, m Message, ttl int) error {
	err := r.session.Query(`INSERT INTO message_by_chat_room (to_id, room_id, id, from_id, content_type, contents)
    VALUES (?, ?, ?, ?, ?, ?) USING TTL ?`,
		toId, roomId, m.Id, m.FromId, m.ContentType, m.Contents, ttl).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save message", "err", err, "toId", toId, "roomId", roomId, "id", m.Id)
		return err
	}
	return nil
}

func (r *repository) DeleteMessage(ctx context.Context, toId, roomId, id gocql.UUID) error {
	err := r.session.Query(`DELETE FROM message_by_chat_room WHERE to_id = ? AND room_id = ? AND id = ?`,
		toId, roomId, id).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to delete message", "err", err, "toId", toId, "roomId", roomId, "id", id)
		return err
	}
	return nil
}

// DeleteMessagesBefore deletes the room's rows older than before with one range tombstone, not one per row
func (r *repository) DeleteMessagesBefore(ctx context.Context, toId, roomId, before gocql.UUID) error {
	err := r.session.Query(`DELETE FROM message_by_chat_room WHERE to_id = ? AND room_id = ? AND id < ?`,
		toId, roomId, before).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to delete messages", "err", err, "toId", toId, "roomId", roomId)
		return err
	}
	return nil
}

// TouchRoom marks the room as having a row as new as id. at is the write time, microseconds
func (r *repository) TouchRoom(ctx context.Context, toId, roomId, id gocql.UUID, ttl int, at int64) error {
	err := r.session.Query(`UPDATE chat_room_by_to_id USING TTL ? AND TIMESTAMP ? SET updated_id = ?
    WHERE to_id = ? AND room_id = ?`, ttl, at, id, toId, roomId).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to touch room", "err", err, "toId", toId, "roomId", roomId, "id", id)
		return err
	}
	return nil
}
