package repository

import (
	"context"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// SaveMessage keeps a row in toId's inbox, what the sync reads: a message, or a quit or delete for the devices.
// roomId is the room as toId sees it
func (r *repository) SaveMessage(ctx context.Context, id, toId, fromId, roomId gocql.UUID, contentType string, contents []string, ttl int) error {
	err := r.session.Query(`INSERT INTO message_by_to_id
    (id, to_id, from_id, room_id, content_type, contents) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?`,
		id, toId, fromId, roomId, contentType, contents, ttl).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save message", "err", err, "toId", toId, "id", id)
		return err
	}
	return nil
}

// SaveRoomMessageId keeps the id of a row of the room's chat in toId's inbox, so a quit can find the room's rows
func (r *repository) SaveRoomMessageId(ctx context.Context, toId, roomId, id gocql.UUID, ttl int) error {
	err := r.session.Query(`INSERT INTO message_id_by_room (to_id, room_id, id) VALUES (?, ?, ?) USING TTL ?`,
		toId, roomId, id, ttl).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save room message id", "err", err, "toId", toId, "roomId", roomId, "id", id)
		return err
	}
	return nil
}

// FindRoomMessageIdsBefore are the ids of the room's rows in toId's inbox older than before
func (r *repository) FindRoomMessageIdsBefore(ctx context.Context, toId, roomId, before gocql.UUID) ([]gocql.UUID, error) {
	iter := r.session.Query(`SELECT id FROM message_id_by_room WHERE to_id = ? AND room_id = ? AND id < ?`,
		toId, roomId, before).IterContext(ctx)
	var ids []gocql.UUID
	var id gocql.UUID
	for iter.Scan(&id) {
		ids = append(ids, id)
	}
	err := iter.Close()
	if err != nil {
		slog.Error("fail to find room message ids", "err", err, "toId", toId, "roomId", roomId)
		return nil, err
	}
	return ids, nil
}

func (r *repository) DeleteMessage(ctx context.Context, toId, id gocql.UUID) error {
	err := r.session.Query(`DELETE FROM message_by_to_id WHERE to_id = ? AND id = ?`, toId, id).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to delete message", "err", err, "toId", toId, "id", id)
		return err
	}
	return nil
}

// DeleteMessages deletes the rows of ids from toId's inbox in one batch: they're one partition, so an unlogged batch
// of them is applied together. keep ids to a few hundred, cassandra warns about and then refuses large batches
func (r *repository) DeleteMessages(ctx context.Context, toId gocql.UUID, ids []gocql.UUID) error {
	batch := r.session.Batch(gocql.UnloggedBatch)
	for _, id := range ids {
		batch.Query(`DELETE FROM message_by_to_id WHERE to_id = ? AND id = ?`, toId, id)
	}
	err := batch.ExecContext(ctx)
	if err != nil {
		slog.Error("fail to delete messages", "err", err, "toId", toId, "count", len(ids))
		return err
	}
	return nil
}

func (r *repository) DeleteRoomMessageId(ctx context.Context, toId, roomId, id gocql.UUID) error {
	err := r.session.Query(`DELETE FROM message_id_by_room WHERE to_id = ? AND room_id = ? AND id = ?`,
		toId, roomId, id).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to delete room message id", "err", err, "toId", toId, "roomId", roomId, "id", id)
		return err
	}
	return nil
}

// DeleteRoomMessageIdsBefore deletes the room's ids older than before with one range tombstone, not one per row
func (r *repository) DeleteRoomMessageIdsBefore(ctx context.Context, toId, roomId, before gocql.UUID) error {
	err := r.session.Query(`DELETE FROM message_id_by_room WHERE to_id = ? AND room_id = ? AND id < ?`,
		toId, roomId, before).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to delete room message ids", "err", err, "toId", toId, "roomId", roomId)
		return err
	}
	return nil
}
