package repository

import (
	"context"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// how many inbox rows one batch deletes: one partition, kept well under cassandra's batch size limit
const deleteBatchSize = 100

// SaveMessage keeps the message in toId's inbox, and the message's id under its room so a quit can find the room's
// messages in the inbox. the index row goes first: every inbox row can then be found, a retry writes both again
func (r *repository) SaveMessage(ctx context.Context, id, toId, fromId, roomId gocql.UUID, contentType string, contents []string) error {
	err := r.session.Query(`INSERT INTO message_id_by_room (to_id, room_id, id) VALUES (?, ?, ?) USING TTL ?`,
		toId, roomId, id, r.messageTTL).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save message id by room", "err", err)
		return err
	}
	err = r.session.Query(`INSERT INTO message_by_to_id
    (id, to_id, from_id, room_id, content_type, contents) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?`,
		id, toId, fromId, roomId, contentType, contents, r.messageTTL).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save messaging by toId", "err", err)
		return err
	}
	return nil
}

// SaveEvent keeps a quit or delete in toId's inbox only, for the sender's devices to apply on their sync. it isn't
// a message of the room, so it's not in message_id_by_room: an inbox whose room has only these has left it.
// they expire on their own, a later quit can't find them to delete
func (r *repository) SaveEvent(ctx context.Context, id, toId, fromId, roomId gocql.UUID, contentType string, contents []string) error {
	err := r.session.Query(`INSERT INTO message_by_to_id
    (id, to_id, from_id, room_id, content_type, contents) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?`,
		id, toId, fromId, roomId, contentType, contents, r.eventTTL).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save event by toId", "err", err)
		return err
	}
	return nil
}

// DeleteRoomMessagesBefore deletes toId's messages of the room older than before, from the inbox and the index. the
// index goes last, so a retry after a failure finds the rest again
func (r *repository) DeleteRoomMessagesBefore(ctx context.Context, toId, roomId, before gocql.UUID) error {
	iter := r.session.Query(`SELECT id FROM message_id_by_room WHERE to_id = ? AND room_id = ? AND id < ?`,
		toId, roomId, before).IterContext(ctx)
	var ids []gocql.UUID
	var id gocql.UUID
	for iter.Scan(&id) {
		ids = append(ids, id)
	}
	err := iter.Close()
	if err != nil {
		slog.Error("fail to find message ids by room", "err", err, "toId", toId, "roomId", roomId)
		return err
	}
	for start := 0; start < len(ids); start += deleteBatchSize {
		// the rows of one inbox are one partition, so an unlogged batch of them is applied together
		batch := r.session.Batch(gocql.UnloggedBatch)
		for _, mid := range ids[start:min(start+deleteBatchSize, len(ids))] {
			batch.Query(`DELETE FROM message_by_to_id WHERE to_id = ? AND id = ?`, toId, mid)
		}
		err = batch.ExecContext(ctx)
		if err != nil {
			slog.Error("fail to delete room messages", "err", err, "toId", toId, "roomId", roomId)
			return err
		}
	}
	// one range tombstone instead of one per row
	err = r.session.Query(`DELETE FROM message_id_by_room WHERE to_id = ? AND room_id = ? AND id < ?`,
		toId, roomId, before).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to delete message ids by room", "err", err, "toId", toId, "roomId", roomId)
		return err
	}
	return nil
}

// DeleteMessage deletes one message of toId's inbox and its index row
func (r *repository) DeleteMessage(ctx context.Context, toId, roomId, id gocql.UUID) error {
	err := r.session.Query(`DELETE FROM message_by_to_id WHERE to_id = ? AND id = ?`, toId, id).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to delete message", "err", err, "toId", toId, "id", id)
		return err
	}
	err = r.session.Query(`DELETE FROM message_id_by_room WHERE to_id = ? AND room_id = ? AND id = ?`,
		toId, roomId, id).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to delete message id by room", "err", err, "toId", toId, "id", id)
		return err
	}
	return nil
}
