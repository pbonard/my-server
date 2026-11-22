package repository

import (
	"backend/chat/internal/projection"
	"context"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// FindRooms is every room of toId's inbox: a quit's room too, and one whose messages expired
func (r *repository) FindRooms(ctx context.Context, toId gocql.UUID) ([]projection.Room, error) {
	iter := r.session.Query(`SELECT room_id, updated_id FROM chat_room_by_to_id WHERE to_id = ?`, toId).IterContext(ctx)
	var rooms []projection.Room
	var room projection.Room
	for iter.Scan(&room.RoomId, &room.UpdatedId) {
		rooms = append(rooms, room)
		room = projection.Room{}
	}
	if err := iter.Close(); err != nil {
		slog.Error("fail to find rooms", "err", err, "toId", toId)
		return nil, err
	}
	return rooms, nil
}

// FindRoomRecent is what came to the room in toId's inbox after after, newest first: up to limit of its messages, and
// every quit and delete among them and past them, which may name messages a device has from before. none past a quit,
// it removed everything older
func (r *repository) FindRoomRecent(ctx context.Context, toId, roomId, after gocql.UUID, limit int) ([]projection.Message, error) {
	iter := r.session.Query(`SELECT id, from_id, content_type, contents FROM message_by_chat_room
    WHERE to_id = ? AND room_id = ? AND id > ?`, toId, roomId, after).PageSize(100).IterContext(ctx)
	messages := make([]projection.Message, 0)
	shown := 0
	var m projection.Message
	for iter.Scan(&m.Id, &m.FromId, &m.ContentType, &m.Contents) {
		switch {
		case m.ContentType == "quit" || m.ContentType == "delete":
			messages = append(messages, m)
		case shown < limit:
			messages = append(messages, m)
			shown++
		}
		if m.ContentType == "quit" {
			break
		}
		m = projection.Message{}
	}
	if err := iter.Close(); err != nil {
		slog.Error("fail to find room recent", "err", err, "toId", toId, "roomId", roomId)
		return nil, err
	}
	return messages, nil
}

// FindRoomHistory is up to limit of the room's messages in toId's inbox older than ahead (the newest, when it's zero)
// and newer than cursor (any, when it's zero), newest first. only what the chat shows: quits and deletes are for
// catching up, what they removed is gone already
func (r *repository) FindRoomHistory(ctx context.Context, toId, roomId, ahead, cursor gocql.UUID, limit int) ([]projection.Message, error) {
	query := `SELECT id, from_id, content_type, contents FROM message_by_chat_room WHERE to_id = ? AND room_id = ?`
	args := []any{toId, roomId}
	if ahead != (gocql.UUID{}) {
		query += ` AND id < ?`
		args = append(args, ahead)
	}
	if cursor != (gocql.UUID{}) {
		query += ` AND id > ?`
		args = append(args, cursor)
	}
	// a few more than limit a page, for the deletes skipped on the way
	iter := r.session.Query(query, args...).PageSize(limit + 10).IterContext(ctx)
	messages := make([]projection.Message, 0)
	var m projection.Message
	for len(messages) < limit && iter.Scan(&m.Id, &m.FromId, &m.ContentType, &m.Contents) {
		// the oldest row there is, nothing before it is left
		if m.ContentType == "quit" {
			break
		}
		if m.ContentType != "delete" {
			messages = append(messages, m)
		}
		m = projection.Message{}
	}
	if err := iter.Close(); err != nil {
		slog.Error("fail to find room history", "err", err, "toId", toId, "roomId", roomId)
		return nil, err
	}
	return messages, nil
}

func (r *repository) SetFilepath(ctx context.Context, key string, filenames []string) error {
	result := r.client.Do(ctx, r.client.B().Sadd().Key(key).Member(filenames...).Build())
	if result.Error() != nil {
		slog.Error("fail to save member ip", "err", result.Error())
		return result.Error()
	}
	return nil
}

func (r *repository) HasFilepath(ctx context.Context, key string, filenames []string) (bool, error) {
	for _, f := range filenames {
		result := r.client.Do(ctx, r.client.B().Sismember().Key(key).Member(f).Build())
		if result.Error() != nil {
			slog.Error("fail to check file path", "err", result.Error())
			return false, result.Error()
		}
		exists, err := result.AsBool()
		if err != nil {
			slog.Error("fail to get result of checking file path", "err", err)
			return false, err
		}
		if !exists {
			slog.Info("this filename not exist")
			return false, nil
		}
	}
	return true, nil
}

func (r *repository) RemoveFilepath(ctx context.Context, key string, filenames []string) error {
	result := r.client.Do(ctx, r.client.B().Srem().Key(key).Member(filenames...).Build())
	if result.Error() != nil {
		slog.Error("fail to check file path", "err", result.Error())
		return result.Error()
	}
	return nil
}

func (r *repository) FindIdsByFilename(ctx context.Context, filename gocql.UUID) (ids []gocql.UUID, err error) {
	err = r.session.Query("SELECT ids FROM ids_by_filename WHERE filename = ?", filename).ScanContext(ctx, &ids)
	if err != nil {
		slog.Error("fail to find ids by filename",
			"err", err)
		return nil, err
	}
	return ids, nil
}
