package repository

import (
	"backend/chat/internal/projection"
	"context"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

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

func (r *repository) FindRoomRecent(ctx context.Context, toId, roomId, after gocql.UUID, limit int) (
	messages []projection.Message, reset bool, err error,
) {
	pageSize := 100
	if limit > 0 {
		pageSize = limit + 1
	}
	iter := r.session.Query(`SELECT id, from_id, content_type, contents FROM message_by_chat_room
    WHERE to_id = ? AND room_id = ? AND id > ?`, toId, roomId, after).PageSize(pageSize).IterContext(ctx)
	messages = make([]projection.Message, 0)
	var m projection.Message
	for iter.Scan(&m.Id, &m.FromId, &m.ContentType, &m.Contents) {
		if limit > 0 && len(messages) == limit {
			reset = true
			break
		}
		messages = append(messages, m)
		if m.ContentType == "quit" {
			break
		}
		m = projection.Message{}
	}
	if err = iter.Close(); err != nil {
		slog.Error("fail to find room recent", "err", err, "toId", toId, "roomId", roomId)
		return nil, false, err
	}
	return messages, reset, nil
}

func (r *repository) FindRoomHistory(ctx context.Context, toId, roomId, cursor gocql.UUID, limit int) ([]projection.Message, error) {
	query := `SELECT id, from_id, content_type, contents FROM message_by_chat_room WHERE to_id = ? AND room_id = ?`
	args := []any{toId, roomId}
	if cursor != (gocql.UUID{}) {
		query += ` AND id < ?`
		args = append(args, cursor)
	}
	iter := r.session.Query(query, args...).PageSize(limit + 10).IterContext(ctx)
	messages := make([]projection.Message, 0)
	var m projection.Message
	for len(messages) < limit && iter.Scan(&m.Id, &m.FromId, &m.ContentType, &m.Contents) {
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
