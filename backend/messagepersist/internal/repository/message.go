package repository

import (
	"backend/messagepersist/internal/constant"
	"context"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func (r *repository) SaveMessage(ctx context.Context, id, toId, fromId, roomId gocql.UUID, contentType string, contents []string) error {
	err := r.session.Query(`INSERT INTO message_by_to_id 
    (id, to_id, from_id, room_id, content_type, contents) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?`,
		id, toId, fromId, roomId, contentType, contents, constant.Message_TTL).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save messaging by toId", "err", err)
		return err
	}
	return nil
}
