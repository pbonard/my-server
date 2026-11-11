package repository

import (
	"backend/messagepersist/internal/constant"
	"context"
	"fmt"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func (r *repository) SaveMessage(ctx context.Context, app string, id, toId, fromId, roomId gocql.UUID, contentType string, contents []string) error {
	messages, err := r.apps.Table(app, "message_by_to_id")
	if err != nil {
		return err
	}
	err = r.session.Query(fmt.Sprintf(`INSERT INTO %s
    (id, to_id, from_id, room_id, content_type, contents) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?`, messages),
		id, toId, fromId, roomId, contentType, contents, constant.Message_TTL).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save messaging by toId", "err", err, "app", app)
		return err
	}
	return nil
}
