package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func (r *repository) FindParticipantIds(ctx context.Context, app string, roomId gocql.UUID) (participantIds []gocql.UUID, err error) {
	chatRoom, err := r.apps.Table(app, "chat_room_by_id")
	if err != nil {
		return nil, err
	}
	err = r.session.Query(fmt.Sprintf(`SELECT participant_ids FROM %s WHERE id = ?`, chatRoom),
		roomId).ScanContext(ctx, &participantIds)
	if err != nil {
		slog.Error("fail to find participant ids by room id", "err", err, "app", app)
		return nil, err
	}
	return participantIds, nil
}

func (r *repository) SaveIdsByFileName(ctx context.Context, app string, ids []gocql.UUID, filename gocql.UUID) error {
	idsByFilename, err := r.apps.Table(app, "ids_by_filename")
	if err != nil {
		return err
	}
	err = r.session.Query(fmt.Sprintf(`INSERT INTO %s (ids, filename) VALUES (?, ?)`, idsByFilename),
		ids, filename).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to ids by filename", "err", err, "app", app)
		return err
	}
	return nil
}

func (r *repository) CreateChatRoom(ctx context.Context, app string, roomId gocql.UUID, memberId gocql.UUID, roomName string) error {
	chatRoom, err := r.apps.Table(app, "chat_room_by_id")
	if err != nil {
		return err
	}
	err = r.session.Query(fmt.Sprintf(`INSERT INTO %s (id, name, room_type, participant_ids) VALUES (?, ?, ?, ?)`, chatRoom),
		roomId, roomName, "group", []gocql.UUID{memberId}).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to create chat room", "err", err,
			"app", app,
			"roomId", roomId,
			"memberId", memberId)
		return err
	}
	return nil
}

func (r *repository) AddParticipantId(ctx context.Context, app string, roomId gocql.UUID, participantId gocql.UUID) error {
	chatRoom, err := r.apps.Table(app, "chat_room_by_id")
	if err != nil {
		return err
	}
	err = r.session.Query(fmt.Sprintf(`UPDATE %s SET participant_ids = participant_ids + ? WHERE id = ?`, chatRoom),
		[]gocql.UUID{participantId}, roomId).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save participant id", "err", err,
			"app", app,
			"roomId", roomId,
			"participantId", participantId)
		return err
	}
	return nil
}

func (r *repository) RemoveParticipantId(ctx context.Context, app string, roomId gocql.UUID, participantId gocql.UUID) error {
	chatRoom, err := r.apps.Table(app, "chat_room_by_id")
	if err != nil {
		return err
	}
	err = r.session.Query(
		fmt.Sprintf(`UPDATE %s SET participant_ids = participant_ids - ? WHERE id = ?`, chatRoom),
		[]gocql.UUID{participantId}, roomId).
		ExecContext(ctx)
	if err != nil {
		slog.Error("fail to delete participant id", "err", err,
			"app", app,
			"roomId", roomId,
			"participantId", participantId)
		return err
	}
	return nil
}

func (r *repository) IsBlocked(ctx context.Context, app string, blockerId gocql.UUID, blockedId gocql.UUID) (bool, error) {
	block, err := r.apps.Table(app, "block")
	if err != nil {
		return false, err
	}
	var a gocql.UUID
	err = r.session.Query(
		fmt.Sprintf(`SELECT blocked_id FROM %s WHERE blocker_id = ? AND blocked_id = ?`, block), blockerId, blockedId,
	).ScanContext(ctx, &a)
	if errors.Is(gocql.ErrNotFound, err) {
		return false, nil
	}
	if err != nil {
		slog.Error("fail to check block", "err", err,
			"app", app,
			"toId", blockerId,
			"fromId", blockedId)
		return false, err
	}
	return true, nil
}

func (r *repository) AddBlock(ctx context.Context, app string, blockerId gocql.UUID, blockedId gocql.UUID) error {
	block, err := r.apps.Table(app, "block")
	if err != nil {
		return err
	}
	err = r.session.Query(
		fmt.Sprintf(`INSERT INTO %s (blocker_id, blocked_id) VALUES (?, ?)`, block),
		blockerId, blockedId).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to add block", "err", err,
			"app", app,
			"fromId", blockerId,
			"toId", blockedId)
		return err
	}
	return nil
}

func (r *repository) RemoveBlock(ctx context.Context, app string, blockerId gocql.UUID, blockedId gocql.UUID) error {
	block, err := r.apps.Table(app, "block")
	if err != nil {
		return err
	}
	err = r.session.Query(
		fmt.Sprintf(`DELETE FROM %s WHERE blocker_id = ? AND blocked_id = ?`, block),
		blockerId, blockedId).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to remove block", "err", err,
			"app", app,
			"fromId", blockerId,
			"toId", blockedId)
		return err
	}
	return nil
}
