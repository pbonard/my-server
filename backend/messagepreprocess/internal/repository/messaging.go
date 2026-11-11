package repository

import (
	"context"
	"errors"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func (r *repository) FindParticipantIds(ctx context.Context, roomId gocql.UUID) (participantIds []gocql.UUID, err error) {
	err = r.session.Query(`SELECT participant_ids FROM chat_room_by_id WHERE id = ?`,
		roomId).ScanContext(ctx, &participantIds)
	if err != nil {
		slog.Error("fail to find participant ids by room id", "err", err)
		return nil, err
	}
	return participantIds, nil
}

func (r *repository) SaveIdsByFileName(ctx context.Context, ids []gocql.UUID, filename gocql.UUID) error {
	err := r.session.Query(`INSERT INTO ids_by_filename (ids, filename) VALUES (?, ?)`,
		ids, filename).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to ids by filename", "err", err)
		return err
	}
	return nil
}

func (r *repository) CreateChatRoom(ctx context.Context, roomId gocql.UUID, memberId gocql.UUID, roomName string) error {
	err := r.session.Query(`INSERT INTO chat_room_by_id (id, name, room_type, participant_ids) VALUES (?, ?, ?, ?)`,
		roomId, roomName, "group", []gocql.UUID{memberId}).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to create chat room", "err", err,
			"roomId", roomId,
			"memberId", memberId)
		return err
	}
	return nil
}

func (r *repository) AddParticipantId(ctx context.Context, roomId gocql.UUID, participantId gocql.UUID) error {
	err := r.session.Query(`UPDATE chat_room_by_id SET participant_ids = participant_ids + ? WHERE id = ?`,
		[]gocql.UUID{participantId}, roomId).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to save participant id", "err", err,
			"roomId", roomId,
			"participantId", participantId)
		return err
	}
	return nil
}

func (r *repository) RemoveParticipantId(ctx context.Context, roomId gocql.UUID, participantId gocql.UUID) error {
	err := r.session.Query(
		`UPDATE chat_room_by_id SET participant_ids = participant_ids - ? WHERE id = ?`,
		[]gocql.UUID{participantId}, roomId).
		ExecContext(ctx)
	if err != nil {
		slog.Error("fail to delete participant id", "err", err,
			"roomId", roomId,
			"participantId", participantId)
		return err
	}
	return nil
}

func (r *repository) IsBlocked(ctx context.Context, blockerId gocql.UUID, blockedId gocql.UUID) (bool, error) {
	var a gocql.UUID
	err := r.session.Query(
		`SELECT blocked_id FROM block WHERE blocker_id = ? AND blocked_id = ?`, blockerId, blockedId,
	).ScanContext(ctx, &a)
	if errors.Is(gocql.ErrNotFound, err) {
		return false, nil
	}
	if err != nil {
		slog.Error("fail to check block", "err", err,
			"toId", blockerId,
			"fromId", blockedId)
		return false, err
	}
	return true, nil
}

func (r *repository) AddBlock(ctx context.Context, blockerId gocql.UUID, blockedId gocql.UUID) error {
	err := r.session.Query(
		`INSERT INTO block (blocker_id, blocked_id) VALUES (?, ?)`,
		blockerId, blockedId).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to add block", "err", err,
			"fromId", blockerId,
			"toId", blockedId)
		return err
	}
	return nil
}

func (r *repository) RemoveBlock(ctx context.Context, blockerId gocql.UUID, blockedId gocql.UUID) error {
	err := r.session.Query(
		`DELETE FROM block WHERE blocker_id = ? AND blocked_id = ?`,
		blockerId, blockedId).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to remove block", "err", err,
			"fromId", blockerId,
			"toId", blockedId)
		return err
	}
	return nil
}
