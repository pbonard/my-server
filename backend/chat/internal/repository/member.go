package repository

import (
	"context"
	"errors"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func (r *repository) SetServerIP(ctx context.Context, memberId, ip string) error {
	result := r.client.Do(ctx, r.client.B().Sadd().Key(memberId).Member(ip).Build())
	if result.Error() != nil {
		slog.Error("fail to save member ip", "err", result.Error())
		return result.Error()
	}
	return nil
}

func (r *repository) RemoveServerIP(ctx context.Context, memberId, ip string) error {
	result := r.client.Do(ctx, r.client.B().Srem().Key(memberId).Member(ip).Build())
	if result.Error() != nil {
		slog.Error("fail to remove member ip", "err", result.Error())
		return result.Error()
	}
	return nil
}

func (r *repository) DidBlock(ctx context.Context, blockerId gocql.UUID, blockedId gocql.UUID) (bool, error) {
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

func (r *repository) FindChatParticipantIds(ctx context.Context, roomId gocql.UUID) (ids []gocql.UUID, err error) {
	err = r.session.Query(
		`SELECT participant_ids FROM chat_room_by_id WHERE id = ?`, roomId).
		ScanContext(ctx, &ids)
	if err != nil {
		slog.Error("fail to find chat participant ids", "err", err,
			"roomId", roomId)
		return nil, err
	}
	return ids, nil
}

func (r *repository) AddReporterIdByReportedId(ctx context.Context, reporterId gocql.UUID, reportedId gocql.UUID) error {
	err := r.session.Query(`UPDATE profile_by_id USING TTL ? SET reporter_ids = reporter_ids + ? WHERE id = ?`,
		30*24*60*60, []gocql.UUID{reporterId}, reportedId).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to add reporter id by reported id",
			"err", err,
			"reporterId", reporterId,
			"reportedId", reportedId)
		return err
	}
	return nil
}

func (r *repository) FindReportCountById(ctx context.Context, reportedId gocql.UUID) (count int, err error) {
	var reporterIds []gocql.UUID
	err = r.session.Query(
		`SELECT reporter_ids FROM profile_by_id WHERE id = ?`,
		reportedId,
	).ScanContext(ctx, &reporterIds)
	if err != nil {
		slog.Error("fail to find report count by id",
			"err", err,
			"reportedId", reportedId)
		return 0, err
	}
	return len(reporterIds), nil
}

func (r *repository) AddBlockedConversation(ctx context.Context, memberId gocql.UUID, conversationId gocql.UUID) error {
	err := r.session.Query(`UPDATE profile_by_id USING TTL ? SET blocked_conversations = blocked_conversations + ? WHERE id = ?`,
		360*24*60*60, []gocql.UUID{conversationId}, memberId).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to add blocked conversation", "err", err,
			"memberId", memberId,
			"conversationId", conversationId)
		return err
	}
	return nil
}

func (r *repository) FindBlockedConversations(ctx context.Context, id gocql.UUID) (ids []gocql.UUID, err error) {
	err = r.session.Query(`SELECT blocked_conversations FROM profile_by_id WHERE id = ?`, id).ScanContext(ctx, &ids)
	if err != nil {
		slog.Error("fail to find blocked conversations by id", "err", err,
			"id", id)
		return nil, err
	}
	return ids, nil
}
