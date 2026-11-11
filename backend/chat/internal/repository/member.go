package repository

import (
	"context"
	"errors"
	"fmt"
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

func (r *repository) DidBlock(ctx context.Context, app string, blockerId gocql.UUID, blockedId gocql.UUID) (bool, error) {
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
			"toId", blockerId,
			"fromId", blockedId)
		return false, err
	}
	return true, nil
}

func (r *repository) FindChatParticipantIds(ctx context.Context, app string, roomId gocql.UUID) (ids []gocql.UUID, err error) {
	chatRoom, err := r.apps.Table(app, "chat_room_by_id")
	if err != nil {
		return nil, err
	}
	err = r.session.Query(
		fmt.Sprintf(`SELECT participant_ids FROM %s WHERE id = ?`, chatRoom), roomId).
		ScanContext(ctx, &ids)
	if err != nil {
		slog.Error("fail to find chat participant ids", "err", err,
			"roomId", roomId)
		return nil, err
	}
	return ids, nil
}

func (r *repository) AddReporterIdByReportedId(ctx context.Context, app string, reporterId gocql.UUID, reportedId gocql.UUID) error {
	profile, err := r.apps.Table(app, "profile_by_id")
	if err != nil {
		return err
	}
	err = r.session.Query(fmt.Sprintf(`UPDATE %s USING TTL ? SET reporter_ids = reporter_ids + ? WHERE id = ?`, profile),
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

func (r *repository) FindReportCountById(ctx context.Context, app string, reportedId gocql.UUID) (count int, err error) {
	profile, err := r.apps.Table(app, "profile_by_id")
	if err != nil {
		return 0, err
	}
	var reporterIds []gocql.UUID
	err = r.session.Query(
		fmt.Sprintf(`SELECT reporter_ids FROM %s WHERE id = ?`, profile),
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

func (r *repository) AddBlockedConversation(ctx context.Context, app string, memberId gocql.UUID, conversationId gocql.UUID) error {
	profile, err := r.apps.Table(app, "profile_by_id")
	if err != nil {
		return err
	}
	err = r.session.Query(fmt.Sprintf(`UPDATE %s USING TTL ? SET blocked_conversations = blocked_conversations + ? WHERE id = ?`, profile),
		360*24*60*60, []gocql.UUID{conversationId}, memberId).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to add blocked conversation", "err", err,
			"memberId", memberId,
			"conversationId", conversationId)
		return err
	}
	return nil
}

func (r *repository) FindBlockedConversations(ctx context.Context, app string, id gocql.UUID) (ids []gocql.UUID, err error) {
	profile, err := r.apps.Table(app, "profile_by_id")
	if err != nil {
		return nil, err
	}
	err = r.session.Query(fmt.Sprintf(`SELECT blocked_conversations FROM %s WHERE id = ?`, profile), id).ScanContext(ctx, &ids)
	if err != nil {
		slog.Error("fail to find blocked conversations by id", "err", err,
			"id", id)
		return nil, err
	}
	return ids, nil
}
