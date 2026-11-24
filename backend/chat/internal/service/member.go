package service

import (
	"backend/chat/internal/dto"
	"backend/common"
	"context"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

func (s *service) SetServerIP(ctx context.Context, memberId uuid.UUID, ip string) error {
	err := s.repository.SetServerIP(ctx, common.ChatServersKey(memberId), ip)
	if err != nil {
		return err
	}
	return nil
}

func (s *service) RemoveServerIP(ctx context.Context, memberId uuid.UUID, ip string) error {
	err := s.repository.RemoveServerIP(ctx, common.ChatServersKey(memberId), ip)
	if err != nil {
		return err
	}
	return nil
}

func (s *service) CheckBlock(ctx context.Context, blockerId uuid.UUID, blockedId uuid.UUID) (map[string]bool, error) {
	w, err := s.repository.DidBlock(ctx, gocql.UUID(blockerId), gocql.UUID(blockedId))
	if err != nil {
		return nil, err
	}
	return map[string]bool{"didBlock": w}, nil
}

// CheckInvite is whether the invitee blocked the sender, and whether the sender still has their chat: a personal
// room is the other member, so the sender's inbox keeps it under the invitee's id. not whether the invitee has it,
// the sender isn't to learn whether the invitee left
func (s *service) CheckInvite(ctx context.Context, fromId, toId uuid.UUID) (*dto.CheckInviteResponse, error) {
	blocked, err := s.repository.DidBlock(ctx, gocql.UUID(toId), gocql.UUID(fromId))
	if err != nil {
		return nil, err
	}
	newest, err := s.repository.FindRoomHistory(ctx, gocql.UUID(fromId), gocql.UUID(toId), gocql.UUID{}, 1)
	if err != nil {
		return nil, err
	}
	return &dto.CheckInviteResponse{
		Blocked:       blocked,
		SenderHasChat: len(newest) > 0,
	}, nil
}

func (s *service) GetChatParticipants(ctx context.Context, roomId uuid.UUID) ([]dto.GetProfileResponse, error) {
	ps, err := s.repository.FindChatParticipantIds(ctx, gocql.UUID(roomId))
	if err != nil {
		return nil, err
	}
	res := make([]dto.GetProfileResponse, 0)
	for _, p := range ps {
		if p == (gocql.UUID{}) {
			continue
		}
		res = append(res, dto.GetProfileResponse{Id: uuid.UUID(p)})
	}
	return res, nil
}

// ReportUser publishes the report, once and nothing else, so it's never half done: messagepreprocess blocks the
// reported for the reporter, records the report and has auth ban a member reported too many times, and retries it
func (s *service) ReportUser(ctx context.Context, reporterId, reportedId uuid.UUID) error {
	_, err := s.PublishMessaging(ctx, reporterId, "personal", reportedId, "report", []string{}, true)
	return err
}

func (s *service) BlockConversation(ctx context.Context, memberId, conversationId uuid.UUID) error {
	err := s.repository.AddBlockedConversation(ctx, gocql.UUID(memberId), gocql.UUID(conversationId))
	if err != nil {
		return err
	}
	return nil
}

func (s *service) GetBlockedConversations(ctx context.Context, memberId uuid.UUID) ([]dto.BlockReport, error) {
	ids, err := s.repository.FindBlockedConversations(ctx, gocql.UUID(memberId))
	if err != nil {
		return nil, err
	}
	res := make([]dto.BlockReport, 0, len(ids))
	for _, id := range ids {
		res = append(res, dto.BlockReport{Id: uuid.UUID(id)})
	}
	return res, nil
}
