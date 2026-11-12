package service

import (
	"backend/chat/internal/dto"
	"backend/common"
	"context"
	"log"

	"github.com/IBM/sarama"
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

func (s *service) ReportUser(ctx context.Context, reporterId, reportedId uuid.UUID) error {
	err := s.repository.AddReporterIdByReportedId(ctx, gocql.UUID(reporterId), gocql.UUID(reportedId))
	if err != nil {
		return err
	}
	rc, err := s.repository.FindReportCountById(ctx, gocql.UUID(reportedId))
	if err != nil {
		return err
	}
	log.Printf("report count of %v : %v", reportedId, rc)
	if rc > 5 {
		// the account and the banned phone numbers are auth's, so auth bans them. the topic of the auth of this chat's
		// app, its PROFILE, so an auth only ever gets its own members. keyed by the member so their events stay ordered.
		// a report past the limit asks again, auth finds the account already gone
		p := common.Marshal(common.AuthEvent{Id: reportedId})
		headers := []sarama.RecordHeader{{Key: []byte(common.TaskTypeHeader), Value: []byte(common.AuthTaskBan)}}
		err = s.producer.Commit(common.AuthTopic(s.profile), reportedId[:], p, headers)
		if err != nil {
			return err
		}
	}
	return nil
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
