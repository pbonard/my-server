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
	err := s.repository.SetServerIP(ctx, string(memberId[:]), ip)
	if err != nil {
		return err
	}
	return nil
}

func (s *service) RemoveServerIP(ctx context.Context, memberId []byte, ip string) error {
	err := s.repository.RemoveServerIP(ctx, string(memberId), ip)
	if err != nil {
		return err
	}
	return nil
}

func (s *service) CheckBlock(ctx context.Context, app string, blockerId uuid.UUID, blockedId uuid.UUID) (map[string]bool, error) {
	w, err := s.repository.DidBlock(ctx, app, gocql.UUID(blockerId), gocql.UUID(blockedId))
	if err != nil {
		return nil, err
	}
	return map[string]bool{"didBlock": w}, nil
}

func (s *service) GetChatParticipants(ctx context.Context, app string, roomId uuid.UUID) ([]dto.GetProfileResponse, error) {
	ps, err := s.repository.FindChatParticipantIds(ctx, app, gocql.UUID(roomId))
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

// ReportUser records a report, app is the app of the reporter: the reported one is of the same app, the members of
// one never meet those of another
func (s *service) ReportUser(ctx context.Context, app string, reporterId, reportedId uuid.UUID) error {
	err := s.repository.AddReporterIdByReportedId(ctx, app, gocql.UUID(reporterId), gocql.UUID(reportedId))
	if err != nil {
		return err
	}
	rc, err := s.repository.FindReportCountById(ctx, app, gocql.UUID(reportedId))
	if err != nil {
		return err
	}
	log.Printf("report count of %v : %v", reportedId, rc)
	if rc > 5 {
		// the account and the banned phone numbers are auth's, so auth bans them. the topic of the auth of their app,
		// so an auth only ever gets its own members. keyed by the member so their events stay ordered.
		// a report past the limit asks again, auth finds the account already gone
		p := common.Marshal(common.AuthEvent{Id: reportedId[:]})
		headers := []sarama.RecordHeader{{Key: []byte(common.TaskTypeHeader), Value: []byte(common.AuthTaskBan)}}
		err = s.producer.Commit(common.AuthTopic(app), reportedId[:], p, headers)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *service) BlockConversation(ctx context.Context, app string, memberId, conversationId uuid.UUID) error {
	err := s.repository.AddBlockedConversation(ctx, app, gocql.UUID(memberId), gocql.UUID(conversationId))
	if err != nil {
		return err
	}
	return nil
}

func (s *service) GetBlockedConversations(ctx context.Context, app string, memberId uuid.UUID) ([]dto.BlockReport, error) {
	ids, err := s.repository.FindBlockedConversations(ctx, app, gocql.UUID(memberId))
	if err != nil {
		return nil, err
	}
	res := make([]dto.BlockReport, 0, len(ids))
	for _, id := range ids {
		res = append(res, dto.BlockReport{Id: uuid.UUID(id)})
	}
	return res, nil
}
