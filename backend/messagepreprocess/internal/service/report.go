package service

import (
	"backend/common"
	"context"
	"log/slog"

	"github.com/IBM/sarama"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

// more reporters than this and the member is banned
const reportLimit = 5

// reportUser records the reportUser, and asks auth to ban a member reported too many times. safe to do again, as the retry of
// a failed message does: a reporter counts once, and auth finds the account of one already banned gone
func (s *service) reportUser(ctx context.Context, reporterId, reportedId uuid.UUID) error {
	err := s.repository.AddReporterIdByReportedId(ctx, gocql.UUID(reporterId), gocql.UUID(reportedId))
	if err != nil {
		return err
	}
	count, err := s.repository.FindReportCountById(ctx, gocql.UUID(reportedId))
	if err != nil {
		return err
	}
	slog.Info("report member", "reportedId", reportedId, "count", count)
	if count <= reportLimit {
		return nil
	}
	// the account and the banned emails and phone numbers are auth's, so auth bans them. the topic of the auth of this
	// app, its PROFILE, so an auth only ever gets its own members. keyed by the member so their events stay ordered.
	// a report past the limit asks again
	headers := []sarama.RecordHeader{{Key: []byte(common.TaskTypeHeader), Value: []byte(common.AuthTaskBan)}}
	return s.producer.Commit(common.AuthTopic(s.profile), reportedId[:], nil, headers)
}
