package repository

import (
	"context"
	"log/slog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

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
