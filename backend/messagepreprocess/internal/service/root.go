package service

import (
	"backend/common/producer"
	"backend/messagepreprocess/internal/repository"
	"context"
	"os"

	"github.com/google/uuid"
)

type Service interface {
	ManageMessage(ctx context.Context, id, fromId uuid.UUID, toIdType string, toId uuid.UUID, contentType string, contents []string, noPush bool) error
}

type service struct {
	repository repository.Repository
	producer   producer.SyncProducer
	// the app this service runs for, read once: its topics are built from it
	profile string
}

func NewService(r repository.Repository, kp producer.SyncProducer) Service {
	s := &service{
		repository: r,
		producer:   kp,
		profile:    os.Getenv("PROFILE"),
	}
	return s
}
