package service

import (
	"backend/common/producer"
	"backend/messagepreprocess/internal/repository"
	"context"

	"github.com/google/uuid"
)

type Service interface {
	ManageMessage(ctx context.Context, app string, id, fromId uuid.UUID, toIdType string, toId uuid.UUID, contentType string, contents []string, noPush bool) error
}

type service struct {
	repository repository.Repository
	producer   producer.SyncProducer
}

func NewService(r repository.Repository, kp producer.SyncProducer) Service {
	s := &service{
		repository: r,
		producer:   kp,
	}
	return s
}
