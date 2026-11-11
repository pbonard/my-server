package service

import (
	"backend/common/producer"
	"backend/messagerelay/internal/client"
	"backend/messagerelay/internal/repository"
	"context"

	"github.com/google/uuid"
)

type Service interface {
	RelayMessage(ctx context.Context, id uuid.UUID, toIds [][]byte, roomId, fromId uuid.UUID, contentType string, contents []string, noPush bool) error
}

type service struct {
	repository  repository.Repository
	producer    producer.SyncProducer
	relayClient client.RelayClient
}

func NewService(r repository.Repository, p producer.SyncProducer, relayClient client.RelayClient) Service {
	return &service{
		repository:  r,
		producer:    p,
		relayClient: relayClient,
	}
}
