package service

import (
	"backend/common"
	"backend/common/producer"
	"backend/notification/internal/repository"
	"context"

	"github.com/google/uuid"
	_ "github.com/joho/godotenv/autoload"
)

type Service interface {
	PreprocessMessageNotification(ctx context.Context, notificationId uint8, messageId uuid.UUID, toIds []uuid.UUID, roomId, fromId uuid.UUID, contentType string, content []string) error
	PreprocessScheduledNotification(ctx context.Context, partitionId uuid.UUID, notifications map[uuid.UUID]map[int]string, contents map[int]string) error
	RegisterNotification(ctx context.Context, id uuid.UUID, os, token string) error
}

type service struct {
	repository repository.Repository
	producer   producer.SyncProducer
	cdnClient  common.CDNClient
}

func NewService(r repository.Repository, p producer.SyncProducer, cdnClient common.CDNClient) Service {
	s := service{
		repository: r,
		producer:   p,
		cdnClient:  cdnClient,
	}

	return &s
}
