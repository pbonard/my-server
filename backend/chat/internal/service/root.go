package service

import (
	"backend/chat/internal/client"
	"backend/chat/internal/dto"
	"backend/chat/internal/repository"
	"backend/common"
	"backend/common/producer"
	"context"

	"github.com/google/uuid"
	_ "github.com/joho/godotenv/autoload"
)

type Service interface {
	SetName(ctx context.Context, app string, memberId uuid.UUID, name string) error
	GetProfile(ctx context.Context, app string, id uuid.UUID) (*dto.GetProfileResponse, error)
	GetChatRoomInfo(ctx context.Context, app string, id uuid.UUID) (*dto.GetChatRoomInfoResponse, error)
	SetServerIP(ctx context.Context, memberId uuid.UUID, ip string) error
	RemoveServerIP(ctx context.Context, memberId []byte, ip string) error
	CheckBlock(ctx context.Context, app string, blockerId uuid.UUID, blockedId uuid.UUID) (map[string]bool, error)
	GetChatParticipants(ctx context.Context, app string, roomId uuid.UUID) ([]dto.GetProfileResponse, error)
	ReportUser(ctx context.Context, app string, reporterId, reportedId uuid.UUID) error
	BlockConversation(ctx context.Context, app string, memberId, conversationId uuid.UUID) error
	GetBlockedConversations(ctx context.Context, app string, memberId uuid.UUID) ([]dto.BlockReport, error)
	GetRecentMessages(ctx context.Context, app string, id, cursor uuid.UUID) (res []dto.MessagingResponse, err error)
	PublishMessaging(ctx context.Context, app string, fromId uuid.UUID, toIdType string, toId uuid.UUID, contentType string, contents []string, noPush bool) (map[string]uuid.UUID, error)
	GeneratePresignedURL(ctx context.Context, id uuid.UUID, contentType string, n int) (res []dto.GeneratePresignedURLResponse, err error)
	GenerateSignedURL(ctx context.Context, app string, memberId uuid.UUID, contentType string, filename uuid.UUID) (map[string]string, error)
}

type service struct {
	repository    repository.Repository
	producer      producer.SyncProducer
	storageClient client.StorageClient
	cdnClient     common.CDNClient
}

func NewService(r repository.Repository, kp producer.SyncProducer, storageClient client.StorageClient, cdnClient common.CDNClient) Service {
	s := service{
		repository:    r,
		producer:      kp,
		storageClient: storageClient,
		cdnClient:     cdnClient,
	}
	return &s
}
