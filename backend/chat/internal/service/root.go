package service

import (
	"backend/chat/internal/client"
	"backend/chat/internal/dto"
	"backend/chat/internal/repository"
	"backend/common"
	"backend/common/producer"
	"context"
	"encoding/json"
	"os"

	"github.com/google/uuid"
	_ "github.com/joho/godotenv/autoload"
)

type Service interface {
	SetName(ctx context.Context, memberId uuid.UUID, name string) error
	GetProfile(ctx context.Context, id uuid.UUID) (*dto.GetProfileResponse, error)
	GetChatRoomInfo(ctx context.Context, id uuid.UUID) (*dto.GetChatRoomInfoResponse, error)
	SetServerIP(ctx context.Context, memberId uuid.UUID, ip string) error
	RemoveServerIP(ctx context.Context, memberId uuid.UUID, ip string) error
	CheckBlock(ctx context.Context, blockerId uuid.UUID, blockedId uuid.UUID) (map[string]bool, error)
	GetChatParticipants(ctx context.Context, roomId uuid.UUID) ([]dto.GetProfileResponse, error)
	ReportUser(ctx context.Context, reporterId, reportedId uuid.UUID) error
	BlockConversation(ctx context.Context, memberId, conversationId uuid.UUID) error
	GetBlockedConversations(ctx context.Context, memberId uuid.UUID) ([]dto.BlockReport, error)
	GetRecentMessages(ctx context.Context, id, cursor uuid.UUID) (res []dto.MessagingResponse, err error)
	PublishMessaging(ctx context.Context, fromId uuid.UUID, toIdType string, toId uuid.UUID, contentType string, contents []string, noPush bool) (map[string]uuid.UUID, error)
	GeneratePresignedURL(ctx context.Context, id uuid.UUID, contentType string, n int) (res []dto.GeneratePresignedURLResponse, err error)
	GenerateSignedURL(ctx context.Context, memberId uuid.UUID, contentType string, filename uuid.UUID) (map[string]string, error)
	GenerateTurn() *dto.GetTurnResponse
	SetCallServerIP(ctx context.Context, memberId uuid.UUID, ip string) error
	RemoveCallServerIP(ctx context.Context, memberId uuid.UUID) error
	GetCallParticipantsWithoutMe(ctx context.Context, callId string, memberId uuid.UUID) ([]uuid.UUID, error)
	AddCallParticipant(ctx context.Context, callId string, memberId uuid.UUID) error
	RemoveCallParticipant(ctx context.Context, callId string, memberId uuid.UUID) error
	PublishCallSignal(fromId uuid.UUID, toIds []uuid.UUID, signal json.RawMessage) error
}

type service struct {
	repository    repository.Repository
	producer      producer.SyncProducer
	storageClient client.StorageClient
	cdnClient     common.CDNClient
	// the app this chat runs for, read once: its topics are built from it
	profile string
	// the secret the turn app checks call credentials with, and its host
	turnSecret string
	turnRealm  string
}

func NewService(r repository.Repository, kp producer.SyncProducer, storageClient client.StorageClient, cdnClient common.CDNClient) Service {
	s := service{
		repository:    r,
		producer:      kp,
		storageClient: storageClient,
		cdnClient:     cdnClient,
		profile:       os.Getenv("PROFILE"),
		turnSecret:    os.Getenv("TURN_SECRET"),
		turnRealm:     os.Getenv("TURN_REALM"),
	}
	return &s
}
