package service

import (
	"backend/messagepersist/internal/repository"
	"context"

	"github.com/google/uuid"
)

type Service interface {
	PersistMessage(ctx context.Context, id uuid.UUID, toIds []uuid.UUID, roomId, fromId uuid.UUID, contentType string, contents []string) error
}

// Config is how long rows are kept, in seconds. 0 keeps them for good
type Config struct {
	// a row of the chat: a message, a block or unblock
	MessageTTL int
	// a quit or delete, which a device applies on its sync. one that syncs after it expired keeps what it removed
	DeleteTTL int
}

type service struct {
	repository repository.Repository
	config     Config
}

func NewService(r repository.Repository, config Config) Service {
	return &service{
		repository: r,
		config:     config,
	}
}
