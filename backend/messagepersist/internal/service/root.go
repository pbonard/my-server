package service

import (
	"backend/messagepersist/internal/repository"
	"context"

	"github.com/google/uuid"
)

type Service interface {
	PersistMessage(ctx context.Context, id uuid.UUID, toIds [][]byte, roomId, fromId uuid.UUID, contentType string, contents []string) error
}

type service struct {
	repository repository.Repository
}

func NewService(r repository.Repository) Service {
	s := &service{
		repository: r,
	}
	return s
}
