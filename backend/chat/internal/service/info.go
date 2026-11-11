package service

import (
	"backend/chat/internal/dto"
	"context"
	"errors"
	"log/slog"
	"strings"
	"unicode"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

func (s *service) SetName(ctx context.Context, memberId uuid.UUID, name string) error {
	sanitizedName := strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, name)
	if len(sanitizedName) == 0 {
		slog.Info("incorrect name")
		return errors.New("incorrect name")
	}
	err := s.repository.SaveNameById(ctx, gocql.UUID(memberId), sanitizedName)
	if err != nil {
		return err
	}
	return nil
}

func (s *service) GetProfile(ctx context.Context, id uuid.UUID) (*dto.GetProfileResponse, error) {
	name, err := s.repository.FindProfileById(ctx, gocql.UUID(id))
	if errors.Is(err, gocql.ErrNotFound) {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	res := dto.GetProfileResponse{
		Name: name,
	}
	return &res, nil
}

func (s *service) GetChatRoomInfo(ctx context.Context, id uuid.UUID) (*dto.GetChatRoomInfoResponse, error) {
	name, roomType, err := s.repository.FindChatRoomInfoById(ctx, gocql.UUID(id))
	if err != nil {
		return nil, err
	}
	res := dto.GetChatRoomInfoResponse{
		Name: name,
		Type: roomType,
	}
	return &res, nil
}
