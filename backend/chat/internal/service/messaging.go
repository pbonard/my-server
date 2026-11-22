package service

import (
	"backend/chat/internal/dto"
	"backend/chat/internal/projection"
	"backend/common"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"slices"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

const catchUpSkew = 10 * time.Second

const roomReads = 8

func (s *service) GetRecentMessages(ctx context.Context, id, cursor uuid.UUID) ([]dto.MessagingResponse, error) {
	after := lowerBound(cursor, catchUpSkew)
	rooms, err := s.repository.FindRooms(ctx, gocql.UUID(id))
	if err != nil {
		return nil, err
	}
	changed := slices.DeleteFunc(rooms, func(room projection.Room) bool {
		return bytes.Compare(room.UpdatedId[:], after[:]) <= 0
	})
	found := make([][]dto.MessagingResponse, len(changed))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(roomReads)
	for i, room := range changed {
		g.Go(func() error {
			messages, reset, err1 := s.repository.FindRoomRecent(gctx, gocql.UUID(id), room.RoomId, gocql.UUID(after), s.recentLimit)
			if err1 != nil {
				return err1
			}
			if reset {
				found[i] = append(found[i], dto.MessagingResponse{RoomId: uuid.UUID(room.RoomId), ContentType: "reset", Contents: []string{}})
			}
			found[i] = append(found[i], toMessagingResponses(uuid.UUID(room.RoomId), messages)...)
			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return nil, err
	}
	res := make([]dto.MessagingResponse, 0)
	for _, messages := range found {
		res = append(res, messages...)
	}
	return res, nil
}

const historyLimit = 30

func (s *service) GetRoomHistory(ctx context.Context, id, roomId, cursor uuid.UUID) ([]dto.MessagingResponse, error) {
	messages, err := s.repository.FindRoomHistory(ctx, gocql.UUID(id), gocql.UUID(roomId), gocql.UUID(cursor), historyLimit)
	if err != nil {
		return nil, err
	}
	return toMessagingResponses(roomId, messages), nil
}

func toMessagingResponses(roomId uuid.UUID, messages []projection.Message) []dto.MessagingResponse {
	res := make([]dto.MessagingResponse, 0, len(messages))
	for _, m := range messages {
		res = append(res, dto.MessagingResponse{
			Id:          uuid.UUID(m.Id),
			RoomId:      roomId,
			FromId:      uuid.UUID(m.FromId),
			ContentType: m.ContentType,
			Contents:    m.Contents,
		})
	}
	return res
}

func lowerBound(id uuid.UUID, d time.Duration) uuid.UUID {
	var b [8]byte
	copy(b[2:], id[:6])
	ms := max(int64(binary.BigEndian.Uint64(b[:]))-d.Milliseconds(), 0)
	binary.BigEndian.PutUint64(b[:], uint64(ms))
	var bound uuid.UUID
	copy(bound[:6], b[2:])
	bound[6] = 0x70
	bound[8] = 0x80
	return bound
}

func (s *service) PublishMessaging(ctx context.Context, fromId uuid.UUID, toIdType string, toId uuid.UUID, contentType string, contents []string, noPush bool) (map[string]uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	if contentType == "image" || contentType == "audio" || contentType == "video" {
		exists, err1 := s.repository.HasFilepath(ctx, common.UploadsKey(contentType, fromId), contents)
		if err1 != nil {
			return nil, err1
		}
		if !exists {
			return nil, errors.New("bad request")
		}
		err = s.repository.RemoveFilepath(ctx, common.UploadsKey(contentType, fromId), contents)
		if err != nil {
			return nil, err
		}
	}
	p := common.Marshal(common.ChatMessage{
		Id:          id,
		FromId:      fromId,
		ToIdType:    toIdType,
		ToId:        toId,
		ContentType: contentType,
		Contents:    contents,
		NoPush:      noPush,
	})
	err = s.producer.Commit(common.AppScoped(s.profile, "chat-message"), nil, p, nil)
	if err != nil {
		return nil, err
	}
	return map[string]uuid.UUID{"id": id}, nil
}

func (s *service) GeneratePresignedURL(ctx context.Context, id uuid.UUID, contentType string, n int) (res []dto.GeneratePresignedURLResponse, err error) {
	var filenames []string
	for range n {
		filename, err1 := uuid.NewV7()
		if err1 != nil {
			slog.Error("fail to generate uuid v7", "err", err1)
			return nil, err1
		}
		filenames = append(filenames, filename.String())
		res = append(res, dto.GeneratePresignedURLResponse{Filename: filename})
	}
	err = s.repository.SetFilepath(ctx, common.UploadsKey(contentType, id), filenames)
	if err != nil {
		slog.Error("fail to save filename", "err", err)
		return nil, err
	}
	for i := range n {
		url, fields, err1 := s.storageClient.PresignUpload(ctx, contentType, filenames[i])
		if err1 != nil {
			slog.Error("fail to generate presigned URL", "err", err1)
			return nil, err1
		}
		res[i].URL = url
		res[i].Fields = fields
	}
	return res, nil
}

func (s *service) GenerateSignedURL(ctx context.Context, memberId uuid.UUID, contentType string, filename uuid.UUID) (map[string]string, error) {
	ids, err := s.repository.FindIdsByFilename(ctx, gocql.UUID(filename))
	if err != nil {
		return nil, err
	}
	if !slices.Contains(ids, gocql.UUID(memberId)) {
		slog.Warn("this member id don't have authority to see file")
		return nil, errors.New("unauthorized request")
	}
	signedURL, err := s.cdnClient.SignedURL(contentType, filename.String())
	if err != nil {
		return nil, err
	}
	slog.Info("success to sign url", "signedURL", signedURL)
	return map[string]string{"url": signedURL}, nil
}
