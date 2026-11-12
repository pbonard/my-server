package service

import (
	"backend/chat/internal/dto"
	"backend/common"
	"context"
	"errors"
	"log/slog"
	"slices"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

func (s *service) GetRecentMessages(ctx context.Context, id, cursor uuid.UUID) (res []dto.MessagingResponse, err error) {
	result, err := s.repository.FindRecentMessagesByToId(ctx, gocql.UUID(id), gocql.UUID(cursor))
	if err != nil {
		return nil, err
	}
	for _, m := range result {
		r := dto.MessagingResponse{
			Id:          uuid.UUID(m.Id),
			FromId:      uuid.UUID(m.FromId),
			RoomId:      uuid.UUID(m.RoomId),
			ContentType: m.ContentType,
			Contents:    m.Contents,
		}
		if m.RoomId != nil {
			r.RoomId = uuid.UUID(m.RoomId)
		}
		res = append(res, r)
	}
	return res, nil
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
