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

// how far before a device's cursor catching up starts: messagepersist saves from a queue, so a message can be saved a
// moment after a newer one moved a device's cursor past it. the devices keep a message they already have once
const catchUpSkew = 10 * time.Second

// how many of each room's newest messages GetRooms hands out, and how many rooms it reads at once
const (
	roomMessageLimit = 20
	roomReads        = 8
)

// GetRooms is the newest messages (roomMessageLimit) of every room of the member's chat list, room by room, each
// room's newest first, for a device that has none of their chats yet, and the cursor it catches up from afterwards.
// the device puts them in order. a room's earlier messages are
// GetRoomHistory's, a page at a time. none of a room they left, or whose messages are all gone
func (s *service) GetRooms(ctx context.Context, id uuid.UUID) (*dto.GetRoomsResponse, error) {
	// before reading: a message saved while they're read is after the cursor, and comes when catching up
	cursor, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	rooms, err := s.repository.FindRooms(ctx, gocql.UUID(id))
	if err != nil {
		return nil, err
	}
	found := make([][]projection.Message, len(rooms))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(roomReads)
	for i, room := range rooms {
		g.Go(func() error {
			messages, err1 := s.repository.FindRoomHistory(gctx, gocql.UUID(id), room.RoomId, gocql.UUID{}, gocql.UUID{}, roomMessageLimit)
			found[i] = messages
			return err1
		})
	}
	if err = g.Wait(); err != nil {
		return nil, err
	}
	res := &dto.GetRoomsResponse{Cursor: cursor, Messages: make([]dto.MessagingResponse, 0)}
	for i, room := range rooms {
		res.Messages = append(res.Messages, toMessagingResponses(uuid.UUID(room.RoomId), found[i])...)
	}
	return res, nil
}

// GetRecentMessages is what came to the member after their cursor, from a little before it (catchUpSkew), room by
// room, each room's newest first: each room that changed since, its newest messages (roomMessageLimit, as GetRooms),
// and every quit and delete, which may name messages the device has from before. the device puts them in order. the messages it skips in between, a device gets with
// GetRoomHistory as it's scrolled: it asks for what's between the oldest it shows and the next it has
func (s *service) GetRecentMessages(ctx context.Context, id, cursor uuid.UUID) ([]dto.MessagingResponse, error) {
	after := lowerBound(cursor, catchUpSkew)
	rooms, err := s.repository.FindRooms(ctx, gocql.UUID(id))
	if err != nil {
		return nil, err
	}
	res := make([]dto.MessagingResponse, 0)
	for _, room := range rooms {
		if bytes.Compare(room.UpdatedId[:], after[:]) <= 0 {
			continue
		}
		messages, err1 := s.repository.FindRoomRecent(ctx, gocql.UUID(id), room.RoomId, gocql.UUID(after), roomMessageLimit)
		if err1 != nil {
			return nil, err1
		}
		res = append(res, toMessagingResponses(uuid.UUID(room.RoomId), messages)...)
	}
	return res, nil
}

// how many messages a page of GetRoomHistory has
const historyLimit = 30

// GetRoomHistory is a page (historyLimit) of the room's messages older than ahead (the newest, when it's uuid.Nil)
// and newer than cursor (any, when it's uuid.Nil), newest first. ahead is the oldest a device shows, cursor the next
// message it has below it, to find a gap: none between them, it reads on from what it has. an empty page: none more
func (s *service) GetRoomHistory(ctx context.Context, id, roomId, ahead, cursor uuid.UUID) ([]dto.MessagingResponse, error) {
	messages, err := s.repository.FindRoomHistory(ctx, gocql.UUID(id), gocql.UUID(roomId), gocql.UUID(ahead), gocql.UUID(cursor), historyLimit)
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

// lowerBound is the smallest v7 id of d before the time in id, a v7 id: a bound that sorts before every id made since
func lowerBound(id uuid.UUID, d time.Duration) uuid.UUID {
	// the time in a v7 id is its first 48 bits, milliseconds
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
