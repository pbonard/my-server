package service

import (
	"backend/common"
	"context"
	"errors"
	"log/slog"
	"sync"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

func (s *service) ManageMessage(ctx context.Context, id, fromId uuid.UUID, toIdType string, toId uuid.UUID, contentType string, contents []string, noPush bool) error {
	var toIds [][]byte
	roomId := toId
	if toIdType == "personal" {
		toIds = append(toIds, fromId[:])
		if contentType != "block" && contentType != "unblock" && contentType != "quit" && contentType != "delete" {
			b, err := s.repository.IsBlocked(ctx, gocql.UUID(toId), gocql.UUID(fromId))
			if err != nil {
				return err
			}
			if !b {
				toIds = append(toIds, toId[:])
			}
		}
	}
	if contentType == "block" {
		err := s.repository.AddBlock(ctx, gocql.UUID(fromId), gocql.UUID(toId))
		if err != nil {
			return err
		}
	}
	if contentType == "unblock" {
		err := s.repository.RemoveBlock(ctx, gocql.UUID(fromId), gocql.UUID(toId))
		if err != nil {
			return err
		}
	}
	if toIdType == "group" && contentType != "create" && contentType != "quit" && contentType != "delete" {
		participantIds, err := s.repository.FindParticipantIds(ctx, gocql.UUID(toId))
		if errors.Is(err, gocql.ErrNotFound) {
			err = nil
		}
		if err != nil {
			return err
		}
		for _, pid := range participantIds {
			toIds = append(toIds, pid[:])
		}
	}
	if contentType == "create" {
		err := s.repository.CreateChatRoom(ctx, gocql.UUID(roomId), gocql.UUID(fromId), contents[0])
		contents = []string{}
		if err != nil {
			return err
		}
	}
	if contentType == "participate" {
		err := s.repository.AddParticipantId(ctx, gocql.UUID(roomId), gocql.UUID(fromId))
		if err != nil {
			return err
		}
	}
	if toIdType == "group" && contentType == "quit" {
		err := s.repository.RemoveParticipantId(ctx, gocql.UUID(roomId), gocql.UUID(fromId))
		if err != nil {
			return err
		}
	}
	if toIdType == "group" && (contentType == "create" || contentType == "participate" || contentType == "quit" || contentType == "delete") {
		toIds = append(toIds, fromId[:])
	}
	if contentType == "image" || contentType == "audio" || contentType == "video" {
		var wg sync.WaitGroup
		var em sync.Mutex
		var es []error
		for _, content := range contents {
			wg.Add(1)
			go func() {
				defer wg.Done()
				filename, err := uuid.Parse(content)
				if err != nil {
					slog.Error("fail to parse uuid from content", "err", err)
					em.Lock()
					es = append(es, err)
					em.Unlock()
					return
				}
				var castedIds []gocql.UUID
				for _, tid := range toIds {
					castedIds = append(castedIds, gocql.UUID(tid))
				}
				err = s.repository.SaveIdsByFileName(ctx, castedIds, gocql.UUID(filename))
				if err != nil {
					em.Lock()
					es = append(es, err)
					em.Unlock()
				}
			}()
		}
		wg.Wait()
		err := errors.Join(es...)
		if err != nil {
			return err
		}
	}

	p := common.Marshal(common.PreparedMessage{
		Id:          id,
		ToIds:       common.UUIDsOf(toIds),
		RoomId:      roomId,
		FromId:      fromId,
		ContentType: contentType,
		Contents:    contents,
		NoPush:      noPush,
	})

	return s.producer.Commit(common.AppScoped(s.profile, "prepared-message"), nil, p, nil)
}
