package service

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

// how many inbox rows a quit deletes in one batch: one partition, kept well under cassandra's batch size limit
const deleteBatchSize = 100

// PersistMessage saves the message in the inbox of each of toIds. a quit or delete first deletes what it removes in
// the inbox, the room's older rows or the one it names, and is saved after it, so the sender's other devices still
// learn of it on their sync. messagepreprocess sends those to the sender's inbox alone: they're "for me"
func (s *service) PersistMessage(
	ctx context.Context,
	id uuid.UUID,
	toIds []uuid.UUID,
	roomId, fromId uuid.UUID,
	contentType string,
	contents []string,
) error {
	var wg sync.WaitGroup
	var es []error
	var em sync.Mutex
	fail := func(err error) {
		em.Lock()
		es = append(es, err)
		em.Unlock()
	}
	for _, tid := range toIds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg, to, from := gocql.UUID(id), gocql.UUID(tid), gocql.UUID(fromId)
			room := gocql.UUID(roomId)
			if tid == roomId {
				room = from
			}
			switch contentType {
			case "quit":
				ids, err := s.repository.FindRoomMessageIdsBefore(ctx, to, room, msg)
				if err != nil {
					fail(err)
					return
				}
				for chunk := range slices.Chunk(ids, deleteBatchSize) {
					if err = s.repository.DeleteMessages(ctx, to, chunk); err != nil {
						fail(err)
						return
					}
				}
				if err = s.repository.DeleteRoomMessageIdsBefore(ctx, to, room, msg); err != nil {
					fail(err)
					return
				}
				if err = s.repository.SaveMessage(ctx, msg, to, from, room, contentType, contents, s.config.DeleteTTL); err != nil {
					fail(err)
				}
			case "delete":
				if len(contents) == 0 {
					slog.Error("delete without the message id")
					return
				}
				target, err := uuid.Parse(contents[0])
				if err != nil {
					slog.Error("fail to parse the deleted message id", "err", err, "content", contents[0])
					return
				}
				if err = s.repository.DeleteMessage(ctx, to, gocql.UUID(target)); err != nil {
					fail(err)
					return
				}
				if err = s.repository.DeleteRoomMessageId(ctx, to, room, gocql.UUID(target)); err != nil {
					fail(err)
					return
				}
				if err = s.repository.SaveMessage(ctx, msg, to, from, room, contentType, contents, s.config.DeleteTTL); err != nil {
					fail(err)
				}
			default:
				if err := s.repository.SaveRoomMessageId(ctx, to, room, msg, s.config.MessageTTL); err != nil {
					fail(err)
					return
				}
				if err := s.repository.SaveMessage(ctx, msg, to, from, room, contentType, contents, s.config.MessageTTL); err != nil {
					fail(err)
				}
			}
		}()
	}
	wg.Wait()
	return errors.Join(es...)
}
