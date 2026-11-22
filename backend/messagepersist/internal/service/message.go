package service

import (
	"backend/messagepersist/internal/repository"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

// PersistMessage saves the message in the room of each of toIds' inboxes, and marks the room as changed (updated_id),
// for catching up. a quit or delete first removes what it removes, the room's older rows or the one it names, and is
// saved after it, so the sender's other devices still learn of it when they catch up. messagepreprocess sends those
// to the sender's inbox alone: they're "for me". updated_id is written at the time in the message's id, so a retry
// writes the same again, and an older message written late doesn't replace a newer one
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
	at := WriteTime(id)
	for _, tid := range toIds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			to := gocql.UUID(tid)
			room := gocql.UUID(roomId)
			if tid == roomId {
				room = gocql.UUID(fromId)
			}
			m := repository.Message{Id: gocql.UUID(id), FromId: gocql.UUID(fromId), ContentType: contentType, Contents: contents}
			var err error
			switch contentType {
			case "quit":
				err = s.persistQuit(ctx, to, room, m, at)
			case "delete":
				err = s.persistDelete(ctx, to, room, m, at)
			default:
				err = s.persistShown(ctx, to, room, m, at)
			}
			if err != nil {
				fail(err)
			}
		}()
	}
	wg.Wait()
	return errors.Join(es...)
}

// a row of the chat: a message, a block or unblock
func (s *service) persistShown(ctx context.Context, to, room gocql.UUID, m repository.Message, at int64) error {
	if err := s.repository.SaveMessage(ctx, to, room, m, s.config.MessageTTL); err != nil {
		return err
	}
	return s.repository.TouchRoom(ctx, to, room, m.Id, s.updatedTTL, at)
}

// the room's older rows go with one range delete. the quit stays its oldest row: the chat list leaves the room out
// until a new start comes after it
func (s *service) persistQuit(ctx context.Context, to, room gocql.UUID, m repository.Message, at int64) error {
	if err := s.repository.DeleteMessagesBefore(ctx, to, room, m.Id); err != nil {
		return err
	}
	if err := s.repository.SaveMessage(ctx, to, room, m, s.config.DeleteTTL); err != nil {
		return err
	}
	return s.repository.TouchRoom(ctx, to, room, m.Id, s.updatedTTL, at)
}

// the message it names goes
func (s *service) persistDelete(ctx context.Context, to, room gocql.UUID, m repository.Message, at int64) error {
	if len(m.Contents) == 0 {
		slog.Error("delete without the message id")
		return nil
	}
	target, err := uuid.Parse(m.Contents[0])
	if err != nil {
		slog.Error("fail to parse the deleted message id", "err", err, "content", m.Contents[0])
		return nil
	}
	if err = s.repository.DeleteMessage(ctx, to, room, gocql.UUID(target)); err != nil {
		return err
	}
	if err = s.repository.SaveMessage(ctx, to, room, m, s.config.DeleteTTL); err != nil {
		return err
	}
	return s.repository.TouchRoom(ctx, to, room, m.Id, s.updatedTTL, at)
}

// WriteTime is the time in a v7 id, in microseconds as cassandra's write times are: its milliseconds, and the 12 bits
// after the version, where google/uuid keeps the time within the millisecond, spread over its microseconds
func WriteTime(id uuid.UUID) int64 {
	var ms [8]byte
	copy(ms[2:], id[:6])
	sub := int64(binary.BigEndian.Uint16(id[6:8]) & 0x0fff)
	return int64(binary.BigEndian.Uint64(ms[:]))*1000 + sub*1000>>12
}
