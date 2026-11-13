package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

// PersistMessage saves the message in the inbox of each of toIds. a quit or delete is the sender's own, to the
// sender's inbox alone: it first deletes what it removes there, the room's older messages or the one it names,
// and is saved after it as an event, so the sender's other devices still learn of it on their sync
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
	for _, tid := range toIds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// a personal message's room is the receiver, in the receiver's inbox the room is the sender
			room := roomId
			if tid == roomId {
				room = fromId
			}
			var err error
			// in the sender's own inbox, a quit deletes the room's older messages and a delete the one it names
			if tid == fromId {
				switch contentType {
				case "quit":
					err = s.repository.DeleteRoomMessagesBefore(ctx, gocql.UUID(tid), gocql.UUID(room), gocql.UUID(id))
				case "delete":
					// a retry can't fix a missing or bad id, the event is still saved for the devices
					if len(contents) == 0 {
						slog.Error("delete without the message id")
					} else if target, err1 := uuid.Parse(contents[0]); err1 != nil {
						slog.Error("fail to parse the deleted message id", "err", err1, "content", contents[0])
					} else {
						err = s.repository.DeleteMessage(ctx, gocql.UUID(tid), gocql.UUID(room), gocql.UUID(target))
					}
				}
			}
			// saved after what it deletes: when the delete fails, the retry deletes and saves again
			if err == nil {
				if isEvent(contentType) {
					err = s.repository.SaveEvent(ctx, gocql.UUID(id), gocql.UUID(tid), gocql.UUID(fromId), gocql.UUID(room), contentType, contents)
				} else {
					err = s.repository.SaveMessage(ctx, gocql.UUID(id), gocql.UUID(tid), gocql.UUID(fromId), gocql.UUID(room), contentType, contents)
				}
			}
			if err != nil {
				em.Lock()
				es = append(es, err)
				em.Unlock()
			}
		}()
	}
	wg.Wait()
	return errors.Join(es...)
}

// isEvent is whether the content changes the chat instead of being a message of it: a quit or delete removes
// messages, a block or unblock is kept by messagepreprocess. a room they alone are in isn't a chat its inbox has
func isEvent(contentType string) bool {
	switch contentType {
	case "quit", "delete", "block", "unblock":
		return true
	}
	return false
}
