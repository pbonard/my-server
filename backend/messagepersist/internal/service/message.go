package service

import (
	"bytes"
	"context"
	"errors"
	"sync"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

func (s *service) PersistMessage(
	ctx context.Context,
	id uuid.UUID,
	toIds [][]byte,
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
			if bytes.Equal(tid, roomId[:]) {
				err := s.repository.SaveMessage(ctx, gocql.UUID(id), gocql.UUID(tid), gocql.UUID(fromId), gocql.UUID(fromId), contentType, contents)
				if err != nil {
					em.Lock()
					es = append(es, err)
					em.Unlock()
				}
				return
			}
			err := s.repository.SaveMessage(ctx, gocql.UUID(id), gocql.UUID(tid), gocql.UUID(fromId), gocql.UUID(roomId), contentType, contents)
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
