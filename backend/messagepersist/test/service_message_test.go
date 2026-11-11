package test

import (
	"backend/messagepersist/internal/service"
	"context"
	"errors"
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	msgId   = uuid.New()
	fromId  = uuid.New()
	roomId  = uuid.New()
	member1 = uuid.New()
	member2 = uuid.New()
)

func TestPersistMessage_groupSavesOneRowPerReceiverWithRoom(t *testing.T) {
	repo := NewMockRepository(t)
	for _, member := range []uuid.UUID{member1, member2} {
		repo.EXPECT().SaveMessage(mock.Anything, gocql.UUID(msgId), gocql.UUID(member), gocql.UUID(fromId),
			gocql.UUID(roomId), "text", []string{"hi"}).Return(nil).Once()
	}

	assert.NoError(t, service.NewService(repo).PersistMessage(context.Background(), msgId,
		[][]byte{member1[:], member2[:]}, roomId, fromId, "text", []string{"hi"}))
}

func TestPersistMessage_receiverThatIsTheRoomUsesSenderAsRoom(t *testing.T) {
	// personal message: the room id is the receiver itself, its row is stored under the sender's room
	repo := NewMockRepository(t)
	repo.EXPECT().SaveMessage(mock.Anything, gocql.UUID(msgId), gocql.UUID(roomId), gocql.UUID(fromId),
		gocql.UUID(fromId), "text", []string{"hi"}).Return(nil).Once()
	repo.EXPECT().SaveMessage(mock.Anything, gocql.UUID(msgId), gocql.UUID(fromId), gocql.UUID(fromId),
		gocql.UUID(roomId), "text", []string{"hi"}).Return(nil).Once()

	assert.NoError(t, service.NewService(repo).PersistMessage(context.Background(), msgId,
		[][]byte{fromId[:], roomId[:]}, roomId, fromId, "text", []string{"hi"}))
}

// the failure is returned so the consumer publishes a retry, the saved rows are upserted again on retry
func TestPersistMessage_oneFailedSaveDoesNotStopTheOthers(t *testing.T) {
	repo := NewMockRepository(t)
	errDB := errors.New("cassandra down")
	repo.EXPECT().SaveMessage(mock.Anything, gocql.UUID(msgId), gocql.UUID(member1), gocql.UUID(fromId),
		gocql.UUID(roomId), "text", []string{"hi"}).Return(errDB).Once()
	repo.EXPECT().SaveMessage(mock.Anything, gocql.UUID(msgId), gocql.UUID(member2), gocql.UUID(fromId),
		gocql.UUID(roomId), "text", []string{"hi"}).Return(nil).Once()

	err := service.NewService(repo).PersistMessage(context.Background(), msgId,
		[][]byte{member1[:], member2[:]}, roomId, fromId, "text", []string{"hi"})

	assert.ErrorIs(t, err, errDB)
}
