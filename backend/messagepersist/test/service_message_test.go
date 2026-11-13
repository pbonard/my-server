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
		[]uuid.UUID{member1, member2}, roomId, fromId, "text", []string{"hi"}))
}

func TestPersistMessage_receiverThatIsTheRoomUsesSenderAsRoom(t *testing.T) {
	// personal message: the room id is the receiver itself, its row is stored under the sender's room
	repo := NewMockRepository(t)
	repo.EXPECT().SaveMessage(mock.Anything, gocql.UUID(msgId), gocql.UUID(roomId), gocql.UUID(fromId),
		gocql.UUID(fromId), "text", []string{"hi"}).Return(nil).Once()
	repo.EXPECT().SaveMessage(mock.Anything, gocql.UUID(msgId), gocql.UUID(fromId), gocql.UUID(fromId),
		gocql.UUID(roomId), "text", []string{"hi"}).Return(nil).Once()

	assert.NoError(t, service.NewService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId, roomId}, roomId, fromId, "text", []string{"hi"}))
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
		[]uuid.UUID{member1, member2}, roomId, fromId, "text", []string{"hi"})

	assert.ErrorIs(t, err, errDB)
}

// a personal quit to the room of another member: the sender's older messages of that room go before it's saved
func TestPersistMessage_quitDeletesTheRoomsOlderMessagesFirst(t *testing.T) {
	repo := NewMockRepository(t)
	deleted := repo.EXPECT().DeleteRoomMessagesBefore(mock.Anything, gocql.UUID(fromId), gocql.UUID(roomId), gocql.UUID(msgId)).
		Return(nil).Once()
	repo.EXPECT().SaveEvent(mock.Anything, gocql.UUID(msgId), gocql.UUID(fromId), gocql.UUID(fromId),
		gocql.UUID(roomId), "quit", []string{}).Return(nil).Once().NotBefore(deleted)

	assert.NoError(t, service.NewService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "quit", []string{}))
}

// the quit isn't saved when the delete fails, the retry deletes and saves again
func TestPersistMessage_quitThatFailsToDeleteIsNotSaved(t *testing.T) {
	repo := NewMockRepository(t)
	errDB := errors.New("cassandra down")
	repo.EXPECT().DeleteRoomMessagesBefore(mock.Anything, gocql.UUID(fromId), gocql.UUID(roomId), gocql.UUID(msgId)).
		Return(errDB).Once()

	err := service.NewService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "quit", []string{})

	assert.ErrorIs(t, err, errDB)
}

func TestPersistMessage_deleteDeletesTheMessageItNames(t *testing.T) {
	repo := NewMockRepository(t)
	target := uuid.New()
	deleted := repo.EXPECT().DeleteMessage(mock.Anything, gocql.UUID(fromId), gocql.UUID(roomId), gocql.UUID(target)).
		Return(nil).Once()
	repo.EXPECT().SaveEvent(mock.Anything, gocql.UUID(msgId), gocql.UUID(fromId), gocql.UUID(fromId),
		gocql.UUID(roomId), "delete", []string{target.String()}).Return(nil).Once().NotBefore(deleted)

	assert.NoError(t, service.NewService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "delete", []string{target.String()}))
}

// a retry can't fix a bad id, the event is still saved for the sender's devices
func TestPersistMessage_deleteOfABadIdIsOnlySaved(t *testing.T) {
	repo := NewMockRepository(t)
	repo.EXPECT().SaveEvent(mock.Anything, gocql.UUID(msgId), gocql.UUID(fromId), gocql.UUID(fromId),
		gocql.UUID(roomId), "delete", []string{"nope"}).Return(nil).Once()

	assert.NoError(t, service.NewService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "delete", []string{"nope"}))
}

// only the sender's own inbox is ever deleted from
func TestPersistMessage_quitInAnotherInboxDeletesNothing(t *testing.T) {
	repo := NewMockRepository(t)
	repo.EXPECT().SaveEvent(mock.Anything, gocql.UUID(msgId), gocql.UUID(member1), gocql.UUID(fromId),
		gocql.UUID(roomId), "quit", []string{}).Return(nil).Once()

	assert.NoError(t, service.NewService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{member1}, roomId, fromId, "quit", []string{}))
}

// a block alone isn't a chat: saved as an event, so the room has no message the invite check would find
func TestPersistMessage_blockIsSavedAsAnEvent(t *testing.T) {
	repo := NewMockRepository(t)
	repo.EXPECT().SaveEvent(mock.Anything, gocql.UUID(msgId), gocql.UUID(fromId), gocql.UUID(fromId),
		gocql.UUID(roomId), "block", []string{}).Return(nil).Once()

	assert.NoError(t, service.NewService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "block", []string{}))
}
