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
	errDB   = errors.New("cassandra down")
)

// distinct ttls, so a test shows which one a row was saved with
var config = service.Config{MessageTTL: 259200, DeleteTTL: 2592000}

func newService(repo *MockRepository) service.Service {
	return service.NewService(repo, config)
}

// expectMessage is a row of the chat saved in toId's inbox: its id under the room first, then the row
func expectMessage(repo *MockRepository, toId, room uuid.UUID, contentType string, contents []string, err error) {
	indexed := repo.EXPECT().SaveRoomMessageId(mock.Anything, gocql.UUID(toId), gocql.UUID(room), gocql.UUID(msgId), config.MessageTTL).
		Return(nil).Once()
	repo.EXPECT().SaveMessage(mock.Anything, gocql.UUID(msgId), gocql.UUID(toId), gocql.UUID(fromId), gocql.UUID(room),
		contentType, contents, config.MessageTTL).Return(err).Once().NotBefore(indexed)
}

// expectEvent is a quit or delete saved in toId's inbox only, with the delete ttl
func expectEvent(repo *MockRepository, toId uuid.UUID, contentType string, contents []string) *mock.Call {
	return repo.EXPECT().SaveMessage(mock.Anything, gocql.UUID(msgId), gocql.UUID(toId), gocql.UUID(fromId), gocql.UUID(roomId),
		contentType, contents, config.DeleteTTL).Return(nil).Once()
}

func TestPersistMessage_groupSavesOneRowPerReceiverWithRoom(t *testing.T) {
	repo := NewMockRepository(t)
	expectMessage(repo, member1, roomId, "text", []string{"hi"}, nil)
	expectMessage(repo, member2, roomId, "text", []string{"hi"}, nil)

	assert.NoError(t, newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{member1, member2}, roomId, fromId, "text", []string{"hi"}))
}

func TestPersistMessage_receiverThatIsTheRoomUsesSenderAsRoom(t *testing.T) {
	// personal message: the room id is the receiver itself, its row is stored under the sender's room
	repo := NewMockRepository(t)
	expectMessage(repo, roomId, fromId, "text", []string{"hi"}, nil)
	expectMessage(repo, fromId, roomId, "text", []string{"hi"}, nil)

	assert.NoError(t, newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId, roomId}, roomId, fromId, "text", []string{"hi"}))
}

// the failure is returned so the consumer publishes a retry, the saved rows are upserted again on retry
func TestPersistMessage_oneFailedSaveDoesNotStopTheOthers(t *testing.T) {
	repo := NewMockRepository(t)
	expectMessage(repo, member1, roomId, "text", []string{"hi"}, errDB)
	expectMessage(repo, member2, roomId, "text", []string{"hi"}, nil)

	err := newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{member1, member2}, roomId, fromId, "text", []string{"hi"})

	assert.ErrorIs(t, err, errDB)
}

// a block is a row of the chat, the app shows it in the conversation: kept like a message, a quit deletes it with the room
func TestPersistMessage_blockIsSavedAsAMessage(t *testing.T) {
	repo := NewMockRepository(t)
	expectMessage(repo, fromId, roomId, "block", []string{}, nil)

	assert.NoError(t, newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "block", []string{}))
}

// a personal quit to the room of another member: the sender's older rows of that room go, in batches, then their ids,
// then the quit is saved
func TestPersistMessage_quitDeletesTheRoomsOlderRowsFirst(t *testing.T) {
	repo := NewMockRepository(t)
	ids := make([]gocql.UUID, 150)
	for i := range ids {
		ids[i] = gocql.UUID(uuid.New())
	}
	to, room, before := gocql.UUID(fromId), gocql.UUID(roomId), gocql.UUID(msgId)
	found := repo.EXPECT().FindRoomMessageIdsBefore(mock.Anything, to, room, before).Return(ids, nil).Once()
	first := repo.EXPECT().DeleteMessages(mock.Anything, to, ids[:100]).Return(nil).Once().NotBefore(found)
	second := repo.EXPECT().DeleteMessages(mock.Anything, to, ids[100:]).Return(nil).Once().NotBefore(first)
	unindexed := repo.EXPECT().DeleteRoomMessageIdsBefore(mock.Anything, to, room, before).Return(nil).Once().NotBefore(second)
	expectEvent(repo, fromId, "quit", []string{}).NotBefore(unindexed)

	assert.NoError(t, newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "quit", []string{}))
}

// a failed delete keeps the ids, so the retry finds the rest again, and the quit isn't saved until it's done
func TestPersistMessage_quitThatFailsToDeleteIsNotSaved(t *testing.T) {
	repo := NewMockRepository(t)
	ids := []gocql.UUID{gocql.UUID(uuid.New())}
	repo.EXPECT().FindRoomMessageIdsBefore(mock.Anything, gocql.UUID(fromId), gocql.UUID(roomId), gocql.UUID(msgId)).
		Return(ids, nil).Once()
	repo.EXPECT().DeleteMessages(mock.Anything, gocql.UUID(fromId), ids).Return(errDB).Once()

	err := newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "quit", []string{})

	assert.ErrorIs(t, err, errDB)
}

func TestPersistMessage_deleteDeletesTheMessageItNames(t *testing.T) {
	repo := NewMockRepository(t)
	target := uuid.New()
	deleted := repo.EXPECT().DeleteMessage(mock.Anything, gocql.UUID(fromId), gocql.UUID(target)).Return(nil).Once()
	unindexed := repo.EXPECT().DeleteRoomMessageId(mock.Anything, gocql.UUID(fromId), gocql.UUID(roomId), gocql.UUID(target)).
		Return(nil).Once().NotBefore(deleted)
	expectEvent(repo, fromId, "delete", []string{target.String()}).NotBefore(unindexed)

	assert.NoError(t, newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "delete", []string{target.String()}))
}

// a retry can't fix a missing or bad id, and the event would delete nothing on the devices: dropped, without an error
func TestPersistMessage_deleteOfABadIdIsDropped(t *testing.T) {
	for name, contents := range map[string][]string{"no id": {}, "bad id": {"nope"}} {
		t.Run(name, func(t *testing.T) {
			// nothing expected: the mock fails the test on any call
			repo := NewMockRepository(t)

			assert.NoError(t, newService(repo).PersistMessage(context.Background(), msgId,
				[]uuid.UUID{fromId}, roomId, fromId, "delete", contents))
		})
	}
}
