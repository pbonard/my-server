package test

import (
	"backend/messagepersist/internal/repository"
	"backend/messagepersist/internal/service"
	"context"
	"errors"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	msgId   = uuid.Must(uuid.NewV7())
	fromId  = uuid.New()
	roomId  = uuid.New()
	member1 = uuid.New()
	member2 = uuid.New()
	errDB   = errors.New("cassandra down")
	at      = service.WriteTime(msgId)
)

// distinct ttls, so a test shows which one a row was saved with
var config = service.Config{MessageTTL: 259200, DeleteTTL: 2592000}

func newService(repo *MockRepository) service.Service {
	return service.NewService(repo, config)
}

func row(contentType string, contents []string) repository.Message {
	return repository.Message{Id: gocql.UUID(msgId), FromId: gocql.UUID(fromId), ContentType: contentType, Contents: contents}
}

// expectShown is a row of the chat saved in toId's room, then the room marked as changed
func expectShown(repo *MockRepository, toId, room uuid.UUID, contentType string, contents []string) {
	to, r, m := gocql.UUID(toId), gocql.UUID(room), row(contentType, contents)
	saved := repo.EXPECT().SaveMessage(mock.Anything, to, r, m, config.MessageTTL).Return(nil).Once()
	repo.EXPECT().TouchRoom(mock.Anything, to, r, m.Id, config.DeleteTTL, at).Return(nil).Once().NotBefore(saved)
}

func TestPersistMessage_groupSavesOneRowPerReceiverWithRoom(t *testing.T) {
	repo := NewMockRepository(t)
	expectShown(repo, member1, roomId, "text", []string{"hi"})
	expectShown(repo, member2, roomId, "text", []string{"hi"})

	assert.NoError(t, newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{member1, member2}, roomId, fromId, "text", []string{"hi"}))
}

func TestPersistMessage_receiverThatIsTheRoomUsesSenderAsRoom(t *testing.T) {
	// personal message: the room id is the receiver itself, its row is stored under the sender's room
	repo := NewMockRepository(t)
	expectShown(repo, roomId, fromId, "text", []string{"hi"})
	expectShown(repo, fromId, roomId, "text", []string{"hi"})

	assert.NoError(t, newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId, roomId}, roomId, fromId, "text", []string{"hi"}))
}

// the failure is returned so the consumer publishes a retry, the saved rows are written again the same on retry
func TestPersistMessage_oneFailedSaveDoesNotStopTheOthers(t *testing.T) {
	repo := NewMockRepository(t)
	repo.EXPECT().SaveMessage(mock.Anything, gocql.UUID(member1), gocql.UUID(roomId), row("text", []string{"hi"}), config.MessageTTL).
		Return(errDB).Once()
	expectShown(repo, member2, roomId, "text", []string{"hi"})

	err := newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{member1, member2}, roomId, fromId, "text", []string{"hi"})

	assert.ErrorIs(t, err, errDB)
}

// a block is a row of the chat, the app shows it in the conversation: kept like a message, a quit deletes it with the room
func TestPersistMessage_blockIsSavedAsAMessage(t *testing.T) {
	repo := NewMockRepository(t)
	expectShown(repo, fromId, roomId, "block", []string{})

	assert.NoError(t, newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "block", []string{}))
}

// a personal quit to the room of another member: the sender's older rows of that room go, then the quit is saved, the
// room's oldest row, and the room marked as changed
func TestPersistMessage_quitDeletesTheRoomsOlderRowsFirst(t *testing.T) {
	repo := NewMockRepository(t)
	to, room, m := gocql.UUID(fromId), gocql.UUID(roomId), row("quit", []string{})
	deleted := repo.EXPECT().DeleteMessagesBefore(mock.Anything, to, room, m.Id).Return(nil).Once()
	saved := repo.EXPECT().SaveMessage(mock.Anything, to, room, m, config.DeleteTTL).Return(nil).Once().NotBefore(deleted)
	repo.EXPECT().TouchRoom(mock.Anything, to, room, m.Id, config.DeleteTTL, at).Return(nil).Once().NotBefore(saved)

	assert.NoError(t, newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "quit", []string{}))
}

// a failed delete isn't followed by the quit, the retry deletes again
func TestPersistMessage_quitThatFailsToDeleteIsNotSaved(t *testing.T) {
	repo := NewMockRepository(t)
	repo.EXPECT().DeleteMessagesBefore(mock.Anything, gocql.UUID(fromId), gocql.UUID(roomId), gocql.UUID(msgId)).
		Return(errDB).Once()

	err := newService(repo).PersistMessage(context.Background(), msgId,
		[]uuid.UUID{fromId}, roomId, fromId, "quit", []string{})

	assert.ErrorIs(t, err, errDB)
}

// the message it names goes, then the delete is saved and the room marked as changed
func TestPersistMessage_deleteDeletesTheMessageItNames(t *testing.T) {
	repo := NewMockRepository(t)
	target := uuid.New()
	to, room, m := gocql.UUID(fromId), gocql.UUID(roomId), row("delete", []string{target.String()})
	deleted := repo.EXPECT().DeleteMessage(mock.Anything, to, room, gocql.UUID(target)).Return(nil).Once()
	saved := repo.EXPECT().SaveMessage(mock.Anything, to, room, m, config.DeleteTTL).Return(nil).Once().NotBefore(deleted)
	repo.EXPECT().TouchRoom(mock.Anything, to, room, m.Id, config.DeleteTTL, at).Return(nil).Once().NotBefore(saved)

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

// the write time is the id's time, and ids made one after another never write out of order, within a millisecond too.
// two made within the same microsecond tie, which cassandra breaks the same way on every replica
func TestWriteTime_followsTheIdsTime(t *testing.T) {
	before := time.Now().UnixMicro()
	ids := make([]uuid.UUID, 50)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewV7())
	}
	after := time.Now().UnixMicro()

	for i, id := range ids {
		assert.GreaterOrEqual(t, service.WriteTime(id), before-1000)
		assert.LessOrEqual(t, service.WriteTime(id), after+1000)
		if i > 0 {
			assert.GreaterOrEqual(t, service.WriteTime(id), service.WriteTime(ids[i-1]))
		}
	}
}

// with no ttl for messages, the room's row is kept for good too
func TestPersistMessage_roomIsKeptForGoodWhenMessagesAre(t *testing.T) {
	repo := NewMockRepository(t)
	to, room, m := gocql.UUID(fromId), gocql.UUID(roomId), row("text", []string{"hi"})
	repo.EXPECT().SaveMessage(mock.Anything, to, room, m, 0).Return(nil).Once()
	repo.EXPECT().TouchRoom(mock.Anything, to, room, m.Id, 0, at).Return(nil).Once()

	s := service.NewService(repo, service.Config{MessageTTL: 0, DeleteTTL: config.DeleteTTL})
	assert.NoError(t, s.PersistMessage(context.Background(), msgId, []uuid.UUID{fromId}, roomId, fromId, "text", []string{"hi"}))
}
