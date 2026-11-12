package test

import (
	"backend/common"
	"backend/common/mocks"
	"backend/messagepreprocess/internal/service"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	msgId   = uuid.New()
	fromId  = uuid.New()
	toId    = uuid.New()
	member1 = uuid.New()
	member2 = uuid.New()
	file1   = uuid.New()
	file2   = uuid.New()
	errRepo = errors.New("repository down")
)

func TestManageMessage(t *testing.T) {
	tests := []struct {
		name        string
		toIdType    string
		contentType string
		contents    []string
		// sets the repository calls the case expects, any other call fails the test
		expect       func(r *MockRepository_Expecter)
		wantErr      error
		wantToIds    []uuid.UUID
		wantContents []string
	}{
		// ---------------------------------------------------------------- personal
		{
			name: "personal text, receiver did not block sender", toIdType: "personal", contentType: "text",
			contents: []string{"hi"},
			expect: func(r *MockRepository_Expecter) {
				r.IsBlocked(mock.Anything, gocql.UUID(toId), gocql.UUID(fromId)).Return(false, nil)
			},
			wantToIds: []uuid.UUID{fromId, toId}, wantContents: []string{"hi"},
		},
		{
			name: "personal text, receiver blocked sender", toIdType: "personal", contentType: "text",
			contents: []string{"hi"},
			expect: func(r *MockRepository_Expecter) {
				r.IsBlocked(mock.Anything, gocql.UUID(toId), gocql.UUID(fromId)).Return(true, nil)
			},
			wantToIds: []uuid.UUID{fromId}, wantContents: []string{"hi"},
		},
		{
			name: "personal text, block lookup fails", toIdType: "personal", contentType: "text",
			contents: []string{"hi"},
			expect: func(r *MockRepository_Expecter) {
				r.IsBlocked(mock.Anything, gocql.UUID(toId), gocql.UUID(fromId)).Return(false, errRepo)
			},
			wantErr: errRepo,
		},
		{
			name: "personal block", toIdType: "personal", contentType: "block",
			contents: []string{},
			expect: func(r *MockRepository_Expecter) {
				r.AddBlock(mock.Anything, gocql.UUID(fromId), gocql.UUID(toId)).Return(nil)
			},
			wantToIds: []uuid.UUID{fromId}, wantContents: []string{},
		},
		{
			name: "personal unblock", toIdType: "personal", contentType: "unblock",
			contents: []string{},
			expect: func(r *MockRepository_Expecter) {
				r.RemoveBlock(mock.Anything, gocql.UUID(fromId), gocql.UUID(toId)).Return(nil)
			},
			wantToIds: []uuid.UUID{fromId}, wantContents: []string{},
		},
		// ---------------------------------------------------------------- group
		{
			name: "group text goes to participants", toIdType: "group", contentType: "text",
			contents: []string{"hi"},
			expect: func(r *MockRepository_Expecter) {
				r.FindParticipantIds(mock.Anything, gocql.UUID(toId)).
					Return([]gocql.UUID{gocql.UUID(member1), gocql.UUID(member2)}, nil)
			},
			wantToIds: []uuid.UUID{member1, member2}, wantContents: []string{"hi"},
		},
		{
			name: "group text, room not found is not an error", toIdType: "group", contentType: "text",
			contents: []string{"hi"},
			expect: func(r *MockRepository_Expecter) {
				r.FindParticipantIds(mock.Anything, gocql.UUID(toId)).Return(nil, gocql.ErrNotFound)
			},
			wantToIds: nil, wantContents: []string{"hi"},
		},
		{
			name: "group create makes the room and clears contents", toIdType: "group", contentType: "create",
			contents: []string{"Seoul"},
			expect: func(r *MockRepository_Expecter) {
				r.CreateChatRoom(mock.Anything, gocql.UUID(toId), gocql.UUID(fromId), "Seoul").Return(nil)
			},
			wantToIds: []uuid.UUID{fromId}, wantContents: []string{},
		},
		{
			name: "group create fails", toIdType: "group", contentType: "create",
			contents: []string{"Seoul"},
			expect: func(r *MockRepository_Expecter) {
				r.CreateChatRoom(mock.Anything, gocql.UUID(toId), gocql.UUID(fromId), "Seoul").Return(errRepo)
			},
			wantErr: errRepo,
		},
		{
			name: "group participate adds member and notifies participants and member", toIdType: "group",
			contentType: "participate", contents: []string{},
			expect: func(r *MockRepository_Expecter) {
				r.FindParticipantIds(mock.Anything, gocql.UUID(toId)).
					Return([]gocql.UUID{gocql.UUID(member1)}, nil)
				r.AddParticipantId(mock.Anything, gocql.UUID(toId), gocql.UUID(fromId)).Return(nil)
			},
			wantToIds: []uuid.UUID{member1, fromId}, wantContents: []string{},
		},
		{
			name: "group quit removes member", toIdType: "group", contentType: "quit",
			contents: []string{},
			expect: func(r *MockRepository_Expecter) {
				r.RemoveParticipantId(mock.Anything, gocql.UUID(toId), gocql.UUID(fromId)).Return(nil)
			},
			wantToIds: []uuid.UUID{fromId}, wantContents: []string{},
		},
		// ---------------------------------------------------------------- media
		{
			name: "group image saves every file for the receivers", toIdType: "group", contentType: "image",
			contents: []string{file1.String(), file2.String()},
			expect: func(r *MockRepository_Expecter) {
				participants := []gocql.UUID{gocql.UUID(member1), gocql.UUID(member2)}
				r.FindParticipantIds(mock.Anything, gocql.UUID(toId)).Return(participants, nil)
				r.SaveIdsByFileName(mock.Anything, participants, gocql.UUID(file1)).Return(nil).Once()
				r.SaveIdsByFileName(mock.Anything, participants, gocql.UUID(file2)).Return(nil).Once()
			},
			wantToIds: []uuid.UUID{member1, member2}, wantContents: []string{file1.String(), file2.String()},
		},
		{
			// the bad content is reported and not saved, the valid file is still saved
			name: "image with a content that is not a uuid", toIdType: "group", contentType: "image",
			contents: []string{"not-a-uuid", file1.String()},
			expect: func(r *MockRepository_Expecter) {
				participants := []gocql.UUID{gocql.UUID(member1)}
				r.FindParticipantIds(mock.Anything, gocql.UUID(toId)).Return(participants, nil)
				r.SaveIdsByFileName(mock.Anything, participants, gocql.UUID(file1)).Return(nil).Once()
			},
			wantErr: errAny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := NewMockRepository(t)
			producer := mocks.NewMockSyncProducer(t)
			tt.expect(repo.EXPECT())

			var pushed []byte
			if tt.wantErr == nil {
				producer.EXPECT().Commit("prepared-message", mock.Anything, mock.Anything, mock.Anything).
					Run(func(_ string, _ []byte, value []byte, _ []sarama.RecordHeader) { pushed = value }).
					Return(nil).Once()
			}

			err := service.NewService(repo, producer).
				ManageMessage(context.Background(), msgId, fromId, tt.toIdType, toId, tt.contentType, tt.contents, false)

			if tt.wantErr != nil {
				require.Error(t, err)
				if tt.wantErr != errAny {
					assert.ErrorIs(t, err, tt.wantErr)
				}
				return
			}
			require.NoError(t, err)

			var msg common.PreparedMessage
			require.NoError(t, json.Unmarshal(pushed, &msg))
			assert.Equal(t, msgId, msg.Id)
			assert.Equal(t, fromId, msg.FromId)
			assert.Equal(t, toId, msg.RoomId)
			assert.Equal(t, tt.contentType, msg.ContentType)
			assert.Equal(t, tt.wantToIds, msg.ToIds)
			assert.Equal(t, tt.wantContents, msg.Contents)
		})
	}
}

func TestManageMessage_sendFailureIsReturned(t *testing.T) {
	repo := NewMockRepository(t)
	producer := mocks.NewMockSyncProducer(t)
	errKafka := errors.New("kafka down")
	repo.EXPECT().IsBlocked(mock.Anything, gocql.UUID(toId), gocql.UUID(fromId)).Return(false, nil)
	producer.EXPECT().Commit("prepared-message", mock.Anything, mock.Anything, mock.Anything).Return(errKafka)

	err := service.NewService(repo, producer).
		ManageMessage(context.Background(), msgId, fromId, "personal", toId, "text", []string{"hi"}, false)

	assert.ErrorIs(t, err, errKafka)
}

// the messagepreprocess of holidayfriend hands its messages on to its own topic, and without a push when asked
func TestManageMessage_noPushIsCarriedToTheTopicOfThisApp(t *testing.T) {
	t.Setenv("PROFILE", "holidayfriend")
	repo := NewMockRepository(t)
	producer := mocks.NewMockSyncProducer(t)
	repo.EXPECT().IsBlocked(mock.Anything, gocql.UUID(toId), gocql.UUID(fromId)).Return(false, nil)
	var pushed []byte
	producer.EXPECT().Commit("holidayfriend.prepared-message", mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ string, _ []byte, value []byte, _ []sarama.RecordHeader) { pushed = value }).Return(nil)

	err := service.NewService(repo, producer).
		ManageMessage(context.Background(), msgId, fromId, "personal", toId, "text", []string{"hi"}, true)

	require.NoError(t, err)
	var msg common.PreparedMessage
	require.NoError(t, json.Unmarshal(pushed, &msg))
	assert.True(t, msg.NoPush)
}

// errAny marks a case that must fail without caring which error it is
var errAny = errors.New("any error")

func toBytes(ids []uuid.UUID) [][]byte {
	if ids == nil {
		return nil
	}
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = id[:]
	}
	return out
}
