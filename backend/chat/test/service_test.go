package test

import (
	"backend/chat/internal/projection"
	"backend/chat/internal/service"
	"backend/common"
	"backend/common/mocks"
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
	memberId = uuid.New()
	otherId  = uuid.New()
	roomId   = uuid.New()
	errDB    = errors.New("cassandra down")
)

type deps struct {
	repo     *MockRepository
	producer *mocks.MockSyncProducer
	storage  *MockStorageClient
	signer   *mocks.MockCDNClient
}

func newService(t *testing.T) (service.Service, deps) {
	d := deps{NewMockRepository(t), mocks.NewMockSyncProducer(t), NewMockStorageClient(t), mocks.NewMockCDNClient(t)}
	return service.NewService(d.repo, d.producer, d.storage, d.signer), d
}

// ---------------------------------------------------------------- report

func TestReportUser(t *testing.T) {
	t.Run("up to 5 reports only records the report", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().AddReporterIdByReportedId(mock.Anything, "production", gocql.UUID(memberId), gocql.UUID(otherId)).Return(nil)
		d.repo.EXPECT().FindReportCountById(mock.Anything, "production", gocql.UUID(otherId)).Return(5, nil)

		assert.NoError(t, s.ReportUser(context.Background(), "production", memberId, otherId))
	})
	t.Run("more than 5 reports asks the auth of their app to ban them", func(t *testing.T) {
		s, d := newService(t)
		// counted in the keyspace of their app, and banned by its auth
		d.repo.EXPECT().AddReporterIdByReportedId(mock.Anything, "holidayfriend", gocql.UUID(memberId), gocql.UUID(otherId)).Return(nil)
		d.repo.EXPECT().FindReportCountById(mock.Anything, "holidayfriend", gocql.UUID(otherId)).Return(6, nil)
		var published []byte
		ban := []sarama.RecordHeader{{Key: []byte("taskType"), Value: []byte("ban")}}
		d.producer.EXPECT().Commit("holidayfriend.auth", otherId[:], mock.Anything, ban).
			Run(func(_ string, _ []byte, value []byte, _ []sarama.RecordHeader) { published = value }).Return(nil)

		assert.NoError(t, s.ReportUser(context.Background(), "holidayfriend", memberId, otherId))
		var e common.AuthEvent
		require.NoError(t, json.Unmarshal(published, &e))
		assert.Equal(t, common.AuthEvent{Id: otherId[:]}, e)
	})
	t.Run("a failed publish is returned", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().AddReporterIdByReportedId(mock.Anything, "production", mock.Anything, mock.Anything).Return(nil)
		d.repo.EXPECT().FindReportCountById(mock.Anything, "production", mock.Anything).Return(6, nil)
		d.producer.EXPECT().Commit("production.auth", mock.Anything, mock.Anything, mock.Anything).Return(errDB)

		assert.ErrorIs(t, s.ReportUser(context.Background(), "production", memberId, otherId), errDB)
	})
}

// ---------------------------------------------------------------- publish

func TestPublishMessaging_textIsProducedAsChatMessage(t *testing.T) {
	s, d := newService(t)
	var pushed []byte
	d.producer.EXPECT().Commit("chat-message", []byte(nil), mock.Anything, []sarama.RecordHeader(nil)).
		Run(func(_ string, _ []byte, value []byte, _ []sarama.RecordHeader) { pushed = value }).Return(nil)

	resp, err := s.PublishMessaging(context.Background(), "production", memberId, "group", roomId, "text", []string{"hi"}, false)

	require.NoError(t, err)
	id := resp["id"]
	var msg common.ChatMessage
	require.NoError(t, json.Unmarshal(pushed, &msg))
	assert.Equal(t, id[:], msg.Id)
	assert.Equal(t, memberId[:], msg.FromId)
	assert.Equal(t, roomId[:], msg.ToId)
	assert.Equal(t, "group", msg.ToIdType)
	assert.Equal(t, []string{"hi"}, msg.Contents)
	// the services after chat use the keyspace of the sender's app
	assert.Equal(t, "production", msg.App)
}

func TestPublishMessaging_sendFailureIsReturned(t *testing.T) {
	s, d := newService(t)
	errKafka := errors.New("kafka down")
	d.producer.EXPECT().Commit("chat-message", mock.Anything, mock.Anything, mock.Anything).Return(errKafka)

	resp, err := s.PublishMessaging(context.Background(), "production", memberId, "group", roomId, "text", []string{"hi"}, false)

	assert.ErrorIs(t, err, errKafka)
	assert.Nil(t, resp)
}

func TestPublishMessaging_mediaMustBeAPresignedUpload(t *testing.T) {
	key := "image" + string(memberId[:])
	t.Run("uploaded file is consumed and published", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().HasFilepath(mock.Anything, key, []string{"f1"}).Return(true, nil)
		d.repo.EXPECT().RemoveFilepath(mock.Anything, key, []string{"f1"}).Return(nil)
		d.producer.EXPECT().Commit("chat-message", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		_, err := s.PublishMessaging(context.Background(), "production", memberId, "personal", otherId, "image", []string{"f1"}, false)

		assert.NoError(t, err)
	})
	t.Run("unknown file is rejected", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().HasFilepath(mock.Anything, key, []string{"f1"}).Return(false, nil)

		_, err := s.PublishMessaging(context.Background(), "production", memberId, "personal", otherId, "image", []string{"f1"}, false)

		assert.EqualError(t, err, "bad request")
	})
}

func TestPublishMessaging_noPushIsCarried(t *testing.T) {
	s, d := newService(t)
	var pushed []byte
	d.producer.EXPECT().Commit("chat-message", mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ string, _ []byte, value []byte, _ []sarama.RecordHeader) { pushed = value }).Return(nil)

	_, err := s.PublishMessaging(context.Background(), "production", memberId, "personal", otherId, "text", []string{"hi"}, true)

	require.NoError(t, err)
	var msg common.ChatMessage
	require.NoError(t, json.Unmarshal(pushed, &msg))
	assert.True(t, msg.NoPush)
}

// ---------------------------------------------------------------- files

func TestGeneratePresignedURL(t *testing.T) {
	s, d := newService(t)
	var saved []string
	d.repo.EXPECT().SetFilepath(mock.Anything, "image"+string(memberId[:]), mock.Anything).
		Run(func(_ context.Context, _ string, filenames []string) { saved = filenames }).Return(nil)
	d.storage.EXPECT().PresignUpload(mock.Anything, "image", mock.Anything).
		RunAndReturn(func(_ context.Context, contentType, filename string) (string, map[string]string, error) {
			return "https://s3", map[string]string{"key": contentType + "/" + filename}, nil
		}).Twice()

	res, err := s.GeneratePresignedURL(context.Background(), memberId, "image", 2)

	require.NoError(t, err)
	require.Len(t, res, 2)
	for i, r := range res {
		assert.Equal(t, saved[i], r.Filename.String())
		assert.Equal(t, "https://s3", r.URL)
		assert.Equal(t, "image/"+saved[i], r.Fields["key"])
	}
}

func TestGenerateSignedURL(t *testing.T) {
	filename := uuid.New()
	t.Run("receiver of the file gets a signed url", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindIdsByFilename(mock.Anything, "production", gocql.UUID(filename)).
			Return([]gocql.UUID{gocql.UUID(otherId), gocql.UUID(memberId)}, nil)
		d.signer.EXPECT().SignedURL("image", filename.String()).Return("https://signed", nil)

		res, err := s.GenerateSignedURL(context.Background(), "production", memberId, "image", filename)

		require.NoError(t, err)
		assert.Equal(t, map[string]string{"url": "https://signed"}, res)
	})
	t.Run("anyone else is rejected", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindIdsByFilename(mock.Anything, "production", gocql.UUID(filename)).Return([]gocql.UUID{gocql.UUID(otherId)}, nil)

		_, err := s.GenerateSignedURL(context.Background(), "production", memberId, "image", filename)

		assert.EqualError(t, err, "unauthorized request")
	})
}

// ---------------------------------------------------------------- info / members

func TestSetName(t *testing.T) {
	t.Run("whitespace is removed", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().SaveNameById(mock.Anything, "production", gocql.UUID(memberId), "JohnDoe").Return(nil)

		assert.NoError(t, s.SetName(context.Background(), "production", memberId, " John\tDoe "))
	})
	t.Run("blank name is rejected", func(t *testing.T) {
		s, _ := newService(t)

		assert.EqualError(t, s.SetName(context.Background(), "production", memberId, " \n "), "incorrect name")
	})
}

func TestGetProfile_notFoundIsEmptyProfile(t *testing.T) {
	s, d := newService(t)
	d.repo.EXPECT().FindProfileById(mock.Anything, "production", gocql.UUID(memberId)).Return("", gocql.ErrNotFound)

	res, err := s.GetProfile(context.Background(), "production", memberId)

	require.NoError(t, err)
	assert.Equal(t, "", res.Name)
}

func TestGetChatParticipants_skipsZeroIds(t *testing.T) {
	s, d := newService(t)
	d.repo.EXPECT().FindChatParticipantIds(mock.Anything, "production", gocql.UUID(roomId)).
		Return([]gocql.UUID{{}, gocql.UUID(memberId)}, nil)

	res, err := s.GetChatParticipants(context.Background(), "production", roomId)

	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, memberId, res[0].Id)
}

func TestGetRecentMessages_mapsRows(t *testing.T) {
	s, d := newService(t)
	msgId, cursor := uuid.New(), uuid.New()
	d.repo.EXPECT().FindRecentMessagesByToId(mock.Anything, "production", gocql.UUID(memberId), gocql.UUID(cursor)).
		Return([]projection.FindMessagesByToIdAndId{{
			Id: gocql.UUID(msgId), RoomId: roomId[:], FromId: gocql.UUID(otherId), ContentType: "text", Contents: []string{"hi"},
		}}, nil)

	res, err := s.GetRecentMessages(context.Background(), "production", memberId, cursor)

	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, msgId, res[0].Id)
	assert.Equal(t, roomId, res[0].RoomId)
	assert.Equal(t, otherId, res[0].FromId)
	assert.Equal(t, []string{"hi"}, res[0].Contents)
}

func TestCheckBlockAndBlockedConversations(t *testing.T) {
	s, d := newService(t)
	d.repo.EXPECT().DidBlock(mock.Anything, "production", gocql.UUID(memberId), gocql.UUID(otherId)).Return(true, nil)
	d.repo.EXPECT().FindBlockedConversations(mock.Anything, "production", gocql.UUID(memberId)).Return([]gocql.UUID{gocql.UUID(roomId)}, nil)

	block, err := s.CheckBlock(context.Background(), "production", memberId, otherId)
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"didBlock": true}, block)

	blocked, err := s.GetBlockedConversations(context.Background(), "production", memberId)
	require.NoError(t, err)
	assert.Equal(t, roomId, blocked[0].Id)
}
