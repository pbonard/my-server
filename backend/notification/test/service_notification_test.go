package test

import (
	"backend/common"
	"backend/common/mocks"
	"backend/notification/internal/projection"
	"backend/notification/internal/service"
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
	memberId  = uuid.New()
	otherId   = uuid.New()
	senderId  = uuid.New()
	roomId    = uuid.New()
	messageId = uuid.New()
	errDB     = errors.New("cassandra down")
)

func android(id uuid.UUID, token string) projection.FindPushTokensById {
	return projection.FindPushTokensById{Id: gocql.UUID(id), OS: "android", DevicePushToken: token}
}

func ios(id uuid.UUID, token string) projection.FindPushTokensById {
	return projection.FindPushTokensById{Id: gocql.UUID(id), OS: "ios", DevicePushToken: token}
}

// captures the pushed messages by topic
func capturePushes(producer *mocks.MockSyncProducer) map[string][]pushed {
	out := map[string][]pushed{}
	producer.EXPECT().Commit(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(topic string, key, value []byte, _ []sarama.RecordHeader) {
			out[topic] = append(out[topic], pushed{key: key, value: value})
		}).Return(nil).Maybe()
	return out
}

type pushed struct {
	key, value []byte
}

func (p pushed) message(t *testing.T) common.NotificationMessage {
	var m common.NotificationMessage
	require.NoError(t, json.Unmarshal(p.value, &m))
	return m
}

// ---------------------------------------------------------------- RegisterNotification

func TestRegisterNotification(t *testing.T) {
	t.Run("token already owned by the member only saves info", func(t *testing.T) {
		repo := NewMockRepository(t)
		repo.EXPECT().FindMemberIdByToken(mock.Anything, "tok").Return(gocql.UUID(memberId), nil)
		repo.EXPECT().SaveNotificationInfoById(mock.Anything, gocql.UUID(memberId), "ios", "tok").Return(nil)

		assert.NoError(t, service.NewService(repo, mocks.NewMockSyncProducer(t), mocks.NewMockCDNClient(t)).
			RegisterNotification(context.Background(), memberId, "ios", "tok"))
	})
	t.Run("token moves from another member", func(t *testing.T) {
		repo := NewMockRepository(t)
		repo.EXPECT().FindMemberIdByToken(mock.Anything, "tok").Return(gocql.UUID(otherId), nil)
		repo.EXPECT().DeleteNotificationInfoByIdAndToken(mock.Anything, gocql.UUID(otherId), "tok").Return(nil)
		repo.EXPECT().UpdateMemberIdByToken(mock.Anything, "tok", gocql.UUID(memberId)).Return(nil)
		repo.EXPECT().SaveNotificationInfoById(mock.Anything, gocql.UUID(memberId), "android", "tok").Return(nil)

		assert.NoError(t, service.NewService(repo, mocks.NewMockSyncProducer(t), mocks.NewMockCDNClient(t)).
			RegisterNotification(context.Background(), memberId, "android", "tok"))
	})
	t.Run("new token writes the owner once and deletes nothing", func(t *testing.T) {
		repo := NewMockRepository(t)
		repo.EXPECT().FindMemberIdByToken(mock.Anything, "tok").Return(gocql.UUID{}, gocql.ErrNotFound)
		repo.EXPECT().UpdateMemberIdByToken(mock.Anything, "tok", gocql.UUID(memberId)).Return(nil).Once()
		repo.EXPECT().SaveNotificationInfoById(mock.Anything, gocql.UUID(memberId), "android", "tok").Return(nil)

		assert.NoError(t, service.NewService(repo, mocks.NewMockSyncProducer(t), mocks.NewMockCDNClient(t)).
			RegisterNotification(context.Background(), memberId, "android", "tok"))
	})
	t.Run("lookup error", func(t *testing.T) {
		repo := NewMockRepository(t)
		repo.EXPECT().FindMemberIdByToken(mock.Anything, "tok").Return(gocql.UUID{}, errDB)

		assert.ErrorIs(t, service.NewService(repo, mocks.NewMockSyncProducer(t), mocks.NewMockCDNClient(t)).
			RegisterNotification(context.Background(), memberId, "android", "tok"), errDB)
	})
}

// ---------------------------------------------------------------- scheduled

func TestPreprocessScheduledNotification_splitsTokensByOS(t *testing.T) {
	repo, producer := NewMockRepository(t), mocks.NewMockSyncProducer(t)
	repo.EXPECT().FindPushTokensById(mock.Anything, gocql.UUID(memberId)).
		Return([]projection.FindPushTokensById{android(memberId, "fcm-token")}, nil)
	repo.EXPECT().FindPushTokensById(mock.Anything, gocql.UUID(otherId)).
		Return([]projection.FindPushTokensById{ios(otherId, "apn-token")}, nil)
	pushes := capturePushes(producer)

	service.NewService(repo, producer, mocks.NewMockCDNClient(t)).PreprocessScheduledNotification(context.Background(), roomId,
		map[uuid.UUID]map[int]string{memberId: nil, otherId: nil}, map[int]string{0: "Hamlet"})

	require.Len(t, pushes["fcm-notification"], 1)
	require.Len(t, pushes["apn-notification"], 1)
	fcm := pushes["fcm-notification"][0]
	assert.Equal(t, append(roomId[:], 2), fcm.key)
	assert.Equal(t, map[string]uuid.UUID{"fcm-token": memberId}, fcm.message(t).TokenMap)
	assert.Equal(t, "Conversation starts soon", fcm.message(t).Title)
	assert.Equal(t, "You can now enter the conversation about Hamlet and talk!", fcm.message(t).Text)
	assert.Equal(t, map[string]uuid.UUID{"apn-token": otherId}, pushes["apn-notification"][0].message(t).TokenMap)
}

func TestPreprocessScheduledNotification_tokenLookupErrorSendsNothing(t *testing.T) {
	repo := NewMockRepository(t)
	repo.EXPECT().FindPushTokensById(mock.Anything, gocql.UUID(memberId)).Return(nil, errDB)

	err := service.NewService(repo, mocks.NewMockSyncProducer(t), mocks.NewMockCDNClient(t)).PreprocessScheduledNotification(
		context.Background(), roomId, map[uuid.UUID]map[int]string{memberId: nil}, map[int]string{0: "Hamlet"})

	assert.ErrorIs(t, err, errDB)
}

func TestPreprocessScheduledNotification_publishFailureIsReturned(t *testing.T) {
	repo, producer := NewMockRepository(t), mocks.NewMockSyncProducer(t)
	repo.EXPECT().FindPushTokensById(mock.Anything, gocql.UUID(memberId)).
		Return([]projection.FindPushTokensById{android(memberId, "fcm-token")}, nil)
	errKafka := errors.New("kafka down")
	producer.EXPECT().Commit("fcm-notification", mock.Anything, mock.Anything, mock.Anything).Return(errKafka)

	err := service.NewService(repo, producer, mocks.NewMockCDNClient(t)).PreprocessScheduledNotification(
		context.Background(), roomId, map[uuid.UUID]map[int]string{memberId: nil}, map[int]string{0: "Hamlet"})

	assert.ErrorIs(t, err, errKafka)
}

// ---------------------------------------------------------------- message

// expectTokens expects one token lookup per receiver, any other lookup fails the test
func expectTokens(repo *MockRepository, tokens ...projection.FindPushTokensById) {
	for _, tok := range tokens {
		repo.EXPECT().FindPushTokensById(mock.Anything, tok.Id).Return([]projection.FindPushTokensById{tok}, nil)
	}
}

func TestPreprocessMessageNotification_personalText(t *testing.T) {
	repo, producer := NewMockRepository(t), mocks.NewMockSyncProducer(t)
	expectTokens(repo, android(memberId, "fcm-token"))
	repo.EXPECT().FindNameById(mock.Anything, gocql.UUID(senderId)).Return("alice", nil)
	pushes := capturePushes(producer)

	// personal message: the first receiver is the room itself, so there is no room name
	service.NewService(repo, producer, mocks.NewMockCDNClient(t)).PreprocessMessageNotification(context.Background(), 1,
		messageId, []uuid.UUID{memberId}, memberId, senderId, "text", []string{"hello"})

	require.Len(t, pushes["fcm-notification"], 1)
	msg := pushes["fcm-notification"][0].message(t)
	assert.Equal(t, append(messageId[:], 1), pushes["fcm-notification"][0].key)
	assert.Equal(t, "alice", msg.Title)
	assert.Equal(t, "", msg.SubTitle)
	assert.Equal(t, "hello", msg.Text)
	assert.Empty(t, pushes["apn-notification"])
}

func TestPreprocessMessageNotification_groupUsesRoomNameAsTitle(t *testing.T) {
	repo, producer := NewMockRepository(t), mocks.NewMockSyncProducer(t)
	expectTokens(repo, ios(memberId, "apn-token"))
	repo.EXPECT().FindNameById(mock.Anything, gocql.UUID(senderId)).Return("alice", nil)
	repo.EXPECT().FindRoomNameById(mock.Anything, gocql.UUID(roomId)).Return("Seoul", nil)
	pushes := capturePushes(producer)

	service.NewService(repo, producer, mocks.NewMockCDNClient(t)).PreprocessMessageNotification(context.Background(), 0,
		messageId, []uuid.UUID{memberId}, roomId, senderId, "text", []string{"hello"})

	require.Len(t, pushes["apn-notification"], 1)
	msg := pushes["apn-notification"][0].message(t)
	assert.Equal(t, "Seoul", msg.Title)
	assert.Equal(t, "alice", msg.SubTitle)
}

func TestPreprocessMessageNotification_imageIsSigned(t *testing.T) {
	repo, producer, signer := NewMockRepository(t), mocks.NewMockSyncProducer(t), mocks.NewMockCDNClient(t)
	expectTokens(repo, android(memberId, "fcm-token"))
	repo.EXPECT().FindNameById(mock.Anything, gocql.UUID(senderId)).Return("alice", nil)
	signer.EXPECT().SignedURL("image", "file-1").Return("https://signed", nil)
	pushes := capturePushes(producer)

	service.NewService(repo, producer, signer).PreprocessMessageNotification(context.Background(), 0,
		messageId, []uuid.UUID{memberId}, memberId, senderId, "image", []string{"file-1"})

	msg := pushes["fcm-notification"][0].message(t)
	assert.Equal(t, "https://signed", msg.ImageURL)
	assert.Equal(t, "(image)", msg.Text)
}

func TestPreprocessMessageNotification_signFailureSendsNothing(t *testing.T) {
	repo, signer := NewMockRepository(t), mocks.NewMockCDNClient(t)
	expectTokens(repo, android(memberId, "fcm-token"))
	repo.EXPECT().FindNameById(mock.Anything, gocql.UUID(senderId)).Return("alice", nil)
	errSign := errors.New("bad key")
	signer.EXPECT().SignedURL(mock.Anything, mock.Anything).Return("", errSign)

	err := service.NewService(repo, mocks.NewMockSyncProducer(t), signer).PreprocessMessageNotification(context.Background(), 0,
		messageId, []uuid.UUID{memberId}, memberId, senderId, "image", []string{"file-1"})

	assert.ErrorIs(t, err, errSign)
}

func TestPreprocessMessageNotification_publishFailureIsReturned(t *testing.T) {
	repo, producer := NewMockRepository(t), mocks.NewMockSyncProducer(t)
	expectTokens(repo, android(memberId, "fcm-token"))
	repo.EXPECT().FindNameById(mock.Anything, gocql.UUID(senderId)).Return("alice", nil)
	errKafka := errors.New("kafka down")
	producer.EXPECT().Commit("fcm-notification", mock.Anything, mock.Anything, mock.Anything).Return(errKafka)

	err := service.NewService(repo, producer, mocks.NewMockCDNClient(t)).PreprocessMessageNotification(context.Background(), 1,
		messageId, []uuid.UUID{memberId}, memberId, senderId, "text", []string{"hello"})

	assert.ErrorIs(t, err, errKafka)
}

func TestPreprocessMessageNotification_otherMediaShowsType(t *testing.T) {
	repo, producer := NewMockRepository(t), mocks.NewMockSyncProducer(t)
	expectTokens(repo, android(memberId, "fcm-token"))
	repo.EXPECT().FindNameById(mock.Anything, gocql.UUID(senderId)).Return("alice", nil)
	pushes := capturePushes(producer)

	service.NewService(repo, producer, mocks.NewMockCDNClient(t)).PreprocessMessageNotification(context.Background(), 0,
		messageId, []uuid.UUID{memberId}, memberId, senderId, "audio", []string{"file-1"})

	assert.Equal(t, "(audio)", pushes["fcm-notification"][0].message(t).Text)
}
