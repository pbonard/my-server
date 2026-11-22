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
	"time"

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
		d.repo.EXPECT().AddReporterIdByReportedId(mock.Anything, gocql.UUID(memberId), gocql.UUID(otherId)).Return(nil)
		d.repo.EXPECT().FindReportCountById(mock.Anything, gocql.UUID(otherId)).Return(5, nil)

		assert.NoError(t, s.ReportUser(context.Background(), memberId, otherId))
	})
	t.Run("more than 5 reports asks the auth of this chat's app to ban them", func(t *testing.T) {
		// the chat of holidayfriend, so the auth of holidayfriend bans them
		t.Setenv("PROFILE", "holidayfriend")
		s, d := newService(t)
		d.repo.EXPECT().AddReporterIdByReportedId(mock.Anything, gocql.UUID(memberId), gocql.UUID(otherId)).Return(nil)
		d.repo.EXPECT().FindReportCountById(mock.Anything, gocql.UUID(otherId)).Return(6, nil)
		var published []byte
		ban := []sarama.RecordHeader{{Key: []byte("taskType"), Value: []byte("ban")}}
		d.producer.EXPECT().Commit("holidayfriend.auth", otherId[:], mock.Anything, ban).
			Run(func(_ string, _ []byte, value []byte, _ []sarama.RecordHeader) { published = value }).Return(nil)

		assert.NoError(t, s.ReportUser(context.Background(), memberId, otherId))
		var e common.AuthEvent
		require.NoError(t, json.Unmarshal(published, &e))
		assert.Equal(t, common.AuthEvent{Id: otherId}, e)
	})
	t.Run("a failed publish is returned", func(t *testing.T) {
		t.Setenv("PROFILE", "production")
		s, d := newService(t)
		d.repo.EXPECT().AddReporterIdByReportedId(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		d.repo.EXPECT().FindReportCountById(mock.Anything, mock.Anything).Return(6, nil)
		d.producer.EXPECT().Commit("production.auth", mock.Anything, mock.Anything, mock.Anything).Return(errDB)

		assert.ErrorIs(t, s.ReportUser(context.Background(), memberId, otherId), errDB)
	})
}

// ---------------------------------------------------------------- publish

func TestPublishMessaging_textIsProducedAsChatMessage(t *testing.T) {
	s, d := newService(t)
	var pushed []byte
	d.producer.EXPECT().Commit("chat-message", []byte(nil), mock.Anything, []sarama.RecordHeader(nil)).
		Run(func(_ string, _ []byte, value []byte, _ []sarama.RecordHeader) { pushed = value }).Return(nil)

	resp, err := s.PublishMessaging(context.Background(), memberId, "group", roomId, "text", []string{"hi"}, false)

	require.NoError(t, err)
	id := resp["id"]
	var msg common.ChatMessage
	require.NoError(t, json.Unmarshal(pushed, &msg))
	// the ids are written as uuid strings, readable on the topic
	assert.Contains(t, string(pushed), `"fromId":"`+memberId.String()+`"`)
	assert.Equal(t, id, msg.Id)
	assert.Equal(t, memberId, msg.FromId)
	assert.Equal(t, roomId, msg.ToId)
	assert.Equal(t, "group", msg.ToIdType)
	assert.Equal(t, []string{"hi"}, msg.Contents)
}

// the chat of holidayfriend hands its messages to its own messagepreprocess, never to the one of production
func TestPublishMessaging_toTheTopicOfThisChatsApp(t *testing.T) {
	t.Setenv("PROFILE", "holidayfriend")
	s, d := newService(t)
	d.producer.EXPECT().Commit("holidayfriend.chat-message", []byte(nil), mock.Anything, []sarama.RecordHeader(nil)).Return(nil)

	_, err := s.PublishMessaging(context.Background(), memberId, "group", roomId, "text", []string{"hi"}, false)

	require.NoError(t, err)
}

func TestPublishMessaging_sendFailureIsReturned(t *testing.T) {
	s, d := newService(t)
	errKafka := errors.New("kafka down")
	d.producer.EXPECT().Commit("chat-message", mock.Anything, mock.Anything, mock.Anything).Return(errKafka)

	resp, err := s.PublishMessaging(context.Background(), memberId, "group", roomId, "text", []string{"hi"}, false)

	assert.ErrorIs(t, err, errKafka)
	assert.Nil(t, resp)
}

func TestPublishMessaging_mediaMustBeAPresignedUpload(t *testing.T) {
	key := "uploads:image:" + memberId.String()
	t.Run("uploaded file is consumed and published", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().HasFilepath(mock.Anything, key, []string{"f1"}).Return(true, nil)
		d.repo.EXPECT().RemoveFilepath(mock.Anything, key, []string{"f1"}).Return(nil)
		d.producer.EXPECT().Commit("chat-message", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		_, err := s.PublishMessaging(context.Background(), memberId, "personal", otherId, "image", []string{"f1"}, false)

		assert.NoError(t, err)
	})
	t.Run("unknown file is rejected", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().HasFilepath(mock.Anything, key, []string{"f1"}).Return(false, nil)

		_, err := s.PublishMessaging(context.Background(), memberId, "personal", otherId, "image", []string{"f1"}, false)

		assert.EqualError(t, err, "bad request")
	})
}

func TestPublishMessaging_noPushIsCarried(t *testing.T) {
	s, d := newService(t)
	var pushed []byte
	d.producer.EXPECT().Commit("chat-message", mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ string, _ []byte, value []byte, _ []sarama.RecordHeader) { pushed = value }).Return(nil)

	_, err := s.PublishMessaging(context.Background(), memberId, "personal", otherId, "text", []string{"hi"}, true)

	require.NoError(t, err)
	var msg common.ChatMessage
	require.NoError(t, json.Unmarshal(pushed, &msg))
	assert.True(t, msg.NoPush)
}

// ---------------------------------------------------------------- files

func TestGeneratePresignedURL(t *testing.T) {
	s, d := newService(t)
	var saved []string
	d.repo.EXPECT().SetFilepath(mock.Anything, "uploads:image:"+memberId.String(), mock.Anything).
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
		d.repo.EXPECT().FindIdsByFilename(mock.Anything, gocql.UUID(filename)).
			Return([]gocql.UUID{gocql.UUID(otherId), gocql.UUID(memberId)}, nil)
		d.signer.EXPECT().SignedURL("image", filename.String()).Return("https://signed", nil)

		res, err := s.GenerateSignedURL(context.Background(), memberId, "image", filename)

		require.NoError(t, err)
		assert.Equal(t, map[string]string{"url": "https://signed"}, res)
	})
	t.Run("anyone else is rejected", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindIdsByFilename(mock.Anything, gocql.UUID(filename)).Return([]gocql.UUID{gocql.UUID(otherId)}, nil)

		_, err := s.GenerateSignedURL(context.Background(), memberId, "image", filename)

		assert.EqualError(t, err, "unauthorized request")
	})
}

// ---------------------------------------------------------------- info / members

func TestSetName(t *testing.T) {
	t.Run("whitespace is removed", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().SaveNameById(mock.Anything, gocql.UUID(memberId), "JohnDoe").Return(nil)

		assert.NoError(t, s.SetName(context.Background(), memberId, " John\tDoe "))
	})
	t.Run("blank name is rejected", func(t *testing.T) {
		s, _ := newService(t)

		assert.EqualError(t, s.SetName(context.Background(), memberId, " \n "), "incorrect name")
	})
}

func TestGetProfile_notFoundIsEmptyProfile(t *testing.T) {
	s, d := newService(t)
	d.repo.EXPECT().FindProfileById(mock.Anything, gocql.UUID(memberId)).Return("", gocql.ErrNotFound)

	res, err := s.GetProfile(context.Background(), memberId)

	require.NoError(t, err)
	assert.Equal(t, "", res.Name)
}

func TestGetChatParticipants_skipsZeroIds(t *testing.T) {
	s, d := newService(t)
	d.repo.EXPECT().FindChatParticipantIds(mock.Anything, gocql.UUID(roomId)).
		Return([]gocql.UUID{{}, gocql.UUID(memberId)}, nil)

	res, err := s.GetChatParticipants(context.Background(), roomId)

	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, memberId, res[0].Id)
}

func idAt(d time.Duration) uuid.UUID {
	id := uuid.Must(uuid.NewV7())
	ms := uint64(time.Now().Add(d).UnixMilli())
	for i := 5; i >= 0; i-- {
		id[i] = byte(ms)
		ms >>= 8
	}
	return id
}

func TestGetRecentMessages_readsTheRoomsThatChanged(t *testing.T) {
	s, d := newService(t)
	cursor := idAt(-time.Minute)
	room2 := uuid.New()
	m1, m2, m3 := idAt(-time.Minute-time.Second), idAt(-30*time.Second), idAt(-10*time.Second)
	d.repo.EXPECT().FindRooms(mock.Anything, gocql.UUID(memberId)).Return([]projection.Room{
		{RoomId: gocql.UUID(roomId), UpdatedId: gocql.UUID(m3)},
		{RoomId: gocql.UUID(room2), UpdatedId: gocql.UUID(m2)},
		{RoomId: gocql.UUID(uuid.New()), UpdatedId: gocql.UUID(idAt(-time.Hour))},
	}, nil)
	var after gocql.UUID
	d.repo.EXPECT().FindRoomRecent(mock.Anything, gocql.UUID(memberId), gocql.UUID(roomId), mock.Anything, 0).
		Run(func(_ context.Context, _, _ gocql.UUID, a gocql.UUID, _ int) { after = a }).
		Return([]projection.Message{
			{Id: gocql.UUID(m3), FromId: gocql.UUID(otherId), ContentType: "text", Contents: []string{"3"}},
			{Id: gocql.UUID(m1), FromId: gocql.UUID(otherId), ContentType: "text", Contents: []string{"1"}},
		}, false, nil)
	d.repo.EXPECT().FindRoomRecent(mock.Anything, gocql.UUID(memberId), gocql.UUID(room2), mock.Anything, 0).
		Return([]projection.Message{{Id: gocql.UUID(m2), FromId: gocql.UUID(memberId), ContentType: "quit"}}, false, nil)

	res, err := s.GetRecentMessages(context.Background(), memberId, cursor)

	require.NoError(t, err)
	require.Len(t, res, 3)
	assert.Equal(t, []uuid.UUID{m3, m1, m2}, []uuid.UUID{res[0].Id, res[1].Id, res[2].Id})
	assert.Equal(t, roomId, res[0].RoomId)
	assert.Equal(t, room2, res[2].RoomId)
	assert.Equal(t, byte(0x70), after[6])
	assert.Less(t, string(after[:6]), string(cursor[:6]))
	earliest := idAt(-time.Minute - 11*time.Second)
	assert.Greater(t, string(after[:6]), string(earliest[:6]))
}

func TestGetRecentMessages_keptForGoodResetsARoomWithMore(t *testing.T) {
	t.Setenv("MESSAGE_TTL", "0")
	s, d := newService(t)
	newest, oldestKept, deleted := idAt(-time.Second), idAt(-2*time.Second), idAt(-3*time.Second)
	d.repo.EXPECT().FindRooms(mock.Anything, gocql.UUID(memberId)).
		Return([]projection.Room{{RoomId: gocql.UUID(roomId), UpdatedId: gocql.UUID(newest)}}, nil)
	d.repo.EXPECT().FindRoomRecent(mock.Anything, gocql.UUID(memberId), gocql.UUID(roomId), mock.Anything, 20).
		Return([]projection.Message{
			{Id: gocql.UUID(newest), ContentType: "text"},
			{Id: gocql.UUID(oldestKept), ContentType: "text"},
			{Id: gocql.UUID(deleted), ContentType: "delete"},
		}, true, nil)

	res, err := s.GetRecentMessages(context.Background(), memberId, idAt(-time.Hour))

	require.NoError(t, err)
	require.Len(t, res, 4)
	reset := res[0]
	assert.Equal(t, "reset", reset.ContentType)
	assert.Equal(t, roomId, reset.RoomId)
	assert.Empty(t, reset.Contents)
	assert.Equal(t, uuid.Nil, reset.Id)
	assert.Equal(t, []uuid.UUID{newest, oldestKept, deleted}, []uuid.UUID{res[1].Id, res[2].Id, res[3].Id})
	encoded, err := json.Marshal(res)
	require.NoError(t, err)
	var rows []map[string]any
	require.NoError(t, json.Unmarshal(encoded, &rows))
	assert.NotContains(t, rows[0], "id")
	assert.NotContains(t, rows[0], "fromId")
	assert.Contains(t, rows[1], "id")
}

func TestGetRecentMessages_failedRead(t *testing.T) {
	s, d := newService(t)
	d.repo.EXPECT().FindRooms(mock.Anything, gocql.UUID(memberId)).
		Return([]projection.Room{{RoomId: gocql.UUID(roomId), UpdatedId: gocql.UUID(idAt(0))}}, nil)
	d.repo.EXPECT().FindRoomRecent(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, false, errDB)

	_, err := s.GetRecentMessages(context.Background(), memberId, idAt(-time.Hour))

	assert.ErrorIs(t, err, errDB)
}

func TestGetRecentMessages_keptForGoodNoResetWhenNoneLeftOut(t *testing.T) {
	t.Setenv("MESSAGE_TTL", "0")
	s, d := newService(t)
	d.repo.EXPECT().FindRooms(mock.Anything, gocql.UUID(memberId)).
		Return([]projection.Room{{RoomId: gocql.UUID(roomId), UpdatedId: gocql.UUID(idAt(0))}}, nil)
	d.repo.EXPECT().FindRoomRecent(mock.Anything, gocql.UUID(memberId), gocql.UUID(roomId), mock.Anything, 20).
		Return([]projection.Message{{Id: gocql.UUID(idAt(0)), ContentType: "text"}}, false, nil)

	res, err := s.GetRecentMessages(context.Background(), memberId, idAt(-time.Hour))

	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, "text", res[0].ContentType)
}

func TestGetRoomHistory_mapsRowsWithTheRoom(t *testing.T) {
	s, d := newService(t)
	cursor, msgId := uuid.New(), uuid.New()
	d.repo.EXPECT().FindRoomHistory(mock.Anything, gocql.UUID(memberId), gocql.UUID(roomId), gocql.UUID(cursor), 30).
		Return([]projection.Message{{Id: gocql.UUID(msgId), FromId: gocql.UUID(otherId), ContentType: "text", Contents: []string{"hi"}}}, nil)

	res, err := s.GetRoomHistory(context.Background(), memberId, roomId, cursor)

	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, msgId, res[0].Id)
	assert.Equal(t, roomId, res[0].RoomId)
	assert.Equal(t, otherId, res[0].FromId)
	assert.Equal(t, []string{"hi"}, res[0].Contents)
}

func TestCheckBlockAndBlockedConversations(t *testing.T) {
	s, d := newService(t)
	d.repo.EXPECT().DidBlock(mock.Anything, gocql.UUID(memberId), gocql.UUID(otherId)).Return(true, nil)
	d.repo.EXPECT().FindBlockedConversations(mock.Anything, gocql.UUID(memberId)).Return([]gocql.UUID{gocql.UUID(roomId)}, nil)

	block, err := s.CheckBlock(context.Background(), memberId, otherId)
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"didBlock": true}, block)

	blocked, err := s.GetBlockedConversations(context.Background(), memberId)
	require.NoError(t, err)
	assert.Equal(t, roomId, blocked[0].Id)
}
