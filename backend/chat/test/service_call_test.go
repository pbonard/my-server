package test

import (
	"backend/common"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGenerateTurn_credentialIsHmacOfExpiry(t *testing.T) {
	t.Setenv("TURN_SECRET", "secret")
	t.Setenv("TURN_REALM", "turn.example.com")
	before := time.Now().Add(2 * time.Hour).Unix()
	s, _ := newService(t)

	resp := s.GenerateTurn()

	// the username is the unix time the credential expires, 2 hours from now
	expires, err := strconv.ParseInt(resp.Username, 10, 64)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, expires, before)
	assert.LessOrEqual(t, expires, time.Now().Add(2*time.Hour).Unix())
	mac := hmac.New(sha1.New, []byte("secret"))
	mac.Write([]byte(resp.Username))
	assert.Equal(t, base64.StdEncoding.EncodeToString(mac.Sum(nil)), resp.Credential)
	assert.Equal(t, []string{
		"turn:turn.example.com:3478?transport=udp",
		"turn:turn.example.com:5349?transport=tcp",
	}, resp.Uris)
}

func TestGetCallParticipantsWithoutMe(t *testing.T) {
	t.Run("filters out the caller", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindCallParticipantIds(mock.Anything, "call-participants:room").
			Return([]string{memberId.String(), otherId.String()}, nil)

		ids, err := s.GetCallParticipantsWithoutMe(context.Background(), "room", memberId)

		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{otherId}, ids)
	})
	t.Run("stored id that is not a uuid", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindCallParticipantIds(mock.Anything, "call-participants:room").Return([]string{"short"}, nil)

		_, err := s.GetCallParticipantsWithoutMe(context.Background(), "room", memberId)

		assert.Error(t, err)
	})
}

func TestCallServerIPAndParticipantKeys(t *testing.T) {
	s, d := newService(t)
	d.repo.EXPECT().SetCallServerIP(mock.Anything, "call-server:"+memberId.String(), "10.0.0.1").Return(nil)
	d.repo.EXPECT().RemoveCallServerIP(mock.Anything, "call-server:"+memberId.String()).Return(nil)
	d.repo.EXPECT().AddCallParticipantId(mock.Anything, "call-participants:room", memberId).Return(nil)
	d.repo.EXPECT().RemoveCallParticipantId(mock.Anything, "call-participants:room", memberId).Return(errDB)

	assert.NoError(t, s.SetCallServerIP(context.Background(), memberId, "10.0.0.1"))
	assert.NoError(t, s.RemoveCallServerIP(context.Background(), memberId))
	assert.NoError(t, s.AddCallParticipant(context.Background(), "room", memberId))
	assert.ErrorIs(t, s.RemoveCallParticipant(context.Background(), "room", memberId), errDB)
}

// a signal for members on other chat servers goes to signalrelay on the topic of this chat's app
func TestPublishCallSignal(t *testing.T) {
	t.Setenv("PROFILE", "production")
	s, d := newService(t)
	var pushed []byte
	d.producer.EXPECT().Commit("conversation-signal", []byte(nil), mock.Anything, []sarama.RecordHeader(nil)).
		Run(func(_ string, _ []byte, value []byte, _ []sarama.RecordHeader) { pushed = value }).Return(nil)

	require.NoError(t, s.PublishCallSignal(memberId, []uuid.UUID{otherId}, json.RawMessage(`{"sdp":"offer"}`)))

	var signal common.CallSignal
	require.NoError(t, json.Unmarshal(pushed, &signal))
	assert.Equal(t, memberId, signal.FromId)
	assert.Equal(t, []uuid.UUID{otherId}, signal.ToIds)
	assert.JSONEq(t, `{"sdp":"offer"}`, string(signal.Signal))
}

func TestPublishCallSignal_toTheTopicOfThisChatsApp(t *testing.T) {
	t.Setenv("PROFILE", "holidayfriend")
	s, d := newService(t)
	d.producer.EXPECT().Commit("holidayfriend.conversation-signal", []byte(nil), mock.Anything, []sarama.RecordHeader(nil)).
		Return(nil)

	assert.NoError(t, s.PublishCallSignal(memberId, []uuid.UUID{otherId}, nil))
}
