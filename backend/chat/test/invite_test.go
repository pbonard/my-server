package test

import (
	"backend/chat/internal/controller"
	"backend/chat/internal/dto"
	"backend/chat/internal/grpccontroller"
	"backend/chat/internal/projection"
	"backend/common/proto"
	"context"
	"net/http"
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// the sender is memberId, the invitee otherId: the sender's inbox keeps their personal room under the invitee's id.
// the invitee's inbox isn't read, so the sender can't learn whether the invitee left
func TestCheckInvite(t *testing.T) {
	for name, tt := range map[string]struct {
		newest []projection.Message
		has    bool
	}{
		"a message newer than any quit": {[]projection.Message{{ContentType: "text"}}, true},
		"none, or a quit newest":        {[]projection.Message{}, false},
	} {
		t.Run(name, func(t *testing.T) {
			s, d := newService(t)
			d.repo.EXPECT().DidBlock(mock.Anything, gocql.UUID(otherId), gocql.UUID(memberId)).Return(true, nil)
			d.repo.EXPECT().FindRoomHistory(mock.Anything, gocql.UUID(memberId), gocql.UUID(otherId), gocql.UUID{}, 1).
				Return(tt.newest, nil).Once()

			res, err := s.CheckInvite(context.Background(), memberId, otherId)

			require.NoError(t, err)
			assert.Equal(t, &dto.CheckInviteResponse{Blocked: true, SenderHasChat: tt.has}, res)
		})
	}
}

func TestCheckInvite_failedLookup(t *testing.T) {
	s, d := newService(t)
	d.repo.EXPECT().DidBlock(mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
	d.repo.EXPECT().FindRoomHistory(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errDB)

	_, err := s.CheckInvite(context.Background(), memberId, otherId)

	assert.ErrorIs(t, err, errDB)
}

func TestGRPCController_checkInvite(t *testing.T) {
	svc := NewMockService(t)
	gc := grpccontroller.NewGRPCController(controller.NewController(svc, http.NewServeMux()), svc)

	t.Run("the facts of the pair", func(t *testing.T) {
		svc.EXPECT().CheckInvite(mock.Anything, memberId, otherId).
			Return(&dto.CheckInviteResponse{SenderHasChat: true}, nil).Once()

		res, err := gc.CheckInvite(context.Background(), &proto.CheckInviteRequest{FromId: memberId[:], ToId: otherId[:]})

		require.NoError(t, err)
		assert.False(t, res.Blocked)
		assert.True(t, res.SenderHasChat)
	})
	t.Run("an id that is not a uuid", func(t *testing.T) {
		_, err := gc.CheckInvite(context.Background(), &proto.CheckInviteRequest{FromId: []byte("short"), ToId: otherId[:]})

		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})
	t.Run("a failed lookup", func(t *testing.T) {
		svc.EXPECT().CheckInvite(mock.Anything, memberId, otherId).Return(nil, errDB).Once()

		_, err := gc.CheckInvite(context.Background(), &proto.CheckInviteRequest{FromId: memberId[:], ToId: otherId[:]})

		assert.Equal(t, codes.Internal, status.Code(err))
	})
}
