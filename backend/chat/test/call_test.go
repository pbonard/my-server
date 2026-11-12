package test

import (
	"backend/chat/internal/controller"
	"backend/chat/internal/dto"
	"backend/chat/internal/grpccontroller"
	"backend/common/proto"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// callServer serves chat behind a stand-in for the gateway, the member of each request is its Test-Member header
func callServer(t *testing.T, svc *MockService) *httptest.Server {
	mux := http.NewServeMux()
	controller.NewController(svc, mux)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-User-Id", r.Header.Get("Test-Member"))
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func joinCall(t *testing.T, ctx context.Context, srv *httptest.Server, member uuid.UUID) *websocket.Conn {
	header := http.Header{}
	header.Set("Test-Member", member.String())
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/chat/call?id=room",
		&websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })
	return conn
}

func readCallSignal(t *testing.T, ctx context.Context, conn *websocket.Conn) dto.CallSignalResponse {
	_, data, err := conn.Read(ctx)
	require.NoError(t, err)
	var res dto.CallSignalResponse
	require.NoError(t, json.Unmarshal(data, &res))
	return res
}

// a member joins the call: the service is asked who is there, and the member is added, which closes the channel
func expectJoin(svc *MockService, member uuid.UUID, already []uuid.UUID) <-chan struct{} {
	added := make(chan struct{})
	svc.EXPECT().SetCallServerIP(mock.Anything, member, mock.Anything).Return(nil)
	svc.EXPECT().GetCallParticipantsWithoutMe(mock.Anything, "room", member).Return(already, nil)
	svc.EXPECT().AddCallParticipant(mock.Anything, "room", member).
		Run(func(context.Context, string, uuid.UUID) { close(added) }).Return(nil)
	svc.EXPECT().RemoveCallServerIP(mock.Anything, member).Return(nil).Maybe()
	svc.EXPECT().RemoveCallParticipant(mock.Anything, "room", member).Return(nil).Maybe()
	return added
}

func TestJoinCall_membersOnThisServerSignalEachOtherDirectly(t *testing.T) {
	svc := NewMockService(t)
	firstAdded := expectJoin(svc, memberId, nil)
	expectJoin(svc, otherId, []uuid.UUID{memberId})
	srv := callServer(t, svc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first := joinCall(t, ctx, srv, memberId)
	// the first is in the call before the second joins, so the second's join is written to it
	<-firstAdded
	second := joinCall(t, ctx, srv, otherId)

	// the one joining learns who is there, the one there learns who joined
	assert.Equal(t, []uuid.UUID{memberId}, readCallSignal(t, ctx, second).FromIds)
	assert.Equal(t, []uuid.UUID{otherId}, readCallSignal(t, ctx, first).FromIds)

	require.NoError(t, second.Write(ctx, websocket.MessageText,
		[]byte(`{"toIds":["`+memberId.String()+`"],"signal":{"sdp":"offer"}}`)))
	got := readCallSignal(t, ctx, first)
	assert.Equal(t, []uuid.UUID{otherId}, got.FromIds)
	assert.JSONEq(t, `{"sdp":"offer"}`, string(got.Signal))
}

func TestJoinCall_signalForAMemberElsewhereIsPublished(t *testing.T) {
	svc := NewMockService(t)
	expectJoin(svc, memberId, nil)
	elsewhere := uuid.New()
	published := make(chan json.RawMessage, 1)
	svc.EXPECT().PublishCallSignal(memberId, []uuid.UUID{elsewhere}, mock.Anything).
		Run(func(_ uuid.UUID, _ []uuid.UUID, signal json.RawMessage) { published <- signal }).Return(nil)
	srv := callServer(t, svc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn := joinCall(t, ctx, srv, memberId)

	require.NoError(t, conn.Write(ctx, websocket.MessageText,
		[]byte(`{"toIds":["`+elsewhere.String()+`"],"signal":{"candidate":"c"}}`)))

	select {
	case signal := <-published:
		assert.JSONEq(t, `{"candidate":"c"}`, string(signal))
	case <-ctx.Done():
		t.Fatal("the signal was not published")
	}
}

func TestController_turnIsJSON(t *testing.T) {
	svc := NewMockService(t)
	svc.EXPECT().GenerateTurn().Return(&dto.GetTurnResponse{Username: "123", Credential: "abc"})

	rec := serve(svc, http.MethodGet, "/chat/turn", memberId.String(), "")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"credential":"abc"`)
}

func TestGRPCController_relaySignalWithoutConnectedMembers(t *testing.T) {
	gc := grpccontroller.NewGRPCController(controller.NewController(NewMockService(t), http.NewServeMux()))

	res, err := gc.RelaySignal(context.Background(), &proto.RelaySignalRequest{
		FromId: memberId[:], ToIds: [][]byte{otherId[:]}, Signal: []byte(`{}`),
	})

	assert.NoError(t, err)
	// a message, grpc can't send a nil one back to signalrelay
	assert.NotNil(t, res)
}
