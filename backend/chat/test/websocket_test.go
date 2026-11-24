package test

import (
	"backend/chat/internal/controller"
	"context"
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

// gatewayServer serves chat behind a stand-in for the gateway, which checks the token and sets X-User-Id
func gatewayServer(t *testing.T, svc *MockService) *httptest.Server {
	mux := http.NewServeMux()
	controller.NewController(svc, mux)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-User-Id", memberId.String())
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func connectedService(t *testing.T) *MockService {
	svc := NewMockService(t)
	svc.EXPECT().SetServerIP(mock.Anything, memberId, mock.Anything).Return(nil)
	svc.EXPECT().RemoveServerIP(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	return svc
}

func TestConnectMessaging_browser(t *testing.T) {
	srv := gatewayServer(t, connectedService(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/chat/messaging/connect?deviceId=" + uuid.NewString()

	// new WebSocket(url, ["bearer", token]) of a browser: the token can't go in a header
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"bearer", "the.access.token"}})

	require.NoError(t, err)
	defer conn.Close(websocket.StatusNormalClosure, "")
	// "bearer" alone comes back, never the token, and the browser keeps the connection
	assert.Equal(t, "bearer", conn.Subprotocol())
}

func TestConnectMessaging_mobileAppIsUnchanged(t *testing.T) {
	srv := gatewayServer(t, connectedService(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	header := http.Header{}
	header.Set("Device-Id", uuid.NewString())

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/chat/messaging/connect",
		&websocket.DialOptions{HTTPHeader: header})

	require.NoError(t, err)
	defer conn.Close(websocket.StatusNormalClosure, "")
	assert.Equal(t, "", conn.Subprotocol())
}

func TestConnectMessaging_withoutADeviceIdIsRefused(t *testing.T) {
	srv := gatewayServer(t, NewMockService(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/chat/messaging/connect",
		&websocket.DialOptions{Subprotocols: []string{"bearer", "the.access.token"}})

	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// a device's socket that went and came back: its new one (B) takes the old one's (A) place before A's end is noticed,
// as when a phone changes networks. A ending afterwards leaves B in place, so messages still reach the device live
func TestConnectMessaging_oldSocketEndingAfterReconnectKeepsTheNewOne(t *testing.T) {
	mux := http.NewServeMux()
	c := controller.NewController(connectedService(t), mux)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-User-Id", memberId.String())
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/chat/messaging/connect?deviceId=" + uuid.NewString()
	dial := func() *websocket.Conn {
		conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"bearer", "the.access.token"}})
		require.NoError(t, err)
		return conn
	}
	roomId, fromId := uuid.New(), uuid.New()
	relay := func() [][]byte {
		pushToIds, err := c.RelayMessaging(ctx, uuid.New(), [][]byte{memberId[:]}, roomId, fromId, "text", []string{"hi"})
		require.NoError(t, err)
		return pushToIds
	}

	a := dial()
	b := dial()
	defer b.Close(websocket.StatusNormalClosure, "")
	// B is registered once a message reaches it
	require.Empty(t, relay())
	_, _, err := b.Read(ctx)
	require.NoError(t, err)

	require.NoError(t, a.Close(websocket.StatusNormalClosure, ""))
	// A's handler cleans up after its close handshake
	time.Sleep(200 * time.Millisecond)

	assert.Empty(t, relay(), "the message went to push: the device has no socket registered")
	readCtx, readCancel := context.WithTimeout(ctx, time.Second)
	defer readCancel()
	_, data, err := b.Read(readCtx)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"hi"`)
}

// a client that stops reading, as its buffers are full: others still get their messages and can connect meanwhile,
// and the relay ends before messagerelay gives up on every member of the call
func TestRelayMessaging_clientThatStopsReadingFailsAlone(t *testing.T) {
	svc := NewMockService(t)
	svc.EXPECT().SetServerIP(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	svc.EXPECT().RemoveServerIP(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mux := http.NewServeMux()
	c := controller.NewController(svc, mux)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-User-Id", r.URL.Query().Get("member"))
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stuckId, healthyId, lateId := uuid.New(), uuid.New(), uuid.New()
	roomId, fromId := uuid.New(), uuid.New()
	relay := func(content string, toIds ...uuid.UUID) [][]byte {
		var raw [][]byte
		for _, id := range toIds {
			raw = append(raw, id[:])
		}
		pushToIds, err := c.RelayMessaging(ctx, uuid.New(), raw, roomId, fromId, "text", []string{content})
		assert.NoError(t, err)
		return pushToIds
	}
	// returns once a message reaches the new connection
	connect := func(id uuid.UUID) *websocket.Conn {
		url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/chat/messaging/connect?member=" + id.String() + "&deviceId=" + uuid.NewString()
		conn, _, err := websocket.Dial(ctx, url, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.CloseNow() })
		conn.SetReadLimit(-1)
		require.Eventually(t, func() bool { return len(relay("hi", id)) == 0 }, time.Second, 10*time.Millisecond)
		return conn
	}
	connect(stuckId)
	healthy := connect(healthyId)
	go func() {
		for {
			if _, _, err := healthy.Read(ctx); err != nil {
				return
			}
		}
	}()

	start := time.Now()
	relayed := make(chan [][]byte, 1)
	go func() { relayed <- relay(strings.Repeat("a", 32<<20), stuckId, healthyId) }()
	time.Sleep(500 * time.Millisecond)
	connect(lateId)
	require.Empty(t, relayed, "the stuck client wasn't stuck")
	assert.Less(t, time.Since(start), 1500*time.Millisecond, "connecting waited for the stuck client")

	assert.Equal(t, [][]byte{stuckId[:]}, <-relayed)
	assert.Less(t, time.Since(start), 4*time.Second)
}
