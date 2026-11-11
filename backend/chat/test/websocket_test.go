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
