package controller

import (
	"backend/chat/internal/dto"
	"backend/common"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

// bearerSubprotocol is how a browser, which can't set headers on a WebSocket, sends its access token:
// new WebSocket(url, ["bearer", token]) sends "Sec-WebSocket-Protocol: bearer, <token>", the gateway checks the token
// from there like an Authorization header, and chat answers with "bearer" alone, never the token, as the browser
// requires one of the offered subprotocols back
const bearerSubprotocol = "bearer"

func (c *Controller) connectMessaging(w http.ResponseWriter, r *http.Request) {
	memberId, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		slog.Error("fail to parse member id from header",
			"err", err)
		handleError(w, errors.New("fail to parse"))
		return
	}
	// the Device-Id header, or the deviceId query parameter of a browser, which can't set the header on a WebSocket.
	// it only tells a member's connections apart, it proves nothing, so the url can carry it
	rawDeviceId := r.Header.Get("Device-Id")
	if rawDeviceId == "" {
		rawDeviceId = r.URL.Query().Get("deviceId")
	}
	deviceId, err := uuid.Parse(rawDeviceId)
	if err != nil {
		slog.Error("fail to parse device id from header",
			"err", err)
		handleError(w, errors.New("fail to parse"))
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		//OriginPatterns:     []string{"example.com"},
		InsecureSkipVerify: true,
		// chosen only when the client offers it, the mobile app sends a header and offers none
		Subprotocols: []string{bearerSubprotocol},
	})
	if err != nil {
		slog.Error("fail to accept connection", "err", err)
		handleError(w, errors.New("fail to accept ws connection"))
		return
	}
	c.csMutex.Lock()
	if c.conns[memberId] == nil {
		c.conns[memberId] = make(map[uuid.UUID]*websocket.Conn)
	}
	c.conns[memberId][deviceId] = conn
	c.numbers = c.numbers + 1
	slog.Info("success to make connection",
		"number of current connection", c.numbers)
	c.csMutex.Unlock()

	ip := c.podIP
	defer func() {
		destroy := context.Background()
		err = conn.Close(websocket.StatusNormalClosure, "")
		if err != nil && !errors.Is(net.ErrClosed, err) {
			slog.Error("fail to close conn", "err", err)
		}
		c.csMutex.Lock()
		delete(c.conns[memberId], deviceId)
		c.numbers = c.numbers - 1
		c.csMutex.Unlock()
		c.csMutex.RLock()
		slog.Info("success to close connection",
			"number of current connection", c.numbers)
		if len(c.conns[memberId]) == 0 {
			c.service.RemoveServerIP(destroy, memberId, ip)
		}
		c.csMutex.RUnlock()
	}()

	init := context.Background()
	err = c.service.SetServerIP(init, memberId, ip)
	if err != nil {
		handleWebsocketError(init, conn, err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
				err1 := conn.Ping(pingCtx)
				pingCancel()
				if err1 != nil {
					slog.Error("ping failed, client unresponsive", "err", err1)
					conn.Close(websocket.StatusInternalError, "ping timeout")
					return
				}
			}
		}
	}()
	msgType, data, err1 := conn.Read(ctx)
	if err1 != nil && err1 != io.EOF {
		if websocket.CloseStatus(err1) != -1 || errors.Is(err1, context.Canceled) {
			slog.Info("Connection closed smoothly", "err", err1)
			return
		}
		slog.Error("read error", "err", err1)
		handleWebsocketError(ctx, conn, errors.New("read error"))
		return
	}
	if msgType != websocket.MessageText {
		slog.Error("incorrect payload types",
			"msgType", msgType,
			"data", data)
		return
	}
}

func (c *Controller) RelayMessaging(
	ctx context.Context,
	id uuid.UUID,
	toIds [][]byte,
	roomId, fromId uuid.UUID,
	contentType string, contents []string,
) ([][]byte, error) {
	var wg sync.WaitGroup
	var pushToIds [][]byte
	var mu sync.Mutex
	res := dto.MessagingResponse{
		Id:          id,
		RoomId:      roomId,
		FromId:      fromId,
		ContentType: contentType,
		Contents:    contents,
	}
	resRaw := common.Marshal(res)
	for _, toId := range toIds {
		r := resRaw
		if bytes.Equal(toId, roomId[:]) {
			res1 := res
			res1.RoomId = fromId
			resRaw1 := common.Marshal(res1)
			r = resRaw1
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.csMutex.RLock()
			ct, ok := c.conns[uuid.UUID(toId)]
			var sm sync.Mutex
			var success bool
			if ok {
				var wg1 sync.WaitGroup
				for _, cd := range ct {
					wg1.Add(1)
					go func() {
						defer wg1.Done()
						err1 := cd.Write(ctx, websocket.MessageText, r)
						if err1 != nil {
							slog.Error("fail to write payload",
								"err", err1,
							)
							err2 := cd.CloseNow()
							if err2 != nil {
								slog.Error("fail to close zombie connection", "err", err2)
							}
							return
						}
						sm.Lock()
						success = true
						sm.Unlock()
					}()
				}
				wg1.Wait()
			}
			c.csMutex.RUnlock()
			if !ok || !success {
				mu.Lock()
				pushToIds = append(pushToIds, toId[:])
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	return pushToIds, nil
}
