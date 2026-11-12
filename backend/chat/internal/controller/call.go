package controller

import (
	"backend/chat/internal/dto"
	"backend/common"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

// the calls of the mobile app: WebRTC signaling between the members of a call, the media goes peer to peer or
// through the turn app. moved here from howwasread's onlineconversation, with the same websocket protocol
func callRouter(c *Controller) {
	c.Router(GET, "/chat/call", c.joinCall)
	c.Router(GET, "/chat/turn", c.getTurn)
}

func (c *Controller) getTurn(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err := w.Write(common.Marshal(c.service.GenerateTurn()))
	if err != nil {
		slog.Error("fail to write turn response", "err", err)
	}
}

// joinCall is a member's websocket in the call ?id=, see common.CallParticipantsKey for what the id is. on joining it
// writes who is already there and tells them who joined, then it relays each signal the member sends to the members
// it names: directly to the ones on this server, through signalrelay to the ones on another
func (c *Controller) joinCall(w http.ResponseWriter, r *http.Request) {
	memberId, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		slog.Error("fail to parse member id from header", "err", err)
		handleError(w, errors.New("fail to parse"))
		return
	}
	callId := r.URL.Query().Get("id")
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
		// chosen only when the client offers it, the mobile app sends a header and offers none
		Subprotocols: []string{bearerSubprotocol},
	})
	if err != nil {
		slog.Error("fail to accept call connection", "err", err)
		return
	}
	c.callMutex.Lock()
	c.callConns[memberId] = conn
	c.callMutex.Unlock()

	defer func() {
		destroy := context.Background()
		conn.Close(websocket.StatusNormalClosure, "")
		c.callMutex.Lock()
		// a newer call websocket of the member replaced this one, it stays
		if c.callConns[memberId] == conn {
			delete(c.callConns, memberId)
		}
		c.callMutex.Unlock()
		c.service.RemoveCallServerIP(destroy, memberId)
		c.service.RemoveCallParticipant(destroy, callId, memberId)
	}()

	init := context.Background()
	err = c.service.SetCallServerIP(init, memberId, c.podIP)
	if err != nil {
		handleWebsocketError(init, conn, err)
		return
	}
	pids, err := c.service.GetCallParticipantsWithoutMe(init, callId, memberId)
	if err != nil {
		handleWebsocketError(init, conn, err)
		return
	}
	if len(pids) > 0 {
		err = conn.Write(init, websocket.MessageText, common.Marshal(dto.CallSignalResponse{FromIds: pids}))
		if err != nil {
			slog.Error("fail to write call participants", "err", err)
			return
		}
	}
	err = c.service.AddCallParticipant(init, callId, memberId)
	if err != nil {
		handleWebsocketError(init, conn, err)
		return
	}
	// the ones already there learn who joined, without a signal
	err = c.sendCallSignal(init, memberId, pids, nil)
	if err != nil {
		slog.Error("fail to publish call signal", "err", err)
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
					slog.Error("ping failed, call client unresponsive", "err", err1)
					conn.Close(websocket.StatusInternalError, "ping timeout")
					return
				}
			}
		}
	}()

	for {
		msgType, data, err1 := conn.Read(ctx)
		if err1 != nil {
			if websocket.CloseStatus(err1) != -1 || errors.Is(err1, context.Canceled) {
				slog.Info("call connection closed", "err", err1)
				return
			}
			slog.Error("call read error", "err", err1)
			handleWebsocketError(ctx, conn, errors.New("read error"))
			return
		}
		if msgType != websocket.MessageText {
			slog.Error("incorrect call payload type", "msgType", msgType)
			handleWebsocketError(ctx, conn, errors.New("incorrect payload types"))
			return
		}
		var req dto.CallSignalRequest
		err1 = json.Unmarshal(data, &req)
		if err1 != nil {
			slog.Error("fail to unmarshal call signal", "err", err1)
			handleWebsocketError(ctx, conn, errors.New("incorrect data"))
			return
		}
		err1 = c.sendCallSignal(ctx, memberId, req.ToIds, req.Signal)
		if err1 != nil {
			slog.Error("fail to publish call signal", "err", err1)
			handleWebsocketError(ctx, conn, errors.New("fail to publish"))
			return
		}
	}
}

// sendCallSignal writes the signal from fromId to the members of toIds on this server, and publishes it for the rest
func (c *Controller) sendCallSignal(ctx context.Context, fromId uuid.UUID, toIds []uuid.UUID, signal json.RawMessage) error {
	elsewhere := c.writeCallSignal(ctx, fromId, toIds, signal)
	if len(elsewhere) == 0 {
		return nil
	}
	return c.service.PublishCallSignal(fromId, elsewhere, signal)
}

// RelaySignal writes a signal signalrelay brought from another chat server to the members of toIds on this one
func (c *Controller) RelaySignal(ctx context.Context, toIds []uuid.UUID, fromId uuid.UUID, signal []byte) {
	c.writeCallSignal(ctx, fromId, toIds, signal)
}

// writeCallSignal writes the signal to the call websockets of toIds on this server, and returns the members it
// couldn't write to: on another server, or whose websocket failed, which is closed
func (c *Controller) writeCallSignal(ctx context.Context, fromId uuid.UUID, toIds []uuid.UUID, signal json.RawMessage) []uuid.UUID {
	p := common.Marshal(dto.CallSignalResponse{FromIds: []uuid.UUID{fromId}, Signal: signal})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var elsewhere []uuid.UUID
	for _, toId := range toIds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.callMutex.RLock()
			to, ok := c.callConns[toId]
			c.callMutex.RUnlock()
			if ok {
				err := to.Write(ctx, websocket.MessageText, p)
				if err == nil {
					return
				}
				slog.Error("fail to write call signal", "err", err)
				if err1 := to.CloseNow(); err1 != nil {
					slog.Error("fail to close zombie call connection", "err", err1)
				}
			}
			mu.Lock()
			elsewhere = append(elsewhere, toId)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return elsewhere
}
