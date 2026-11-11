package controller

import (
	"backend/chat/internal/service"
	"context"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

type HTTPMethod int

const (
	GET HTTPMethod = iota
	POST
	DELETE
	PUT
)

type Controller struct {
	service service.Service
	mux     *http.ServeMux
	conns   map[uuid.UUID]map[uuid.UUID]*websocket.Conn
	numbers int
	csMutex *sync.RWMutex
	podIP   string
}

func NewController(s service.Service, m *http.ServeMux) *Controller {

	c := &Controller{
		service: s,
		mux:     m,
		conns:   make(map[uuid.UUID]map[uuid.UUID]*websocket.Conn),
		csMutex: &sync.RWMutex{},
		podIP:   os.Getenv("POD_IP"),
	}

	messagingRouter(c)
	infoRouter(c)

	return c
}

func (c *Controller) Router(httpMethod HTTPMethod, path string, handler http.HandlerFunc) {
	m := c.mux

	switch httpMethod {
	case GET:
		m.HandleFunc("GET "+path, handler)
	case POST:
		m.HandleFunc("POST "+path, handler)
	case PUT:
		m.HandleFunc("PUT "+path, handler)
	case DELETE:
		m.HandleFunc("DELETE "+path, handler)

	default:
		panic("This HTTP method is not supported")
	}
}

func getStatusCode(err error) int {
	return http.StatusBadRequest
}

func handleWebsocketError(ctx context.Context, conn *websocket.Conn, err error) {
	err = conn.Write(ctx, websocket.MessageText, []byte(err.Error()))
	if err != nil {
		slog.Error("fail to write payload",
			"err", err,
		)
	}
}

func handleError(w http.ResponseWriter, err error) {
	w.WriteHeader(getStatusCode(err))
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		slog.Error("fail to write response body", "err", err)
	}
}
