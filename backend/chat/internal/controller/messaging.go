package controller

import (
	"backend/chat/internal/dto"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

func messagingRouter(c *Controller) {
	c.Router(GET, "/chat/messaging/connect", c.connectMessaging)
	c.Router(GET, "/chat/messaging/recent", c.getRecentMessages)
	c.Router(POST, "/chat/messaging/send", c.sendMessaging)
	c.Router(POST, "/chat/messaging/presigned", c.generatePresignedURL)
	c.Router(GET, "/chat/messaging/signed", c.getSignedURL)
}

func (c *Controller) sendMessaging(w http.ResponseWriter, r *http.Request) {
	memberIdRaw := r.Header.Get("X-User-Id")
	memberId, err := uuid.Parse(memberIdRaw)
	if err != nil {
		handleError(w, errors.New("fail to parse"))
		return
	}
	var req dto.SendMessagingRequest
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		handleError(w, errors.New("fail to parse"))
		return
	}
	result, err := c.service.PublishMessaging(r.Context(), memberId, req.ToIdType, req.ToId, req.ContentType, req.Contents, req.NoPush)
	if err != nil {
		handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(result)
	if err != nil {
		slog.Error("fail to write response body",
			"err", err)
	}
}

func (c *Controller) getRecentMessages(w http.ResponseWriter, r *http.Request) {
	slog.Info("getRecentMessages")
	memberId, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		handleError(w, errors.New("fail to parse"))
		return
	}
	cursor, err := uuid.Parse(r.URL.Query().Get("cursor"))
	if err != nil {
		handleError(w, errors.New("fail to parse"))
		return
	}
	result, err := c.service.GetRecentMessages(r.Context(), memberId, cursor)
	if err != nil {
		handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(result)
	if err != nil {
		slog.Error("fail to write response body",
			"err", err)
	}
}

func (c *Controller) generatePresignedURL(w http.ResponseWriter, r *http.Request) {
	slog.Info("get presigned url request incoming")
	memberId, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		handleError(w, errors.New("fail to parse"))
		return
	}
	var req dto.GeneratePresignedURLRequest
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		handleError(w, errors.New("fail to parse"))
		return
	}
	if req.ContentType != "audio" && req.ContentType != "image" && req.ContentType != "video" {
		handleError(w, errors.New("bad request"))
		return
	}
	result, err := c.service.GeneratePresignedURL(r.Context(), memberId, req.ContentType, req.N)
	if err != nil {
		handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(result)
	if err != nil {
		slog.Error("fail to write response body",
			"err", err)
	}
}

func (c *Controller) getSignedURL(w http.ResponseWriter, r *http.Request) {
	slog.Info("get signed url request incoming")
	memberId, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		handleError(w, errors.New("fail to parse"))
		return
	}
	contentType := r.URL.Query().Get("contentType")
	if contentType != "audio" && contentType != "image" && contentType != "video" {
		handleError(w, errors.New("bad request"))
		return
	}
	filename, err := uuid.Parse(r.URL.Query().Get("filename"))
	if err != nil {
		handleError(w, errors.New("bad request"))
		return
	}
	result, err := c.service.GenerateSignedURL(r.Context(), memberId, contentType, filename)
	if err != nil {
		handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(result)
	if err != nil {
		slog.Error("fail to write response body",
			"err", err)
	}
}
