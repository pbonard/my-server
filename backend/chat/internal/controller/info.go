package controller

import (
	"backend/chat/internal/dto"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

func infoRouter(c *Controller) {
	c.Router(GET, "/chat/profile/my", c.getMyProfile)
	c.Router(PUT, "/chat/profile/name", c.setName)
	c.Router(GET, "/chat/profile", c.getProfile)
	c.Router(GET, "/chat/room/info", c.getChatRoomInfo)
	c.Router(GET, "/chat/block/check", c.checkBlock)
	c.Router(GET, "/chat/participants", c.getChatParticipants)
	c.Router(POST, "/chat/report/user", c.reportUser)
	c.Router(POST, "/chat/block/conversation", c.blockConversation)
	c.Router(GET, "/chat/block/conversations", c.GetBlockedConversations)
}

func (c *Controller) getChatRoomInfo(w http.ResponseWriter, r *http.Request) {
	roomId, err := uuid.Parse(r.URL.Query().Get("id"))
	if err != nil {
		handleError(w, errors.New("fail to parse"))
		return
	}
	result, err := c.service.GetChatRoomInfo(r.Context(), roomId)
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

func (c *Controller) getProfile(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.URL.Query().Get("id"))
	if err != nil {
		handleError(w, errors.New("fail to parse"))
		return
	}
	result, err := c.service.GetProfile(r.Context(), id)
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

func (c *Controller) getMyProfile(w http.ResponseWriter, r *http.Request) {
	memberId, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		slog.Error("fail to parse userId from X-User-Id header",
			"err", err,
		)
		handleError(w, errors.New("fail to parse"))
		return
	}
	result, err := c.service.GetProfile(r.Context(), memberId)
	if err != nil {
		handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	result.Id = memberId
	err = json.NewEncoder(w).Encode(result)
	if err != nil {
		slog.Error("fail to write response body",
			"err", err)
	}
}

func (c *Controller) setName(w http.ResponseWriter, r *http.Request) {
	memberId, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		slog.Error("fail to parse userId from X-User-Id header",
			"err", err,
		)
		handleError(w, errors.New("fail to parse"))
		return
	}
	var req dto.SetNameRequest
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		slog.Info("incorrect body",
			"err", err,
		)
		handleError(w, errors.New("fail to parse"))
		return
	}
	err = c.service.SetName(r.Context(), memberId, req.Name)
	if err != nil {
		handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Controller) checkBlock(w http.ResponseWriter, r *http.Request) {
	memberId, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		slog.Error("fail to parse userId from X-User-Id header",
			"err", err)
		handleError(w, errors.New("fail to parse"))
		return
	}
	checkingId, err := uuid.Parse(r.URL.Query().Get("id"))
	if err != nil {
		slog.Error("fail to parse checkingId from query param",
			"err", err)
		handleError(w, errors.New("fail to parse"))
		return
	}
	result, err := c.service.CheckBlock(r.Context(), memberId, checkingId)
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

func (c *Controller) getChatParticipants(w http.ResponseWriter, r *http.Request) {
	roomId, err := uuid.Parse(r.URL.Query().Get("roomId"))
	if err != nil {
		slog.Error("fail to parse roomId from query param",
			"err", err)
		handleError(w, errors.New("fail to parse"))
		return
	}
	result, err := c.service.GetChatParticipants(r.Context(), roomId)
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

func (c *Controller) reportUser(w http.ResponseWriter, r *http.Request) {
	reporterId, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		slog.Error("fail to parse userId from X-User-Id header",
			"err", err)
		handleError(w, errors.New("fail to parse"))
		return
	}
	var req dto.BlockReport
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		slog.Info("incorrect body",
			"err", err,
		)
		handleError(w, errors.New("fail to parse"))
		return
	}
	err = c.service.ReportUser(r.Context(), reporterId, req.Id)
	if err != nil {
		handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Controller) blockConversation(w http.ResponseWriter, r *http.Request) {
	memberId, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		slog.Error("fail to parse userId from X-User-Id header",
			"err", err)
		handleError(w, errors.New("fail to parse"))
		return
	}
	var req dto.BlockReport
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		slog.Info("incorrect body",
			"err", err,
		)
		handleError(w, errors.New("fail to parse"))
		return
	}
	err = c.service.BlockConversation(r.Context(), memberId, req.Id)
	if err != nil {
		handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Controller) GetBlockedConversations(w http.ResponseWriter, r *http.Request) {
	memberId, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		slog.Error("fail to parse userId from X-User-Id header",
			"err", err)
		handleError(w, errors.New("fail to parse"))
		return
	}
	result, err := c.service.GetBlockedConversations(r.Context(), memberId)
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
