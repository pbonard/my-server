package controller

import (
	"backend/auth/internal/service"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

func tokenRouter(n *Controller) {
	n.Router(POST, "/auth/refresh-token", n.refreshToken)
	n.Router(POST, "/auth/account/logout", n.logout)
	n.Router(DELETE, "/auth/account/delete", n.deleteAccount)
}

func (c *Controller) refreshToken(w http.ResponseWriter, r *http.Request) {
	rt, err := r.Cookie("refresh_token")
	if err != nil {
		handleError(w, err)
		return
	}
	result, err := c.service.GenerateAccessToken(rt.Value)
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

func (c *Controller) logout(w http.ResponseWriter, r *http.Request) {
	rt, err := r.Cookie("refresh_token")
	if err != nil {
		handleError(w, err)
		return
	}
	err = c.service.RemoveJTI(rt.Value)
	if err != nil && !errors.Is(err, service.ErrFailToSignOut) {
		handleError(w, err)
		return
	}
	clearRefreshTokenCookie(w)
	w.WriteHeader(http.StatusOK)
}

func (c *Controller) deleteAccount(w http.ResponseWriter, r *http.Request) {
	rt, err := r.Cookie("refresh_token")
	if err != nil {
		handleError(w, err)
		return
	}
	err = c.service.DeleteAccount(r.Context(), rt.Value)
	if err != nil {
		handleError(w, err)
		return
	}
	clearRefreshTokenCookie(w)
	w.WriteHeader(http.StatusOK)
}

func clearRefreshTokenCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{Name: "refresh_token",
		Value:    "",
		Expires:  time.Now(),
		Path:     "/",
		Domain:   "",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
	})
}
