package controller

import (
	"backend/auth/internal/service"
	"log/slog"
	"net/http"
)

type HTTPMethod int

const (
	GET HTTPMethod = iota
	POST
	DELETE
	PUT
	PATCH
)

type Controller struct {
	service service.Service
	mux     *http.ServeMux
}

func NewController(s service.Service, m *http.ServeMux) *Controller {
	c := &Controller{
		service: s,
		mux:     m,
	}
	emailRouter(c)
	smsRouter(c)
	tokenRouter(c)

	return c
}

func handleError(w http.ResponseWriter, err error) {
	w.WriteHeader(getStatusCode(err))
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		slog.Error("fail to write response body", "err", err)
	}
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
	case PATCH:
		m.HandleFunc("PATCH "+path, handler)
	case DELETE:
		m.HandleFunc("DELETE "+path, handler)

	default:
		panic("This HTTP method is not supported")
	}
}

func getStatusCode(err error) int {
	switch err {
	case service.ErrInternalServer:
		return http.StatusInternalServerError

	case service.ErrSignInWithApple:
		return http.StatusUnauthorized
	case service.ErrCheckEmail:
		return http.StatusBadRequest
	case service.ErrSignUpWithEmail:
		return http.StatusBadRequest
	case service.ErrLoginWithEmail:
		return http.StatusUnauthorized
	case service.ErrSendEmailOTP:
		return http.StatusBadRequest
	case service.ErrVerifyEmailOTP:
		return http.StatusUnauthorized
	case service.ErrEmailOTPTooSoon:
		return http.StatusTooManyRequests
	case service.ErrUseThirdPartySignIn:
		return http.StatusConflict
	case service.ErrVerifyTOTP:
		return http.StatusUnauthorized
	case service.ErrTOTPTooManyAttempts:
		return http.StatusTooManyRequests
	case service.ErrEmailSignedUpAgain:
		return http.StatusConflict
	case service.ErrSessionExpired:
		return http.StatusUnauthorized
	case service.ErrSendSMSOTP:
		return http.StatusBadRequest
	case service.ErrPhoneNumberAlreadyLinked:
		return http.StatusBadRequest
	case service.ErrVerifySMSOTP:
		return http.StatusUnauthorized
	case service.ErrGenerateToken:
		return http.StatusUnauthorized
	case service.ErrEmailBanned:
		return http.StatusForbidden
	}
	return http.StatusBadRequest
}
