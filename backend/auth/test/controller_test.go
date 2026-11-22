package test

import (
	"backend/auth/internal/controller"
	"backend/auth/internal/dto"
	"backend/auth/internal/service"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func serve(svc *MockService, method, target, body string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	controller.NewController(svc, mux)
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func refreshCookie(rec *httptest.ResponseRecorder) *http.Cookie {
	for _, c := range rec.Result().Cookies() {
		if c.Name == "refresh_token" {
			return c
		}
	}
	return nil
}

func TestController_errorStatusCodes(t *testing.T) {
	tests := []struct {
		err  error
		want int
	}{
		{service.ErrInternalServer, http.StatusInternalServerError},
		{service.ErrLoginWithEmail, http.StatusUnauthorized},
		{service.ErrVerifyEmailOTP, http.StatusUnauthorized},
		{service.ErrSignUpWithEmail, http.StatusBadRequest},
		{errors.New("anything else"), http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.err.Error(), func(t *testing.T) {
			svc := NewMockService(t)
			svc.EXPECT().LoginWithEmail(email, "pw").Return(nil, "", tt.err)

			rec := serve(svc, http.MethodPost, "/auth/email/login", `{"email":"alice@example.com","password":"pw"}`)

			assert.Equal(t, tt.want, rec.Code)
			assert.Equal(t, tt.err.Error(), rec.Body.String())
		})
	}
}

func TestController_loginSetsRefreshTokenCookie(t *testing.T) {
	svc := NewMockService(t)
	svc.EXPECT().LoginWithEmail(email, "pw").Return(&dto.LoginWithEmailResponse{AccessToken: "at"}, "rt", nil)

	rec := serve(svc, http.MethodPost, "/auth/email/login", `{"email":"alice@example.com","password":"pw"}`)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"accessToken":"at"}`, rec.Body.String())
	cookie := refreshCookie(rec)
	require.NotNil(t, cookie)
	assert.Equal(t, "rt", cookie.Value)
	assert.True(t, cookie.HttpOnly)
	assert.True(t, cookie.Secure)
}

func TestController_loginWithoutRefreshTokenSetsNoCookie(t *testing.T) {
	svc := NewMockService(t)
	sid := uuid.New()
	svc.EXPECT().LoginWithEmail(email, "pw").Return(&dto.LoginWithEmailResponse{SessionId: sid}, "", nil)

	rec := serve(svc, http.MethodPost, "/auth/email/login", `{"email":"alice@example.com","password":"pw"}`)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Nil(t, refreshCookie(rec))
}

func TestController_thirdPartySignInSetsCookie(t *testing.T) {
	svc := NewMockService(t)
	svc.EXPECT().SignInWithGoogle(mock.Anything, "google-token").
		Return(&dto.SignInWithThirdPartyResponse{AccessToken: "at"}, "rt", nil)
	svc.EXPECT().SignInWithApple(mock.Anything, "apple-token").
		Return(nil, "", service.ErrSignInWithApple)

	google := serve(svc, http.MethodPost, "/auth/email/google", `{"idToken":"google-token"}`)
	apple := serve(svc, http.MethodPost, "/auth/email/apple", `{"identityToken":"apple-token"}`)

	assert.Equal(t, http.StatusOK, google.Code)
	assert.Equal(t, "rt", refreshCookie(google).Value)
	assert.Equal(t, http.StatusUnauthorized, apple.Code)
}

func TestController_emailFlows(t *testing.T) {
	vid, sid := uuid.New(), uuid.New()
	svc := NewMockService(t)
	svc.EXPECT().CreateMemberByEmail(mock.Anything, email, "password123").Return(map[string]uuid.UUID{"verificationId": vid}, nil)
	svc.EXPECT().VerifyEmailOTP("123456", vid).Return(&dto.VerifyEmailOTPResponse{SessionId: sid}, nil)
	svc.EXPECT().ForgetPassword(mock.Anything, email).Return(map[string]uuid.UUID{"verificationId": vid}, nil)
	svc.EXPECT().SetNewPassword(mock.Anything, "new-password", sid).Return(nil)

	create := serve(svc, http.MethodPost, "/auth/email/create", `{"email":"alice@example.com","password":"password123"}`)
	verify := serve(svc, http.MethodPost, "/auth/email/otp/verify", fmt.Sprintf(`{"verificationId":"%s","otp":"123456"}`, vid))
	forget := serve(svc, http.MethodPost, "/auth/email/password/forget", `{"email":"alice@example.com"}`)
	setNew := serve(svc, http.MethodPatch, "/auth/email/password/set-new", fmt.Sprintf(`{"password":"new-password","sessionId":"%s"}`, sid))

	assert.JSONEq(t, fmt.Sprintf(`{"verificationId":"%s"}`, vid), create.Body.String())
	assert.JSONEq(t, fmt.Sprintf(`{"sessionId":"%s"}`, sid), verify.Body.String())
	assert.Equal(t, http.StatusOK, forget.Code)
	assert.Equal(t, http.StatusOK, setNew.Code)
}

func TestController_smsFlows(t *testing.T) {
	vid := uuid.New()
	svc := NewMockService(t)
	svc.EXPECT().SendSMSOTP(uuid.Nil, phone).Return(map[string]uuid.UUID{"verificationId": vid}, nil)
	svc.EXPECT().VerifySMSOTP(uuid.Nil, vid, "123456").Return(&dto.VerifySMSOTPResponse{AccessToken: "at"}, "rt", nil)

	send := serve(svc, http.MethodPost, "/auth/sms/otp/send", `{"phoneNumber":"+821012345678"}`)
	verify := serve(svc, http.MethodPost, "/auth/sms/otp/verify", fmt.Sprintf(`{"verificationId":"%s","otp":"123456"}`, vid))

	assert.JSONEq(t, fmt.Sprintf(`{"verificationId":"%s"}`, vid), send.Body.String())
	assert.Equal(t, http.StatusOK, verify.Code)
	assert.Equal(t, "rt", refreshCookie(verify).Value)
}

func TestController_badBodiesAreRejected(t *testing.T) {
	for _, target := range []string{"/auth/email/create", "/auth/email/login", "/auth/sms/otp/send", "/auth/sms/otp/verify"} {
		t.Run(target, func(t *testing.T) {
			rec := serve(NewMockService(t), http.MethodPost, target, `{bad`)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestController_refreshToken(t *testing.T) {
	t.Run("uses the refresh token cookie", func(t *testing.T) {
		svc := NewMockService(t)
		svc.EXPECT().GenerateAccessToken("rt").Return(map[string]string{"accessToken": "at"}, nil)

		rec := serve(svc, http.MethodPost, "/auth/refresh-token", "", &http.Cookie{Name: "refresh_token", Value: "rt"})

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.JSONEq(t, `{"accessToken":"at"}`, rec.Body.String())
	})
	t.Run("missing cookie", func(t *testing.T) {
		rec := serve(NewMockService(t), http.MethodPost, "/auth/refresh-token", "")

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
	t.Run("rejected token", func(t *testing.T) {
		svc := NewMockService(t)
		svc.EXPECT().GenerateAccessToken("rt").Return(nil, service.ErrGenerateToken)

		rec := serve(svc, http.MethodPost, "/auth/refresh-token", "", &http.Cookie{Name: "refresh_token", Value: "rt"})

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})
}

func TestController_logout(t *testing.T) {
	t.Run("clears the refresh token cookie", func(t *testing.T) {
		svc := NewMockService(t)
		svc.EXPECT().RemoveJTI("rt").Return(nil)

		rec := serve(svc, http.MethodPost, "/auth/account/logout", "", &http.Cookie{Name: "refresh_token", Value: "rt"})

		assert.Equal(t, http.StatusOK, rec.Code)
		cookie := refreshCookie(rec)
		require.NotNil(t, cookie)
		assert.Empty(t, cookie.Value)
		assert.Negative(t, cookie.MaxAge)
	})
	t.Run("a token that can't be used anyway is logged out", func(t *testing.T) {
		svc := NewMockService(t)
		svc.EXPECT().RemoveJTI("rt").Return(service.ErrFailToSignOut)

		rec := serve(svc, http.MethodPost, "/auth/account/logout", "", &http.Cookie{Name: "refresh_token", Value: "rt"})

		assert.Equal(t, http.StatusOK, rec.Code)
		cookie := refreshCookie(rec)
		require.NotNil(t, cookie)
		assert.Negative(t, cookie.MaxAge)
	})
	t.Run("no cookie fails", func(t *testing.T) {
		svc := NewMockService(t)

		rec := serve(svc, http.MethodPost, "/auth/account/logout", "")

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Nil(t, refreshCookie(rec))
	})
	t.Run("failed revoke keeps the cookie", func(t *testing.T) {
		svc := NewMockService(t)
		svc.EXPECT().RemoveJTI("rt").Return(service.ErrInternalServer)

		rec := serve(svc, http.MethodPost, "/auth/account/logout", "", &http.Cookie{Name: "refresh_token", Value: "rt"})

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Nil(t, refreshCookie(rec))
	})
}

func TestController_deleteAccount(t *testing.T) {
	svc := NewMockService(t)
	svc.EXPECT().DeleteAccount(mock.Anything, "rt").Return(nil)

	rec := serve(svc, http.MethodDelete, "/auth/account/delete", "", &http.Cookie{Name: "refresh_token", Value: "rt"})

	assert.Equal(t, http.StatusOK, rec.Code)
	cookie := refreshCookie(rec)
	require.NotNil(t, cookie)
	assert.Empty(t, cookie.Value)
	assert.Negative(t, cookie.MaxAge)
}

func TestController_deleteAccountFailureKeepsTheCookie(t *testing.T) {
	svc := NewMockService(t)
	svc.EXPECT().DeleteAccount(mock.Anything, "rt").Return(service.ErrInternalServer)

	rec := serve(svc, http.MethodDelete, "/auth/account/delete", "", &http.Cookie{Name: "refresh_token", Value: "rt"})

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Nil(t, refreshCookie(rec))
}

func enableNoPasswordLogin(t *testing.T) {
	t.Setenv("NO_PASSWORD_EMAIL_LOGIN", "true")
}

func TestController_noPasswordRoutesAreOffUnlessEnabled(t *testing.T) {
	svc := NewMockService(t)
	for _, path := range []string{"/auth/email/login/start", "/auth/email/login/otp/verify", "/auth/email/login/totp/confirm", "/auth/email/login/totp/verify", "/auth/email/login/reset/start"} {
		rec := serve(svc, http.MethodPost, path, `{}`)
		assert.Equal(t, http.StatusNotFound, rec.Code, path)
	}
}

func TestController_passwordRoutesAreOffWithNoPasswordLogin(t *testing.T) {
	enableNoPasswordLogin(t)
	svc := NewMockService(t)
	for _, path := range []string{"/auth/email/create", "/auth/email/login", "/auth/email/otp/verify", "/auth/email/password/forget"} {
		rec := serve(svc, http.MethodPost, path, `{}`)
		assert.Equal(t, http.StatusNotFound, rec.Code, path)
	}
	rec := serve(svc, http.MethodPatch, "/auth/email/password/set-new", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestController_emailOTPVerifyGivesSetupWithoutCookie(t *testing.T) {
	enableNoPasswordLogin(t)
	svc := NewMockService(t)
	vid := uuid.New()
	svc.EXPECT().VerifyNoPasswordEmailOTP(mock.Anything, vid, "123456").
		Return(&dto.TOTPSetupResponse{SetupToken: uuid.New()}, nil)

	rec := serve(svc, http.MethodPost, "/auth/email/login/otp/verify", `{"verificationId":"`+vid.String()+`","otp":"123456"}`)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Nil(t, refreshCookie(rec))
}

func TestController_totpConfirmSetsCookie(t *testing.T) {
	enableNoPasswordLogin(t)
	svc := NewMockService(t)
	token := uuid.New()
	svc.EXPECT().ConfirmTOTPSetup(mock.Anything, token, "123456").
		Return(&dto.SignInWithThirdPartyResponse{AccessToken: "at"}, "rt", nil)

	rec := serve(svc, http.MethodPost, "/auth/email/login/totp/confirm", `{"setupToken":"`+token.String()+`","code":"123456"}`)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "rt", refreshCookie(rec).Value)
}

func TestController_totpLoginSetsCookie(t *testing.T) {
	enableNoPasswordLogin(t)
	svc := NewMockService(t)
	svc.EXPECT().LoginWithTOTP(mock.Anything, email, "123456").
		Return(&dto.SignInWithThirdPartyResponse{AccessToken: "at"}, "rt", nil)

	rec := serve(svc, http.MethodPost, "/auth/email/login/totp/verify", `{"email":"alice@example.com","code":"123456"}`)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "rt", refreshCookie(rec).Value)
}

func TestController_tooManyAttemptsStatuses(t *testing.T) {
	enableNoPasswordLogin(t)
	svc := NewMockService(t)
	svc.EXPECT().StartNoPasswordEmailLogin(mock.Anything, email).Return(nil, service.ErrEmailOTPTooSoon)
	svc.EXPECT().LoginWithTOTP(mock.Anything, email, "1").Return(nil, "", service.ErrTOTPTooManyAttempts)

	send := serve(svc, http.MethodPost, "/auth/email/login/start", `{"email":"alice@example.com"}`)
	login := serve(svc, http.MethodPost, "/auth/email/login/totp/verify", `{"email":"alice@example.com","code":"1"}`)

	assert.Equal(t, http.StatusTooManyRequests, send.Code)
	assert.Equal(t, http.StatusTooManyRequests, login.Code)
}

func TestController_useThirdPartySignInStatus(t *testing.T) {
	enableNoPasswordLogin(t)
	svc := NewMockService(t)
	svc.EXPECT().StartNoPasswordEmailLogin(mock.Anything, email).Return(nil, service.ErrUseThirdPartySignIn)

	send := serve(svc, http.MethodPost, "/auth/email/login/start", `{"email":"alice@example.com"}`)

	assert.Equal(t, http.StatusConflict, send.Code)
}
