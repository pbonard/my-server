package service

import (
	"backend/auth/internal/constant"
	"backend/auth/internal/dto"
	"backend/auth/internal/repository"
	"backend/auth/internal/totp"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/url"
	"strings"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
	"rsc.io/qr"
)

// StartNoPasswordEmailLogin starts a login: a member with an authenticator uses it, anyone else gets a code by email
func (s *service) StartNoPasswordEmailLogin(ctx context.Context, email string) (*dto.StartNoPasswordEmailLoginResponse, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if !isValidEmail(email) {
		return nil, ErrSendEmailOTP
	}
	_, _, err := s.repository.FindTOTP(ctx, email)
	if err == nil {
		return &dto.StartNoPasswordEmailLoginResponse{IsSignup: false}, nil
	}
	if !errors.Is(err, gocql.ErrNotFound) {
		return nil, ErrInternalServer
	}
	_, _, _, _, _, err = s.repository.FindLoginInfoByEmail(email)
	if err == nil {
		return nil, ErrUseThirdPartySignIn
	}
	if !errors.Is(err, gocql.ErrNotFound) {
		return nil, ErrInternalServer
	}
	// before the code is mailed, so a banned email doesn't get one
	if err = s.checkEmailNotBanned(ctx, email); err != nil {
		return nil, err
	}
	vid, err := s.sendNoPasswordEmailOTP(ctx, email, constant.OTPPurposeSignup)
	if err != nil {
		return nil, err
	}
	return &dto.StartNoPasswordEmailLoginResponse{IsSignup: true, VerificationId: uuid.UUID(vid)}, nil
}

// StartAuthenticatorReset mails a code to a member who lost their authenticator, the old one keeps working until a
// new one is confirmed
func (s *service) StartAuthenticatorReset(ctx context.Context, email string) (*dto.StartAuthenticatorResetResponse, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	_, _, err := s.repository.FindTOTP(ctx, email)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, ErrSendEmailOTP
	}
	if err != nil {
		return nil, ErrInternalServer
	}
	vid, err := s.sendNoPasswordEmailOTP(ctx, email, constant.OTPPurposeReset)
	if err != nil {
		return nil, err
	}
	return &dto.StartAuthenticatorResetResponse{VerificationId: uuid.UUID(vid)}, nil
}

func (s *service) sendNoPasswordEmailOTP(ctx context.Context, email, purpose string) (gocql.UUID, error) {
	err := s.repository.ClaimNoPasswordEmailOTPCooldown(ctx, email)
	if errors.Is(err, repository.ErrAlreadyExists) {
		return gocql.UUID{}, ErrEmailOTPTooSoon
	}
	if err != nil {
		return gocql.UUID{}, ErrInternalServer
	}
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		slog.Error("fail to create email otp", "err", err)
		return gocql.UUID{}, ErrInternalServer
	}
	otp := fmt.Sprintf("%06d", n.Int64())
	vid, err := gocql.RandomUUID()
	if err != nil {
		slog.Error("fail to make random uuid for verification id", "err", err)
		return gocql.UUID{}, ErrInternalServer
	}
	err = s.repository.SaveNoPasswordEmailOTP(ctx, vid, email, otp, purpose)
	if err != nil {
		return gocql.UUID{}, ErrInternalServer
	}
	go func() {
		err := s.smtpClient.SendOTP(email, otp)
		if err != nil {
			slog.Error("fail to send email otp", "err", err)
		}
	}()
	return vid, nil
}

// VerifyNoPasswordEmailOTP proves the email and hands out the setup of an authenticator, it never signs in: only
// confirming the setup does. What the setup is for comes from what the code was mailed for
func (s *service) VerifyNoPasswordEmailOTP(ctx context.Context, verificationId uuid.UUID, otp string) (*dto.TOTPSetupResponse, error) {
	vid := gocql.UUID(verificationId)
	email, dbOTP, purpose, attempts, err := s.repository.FindNoPasswordEmailOTP(ctx, vid)
	if err != nil {
		return nil, ErrVerifyEmailOTP
	}
	if dbOTP == "" || attempts >= constant.NoPasswordEmailOTPMaxAttempts {
		return nil, ErrVerifyEmailOTP
	}
	// counted before comparing, so every guess is paid for even when requests arrive together
	err = s.repository.AddNoPasswordEmailOTPAttempt(ctx, vid, attempts)
	if err != nil {
		return nil, ErrVerifyEmailOTP
	}
	if subtle.ConstantTimeCompare([]byte(otp), []byte(dbOTP)) != 1 {
		return nil, ErrVerifyEmailOTP
	}
	err = s.repository.DeleteNoPasswordEmailOTP(ctx, vid)
	if err != nil {
		return nil, ErrInternalServer
	}

	// 20 bytes is the size RFC 4226 recommends, base32 is the form authenticator apps read
	b := make([]byte, 20)
	_, err = rand.Read(b)
	if err != nil {
		slog.Error("fail to create totp secret", "err", err)
		return nil, ErrInternalServer
	}
	secret := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)
	sealed, err := totp.Encrypt(s.totpKey, secret, email)
	if err != nil {
		slog.Error("fail to encrypt totp secret", "err", err)
		return nil, ErrInternalServer
	}
	token, err := gocql.RandomUUID()
	if err != nil {
		slog.Error("fail to make random uuid for setup token", "err", err)
		return nil, ErrInternalServer
	}
	err = s.repository.SaveTOTPSetup(ctx, token, email, sealed, purpose)
	if err != nil {
		return nil, ErrInternalServer
	}
	// what an authenticator app reads, from the QR code or as a link on the phone itself
	u := url.URL{
		Scheme: "otpauth",
		Host:   "totp",
		Path:   "/" + s.issuer + ":" + email,
	}
	p := url.Values{}
	p.Add("secret", secret)
	p.Add("issuer", s.issuer)
	u.RawQuery = p.Encode()
	code, err := qr.Encode(u.String(), qr.Q)
	if err != nil {
		slog.Error("fail to create qr from url", "err", err)
		return nil, ErrInternalServer
	}
	return &dto.TOTPSetupResponse{
		SetupToken: uuid.UUID(token),
		QRCode:     base64.StdEncoding.EncodeToString(code.PNG()),
		Secret:     secret,
		OtpauthURL: u.String(),
	}, nil
}

// ConfirmTOTPSetup turns the authenticator on once its first code proves the scan worked, and signs in. A setup for
// a reset replaces the one the member has, ends every other session and tells the owner
func (s *service) ConfirmTOTPSetup(ctx context.Context, setupToken uuid.UUID, code string) (*dto.SignInWithThirdPartyResponse, string, error) {
	token := gocql.UUID(setupToken)
	email, sealed, purpose, err := s.repository.FindTOTPSetup(ctx, token)
	if err != nil {
		return nil, "", ErrVerifyTOTP
	}
	secret, err := totp.Decrypt(s.totpKey, sealed, email)
	if err != nil {
		slog.Error("fail to decrypt totp secret", "err", err)
		return nil, "", ErrInternalServer
	}
	err = s.claimTOTPAttempt(ctx, email)
	if err != nil {
		return nil, "", err
	}
	step, ok, err := totp.MatchStep(secret, code, time.Now(), 0)
	if err != nil {
		slog.Error("fail to check totp code", "err", err)
		return nil, "", ErrInternalServer
	}
	if !ok {
		return nil, "", ErrVerifyTOTP
	}
	isReset := purpose == constant.OTPPurposeReset
	if isReset {
		err = s.repository.ReplaceTOTP(ctx, email, sealed, step)
	} else {
		err = s.repository.EnableTOTP(ctx, email, sealed, step)
	}
	if errors.Is(err, repository.ErrAlreadyExists) {
		return nil, "", ErrVerifyTOTP
	}
	if err != nil {
		return nil, "", ErrInternalServer
	}
	if isReset {
		// a request that fails from here can be retried, the setup is only deleted at the end
		_, _, id, _, _, err := s.repository.FindLoginInfoByEmail(email)
		if err != nil && !errors.Is(err, gocql.ErrNotFound) {
			return nil, "", ErrInternalServer
		}
		if err == nil {
			err = s.repository.ClearRefreshTokenJTIsById(ctx, id)
			if err != nil {
				return nil, "", ErrInternalServer
			}
		}
		go func() {
			err := s.smtpClient.SendAuthenticatorChanged(email)
			if err != nil {
				slog.Error("fail to send authenticator changed email", "err", err)
			}
		}()
	}
	err = s.repository.DeleteTOTPSetup(ctx, token)
	if err != nil {
		return nil, "", ErrInternalServer
	}
	err = s.repository.DeleteTOTPAttempts(ctx, email)
	if err != nil {
		return nil, "", ErrInternalServer
	}
	return s.signInVerifiedEmail(ctx, email)
}

// LoginWithTOTP signs in with a code of the authenticator
func (s *service) LoginWithTOTP(ctx context.Context, email, code string) (*dto.SignInWithThirdPartyResponse, string, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	sealed, lastStep, err := s.repository.FindTOTP(ctx, email)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, "", ErrVerifyTOTP
	}
	if err != nil {
		return nil, "", ErrInternalServer
	}
	err = s.claimTOTPAttempt(ctx, email)
	if err != nil {
		return nil, "", err
	}
	secret, err := totp.Decrypt(s.totpKey, sealed, email)
	if err != nil {
		slog.Error("fail to decrypt totp secret", "err", err)
		return nil, "", ErrInternalServer
	}
	step, ok, err := totp.MatchStep(secret, code, time.Now(), lastStep)
	if err != nil {
		slog.Error("fail to check totp code", "err", err)
		return nil, "", ErrInternalServer
	}
	if !ok {
		return nil, "", ErrVerifyTOTP
	}
	err = s.repository.MarkTOTPStep(ctx, email, lastStep, step)
	if errors.Is(err, repository.ErrAlreadyExists) {
		return nil, "", ErrVerifyTOTP
	}
	if err != nil {
		return nil, "", ErrInternalServer
	}
	err = s.repository.DeleteTOTPAttempts(ctx, email)
	if err != nil {
		return nil, "", ErrInternalServer
	}
	return s.signInVerifiedEmail(ctx, email)
}

// claimTOTPAttempt pays for a guess before it is checked, so 6 digits can't be tried one after another
func (s *service) claimTOTPAttempt(ctx context.Context, email string) error {
	attempts, err := s.repository.FindTOTPAttempts(ctx, email)
	if err != nil {
		return ErrInternalServer
	}
	if attempts >= constant.TOTPMaxAttempts {
		return ErrTOTPTooManyAttempts
	}
	err = s.repository.AddTOTPAttempt(ctx, email, attempts)
	if errors.Is(err, repository.ErrAlreadyExists) {
		return ErrVerifyTOTP
	}
	if err != nil {
		return ErrInternalServer
	}
	return nil
}

// signInVerifiedEmail signs in the owner of a proven email, and signs them up when it has no account yet
func (s *service) signInVerifiedEmail(ctx context.Context, email string) (*dto.SignInWithThirdPartyResponse, string, error) {
	emailVerified, phoneNumberVerified, id, _, role, err := s.repository.FindLoginInfoByEmail(email)
	if errors.Is(err, gocql.ErrNotFound) {
		// banned after the code was mailed
		if err = s.checkEmailNotBanned(ctx, email); err != nil {
			return nil, "", err
		}
		idv7, err := uuid.NewV7()
		if err != nil {
			slog.Error("fail to create uuid v7 for email signup user")
			return nil, "", ErrInternalServer
		}
		err = s.repository.SaveThirdPartySignInInfo(ctx, gocql.UUID(idv7), email, false, true)
		if err != nil {
			return nil, "", ErrInternalServer
		}
		return s.provideBasedOnPhoneVerificationPolicy(gocql.UUID(idv7), email, constant.RoleUser)
	}
	if err != nil {
		return nil, "", ErrInternalServer
	}
	err = s.repository.SaveThirdPartySignInInfo(ctx, id, email, phoneNumberVerified, emailVerified)
	if err != nil {
		return nil, "", ErrInternalServer
	}
	if !phoneNumberVerified {
		return s.provideBasedOnPhoneVerificationPolicy(id, email, role)
	}
	return s.provideTokens(id, role)
}
