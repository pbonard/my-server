package service

import (
	"backend/auth/internal/dto"
	"backend/auth/internal/repository"
	"context"
	"errors"
	"log"
	"log/slog"
	"math/rand"
	"regexp"
	"strconv"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
	_ "github.com/joho/godotenv/autoload"
	"golang.org/x/crypto/bcrypt"
)

func (s *service) CreateMemberByEmail(ctx context.Context, email, password string) (map[string]uuid.UUID, error) {
	if !isValidEmail(email) {
		return nil, ErrSignUpWithEmail
	}

	if len(password) < 8 {
		slog.Info("not valid password",
			"email", email,
		)
		return nil, ErrSignUpWithEmail
	}
	if err := s.checkEmailNotBanned(ctx, email); err != nil {
		return nil, err
	}

	exist, err := s.repository.VerifiedEmailExists(ctx, email)
	if err != nil {
		return nil, ErrSignUpWithEmail
	}
	if exist {
		slog.Info("this email already exist",
			"email", email,
		)
		return nil, ErrSignUpWithEmail
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		slog.Error("fail to hash password",
			"err", err,
		)
		return nil, ErrSignUpWithEmail
	}

	password = string(hashedPassword)
	idv7, err := uuid.NewV7()
	if err != nil {
		slog.Error("fail to create uuid v7 for email password sign in user")
		return nil, ErrInternalServer
	}
	id := gocql.UUID(idv7)
	err = s.repository.SaveEmailLoginInfo(id, email, password)
	if errors.Is(err, repository.ErrAlreadyExists) {
		return nil, ErrSignUpWithEmail
	}
	if err != nil {
		return nil, ErrInternalServer
	}

	vid, err := s.sendEmailOTP(id, email)
	if err != nil {
		return nil, ErrInternalServer
	}

	return map[string]uuid.UUID{"verificationId": vid}, nil
}

func (s *service) LoginWithEmail(email, password string) (*dto.LoginWithEmailResponse, string /*refreshToken*/, error) {
	var resp dto.LoginWithEmailResponse

	emailVerified, phoneNumberVerified, id, dbPassword, role, err :=
		s.repository.FindLoginInfoByEmail(email)
	if err != nil {
		return nil, "", errors.New("this account does not exist")
	}

	err = bcrypt.CompareHashAndPassword([]byte(dbPassword), []byte(password))
	if err != nil {
		slog.Info("invalid password",
			"err", err,
		)
		return nil, "", ErrLoginWithEmail
	}

	if !emailVerified {
		resp.VerificationId, err = s.sendEmailOTP(id, email)
		if err != nil {
			return nil, "", ErrInternalServer
		}
		return &resp, "", nil
	}

	if !phoneNumberVerified && s.phoneVerificationRequired {
		sid := uuid.New()
		err = s.repository.SaveEmailBySessionId(gocql.UUID(sid), email)
		if err != nil {
			return nil, "", ErrInternalServer
		}
		resp.SessionId = sid
		return &resp, "", nil
	}
	if !phoneNumberVerified {
		err = s.repository.SaveProfileId(id)
		if err != nil {
			return nil, "", ErrInternalServer
		}
	}

	jti, err := gocql.RandomUUID()
	if err != nil {
		slog.Error("fail to create random uuid for jti")
		return nil, "", ErrInternalServer
	}
	at, rt, err := s.createLoginTokens(id.String(), jti.String(), role)
	if err != nil {
		return nil, "", ErrInternalServer
	}
	err = s.repository.SaveRefreshTokenJTIById(id, jti)
	if err != nil {
		return nil, "", ErrInternalServer
	}
	resp.AccessToken = at
	return &resp, rt, nil
}

func (s *service) sendEmailOTP(id gocql.UUID, email string) (uuid.UUID, error) {
	otp := strconv.Itoa(rand.Intn(900000) + 100000)
	vid, err := gocql.RandomUUID()
	if err != nil {
		slog.Error("fail to make random uuid for verification id",
			"err", err,
		)
		return uuid.UUID{}, ErrInternalServer
	}
	err = s.repository.SaveEmailAndOtpByVerificationId(vid, id, email, otp)
	if err != nil {
		return uuid.UUID{}, ErrInternalServer
	}
	go func() {
		err := s.smtpClient.SendOTP(email, otp)
		if err != nil {
			slog.Error("fail to send email OTP",
				"err", err,
			)
		}
	}()

	return uuid.UUID(vid), nil
}

func (s *service) VerifyEmailOTP(otp string, verificationId uuid.UUID) (*dto.VerifyEmailOTPResponse, error) {
	id, email, dbOTP, err := s.repository.FindEmailAndOTPByVerificationId(gocql.UUID(verificationId))
	if err != nil {
		return nil, ErrVerifyEmailOTP
	}
	if otp != dbOTP {
		log.Printf(
			"code is not same with db code- received code: %v, db code: %v",
			otp, dbOTP,
		)
		return nil, ErrVerifyEmailOTP
	}
	err = s.repository.MarkEmailVerified(id, email)
	if errors.Is(err, repository.ErrNotOwner) {
		return nil, ErrEmailSignedUpAgain
	}
	if err != nil {
		return nil, ErrInternalServer
	}

	sid, err := gocql.RandomUUID()
	if err != nil {
		slog.Error("fail to make random uuid for session id")
		return nil, ErrInternalServer
	}
	err = s.repository.SaveEmailBySessionId(sid, email)
	if err != nil {
		return nil, ErrInternalServer
	}

	resp := dto.VerifyEmailOTPResponse{
		SessionId: uuid.UUID(sid),
	}
	return &resp, nil
}

func (s *service) ForgetPassword(ctx context.Context, email string) (map[string]uuid.UUID, error) {
	emailVerified, _, id, _, _, err := s.repository.FindLoginInfoByEmail(email)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !emailVerified {
		return nil, nil
	}
	vid, err := s.sendEmailOTP(id, email)
	if err != nil {
		return nil, ErrInternalServer
	}
	return map[string]uuid.UUID{"verificationId": vid}, nil
}

func (s *service) SetNewPassword(ctx context.Context, password string, sessionId uuid.UUID) error {
	email, err := s.repository.FindEmailBySessionId(gocql.UUID(sessionId))
	if err != nil {
		return ErrSessionExpired
	}
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		slog.Error("fail to hash password",
			"err", err,
		)
		return err
	}
	password = string(hashedPassword)
	err = s.repository.UpdatePasswordByEmail(ctx, password, email)
	if err != nil {
		return err
	}
	return nil
}

var emailRegex = regexp.MustCompile(`^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$`)

func isValidEmail(email string) bool {
	return emailRegex.MatchString(email)
}
