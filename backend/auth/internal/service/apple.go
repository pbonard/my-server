package service

import (
	"backend/auth/internal/constant"
	"backend/auth/internal/dto"
	"backend/auth/internal/repository"
	"context"
	"errors"
	"log/slog"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func (s *service) SignInWithApple(ctx context.Context, identityToken string) (*dto.SignInWithThirdPartyResponse, string, error) {
	idt, err := jwt.Parse(identityToken, s.appleKeyFunc)
	if err != nil {
		slog.Info("fail to parse identityToken with apple JWKs", "err", err)
		return nil, "", ErrSignInWithApple
	}
	if !idt.Valid {
		return nil, "", ErrSignInWithApple
	}
	issFromClaims, err := idt.Claims.GetIssuer()
	if err != nil {
		slog.Info("fail to get issuer",
			"err", err,
			"claims", idt.Claims,
		)
		return nil, "", ErrSignInWithApple
	}
	if issFromClaims != constant.AppleIssuerUrl {
		slog.Info("not expected apple issuer",
			"iss", issFromClaims,
		)
		return nil, "", ErrSignInWithApple
	}
	audsFromClaims, err := idt.Claims.GetAudience()
	if err != nil {
		slog.Info("fail to get audience",
			"err", err,
			"claims", idt.Claims,
		)
		return nil, "", ErrSignInWithApple
	}
	if len(audsFromClaims) == 0 || audsFromClaims[0] != s.audience {
		slog.Info("no audience or not expected audience")
		return nil, "", ErrSignInWithApple
	}
	exp, err := idt.Claims.GetExpirationTime()
	if err != nil {
		slog.Info("fail to get expiration time")
		return nil, "", ErrSignInWithApple
	}
	if exp.Unix() < time.Now().Unix() {
		slog.Info("stale token")
		return nil, "", ErrSignInWithApple
	}
	nonce, ok := idt.Claims.(jwt.MapClaims)["nonce"].(string)
	if !ok {
		slog.Info("no nonce in claims")
		return nil, "", ErrSignInWithApple
	}
	err = s.repository.SaveNonce(nonce)
	if errors.Is(err, repository.ErrAlreadyExists) {
		return nil, "", ErrSignInWithApple
	}
	if err != nil {
		return nil, "", ErrInternalServer
	}
	email, ok := idt.Claims.(jwt.MapClaims)["email"].(string)
	if !ok {
		slog.Info("no email in claims")
		return nil, "", ErrSignInWithApple
	}
	emailVerified, phoneNumberVerified, id, _, role, err1 := s.repository.FindLoginInfoByEmail(email)
	if errors.Is(err1, gocql.ErrNotFound) {
		err1 = nil
		if err = s.checkEmailNotBanned(ctx, email); err != nil {
			return nil, "", err
		}
		idv7, err2 := uuid.NewV7()
		if err2 != nil {
			slog.Error("fail to create uuid v7 for apple sign in user")
			return nil, "", ErrInternalServer
		}
		err = s.repository.SaveThirdPartySignInInfo(ctx, gocql.UUID(idv7), email, false, true)
		if err != nil {
			return nil, "", ErrInternalServer
		}
		return s.provideBasedOnPhoneVerificationPolicy(gocql.UUID(idv7), email, constant.RoleUser)
	}
	if err1 != nil {
		return nil, "", ErrSignInWithApple
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
