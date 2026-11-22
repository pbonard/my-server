package service

import (
	"backend/auth/internal/constant"
	"context"
	"crypto/rsa"
	"errors"
	"log/slog"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/golang-jwt/jwt/v5"
)

func (s *service) GenerateAccessToken(refreshToken string) (map[string]string, error) {
	rt, err := jwt.Parse(refreshToken, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != jwt.SigningMethodRS256.Alg() {
			slog.Info("unexpected signing method")
			return nil, ErrGenerateToken
		}
		return s.publicKeyRT, nil
	})
	if err != nil {
		slog.Info("fail to parse token",
			"err", err)
		return nil, ErrGenerateToken
	}
	if !rt.Valid {
		slog.Info("invalid token",
			"rt", rt)
		return nil, ErrGenerateToken
	}
	exp, err := rt.Claims.GetExpirationTime()
	if err != nil {
		slog.Info("fail to get expiration time")
		return nil, ErrGenerateToken
	}
	if exp.Unix() < time.Now().Unix() {
		slog.Info("stale token")
		return nil, ErrGenerateToken
	}
	rawId, err := rt.Claims.GetSubject()
	if err != nil {
		slog.Info("fail to get subject from claim",
			"err", err,
		)
		return nil, ErrGenerateToken
	}
	id, err := gocql.ParseUUID(rawId)
	if err != nil {
		slog.Info("fail to parse gocql uuid from id",
			"err", err)
		return nil, ErrGenerateToken
	}
	jtis, err := s.repository.FindRefreshTokenJTIsById(id)
	if err != nil {
		return nil, ErrGenerateToken
	}
	var jtiValidity bool
	for _, jti := range jtis {
		if rt.Claims.(jwt.MapClaims)["jti"].(string) == jti.String() {
			jtiValidity = true
		}
	}
	if !jtiValidity {
		slog.Info("refresh token jti is not same with DB")
		return nil, ErrGenerateToken
	}
	role, ok := rt.Claims.(jwt.MapClaims)["role"].(string)
	if !ok {
		slog.Info("fail to get role")
		return nil, ErrGenerateToken
	}
	at, err := createToken(rawId, role, s.privateKeyAT, constant.AccessTokenTTL)
	if err != nil {
		return nil, ErrInternalServer
	}
	resp := map[string]string{"accessToken": at}

	return resp, nil
}

func (s *service) RemoveJTI(refreshToken string) error {
	rt, err := jwt.Parse(refreshToken, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != jwt.SigningMethodRS256.Alg() {
			slog.Info("unexpected signing method")
			return nil, ErrFailToSignOut
		}
		return s.publicKeyRT, nil
	})
	if err != nil {
		slog.Info("fail to parse token",
			"err", err)
		return ErrFailToSignOut
	}
	if !rt.Valid {
		slog.Info("invalid token",
			"rt", rt)
		return ErrFailToSignOut
	}
	exp, err := rt.Claims.GetExpirationTime()
	if err != nil {
		slog.Info("fail to get expiration time")
		return ErrFailToSignOut
	}
	if exp.Unix() < time.Now().Unix() {
		slog.Info("stale token")
		return ErrFailToSignOut
	}
	rawId, err := rt.Claims.GetSubject()
	if err != nil {
		slog.Info("fail to get subject from claim",
			"err", err,
		)
		return ErrFailToSignOut
	}
	id, err := gocql.ParseUUID(rawId)
	if err != nil {
		slog.Info("fail to parse gocql uuid from id",
			"err", err)
		return ErrFailToSignOut
	}
	jti, err := gocql.ParseUUID(rt.Claims.(jwt.MapClaims)["jti"].(string))
	if err != nil {
		slog.Info("fail to parse gocql uuid from id",
			"err", err)
		return ErrFailToSignOut
	}
	err = s.repository.RemoveRefreshTokenJTIById(id, jti)
	if err != nil {
		return ErrInternalServer
	}
	return nil
}

func (s *service) createLoginTokens(id, jti, role string) (accessToken, refreshToken string, err error) {
	at, err := createToken(id, role, s.privateKeyAT, constant.AccessTokenTTL)
	if err != nil {
		slog.Error("fail to create access token",
			"err", err,
			"id", id,
		)
		return "", "", err
	}
	rt, err := createTokenWithJTI(id, jti, role, s.privateKeyRT, constant.RefreshTokenTTL)
	if err != nil {
		slog.Error("fail to create refresh token",
			"err", err,
			"id", id,
		)
		return "", "", err
	}
	return at, rt, nil
}

func createToken(id, role string, privateKey *rsa.PrivateKey, ttl int64) (token string, err error) {
	claims := jwt.MapClaims{
		"sub":  id,
		"role": role,
		"iat":  time.Now().Unix(),
		"exp":  time.Now().Add(time.Second * time.Duration(ttl)).Unix(),
	}

	token, err = jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(privateKey)
	if err != nil {
		slog.Error("fail to make token", "err", err)
		return "", err
	}
	return token, nil
}

func createTokenWithJTI(id, jti, role string, secretKey *rsa.PrivateKey, ttl int64) (token string, err error) {
	claims := jwt.MapClaims{
		"sub":  id,
		"jti":  jti,
		"role": role,
		"iat":  time.Now().Unix(),
		"exp":  time.Now().Add(time.Second * time.Duration(ttl)).Unix(),
	}

	token, err = jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(secretKey)
	if err != nil {
		slog.Error("fail to make token with JTI",
			"err", err,
		)
		return "", err
	}
	return token, nil
}

func (s *service) DeleteAccount(ctx context.Context, refreshToken string) error {
	rt, err := jwt.Parse(refreshToken, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != jwt.SigningMethodRS256.Alg() {
			slog.Info("unexpected signing method")
			return nil, ErrGenerateToken
		}
		return s.publicKeyRT, nil
	})
	if err != nil {
		slog.Info("fail to parse token",
			"err", err)
		return err
	}
	if !rt.Valid {
		slog.Info("invalid token",
			"rt", rt)
		return errors.New("something went wrong")
	}
	exp, err := rt.Claims.GetExpirationTime()
	if err != nil {
		slog.Info("fail to get expiration time")
		return err
	}
	if exp.Unix() < time.Now().Unix() {
		slog.Info("stale token")
		return errors.New("something went wrong")
	}
	rawId, err := rt.Claims.GetSubject()
	if err != nil {
		slog.Info("fail to get subject from claim",
			"err", err,
		)
		return err
	}
	id, err := gocql.ParseUUID(rawId)
	if err != nil {
		slog.Info("fail to parse gocql uuid from id",
			"err", err)
		return err
	}
	jtis, err := s.repository.FindRefreshTokenJTIsById(id)
	if err != nil {
		return err
	}
	var jtiValidity bool
	for _, jti := range jtis {
		if rt.Claims.(jwt.MapClaims)["jti"].(string) == jti.String() {
			jtiValidity = true
		}
	}
	if !jtiValidity {
		slog.Info("refresh token jti is not same with DB")
		return errors.New("something went wrong")
	}
	email, phoneNumber, err := s.repository.FindEmailAndPhoneNumberById(ctx, id)
	if err != nil {
		return err
	}
	slog.Info("find email and phone number for account deletion",
		"id", id,
		"email", email,
		"phone number", phoneNumber)
	err = s.repository.DeleteAccount(ctx, id, email, phoneNumber)
	if err != nil {
		return err
	}
	return nil
}
