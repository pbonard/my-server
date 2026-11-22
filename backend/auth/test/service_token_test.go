package test

import (
	"backend/auth/internal/service"
	"context"
	"errors"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	memberId = uuid.New()
	jti      = uuid.New()
	future   = time.Now().Add(time.Hour)
)

func TestGenerateAccessToken(t *testing.T) {
	t.Run("known refresh token gets a new access token", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindRefreshTokenJTIsById(gid(memberId)).Return([]gocql.UUID{gid(uuid.New()), gid(jti)}, nil)

		resp, err := s.GenerateAccessToken(refreshToken(t, keyRT, memberId, jti, future))

		require.NoError(t, err)
		at := claims(t, resp["accessToken"], keyAT)
		assert.Equal(t, memberId.String(), at["sub"])
		assert.Equal(t, "user", at["role"])
	})
	t.Run("revoked jti", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindRefreshTokenJTIsById(gid(memberId)).Return([]gocql.UUID{gid(uuid.New())}, nil)

		_, err := s.GenerateAccessToken(refreshToken(t, keyRT, memberId, jti, future))

		assert.ErrorIs(t, err, service.ErrGenerateToken)
	})
	t.Run("signed by another key", func(t *testing.T) {
		s, _ := newService(t)

		_, err := s.GenerateAccessToken(refreshToken(t, keyOther, memberId, jti, future))

		assert.ErrorIs(t, err, service.ErrGenerateToken)
	})
	t.Run("expired", func(t *testing.T) {
		s, _ := newService(t)

		_, err := s.GenerateAccessToken(refreshToken(t, keyRT, memberId, jti, time.Now().Add(-time.Minute)))

		assert.ErrorIs(t, err, service.ErrGenerateToken)
	})
}

func TestRemoveJTI(t *testing.T) {
	t.Run("removes the token's jti", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().RemoveRefreshTokenJTIById(gid(memberId), gid(jti)).Return(nil)

		assert.NoError(t, s.RemoveJTI(refreshToken(t, keyRT, memberId, jti, future)))
	})
	t.Run("invalid token", func(t *testing.T) {
		s, _ := newService(t)

		assert.ErrorIs(t, s.RemoveJTI("not-a-token"), service.ErrFailToSignOut)
	})
	t.Run("expired token", func(t *testing.T) {
		s, _ := newService(t)

		assert.ErrorIs(t, s.RemoveJTI(refreshToken(t, keyRT, memberId, jti, time.Now().Add(-time.Minute))), service.ErrFailToSignOut)
	})
	t.Run("failed revoke", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().RemoveRefreshTokenJTIById(gid(memberId), gid(jti)).Return(errors.New("cassandra down"))

		assert.ErrorIs(t, s.RemoveJTI(refreshToken(t, keyRT, memberId, jti, future)), service.ErrInternalServer)
	})
}

func TestDeleteAccount(t *testing.T) {
	t.Run("valid refresh token deletes the account", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindRefreshTokenJTIsById(gid(memberId)).Return([]gocql.UUID{gid(jti)}, nil)
		d.repo.EXPECT().FindEmailAndPhoneNumberById(mock.Anything, gid(memberId)).Return(email, "+821012345678", nil)
		d.repo.EXPECT().DeleteAccount(mock.Anything, gid(memberId), email, "+821012345678").Return(nil)

		assert.NoError(t, s.DeleteAccount(context.Background(), refreshToken(t, keyRT, memberId, jti, future)))
	})
	t.Run("revoked jti", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindRefreshTokenJTIsById(gid(memberId)).Return(nil, nil)

		assert.Error(t, s.DeleteAccount(context.Background(), refreshToken(t, keyRT, memberId, jti, future)))
	})
}
