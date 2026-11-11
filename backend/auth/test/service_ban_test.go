package test

import (
	"backend/auth/internal/service"
	"context"
	"errors"
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestBanMember(t *testing.T) {
	errDown := errors.New("cassandra down")

	t.Run("bans the email and phone, signs out and deletes the account", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindEmailAndPhoneNumberById(mock.Anything, gid(memberId)).Return(" A@B.c", "+821012345678", nil)
		// lowercased, the way sign up compares it
		d.repo.EXPECT().BanEmail(mock.Anything, "a@b.c").Return(nil)
		d.repo.EXPECT().BanPhoneNumber(mock.Anything, "+821012345678").Return(nil)
		d.repo.EXPECT().ClearRefreshTokenJTIsById(mock.Anything, gid(memberId)).Return(nil)
		d.repo.EXPECT().DeleteAccount(mock.Anything, gid(memberId), " A@B.c", "+821012345678").Return(nil)

		assert.NoError(t, s.BanMember(context.Background(), memberId))
	})
	t.Run("an account without a phone number is deleted without a phone ban", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindEmailAndPhoneNumberById(mock.Anything, gid(memberId)).Return("a@b.c", "", nil)
		d.repo.EXPECT().BanEmail(mock.Anything, "a@b.c").Return(nil)
		d.repo.EXPECT().ClearRefreshTokenJTIsById(mock.Anything, gid(memberId)).Return(nil)
		d.repo.EXPECT().DeleteAccount(mock.Anything, gid(memberId), "a@b.c", "").Return(nil)

		assert.NoError(t, s.BanMember(context.Background(), memberId))
	})
	t.Run("an account already gone is a ban already done", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindEmailAndPhoneNumberById(mock.Anything, gid(memberId)).Return("", "", gocql.ErrNotFound)

		assert.NoError(t, s.BanMember(context.Background(), memberId))
	})
	t.Run("a failed phone ban keeps the account for the retry", func(t *testing.T) {
		s, d := newService(t)
		d.repo.EXPECT().FindEmailAndPhoneNumberById(mock.Anything, gid(memberId)).Return("a@b.c", "+821012345678", nil)
		d.repo.EXPECT().BanEmail(mock.Anything, "a@b.c").Return(nil)
		d.repo.EXPECT().BanPhoneNumber(mock.Anything, "+821012345678").Return(errDown)

		assert.ErrorIs(t, s.BanMember(context.Background(), memberId), errDown)
	})
}

// a banned email gets no account again, whichever way it comes back
func TestBannedEmailCantSignUp(t *testing.T) {
	notFound := func(d deps) {
		d.repo.EXPECT().FindLoginInfoByEmail(bannedEmail).Return(false, false, gocql.UUID{}, "", "", gocql.ErrNotFound)
	}
	t.Run("google", func(t *testing.T) {
		s, d := newService(t)
		d.google.EXPECT().Email(mock.Anything, "token").Return(bannedEmail, nil)
		notFound(d)

		_, _, err := s.SignInWithGoogle(context.Background(), "token")

		assert.ErrorIs(t, err, service.ErrEmailBanned)
	})
	t.Run("email and password", func(t *testing.T) {
		s, _ := newService(t)

		_, err := s.CreateMemberByEmail(context.Background(), bannedEmail, "password1234")

		assert.ErrorIs(t, err, service.ErrEmailBanned)
	})
	t.Run("email code, before the code is mailed", func(t *testing.T) {
		s, d := newNoPasswordService(t)
		d.repo.EXPECT().FindTOTP(mock.Anything, bannedEmail).Return("", int64(0), gocql.ErrNotFound)
		notFound(d)

		_, err := s.StartNoPasswordEmailLogin(context.Background(), bannedEmail)

		assert.ErrorIs(t, err, service.ErrEmailBanned)
	})
}
