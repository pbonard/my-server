package service

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

// BanMember deletes the account of a member reported too many times and bans their email and phone number, so neither
// signs up again, from an AuthEvent.
// the account goes last: a retry after a failure in between finds it still there and does the rest again, while
// one already deleted is a ban that was done, so it's nil
func (s *service) BanMember(ctx context.Context, id uuid.UUID) error {
	email, phoneNumber, err := s.repository.FindEmailAndPhoneNumberById(ctx, gocql.UUID(id))
	if errors.Is(err, gocql.ErrNotFound) {
		slog.Info("the member to ban has no account, already banned or deleted", "id", id)
		return nil
	}
	if err != nil {
		return err
	}
	if email != "" {
		err = s.repository.BanEmail(ctx, normalizeEmail(email))
		if err != nil {
			return err
		}
	}
	// a member who signed up with email alone has no phone number to ban
	if phoneNumber != "" {
		err = s.repository.BanPhoneNumber(ctx, phoneNumber)
		if err != nil {
			return err
		}
	}
	// their refresh tokens stop working, so they're signed out once the access token expires
	err = s.repository.ClearRefreshTokenJTIsById(ctx, gocql.UUID(id))
	if err != nil {
		return err
	}
	err = s.repository.DeleteAccount(ctx, gocql.UUID(id), email, phoneNumber)
	if err != nil {
		return err
	}
	slog.Info("ban member", "id", id)
	return nil
}

// normalizeEmail is an email the way bans compare it: two spellings of one address are one ban
func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// checkEmailNotBanned stops an account being made for a banned email. a banned member's account is deleted, so
// every way back in is a new account: this runs wherever one is made
func (s *service) checkEmailNotBanned(ctx context.Context, email string) error {
	banned, err := s.repository.WasEmailBanned(ctx, normalizeEmail(email))
	if err != nil {
		return ErrInternalServer
	}
	if banned {
		slog.Info("a banned email tried to sign up")
		return ErrEmailBanned
	}
	return nil
}
