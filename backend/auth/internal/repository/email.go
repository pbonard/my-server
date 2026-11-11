package repository

import (
	"backend/auth/internal/constant"
	"context"
	"errors"
	"log/slog"

	"github.com/apache/cassandra-gocql-driver/v2"
)

func (r *repository) SaveEmailLoginInfo(id gocql.UUID, email, password string) error {
	applied, err := r.session.Query(
		`INSERT INTO member_by_email (
                             email_verified, phone_number_verified, id, email, password, role
                             ) VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
		false, false, id, email, password, constant.RoleUser).MapScanCAS(map[string]any{})
	if err == nil && !applied {
		applied, err = r.session.Query(
			`UPDATE member_by_email SET phone_number_verified = ?, id = ?, password = ?, role = ?
                             WHERE email = ? IF email_verified = ?`,
			false, id, password, constant.RoleUser, email, false).MapScanCAS(map[string]any{})
	}
	if err == nil && !applied {
		slog.Info("email is already verified by another member", "id", id.String())
		return ErrAlreadyExists
	}
	if err == nil {
		err = r.session.Query(
			`INSERT INTO member_by_id (
                          email_verified, phone_number_verified, id, email, role
                          ) VALUES (?, ?, ?, ?, ?)`,
			false, false, id, email, constant.RoleUser).Exec()
	}
	if err != nil {
		slog.Error("fail to save member",
			"err", err,
			"id", id.String(),
		)
		return err
	}
	return nil
}

func (r *repository) VerifiedEmailExists(ctx context.Context, email string) (bool, error) {
	var emailVerified bool
	err := r.session.Query(
		`SELECT email_verified FROM member_by_email WHERE email = ?`,
		email,
	).ScanContext(ctx, &emailVerified)
	if errors.Is(gocql.ErrNotFound, err) {
		return false, nil
	}
	if err != nil {
		slog.Error("fail to check email existence",
			"err", err,
			"email", email,
		)
		return true, err
	}
	return emailVerified, nil
}

func (r *repository) FindLoginInfoByEmail(email string) (emailVerified, phoneNumberVerified bool, id gocql.UUID, password, role string, err error) {
	err = r.session.Query(
		`SELECT email_verified, phone_number_verified, id, password, role FROM member_by_email WHERE email = ?`,
		email,
	).Scan(&emailVerified, &phoneNumberVerified, &id, &password, &role)
	if err != nil {
		slog.Info("fail to find by email",
			"err", err,
			"email", email,
		)
		return false, false, gocql.UUID{}, "", "", err
	}
	return emailVerified, phoneNumberVerified, id, password, role, nil
}

func (r *repository) SaveEmailAndOtpByVerificationId(verificationId, id gocql.UUID, email, otp string) error {
	err := r.session.Query(
		"INSERT INTO member_by_verification_id (verification_id, id, email, otp) VALUES (?, ?, ?, ?) USING TTL ?",
		verificationId, id, email, otp, constant.AuthIdTTL,
	).Exec()
	if err != nil {
		slog.Error("fail to save email otp by verificationId",
			"err", err,
		)
		return err
	}
	return nil
}

func (r *repository) FindEmailAndOTPByVerificationId(verificationId gocql.UUID) (id gocql.UUID, email string, otp string, err error) {
	err = r.session.Query(
		"SELECT id, email, otp FROM member_by_verification_id WHERE verification_id = ?",
		verificationId,
	).Scan(&id, &email, &otp)
	if err != nil {
		slog.Info("fail to select email and otp by verification_id",
			"err", err,
			"verificationId", verificationId.String(),
		)
		return gocql.UUID{}, "", "", err
	}
	return id, email, otp, nil
}

func (r *repository) MarkEmailVerified(id gocql.UUID, email string) error {
	applied, err := r.session.Query(
		"UPDATE member_by_email SET email_verified = ? WHERE email = ? IF id = ?",
		true, email, id,
	).MapScanCAS(map[string]any{})
	if err != nil {
		slog.Error("fail to update email_verified at member_by_email",
			"err", err,
			"id", id.String(),
			"email", email,
		)
		return err
	}
	if !applied {
		slog.Info("email is signed up again by another member after its otp was sent",
			"id", id.String(),
			"email", email,
		)
		return ErrNotOwner
	}
	err = r.session.Query(
		"UPDATE member_by_id SET email_verified = ? WHERE id = ?",
		true, id,
	).Exec()
	if err != nil {
		slog.Error("fail to update email_verified at member_by_id",
			"err", err,
			"id", id.String(),
			"email", email,
		)
		return err
	}
	return nil
}

func (r *repository) SaveEmailBySessionId(sessionId gocql.UUID, email string) error {
	err := r.session.Query(
		"INSERT INTO member_by_session_id (session_id, email) VALUES (?, ?) USING TTL ?",
		sessionId, email, constant.AuthIdTTL,
	).Exec()
	if err != nil {
		msg := "fail to insert email by session_id at email_by_session_id"
		slog.Error(msg,
			"err", err,
			"sessionId", sessionId.String(),
			"email", email,
		)
		return err
	}
	return nil
}

func (r *repository) FindEmailBySessionId(sessionId gocql.UUID) (email string, err error) {
	err = r.session.Query(
		"SELECT email FROM member_by_session_id WHERE session_id = ?",
		sessionId,
	).Scan(&email)
	if err != nil {
		slog.Info("fail to find email by sessionId, might be expired sessionId",
			"err", err,
			"sessionId", sessionId,
		)
		return "", err
	}
	return email, nil
}

func (r *repository) UpdatePasswordByEmail(ctx context.Context, password string, email string) error {
	err := r.session.Query(`UPDATE member_by_email SET password = ? WHERE email = ?`, password, email).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to update password by email",
			"err", err,
			"email", email)
		return err
	}
	return nil
}

// BanEmail keeps an email from signing up again, lowercased
func (r *repository) BanEmail(ctx context.Context, email string) error {
	err := r.session.Query(`INSERT INTO banned_email (email) VALUES (?)`, email).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to ban email", "err", err)
		return err
	}
	return nil
}

// WasEmailBanned is whether the email, lowercased, was banned
func (r *repository) WasEmailBanned(ctx context.Context, email string) (bool, error) {
	var e string
	err := r.session.Query(`SELECT email FROM banned_email WHERE email = ?`, email).ScanContext(ctx, &e)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		slog.Error("fail to check whether email was banned", "err", err)
		return false, err
	}
	return true, nil
}
