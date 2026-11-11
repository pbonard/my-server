package repository

import (
	"backend/auth/internal/constant"
	"context"
	"log/slog"

	"github.com/apache/cassandra-gocql-driver/v2"
)

func (r *repository) SavePhoneNumberByVerificationId(verificationId gocql.UUID, phoneNumber string) error {
	err := r.session.Query("INSERT INTO member_by_verification_id (phone_number, verification_id) values (?,?) USING TTL ?",
		phoneNumber, verificationId, constant.OtpTTL,
	).Exec()
	if err != nil {
		slog.Error("fail to insert phone number with id",
			"err", err,
			"verificationId", verificationId,
			"phoneNumber", phoneNumber,
		)
		return err
	}
	return nil
}

func (r *repository) FindPhoneNumberByVerificationId(verificationId gocql.UUID) (phoneNumber string, err error) {
	err = r.session.Query(
		"SELECT phone_number FROM member_by_verification_id WHERE verification_id = ?",
		verificationId,
	).Scan(&phoneNumber)
	if err != nil {
		slog.Info("fail to find phone_number by verification_id",
			"err", err,
			"verificationId", verificationId,
		)
		return "", err
	}
	return phoneNumber, nil
}

// SavePhoneNumberLoginInfo claims member_by_phone_number before writing member_by_id, the phone number of the member
// itself is left as it is, one claimed by another member returns ErrAlreadyExists
func (r *repository) SavePhoneNumberLoginInfo(phoneNumber string, id gocql.UUID) error {
	existing := map[string]any{}
	applied, err := r.session.Query(
		"INSERT INTO member_by_phone_number (phone_number_verified, id, phone_number, role) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		true, id, phoneNumber, constant.RoleUser,
	).MapScanCAS(existing)
	if err == nil && !applied && existing["id"] != id {
		slog.Info("phone number is already claimed by another member", "id", id.String())
		return ErrAlreadyExists
	}
	if err == nil {
		err = r.session.Query(
			"INSERT INTO member_by_id (phone_number_verified, id, phone_number, role) VALUES (?, ?, ?, ?)",
			true, id, phoneNumber, constant.RoleUser,
		).Exec()
	}
	if err != nil {
		slog.Error("fail to insert member at member_by_id",
			"err", err,
			"phoneNumber", phoneNumber,
		)
		return err
	}
	return nil
}

// LinkAndMarkVerifiedPhoneNumber claims member_by_phone_number before linking it, a phone number claimed by another
// member in the meantime returns ErrAlreadyExists
func (r *repository) LinkAndMarkVerifiedPhoneNumber(id gocql.UUID, email, phoneNumber, role string) error {
	applied, err := r.session.Query(
		"INSERT INTO member_by_phone_number (phone_number_verified, id, email, phone_number, role) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS",
		true, id, email, phoneNumber, role,
	).MapScanCAS(map[string]any{})
	if err == nil && !applied {
		slog.Info("phone number is already claimed by another member", "id", id.String())
		return ErrAlreadyExists
	}
	if err == nil {
		err = r.session.Batch(gocql.LoggedBatch).
			Query("UPDATE member_by_email SET phone_number_verified = ?, phone_number = ? WHERE email = ?",
				true, phoneNumber, email).
			Query("UPDATE member_by_id SET phone_number_verified = ?, phone_number = ? WHERE id = ?",
				true, phoneNumber, id).
			Exec()
	}
	if err != nil {
		slog.Error("fail to set phone_number",
			"err", err,
			"id", id,
			"email", email,
			"phoneNumber", phoneNumber,
		)
		return err
	}
	return nil
}

func (r *repository) FindIdByPhoneNumber(phoneNumber string) (id gocql.UUID, err error) {
	err = r.session.Query(
		"SELECT id FROM member_by_phone_number WHERE phone_number = ?",
		phoneNumber,
	).Scan(&id)
	if err != nil {
		slog.Info("fail to find id by phone number")
		return gocql.UUID{}, err
	}
	return id, nil
}

func (r *repository) FindEmailByPhoneNumber(phoneNumber string) (email string, err error) {
	err = r.session.Query(
		"SELECT email FROM member_by_phone_number WHERE phone_number = ?",
		phoneNumber,
	).Scan(&email)
	if err != nil {
		slog.Info("fail to find email by phone number",
			"err", err,
			"phoneNumber", phoneNumber,
		)
		return "", err
	}
	return email, nil
}

func (r *repository) ReplaceAndLinkMemberWithOldAccount(newId, oldAccountId gocql.UUID, email, phoneNumber string) error {
	err := r.session.Batch(gocql.LoggedBatch).
		Query("DELETE FROM member_by_id WHERE id = ?",
			newId).
		Query("UPDATE member_by_id SET email = ?, email_verified = ? WHERE id = ?",
			email, true, oldAccountId,
		).
		Query("UPDATE member_by_email SET id = ?, phone_number = ?, phone_number_verified = ? WHERE email = ?",
			oldAccountId, phoneNumber, true, email,
		).
		Query("UPDATE member_by_phone_number SET email = ? WHERE phone_number = ?",
			email, phoneNumber).
		Exec()
	if err != nil {
		slog.Error("fail to replace and link member with old phone number account", "err", err)
		return err
	}
	return nil
}

func (r *repository) WasBanned(phoneNumber string) error {
	var p string
	err := r.session.Query(`SELECT phone_number FROM banned_phone_number WHERE phone_number = ?`, phoneNumber).Scan(&p)
	if err != nil {
		slog.Info("fail to check whether phone number was banned",
			"err", err)
		return err
	}
	return nil
}

func (r *repository) BanPhoneNumber(ctx context.Context, phoneNumber string) error {
	err := r.session.Query(`INSERT INTO banned_phone_number (phone_number) VALUES (?)`, phoneNumber).ExecContext(ctx)
	if err != nil {
		slog.Error("fail to ban phone number",
			"err", err)
		return err
	}
	return nil
}
