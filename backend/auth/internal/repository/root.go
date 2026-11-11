package repository

import (
	"backend/common"
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	_ "github.com/joho/godotenv/autoload"
)

// ErrAlreadyExists is returned when an email, phone number or nonce is already claimed by someone else
var ErrAlreadyExists = errors.New("already exists")

// ErrNotOwner is returned when an email is signed up again by another member after its OTP was sent
var ErrNotOwner = errors.New("not owner")

type Repository interface {
	SaveEmailLoginInfo(id gocql.UUID, email, password string) error
	VerifiedEmailExists(ctx context.Context, email string) (bool, error)
	FindLoginInfoByEmail(email string) (emailVerified, phoneNumberVerified bool, id gocql.UUID, password, role string, err error)
	SaveEmailAndOtpByVerificationId(verificationId, id gocql.UUID, email, otp string) error
	FindEmailAndOTPByVerificationId(verificationId gocql.UUID) (id gocql.UUID, email string, otp string, err error)
	MarkEmailVerified(id gocql.UUID, email string) error
	SaveEmailBySessionId(sessionId gocql.UUID, email string) error
	FindEmailBySessionId(sessionId gocql.UUID) (email string, err error)
	UpdatePasswordByEmail(ctx context.Context, password string, email string) error
	FindRefreshTokenJTIsById(id gocql.UUID) (jtis []gocql.UUID, err error)
	SaveRefreshTokenJTIById(id, jti gocql.UUID) error
	RemoveRefreshTokenJTIById(id, jti gocql.UUID) error
	ClearRefreshTokenJTIsById(ctx context.Context, id gocql.UUID) error
	FindEmailAndPhoneNumberById(ctx context.Context, id gocql.UUID) (email, phoneNumber string, err error)
	DeleteAccount(ctx context.Context, id gocql.UUID, email, phoneNumber string) error
	SaveProfileId(id gocql.UUID) error
	SavePhoneNumberByVerificationId(verificationId gocql.UUID, phoneNumber string) error
	FindPhoneNumberByVerificationId(verificationId gocql.UUID) (phoneNumber string, err error)
	SavePhoneNumberLoginInfo(phoneNumber string, id gocql.UUID) error
	LinkAndMarkVerifiedPhoneNumber(id gocql.UUID, email, phoneNumber, role string) error
	FindIdByPhoneNumber(phoneNumber string) (id gocql.UUID, err error)
	FindEmailByPhoneNumber(phoneNumber string) (email string, err error)
	ReplaceAndLinkMemberWithOldAccount(newId, oldAccountId gocql.UUID, email, phoneNumber string) error
	WasBanned(phoneNumber string) error
	BanPhoneNumber(ctx context.Context, phoneNumber string) error
	BanEmail(ctx context.Context, email string) error
	WasEmailBanned(ctx context.Context, email string) (bool, error)
	SaveNonce(nonce string) error
	ClaimNoPasswordEmailOTPCooldown(ctx context.Context, email string) error
	SaveNoPasswordEmailOTP(ctx context.Context, verificationId gocql.UUID, email, otp, purpose string) error
	FindNoPasswordEmailOTP(ctx context.Context, verificationId gocql.UUID) (email, otp, purpose string, attempts int, err error)
	AddNoPasswordEmailOTPAttempt(ctx context.Context, verificationId gocql.UUID, attempts int) error
	DeleteNoPasswordEmailOTP(ctx context.Context, verificationId gocql.UUID) error
	FindTOTP(ctx context.Context, email string) (sealedSecret string, lastStep int64, err error)
	SaveTOTPSetup(ctx context.Context, token gocql.UUID, email, sealedSecret, purpose string) error
	FindTOTPSetup(ctx context.Context, token gocql.UUID) (email, sealedSecret, purpose string, err error)
	DeleteTOTPSetup(ctx context.Context, token gocql.UUID) error
	EnableTOTP(ctx context.Context, email, sealedSecret string, step int64) error
	ReplaceTOTP(ctx context.Context, email, sealedSecret string, step int64) error
	MarkTOTPStep(ctx context.Context, email string, lastStep, step int64) error
	FindTOTPAttempts(ctx context.Context, email string) (int, error)
	AddTOTPAttempt(ctx context.Context, email string, attempts int) error
	DeleteTOTPAttempts(ctx context.Context, email string) error
	SaveThirdPartySignInInfo(ctx context.Context, id gocql.UUID, email string, phoneNumberVerified, emailVerified bool) error
}

type repository struct {
	session *gocql.Session
}

func NewRepository() Repository {
	k8ssandraHost := os.Getenv("K8SSANDRA_HOST")
	cluster := gocql.NewCluster(k8ssandraHost)
	cluster.Port = 9042
	cluster.Keyspace = os.Getenv("PROFILE")
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: os.Getenv("K8SSANDRA_USERNAME"),
		Password: os.Getenv("K8SSANDRA_PASSWORD"),
	}
	cluster.Timeout = 1 * time.Minute
	cluster.Consistency = gocql.LocalOne
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Compressor = &lz4.LZ4Compressor{}
	cluster.PageSize = 1000
	cluster.NextPagePrefetch = 0.25
	tlSConfig, err := common.CreateTlSConfig("", "", os.Getenv("K8SSANDRA_CA_CERT_PATH"))
	if err != nil {
		panic(err)
	}
	tlSConfig.ServerName = k8ssandraHost
	cluster.SslOpts = &gocql.SslOptions{
		Config:                 tlSConfig,
		EnableHostVerification: true,
	}

	session, err := gocql.NewSession(*cluster)
	if err != nil {
		panic(err)
	}

	queries := []string{
		`CREATE TABLE IF NOT EXISTS member_by_email (
			email text PRIMARY KEY, id uuid, email_verified boolean, phone_number_verified boolean,
			phone_number text, password text, role text
		);`,
		`CREATE TABLE IF NOT EXISTS member_by_id (
			id uuid PRIMARY KEY, email text, email_verified boolean, phone_number_verified boolean,
			phone_number text, role text, refresh_token_jtis set<uuid>
		);`,
		`CREATE TABLE IF NOT EXISTS member_by_phone_number (
			phone_number text PRIMARY KEY, id uuid, email text, phone_number_verified boolean, role text
		);`,
		`CREATE TABLE IF NOT EXISTS member_by_verification_id (
			verification_id uuid PRIMARY KEY, email text, phone_number text, otp text
		);`,
		`ALTER TABLE member_by_verification_id ADD IF NOT EXISTS id uuid;`,
		`CREATE TABLE IF NOT EXISTS member_by_session_id (
			session_id uuid PRIMARY KEY, email text
		);`,
		`CREATE TABLE IF NOT EXISTS nonce (
    		nonce text PRIMARY KEY
    	);`,
		`CREATE TABLE IF NOT EXISTS email_otp (
			verification_id uuid PRIMARY KEY, email text, otp text, purpose text, attempts int
		);`,
		`CREATE TABLE IF NOT EXISTS email_otp_cooldown (
			email text PRIMARY KEY
		);`,
		`CREATE TABLE IF NOT EXISTS member_totp (
			email text PRIMARY KEY, secret text, last_step bigint
		);`,
		`CREATE TABLE IF NOT EXISTS totp_setup (
			setup_token uuid PRIMARY KEY, email text, secret text, purpose text
		);`,
		`CREATE TABLE IF NOT EXISTS totp_attempt (
			email text PRIMARY KEY, attempts int
		);`,
		`CREATE TABLE IF NOT EXISTS profile_by_id (
			id uuid PRIMARY KEY, name text, reporter_ids set<uuid>, blocked_conversations set<uuid>
		);`,
		`CREATE TABLE IF NOT EXISTS banned_phone_number (
			phone_number text PRIMARY KEY
		);`,
		// lowercased, the way an email is compared at sign up
		`CREATE TABLE IF NOT EXISTS banned_email (
			email text PRIMARY KEY
		);`,
	}

	for _, q := range queries {
		err = session.Query(q).Exec()
		if err != nil {
			slog.Error("failed to execute migration query", "err", err, "query", q)
			panic(err)
		}
	}
	log.Print("success to connect cassandra")
	r := &repository{
		session: session,
	}

	return r
}
