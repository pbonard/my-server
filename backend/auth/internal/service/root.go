package service

import (
	"backend/auth/internal/client"
	"backend/auth/internal/dto"
	"backend/auth/internal/repository"
	"context"
	"crypto/rsa"
	"encoding/base64"
	"log"
	"os"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	_ "github.com/joho/godotenv/autoload"
)

type Service interface {
	SignInWithApple(ctx context.Context, identityToken string) (*dto.SignInWithThirdPartyResponse, string, error)
	CreateMemberByEmail(ctx context.Context, email, password string) (map[string]uuid.UUID, error)
	LoginWithEmail(email, password string) (*dto.LoginWithEmailResponse, string /*refreshToken*/, error)
	VerifyEmailOTP(otp string, verificationId uuid.UUID) (*dto.VerifyEmailOTPResponse, error)
	ForgetPassword(ctx context.Context, email string) (map[string]uuid.UUID, error)
	SetNewPassword(ctx context.Context, password string, sessionId uuid.UUID) error
	SignInWithGoogle(ctx context.Context, token string) (*dto.SignInWithThirdPartyResponse, string, error)
	StartNoPasswordEmailLogin(ctx context.Context, email string) (*dto.StartNoPasswordEmailLoginResponse, error)
	VerifyNoPasswordEmailOTP(ctx context.Context, verificationId uuid.UUID, otp string) (*dto.TOTPSetupResponse, error)
	ConfirmTOTPSetup(ctx context.Context, setupToken uuid.UUID, code string) (*dto.SignInWithThirdPartyResponse, string, error)
	LoginWithTOTP(ctx context.Context, email, code string) (*dto.SignInWithThirdPartyResponse, string, error)
	StartAuthenticatorReset(ctx context.Context, email string) (*dto.StartAuthenticatorResetResponse, error)
	SendSMSOTP(sessionId uuid.UUID, phoneNumber string) (map[string]uuid.UUID, error)
	VerifySMSOTP(sessionId uuid.UUID, verificationId uuid.UUID, otp string) (*dto.VerifySMSOTPResponse, string, error)
	GenerateAccessToken(refreshToken string) (map[string]string, error)
	RemoveJTI(refreshToken string) error
	DeleteAccount(ctx context.Context, refreshToken string) error
	BanMember(ctx context.Context, id uuid.UUID) error
}

type service struct {
	repository       repository.Repository
	privateKeyAT     *rsa.PrivateKey
	privateKeyRT     *rsa.PrivateKey
	publicKeyRT      *rsa.PublicKey
	issuer           string
	audience         string
	smsClient        client.SMSClient
	googleAuthClient client.GoogleAuthClient
	appleKeyFunc     jwt.Keyfunc
	smtpClient       client.SMTPClient
	// members must verify a phone number before they get tokens
	phoneVerificationRequired bool
	// seals the secrets of authenticators
	totpKey []byte
}

func NewService(r repository.Repository, keys Keys, smsClient client.SMSClient, googleAuthClient client.GoogleAuthClient,
	appleKeyFunc jwt.Keyfunc, smtpClient client.SMTPClient) Service {
	var totpKey []byte
	if os.Getenv("NO_PASSWORD_EMAIL_LOGIN") == "true" {
		var err error
		totpKey, err = base64.StdEncoding.DecodeString(os.Getenv("TOTP_ENCRYPTION_KEY"))
		if err != nil {
			log.Fatalf("Failed to read TOTP_ENCRYPTION_KEY: %v", err)
		}
		if len(totpKey) != 32 {
			log.Fatal("TOTP_ENCRYPTION_KEY must be 32 bytes")
		}
	}
	return &service{
		repository:       r,
		privateKeyAT:     keys.PrivateKeyAT,
		privateKeyRT:     keys.PrivateKeyRT,
		publicKeyRT:      keys.PublicKeyRT,
		issuer:           os.Getenv("ISSUER"),
		audience:         os.Getenv("BUNDLE_IDENTIFIER"),
		smsClient:        smsClient,
		googleAuthClient: googleAuthClient,
		appleKeyFunc:     appleKeyFunc,
		smtpClient:       smtpClient,
		// only an explicit false turns it off, so a missing or mistyped value can't skip the verification
		phoneVerificationRequired: os.Getenv("PHONE_VERIFICATION_REQUIRED") != "false",
		totpKey:                   totpKey,
	}
}
