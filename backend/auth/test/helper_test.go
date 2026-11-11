package test

import (
	"backend/auth/internal/service"
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// generated once, RSA key generation is slow
var (
	keyAT    = mustKey()
	keyRT    = mustKey()
	keyApple = mustKey()
	keyOther = mustKey()
)

func mustKey() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return k
}

type deps struct {
	repo   *MockRepository
	sms    *MockSMSClient
	google *MockGoogleAuthClient
	mailer *MockSMTPClient
}

// bannedEmail is the one email the repository reports as banned, every other one signs up as before
const bannedEmail = "banned@example.com"

func newService(t *testing.T) (service.Service, deps) {
	t.Setenv("BUNDLE_IDENTIFIER", "com.example.app")
	d := deps{NewMockRepository(t), NewMockSMSClient(t), NewMockGoogleAuthClient(t), NewMockSMTPClient(t)}
	d.repo.EXPECT().WasEmailBanned(mock.Anything, mock.MatchedBy(func(e string) bool { return e != bannedEmail })).
		Return(false, nil).Maybe()
	d.repo.EXPECT().WasEmailBanned(mock.Anything, bannedEmail).Return(true, nil).Maybe()
	appleKeyFunc := func(*jwt.Token) (any, error) { return &keyApple.PublicKey, nil }
	keys := service.Keys{PrivateKeyAT: keyAT, PrivateKeyRT: keyRT, PublicKeyRT: &keyRT.PublicKey}
	return service.NewService(d.repo, keys, d.sms, d.google, appleKeyFunc, d.mailer), d
}

func sign(t *testing.T, key *rsa.PrivateKey, claims jwt.MapClaims) string {
	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(key)
	require.NoError(t, err)
	return token
}

// refreshToken signs a refresh token like the service does
func refreshToken(t *testing.T, key *rsa.PrivateKey, id, jti uuid.UUID, exp time.Time) string {
	return sign(t, key, jwt.MapClaims{
		"sub": id.String(), "jti": jti.String(), "role": "user", "iat": time.Now().Unix(), "exp": exp.Unix(),
	})
}

// claims parses a token issued by the service with the matching public key
func claims(t *testing.T, token string, key *rsa.PrivateKey) jwt.MapClaims {
	parsed, err := jwt.Parse(token, func(*jwt.Token) (any, error) { return &key.PublicKey, nil })
	require.NoError(t, err)
	return parsed.Claims.(jwt.MapClaims)
}

// expectMail catches the OTP mailed to to in a goroutine
func expectMail(t *testing.T, mailer *MockSMTPClient, to string) <-chan string {
	sent := make(chan string, 1)
	mailer.EXPECT().SendOTP(to, mock.Anything).Run(func(_ string, otp string) { sent <- otp }).Return(nil)
	return sent
}

func waitMail(t *testing.T, sent <-chan string) string {
	select {
	case msg := <-sent:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("otp mail was not sent")
		return ""
	}
}

func gid(id uuid.UUID) gocql.UUID {
	return gocql.UUID(id)
}
