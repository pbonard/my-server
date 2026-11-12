package repository

import (
	"backend/common"
	"context"
	"log"
	"os"

	_ "github.com/joho/godotenv/autoload"
	"github.com/valkey-io/valkey-go"
)

type Repository interface {
	GetServerIP(ctx context.Context, key string) (string, error)
	RemoveServerIP(ctx context.Context, key string) error
}

type repository struct {
	client valkey.Client
}

func NewRepository() Repository {
	clientOption := valkey.ClientOption{
		InitAddress: []string{os.Getenv("VALKEY_ADDRESS")},
	}

	tlsConfig, err := common.CreateTlSConfig("", "", os.Getenv("VALKEY_CA_CERT_PATH"))
	if err != nil {
		log.Panicf("fail to create tls config for valkey")
	}
	tlsConfig.ServerName = os.Getenv("VALKEY_HOST")
	clientOption.TLSConfig = tlsConfig

	clientOption.Username = os.Getenv("VALKEY_USERNAME")
	clientOption.Password = os.Getenv("VALKEY_PASSWORD")
	client, err := valkey.NewClient(clientOption)
	if err != nil {
		log.Panicf("Fail to connect to valkey: %v", err)
	}
	log.Print("success to connect valkey")

	r := repository{client: client}

	return &r
}
