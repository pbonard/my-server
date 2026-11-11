package repository

import (
	"backend/common"
	"context"
	"log"
	"os"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	_ "github.com/joho/godotenv/autoload"
	"github.com/valkey-io/valkey-go"
)

type Repository interface {
	FindParticipantIds(ctx context.Context, roomId gocql.UUID) ([]gocql.UUID, error)
	SaveIdsByFileName(ctx context.Context, ids []gocql.UUID, filename gocql.UUID) error
	CreateChatRoom(ctx context.Context, roomId gocql.UUID, memberId gocql.UUID, roomName string) error
	AddParticipantId(ctx context.Context, roomId gocql.UUID, participantId gocql.UUID) error
	RemoveParticipantId(ctx context.Context, roomId gocql.UUID, participantId gocql.UUID) error
	IsBlocked(ctx context.Context, blockerId gocql.UUID, blockedId gocql.UUID) (bool, error)
	AddBlock(ctx context.Context, blockerId gocql.UUID, blockedId gocql.UUID) error
	RemoveBlock(ctx context.Context, blockerId gocql.UUID, blockedId gocql.UUID) error
}

type repository struct {
	client  valkey.Client
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
	err = session.Query(`CREATE TABLE IF NOT EXISTS ids_by_filename (
    filename uuid,
    ids set<uuid>,
    PRIMARY KEY (filename)
    );`).Exec()
	if err != nil {
		log.Panicf("fail to create table ids_by_filename: %v", err)
	}
	err = session.Query(`CREATE TABLE IF NOT EXISTS block (
    blocker_id uuid,
    blocked_id uuid,
    PRIMARY KEY ((blocker_id), blocked_id)
	);`).Exec()
	if err != nil {
		log.Panicf("fail to create table block: %v", err)
	}

	log.Print("success to connect cassandra")

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
		log.Panicf("Fail to connect to redis: %v", err)
	}

	r := repository{
		session: session,
		client:  client,
	}

	return &r
}
