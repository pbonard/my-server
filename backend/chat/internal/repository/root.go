package repository

import (
	"backend/chat/internal/projection"
	"backend/common"
	"context"
	"log"
	"os"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	"github.com/google/uuid"
	_ "github.com/joho/godotenv/autoload"
	"github.com/valkey-io/valkey-go"
)

type Repository interface {
	FindChatRoomInfoById(ctx context.Context, id gocql.UUID) (name string, roomType string, err error)
	FindProfileById(ctx context.Context, id gocql.UUID) (name string, err error)
	SaveNameById(ctx context.Context, id gocql.UUID, name string) error
	SetServerIP(ctx context.Context, key, ip string) error
	RemoveServerIP(ctx context.Context, key, ip string) error
	DidBlock(ctx context.Context, blockerId gocql.UUID, blockedId gocql.UUID) (bool, error)
	FindChatParticipantIds(ctx context.Context, roomId gocql.UUID) (ids []gocql.UUID, err error)
	AddReporterIdByReportedId(ctx context.Context, reporterId gocql.UUID, reportedId gocql.UUID) error
	FindReportCountById(ctx context.Context, reportedId gocql.UUID) (count int, err error)
	AddBlockedConversation(ctx context.Context, memberId gocql.UUID, conversationId gocql.UUID) error
	FindBlockedConversations(ctx context.Context, id gocql.UUID) (ids []gocql.UUID, err error)
	FindRecentMessagesByToId(ctx context.Context, id, cursor gocql.UUID) (result []projection.FindMessagesByToIdAndId, err error)
	SetFilepath(ctx context.Context, key string, filenames []string) error
	HasFilepath(ctx context.Context, key string, filenames []string) (bool, error)
	RemoveFilepath(ctx context.Context, key string, filenames []string) error
	FindIdsByFilename(ctx context.Context, filename gocql.UUID) (ids []gocql.UUID, err error)
	SetCallServerIP(ctx context.Context, key, ip string) error
	RemoveCallServerIP(ctx context.Context, key string) error
	FindCallParticipantIds(ctx context.Context, key string) ([]string, error)
	AddCallParticipantId(ctx context.Context, key string, memberId uuid.UUID) error
	RemoveCallParticipantId(ctx context.Context, key string, memberId uuid.UUID) error
}

type repository struct {
	session *gocql.Session
	client  valkey.Client
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

	err = session.Query(`CREATE TABLE IF NOT EXISTS message_by_to_id (
    id uuid,
    to_id uuid,
    from_id uuid,
    room_id uuid,
    content_type text,
    contents set<text>,
    PRIMARY KEY ((to_id), id)
    ) WITH CLUSTERING ORDER BY (id DESC);`).Exec()
	if err != nil {
		log.Panicf("fail to create table payload: %v", err)
	}
	err = session.Query(`CREATE TABLE IF NOT EXISTS profile_by_id (
    id uuid,
    name text,
    reporter_ids set<uuid>,
    blocked_conversations set<uuid>,
    PRIMARY KEY (id)
    );`).Exec()
	if err != nil {
		log.Panicf("fail to create table profile_by_id: %v", err)
	}
	err = session.Query(`CREATE TABLE IF NOT EXISTS chat_room_by_id (
    id uuid,
    name text,
    room_type text,
    participant_ids set<uuid>,
    PRIMARY KEY (id)
    );`).Exec()
	if err != nil {
		log.Panicf("fail to create table chat_room_by_id: %v", err)
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
		log.Panicf("Fail to connect to valkey: %v", err)
	}
	log.Print("success to connect valkey")

	r := &repository{
		session: session,
		client:  client,
	}

	return r
}
