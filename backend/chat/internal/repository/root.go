package repository

import (
	"backend/chat/internal/projection"
	"backend/common"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	_ "github.com/joho/godotenv/autoload"
	"github.com/valkey-io/valkey-go"
)

// Repository reads and writes the cassandra tables of an app, its keyspace, and valkey, which all the apps share:
// its keys are member ids and file names, never the same in two apps
type Repository interface {
	FindChatRoomInfoById(ctx context.Context, app string, id gocql.UUID) (name string, roomType string, err error)
	FindProfileById(ctx context.Context, app string, id gocql.UUID) (name string, err error)
	SaveNameById(ctx context.Context, app string, id gocql.UUID, name string) error
	SetServerIP(ctx context.Context, memberId, ip string) error
	RemoveServerIP(ctx context.Context, memberId, ip string) error
	DidBlock(ctx context.Context, app string, blockerId gocql.UUID, blockedId gocql.UUID) (bool, error)
	FindChatParticipantIds(ctx context.Context, app string, roomId gocql.UUID) (ids []gocql.UUID, err error)
	AddReporterIdByReportedId(ctx context.Context, app string, reporterId gocql.UUID, reportedId gocql.UUID) error
	FindReportCountById(ctx context.Context, app string, reportedId gocql.UUID) (count int, err error)
	AddBlockedConversation(ctx context.Context, app string, memberId gocql.UUID, conversationId gocql.UUID) error
	FindBlockedConversations(ctx context.Context, app string, id gocql.UUID) (ids []gocql.UUID, err error)
	FindRecentMessagesByToId(ctx context.Context, app string, id, cursor gocql.UUID) (result []projection.FindMessagesByToIdAndId, err error)
	SetFilepath(ctx context.Context, id string, filenames []string) error
	HasFilepath(ctx context.Context, id string, filenames []string) (bool, error)
	RemoveFilepath(ctx context.Context, id string, filenames []string) error
	FindIdsByFilename(ctx context.Context, app string, filename gocql.UUID) (ids []gocql.UUID, err error)
}

type repository struct {
	// without a keyspace of its own, so a query names the keyspace of its app and one without fails instead of
	// reading some default
	session *gocql.Session
	apps    common.Apps
	client  valkey.Client
}

func NewRepository() Repository {
	k8ssandraHost := os.Getenv("K8SSANDRA_HOST")
	cluster := gocql.NewCluster(k8ssandraHost)
	cluster.Port = 9042
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

	apps := common.LoadApps()
	// in the keyspace of every app, which bootstrap creates
	for app := range apps {
		err = session.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.message_by_to_id (
    id uuid,
    to_id uuid,
    from_id uuid,
    room_id uuid,
    content_type text,
    contents set<text>,
    PRIMARY KEY ((to_id), id)
    ) WITH CLUSTERING ORDER BY (id DESC);`, app)).Exec()
		if err != nil {
			log.Panicf("fail to create table %s.message_by_to_id: %v", app, err)
		}
		err = session.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.profile_by_id (
    id uuid,
    name text,
    reporter_ids set<uuid>,
    blocked_conversations set<uuid>,
    PRIMARY KEY (id)
    );`, app)).Exec()
		if err != nil {
			log.Panicf("fail to create table %s.profile_by_id: %v", app, err)
		}
		err = session.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.chat_room_by_id (
    id uuid,
    name text,
    room_type text,
    participant_ids set<uuid>,
    PRIMARY KEY (id)
    );`, app)).Exec()
		if err != nil {
			log.Panicf("fail to create table %s.chat_room_by_id: %v", app, err)
		}
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
		apps:    apps,
		client:  client,
	}

	return r
}
