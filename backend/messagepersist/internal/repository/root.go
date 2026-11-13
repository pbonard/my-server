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
)

type Repository interface {
	SaveMessage(ctx context.Context, id, toId, fromId, roomId gocql.UUID, contentType string, contents []string, ttl int) error
	SaveRoomMessageId(ctx context.Context, toId, roomId, id gocql.UUID, ttl int) error
	FindRoomMessageIdsBefore(ctx context.Context, toId, roomId, before gocql.UUID) ([]gocql.UUID, error)
	DeleteMessage(ctx context.Context, toId, id gocql.UUID) error
	DeleteMessages(ctx context.Context, toId gocql.UUID, ids []gocql.UUID) error
	DeleteRoomMessageId(ctx context.Context, toId, roomId, id gocql.UUID) error
	DeleteRoomMessageIdsBefore(ctx context.Context, toId, roomId, before gocql.UUID) error
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

	// the ids of each inbox's messages by room, so a quit finds the room's messages in message_by_to_id, whose
	// key is the inbox alone for the sync's "everything after my cursor". messages only, no quit or delete: a room
	// with a row here is one its inbox still has, which chat checks before an invite
	err = session.Query(`CREATE TABLE IF NOT EXISTS message_id_by_room (
    to_id uuid,
    room_id uuid,
    id uuid,
    PRIMARY KEY ((to_id, room_id), id)
    ) WITH CLUSTERING ORDER BY (id DESC);`).Exec()
	if err != nil {
		log.Panicf("fail to create table message_id_by_room: %v", err)
	}

	log.Print("success to connect cassandra")

	r := repository{
		session: session,
	}

	return &r
}
