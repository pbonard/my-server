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
	SaveMessage(ctx context.Context, toId, roomId gocql.UUID, m Message, ttl int) error
	DeleteMessage(ctx context.Context, toId, roomId, id gocql.UUID) error
	DeleteMessagesBefore(ctx context.Context, toId, roomId, before gocql.UUID) error
	TouchRoom(ctx context.Context, toId, roomId, id gocql.UUID, ttl int, at int64) error
}

type Message struct {
	Id          gocql.UUID
	FromId      gocql.UUID
	ContentType string
	Contents    []string
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

	err = session.Query(`CREATE TABLE IF NOT EXISTS message_by_chat_room (
    to_id uuid,
    room_id uuid,
    id uuid,
    from_id uuid,
    content_type text,
    contents set<text>,
    PRIMARY KEY ((to_id, room_id), id)
    ) WITH CLUSTERING ORDER BY (id DESC);`).Exec()
	if err != nil {
		log.Panicf("fail to create table message_by_chat_room: %v", err)
	}

	err = session.Query(`CREATE TABLE IF NOT EXISTS chat_room_by_to_id (
    to_id uuid,
    room_id uuid,
    updated_id uuid,
    PRIMARY KEY ((to_id), room_id)
    );`).Exec()
	if err != nil {
		log.Panicf("fail to create table chat_room_by_to_id: %v", err)
	}

	log.Print("success to connect cassandra")

	r := repository{
		session: session,
	}

	return &r
}
