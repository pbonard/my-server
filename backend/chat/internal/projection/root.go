package projection

import (
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type Message struct {
	Id          gocql.UUID
	FromId      gocql.UUID
	ContentType string
	Contents    []string
}

type Room struct {
	RoomId    gocql.UUID
	UpdatedId gocql.UUID
}
