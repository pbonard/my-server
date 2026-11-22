package projection

import (
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Message is a row of a room in an inbox, message_by_chat_room
type Message struct {
	Id          gocql.UUID
	FromId      gocql.UUID
	ContentType string
	Contents    []string
}

// Room is a room of an inbox, chat_room_by_to_id, and its newest row of any kind
type Room struct {
	RoomId    gocql.UUID
	UpdatedId gocql.UUID
}
