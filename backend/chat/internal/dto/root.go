package dto

import (
	"encoding/json"

	"github.com/google/uuid"
)

type SendMessagingRequest struct {
	ToIdType    string    `json:"toIdType"`
	ToId        uuid.UUID `json:"toId"`
	ContentType string    `json:"contentType"`
	Contents    []string  `json:"contents"`
	// true from a client whose users aren't pushed about messages, the holidayfriend web app. the mobile app leaves it out
	NoPush bool `json:"noPush"`
}

type MessagingResponse struct {
	Id          uuid.UUID `json:"id"`
	RoomId      uuid.UUID `json:"roomId"`
	FromId      uuid.UUID `json:"fromId"`
	ContentType string    `json:"contentType"`
	Contents    []string  `json:"contents"`
}

type GetChatRoomInfoResponse struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type SetNameRequest struct {
	Name string `json:"name"`
}

type GetProfileResponse struct {
	Id   uuid.UUID `json:"id,omitzero"`
	Name string    `json:"name,omitempty"`
}

type GeneratePresignedURLRequest struct {
	ContentType string `json:"contentType"`
	N           int    `json:"n"`
}

type GeneratePresignedURLResponse struct {
	Filename uuid.UUID         `json:"filename"`
	URL      string            `json:"url"`
	Fields   map[string]string `json:"fields"`
}

type BlockReport struct {
	Id uuid.UUID `json:"id"`
}

// CallSignalRequest is what a member sends on the call websocket: a WebRTC signal for some of the other members
type CallSignalRequest struct {
	ToIds  []uuid.UUID     `json:"toIds"`
	Signal json.RawMessage `json:"signal"`
}

// CallSignalResponse is what the call websocket writes: who sent the signal, or on joining who is already there
type CallSignalResponse struct {
	FromIds []uuid.UUID     `json:"fromIds"`
	Signal  json.RawMessage `json:"signal,omitempty"`
}

// GetTurnResponse is the TURN server and its short-lived credentials, the turn app checks them with the same secret
type GetTurnResponse struct {
	Uris       []string `json:"uris"`
	Username   string   `json:"username"`
	Credential string   `json:"credential"`
}
