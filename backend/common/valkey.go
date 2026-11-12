package common

import "github.com/google/uuid"

// The valkey keys, one function each so the services that write a key and the ones that read it build the same one.
// a prefix says what the key holds and the id is the uuid's string, so a key reads as what it is in valkey-cli

// ChatServersKey is the set of the ips of the chat servers a member is connected to by websocket.
// chat adds and removes them, messagerelay reads them to deliver a message
func ChatServersKey(memberId uuid.UUID) string {
	return "chat-servers:" + memberId.String()
}

// CallServerKey is the ip of the chat server a member is connected to for a call. chat sets it, signalrelay reads it
// to deliver a signal to a member on another chat server
func CallServerKey(memberId uuid.UUID) string {
	return "call-server:" + memberId.String()
}

// CallParticipantsKey is the set of the ids (uuid strings) of the members in a call. chat keeps it. callId is the
// mobile app's: the conversation's uuid for its call, or for a personal call from a chat the two members' uuids
// joined, smaller first
func CallParticipantsKey(callId string) string {
	return "call-participants:" + callId
}

// UploadsKey is the set of the filenames of a member's presigned uploads of the content type ("image", "audio",
// "video") that no message has used yet. chat keeps it
func UploadsKey(contentType string, memberId uuid.UUID) string {
	return "uploads:" + contentType + ":" + memberId.String()
}
