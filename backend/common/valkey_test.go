package common

import (
	"testing"

	"github.com/google/uuid"
)

func TestValkeyKeys(t *testing.T) {
	id := uuid.MustParse("0192f3a4-7e71-750a-8f4e-2b3c4d5e6f70")
	for _, tt := range []struct{ got, want string }{
		{ChatServersKey(id), "chat-servers:0192f3a4-7e71-750a-8f4e-2b3c4d5e6f70"},
		{CallServerKey(id), "call-server:0192f3a4-7e71-750a-8f4e-2b3c4d5e6f70"},
		{CallParticipantsKey("room"), "call-participants:room"},
		{UploadsKey("image", id), "uploads:image:0192f3a4-7e71-750a-8f4e-2b3c4d5e6f70"},
	} {
		if tt.got != tt.want {
			t.Errorf("got %q, want %q", tt.got, tt.want)
		}
	}
}
