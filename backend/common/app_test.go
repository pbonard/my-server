package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppScoped(t *testing.T) {
	tests := []struct {
		profile string
		want    string
	}{
		{profile: "production", want: "chat-message"},
		// a service without PROFILE is the production one, as before there were other apps
		{profile: "", want: "chat-message"},
		{profile: "holidayfriend", want: "holidayfriend.chat-message"},
	}
	for _, tt := range tests {
		t.Run(tt.profile, func(t *testing.T) {
			assert.Equal(t, tt.want, AppScoped(tt.profile, "chat-message"))
		})
	}
}
