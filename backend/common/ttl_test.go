package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTTLSeconds(t *testing.T) {
	tests := []struct {
		raw     string
		want    int
		wantErr bool
	}{
		{"", 259_200, false},
		{"72h", 259_200, false},
		{"30m", 1_800, false},
		{"7days", 0, true},
		// no ttl, kept for good
		{"0", 0, false},
		{"0s", 0, false},
		{"500ms", 0, true},
		{"-1h", 0, true},
		{"200000h", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := TTLSeconds(tt.raw, 72*time.Hour)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
