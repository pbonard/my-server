package common

import (
	"fmt"
	"time"
)

// cassandra refuses a ttl over 20 years
const maxCassandraTTL = 20 * 365 * 24 * time.Hour

// TTLSeconds is a cassandra ttl, in seconds, from an env value like "72h" or "30m" (time.ParseDuration).
// fallback when it's empty. "0" is no ttl: cassandra keeps a row written USING TTL 0 for good, over a table's
// default_time_to_live too. an invalid one is an error, so a service fails at start instead of keeping rows
// for a ttl no one meant
func TTLSeconds(raw string, fallback time.Duration) (int, error) {
	ttl := fallback
	if raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return 0, fmt.Errorf("invalid ttl %q: %w", raw, err)
		}
		ttl = parsed
	}
	if ttl == 0 {
		return 0, nil
	}
	if ttl < time.Second || ttl > maxCassandraTTL {
		return 0, fmt.Errorf("ttl %v is not 0 (none) or between 1s and 20 years", ttl)
	}
	return int(ttl / time.Second), nil
}
