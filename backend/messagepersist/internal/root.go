package internal

import (
	"backend/common"
	"backend/common/producer"
	"backend/messagepersist/internal/consumer"
	"backend/messagepersist/internal/repository"
	"backend/messagepersist/internal/service"
	"log"
	"log/slog"
	"os"
	"time"
)

func NewServer() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
	}))
	slog.SetDefault(logger)

	r := repository.NewRepository()

	s := service.NewService(r, service.Config{
		// MESSAGE_TTL "72h", 3 days when it's not set, "0" for good
		MessageTTL: ttlFromEnv("MESSAGE_TTL", 72*time.Hour),
		// DELETE_TTL, 30 days when it's not set
		DeleteTTL: ttlFromEnv("DELETE_TTL", 30*24*time.Hour),
	})

	p := producer.NewSyncProducer("message-persist")

	c := consumer.NewConsumer(s, p)

	err := c.GetMessage([]string{"prepared-message"})
	if err != nil {
		panic(err)
	}
}

// ttlFromEnv is the ttl, in seconds, of the env value name. an invalid one stops the start, instead of keeping rows
// for a ttl no one meant
func ttlFromEnv(name string, fallback time.Duration) int {
	ttl, err := common.TTLSeconds(os.Getenv(name), fallback)
	if err != nil {
		log.Panicf("fail to read %s: %v", name, err)
	}
	return ttl
}
