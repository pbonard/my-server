package internal

import (
	"backend/auth/internal/client"
	"backend/auth/internal/constant"
	"backend/auth/internal/consumer"
	"backend/auth/internal/controller"
	"backend/auth/internal/repository"
	"backend/auth/internal/service"
	"backend/common/producer"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/MicahParks/keyfunc/v3"
)

func NewServer() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
	}))

	slog.SetDefault(logger)

	log.Print("success to set logger")

	r := repository.NewRepository()

	appleJWKs, err := keyfunc.NewDefault([]string{constant.AppleKeyUrl})
	if err != nil {
		log.Fatalf("Failed to create JWKS from Apple: %v", err)
	}

	s := service.NewService(r, service.LoadKeys(), client.NewSMSClient(), client.NewGoogleAuthClient(), appleJWKs.Keyfunc,
		client.NewSMTPClient())

	// the AuthEvents of this app, like bans from chat's reports, beside the http server
	go consumer.NewConsumer(s, producer.NewSyncProducer("auth-"+os.Getenv("PROFILE"))).Run()

	mux := http.NewServeMux()

	controller.NewController(s, mux)

	err = http.ListenAndServe(":8080", mux)
	if err != nil {
		panic(err)
	}
}
