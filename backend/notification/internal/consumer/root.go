package consumer

import (
	"backend/common"
	"backend/common/producer"
	"backend/notification/internal/service"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/IBM/sarama"

	_ "github.com/joho/godotenv/autoload"
)

const groupId = "notification-preprocess"

type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	service       service.Service
	producer      producer.SyncProducer
}

func NewConsumer(s service.Service, p producer.SyncProducer) *Consumer {
	consumerGroup, err := connectConsumer(groupId)
	if err != nil {
		log.Panicf("fail to create consumer group client: %v", err)
	}
	return &Consumer{
		consumerGroup: consumerGroup,
		service:       s,
		producer:      p,
	}
}

func connectConsumer(groupID string) (sarama.ConsumerGroup, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = "notification"
	tlsConfig, err1 := common.CreateTlSConfig(os.Getenv("KAFKA_USER_CERT_PATH"), os.Getenv("KAFKA_USER_KEY_PATH"), os.Getenv("KAFKA_CA_CERT_PATH"))
	if err1 != nil {
		return nil, err1
	}
	cfg.Net.TLS.Config = tlsConfig
	cfg.Net.TLS.Enable = true
	if os.Getenv("KAFKA_API_KEY") != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Version = 1
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		cfg.Net.SASL.User = os.Getenv("KAFKA_API_KEY")
		cfg.Net.SASL.Password = os.Getenv("KAFKA_API_SECRET")
		cfg.Net.SASL.Handshake = true
	}

	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	//if balance strategy need to be change flexible, use switch-case with config di
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	//this setting make possible to consume payload which is stored but not consumed for certain reason like worker internal down

	return sarama.NewConsumerGroup([]string{os.Getenv("KAFKA_ADDRESS")}, groupID, cfg)
}

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			log.Print("Kafka message incoming...")
			// a failed retry publish leaves the offset unmarked, the session restarts from the last commit
			if err := c.distinguishMessage(session.Context(), msg); err != nil {
				return err
			}
			session.MarkMessage(msg, "")
			continue
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *Consumer) GetMessage(topics []string) error {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for err := range c.consumerGroup.Errors() {
			log.Printf("Consumer group error: %v", err)
		}
	}()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if ctx.Err() != nil {
				return
			}
			if err := c.consumerGroup.Consume(ctx, topics, c); err != nil {
				return
			}
		}
	}()

	log.Println("Sarama consumer up and running")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	keepRunning := true
	consumptionIsPaused := false
	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(c.consumerGroup, &consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()

	if err := c.consumerGroup.Close(); err != nil {
		log.Printf("Error closing client: %v", err)
		return err
	}
	return nil
}

func toggleConsumptionFlow(consumerGroup sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		consumerGroup.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		consumerGroup.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

func (c *Consumer) distinguishMessage(ctx context.Context, message *sarama.ConsumerMessage) error {
	for _, h := range message.Headers {
		// a copy re-sent for another group's retry, this group handles the original record itself
		if string(h.Key) == "partitionId" && !strings.HasPrefix(string(h.Value), groupId+":") {
			return nil
		}
	}
	var t string
	for _, header := range message.Headers {
		if bytes.Equal(header.Key, []byte("taskType")) {
			t = string(header.Value)
			break
		}
	}
	var err error
	if t == "scheduled-notification" {
		var p common.NotificationScheduled
		err = json.Unmarshal(message.Value, &p)
		if err != nil {
			slog.Error("fail to unmarshal payload value", "err", err)
			return nil
		}
		if p.PartitionType != "online-conversation" {
			return nil
		}
		err = c.service.PreprocessScheduledNotification(ctx, p.PartitionId, p.Notifications, p.SharedContents)
	} else {
		var p common.PreparedMessage
		err = json.Unmarshal(message.Value, &p)
		if err != nil {
			slog.Error("fail to unmarshal payload value",
				"err", err,
				"payload.Value", message.Value)
			return nil
		}
		err = c.service.PreprocessMessageNotification(ctx, p.NotificationId, p.Id, p.ToIds, p.RoomId, p.FromId, p.ContentType, p.Contents)
	}
	if err != nil {
		e := common.RetryEvent{Reason: err.Error()}
		for _, h := range message.Headers {
			// a re-sent record carries the partition id of the group that failed, other groups treat it as a new record
			if string(h.Key) == "partitionId" && strings.HasPrefix(string(h.Value), groupId+":") {
				e.PartitionId = string(h.Value)
			}
		}
		if e.PartitionId == "" {
			e.PartitionId = fmt.Sprintf("%s:%s:%d:%d", groupId, message.Topic, message.Partition, message.Offset)
			e.Backoff = common.AddJitter(2000)
			e.Multiplier = 2
			e.Cap = 15000000
			e.MaxFailure = 5
			e.Topic = message.Topic
			e.Key = message.Key
			e.Headers = make(map[string][]byte, len(message.Headers))
			for _, h := range message.Headers {
				if string(h.Key) != "partitionId" {
					e.Headers[string(h.Key)] = h.Value
				}
			}
			e.Value = message.Value
		}
		// keyed by partition id so the events of one retry stay ordered in the job
		return c.producer.Commit("exponential-backoff-retry", []byte(e.PartitionId), common.Marshal(e), nil)
	}
	return nil
}
