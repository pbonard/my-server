package consumer

import (
	"backend/auth/internal/service"
	"backend/common"
	"backend/common/producer"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/joho/godotenv/autoload"
)

// Consumer takes the AuthEvents of this app's auth, from the topic of its keyspace (production.auth,
// holidayfriend.auth), so the auth of one app never sees the members of another
type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	service       service.Service
	producer      producer.SyncProducer
	// auth-<keyspace>: the auths of the apps are different groups, each with its own offsets
	groupId string
	topic   string
}

func NewConsumer(s service.Service, p producer.SyncProducer) *Consumer {
	keyspace := os.Getenv("PROFILE")
	groupId := "auth-" + keyspace
	consumerGroup, err := connectConsumer(groupId)
	if err != nil {
		log.Panicf("fail to create consumer group client: %v", err)
	}
	return &Consumer{
		consumerGroup: consumerGroup,
		service:       s,
		producer:      p,
		groupId:       groupId,
		topic:         common.AuthTopic(keyspace),
	}
}

func connectConsumer(groupID string) (sarama.ConsumerGroup, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = groupID
	tlsConfig, err := common.CreateTlSConfig(os.Getenv("KAFKA_USER_CERT_PATH"), os.Getenv("KAFKA_USER_KEY_PATH"), os.Getenv("KAFKA_CA_CERT_PATH"))
	if err != nil {
		return nil, err
	}
	cfg.Net.TLS.Config = tlsConfig
	cfg.Net.TLS.Enable = true

	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	// a ban published while auth was down is still done when it's back
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	return sarama.NewConsumerGroup([]string{os.Getenv("KAFKA_ADDRESS")}, groupID, cfg)
}

// Run consumes until the process ends. auth's http server is the process, so this runs beside it in a goroutine
func (c *Consumer) Run() {
	go func() {
		for err := range c.consumerGroup.Errors() {
			slog.Error("consumer group error", "err", err)
		}
	}()
	log.Printf("consume %s as %s", c.topic, c.groupId)
	for {
		// returns at every rebalance, and on an error after which it's tried again
		err := c.consumerGroup.Consume(context.Background(), []string{c.topic}, c)
		if err != nil {
			slog.Error("fail to consume", "err", err, "topic", c.topic)
			time.Sleep(time.Second)
		}
	}
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
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			// a failed retry publish leaves the offset unmarked, the session restarts from the last commit
			if err := c.distinguishMessage(session.Context(), msg); err != nil {
				return err
			}
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *Consumer) distinguishMessage(ctx context.Context, message *sarama.ConsumerMessage) error {
	for _, h := range message.Headers {
		// a copy re-sent for another group's retry, this group handles the original record itself
		if string(h.Key) == "partitionId" && !strings.HasPrefix(string(h.Value), c.groupId+":") {
			return nil
		}
	}
	var taskType string
	for _, h := range message.Headers {
		if string(h.Key) == common.TaskTypeHeader {
			taskType = string(h.Value)
			break
		}
	}
	var e common.AuthEvent
	err := json.Unmarshal(message.Value, &e)
	if err != nil {
		slog.Error("fail to unmarshal payload value",
			"err", err,
			"payload.Value", message.Value)
		return nil
	}
	id := e.Id
	switch taskType {
	case common.AuthTaskBan:
		err = c.service.BanMember(ctx, id)
	default:
		// none, or from a newer publisher than this auth: skipped rather than retried forever
		slog.Warn("unknown auth task type", "taskType", taskType)
		return nil
	}
	if err != nil {
		return c.retry(message, err)
	}
	return nil
}

// retry hands the record to the exponential backoff retry job, the same way the other consumers do
func (c *Consumer) retry(message *sarama.ConsumerMessage, err error) error {
	e := common.RetryEvent{Reason: err.Error()}
	for _, h := range message.Headers {
		// a re-sent record carries the partition id of the group that failed, other groups treat it as a new record
		if string(h.Key) == "partitionId" && strings.HasPrefix(string(h.Value), c.groupId+":") {
			e.PartitionId = string(h.Value)
		}
	}
	if e.PartitionId == "" {
		e.PartitionId = fmt.Sprintf("%s:%s:%d:%d", c.groupId, message.Topic, message.Partition, message.Offset)
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
