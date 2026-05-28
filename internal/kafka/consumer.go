package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/boeing/go-ledger-x/internal/repository"
)

const topicUserEvents = "user.events"

type UserEvent struct {
	EventType string `json:"event_type"`
	UserID    string `json:"user_id"`
}

type Consumer struct {
	reader   *kafka.Reader
	repo     *repository.WalletRepository
	producer *Producer // publishes wallet.created back to ledger.events
}

func NewConsumer(brokers []string, groupID string, repo *repository.WalletRepository, producer *Producer) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			GroupID:        groupID,
			Topic:          topicUserEvents,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0,
		}),
		repo:     repo,
		producer: producer,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	go c.loop(ctx)
}

func (c *Consumer) loop(ctx context.Context) {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("kafka fetch", "topic", topicUserEvents, "err", err)
			continue
		}

		if processErr := c.handle(ctx, msg); processErr != nil {
			slog.Error("handle user event", "err", processErr)
			continue
		}

		if err = c.reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("kafka commit", "err", err)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var event UserEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}

	if event.EventType != "user.created" {
		return nil
	}

	userID, err := strconv.ParseInt(event.UserID, 10, 64)
	if err != nil {
		return fmt.Errorf("parse user_id %q: %w", event.UserID, err)
	}

	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	wallet, err := c.repo.CreateWallet(tctx, userID)
	if err != nil {
		return err
	}

	slog.Info("wallet created from user event", "user_id", userID, "wallet_id", wallet.ID)

	// Publish wallet.created so user-service can store the wallet_id on the user.
	return c.producer.PublishTransactionEvent(ctx, TransactionEvent{
		EventType:     "wallet.created",
		TransactionID: wallet.ID,
		Amount:        0,
		Status:        "created",
	})
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
