package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

const topicLedgerEvents = "ledger.events"

type TransactionEvent struct {
	EventType      string `json:"event_type"`
	TransactionID  int64  `json:"transaction_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Type           string `json:"type"`
	Amount         int64  `json:"amount"`
	Status         string `json:"status"`
}

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topicLedgerEvents,
			Balancer:     &kafka.LeastBytes{},
			WriteTimeout: 10 * time.Second,
			Async:        false,
		},
	}
}

func (p *Producer) PublishTransactionEvent(ctx context.Context, event TransactionEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", event.TransactionID)),
		Value: data,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
