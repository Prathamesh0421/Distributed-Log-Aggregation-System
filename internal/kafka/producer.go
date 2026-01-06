package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/prathamesh/log-aggregation-system/internal/config"
	"github.com/rs/zerolog/log"
)

// Producer wraps the Kafka producer with helper methods
// This abstraction makes it easier to:
// 1. Mock for testing
// 2. Add retry logic
// 3. Handle errors consistently
type Producer struct {
	producer sarama.SyncProducer // SyncProducer waits for ack before returning
	topic    string
}

// NewProducer creates a new Kafka producer
// Parameters:
//   - cfg: Configuration with broker addresses and settings
//
// Returns:
//   - *Producer: Configured producer ready to send messages
//   - error: Any error during setup
func NewProducer(cfg *config.Config) (*Producer, error) {
	// Sarama is the Go client library for Kafka
	// Create a new config with sensible defaults
	config := sarama.NewConfig()

	// Producer settings
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack
	config.Producer.Retry.Max = 5                     // Retry up to 5 times
	config.Producer.Return.Successes = true           // We want to know when it succeeds
	config.Producer.Compression = sarama.CompressionSnappy // Compress messages (saves bandwidth)

	// Network timeouts
	config.Net.DialTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second

	// Partitioning strategy
	// We'll use a custom partitioner later to ensure logs from same tenant go to same partition
	config.Producer.Partitioner = sarama.NewHashPartitioner

	// Create the actual producer connection
	producer, err := sarama.NewSyncProducer(cfg.Kafka.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	log.Info().
		Strs("brokers", cfg.Kafka.Brokers).
		Str("topic", cfg.Kafka.Topic).
		Msg("Kafka producer created successfully")

	return &Producer{
		producer: producer,
		topic:    cfg.Kafka.Topic,
	}, nil
}

// SendMessage sends a message to Kafka
// Parameters:
//   - ctx: Context for cancellation
//   - key: Kafka partition key (we use tenant_id + service for locality)
//   - value: The actual message bytes (JSON-encoded LogEvent)
//
// Returns:
//   - partition: Which Kafka partition received the message
//   - offset: The offset within that partition
//   - error: Any error during send
func (p *Producer) SendMessage(ctx context.Context, key, value []byte) (partition int32, offset int64, err error) {
	// Create a Kafka message
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.ByteEncoder(key),   // Key determines partition
		Value: sarama.ByteEncoder(value), // Actual payload
		// We could add headers here for metadata like trace IDs
	}

	// Send the message synchronously
	// This blocks until Kafka acks or times out
	partition, offset, err = p.producer.SendMessage(msg)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to send message to kafka: %w", err)
	}

	return partition, offset, nil
}

// SendBatch sends multiple messages in a batch
// This is more efficient than sending one at a time
// Parameters:
//   - ctx: Context for cancellation
//   - messages: Slice of (key, value) pairs to send
//
// Returns:
//   - error: Any error during batch send
func (p *Producer) SendBatch(ctx context.Context, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	// Convert our messages to Kafka format
	kafkaMessages := make([]*sarama.ProducerMessage, len(messages))
	for i, msg := range messages {
		kafkaMessages[i] = &sarama.ProducerMessage{
			Topic: p.topic,
			Key:   sarama.ByteEncoder(msg.Key),
			Value: sarama.ByteEncoder(msg.Value),
		}
	}

	// Send all messages at once
	err := p.producer.SendMessages(kafkaMessages)
	if err != nil {
		return fmt.Errorf("failed to send batch to kafka: %w", err)
	}

	log.Debug().
		Int("batch_size", len(messages)).
		Msg("Successfully sent batch to Kafka")

	return nil
}

// Close cleanly shuts down the producer
// Always call this when your application is shutting down
// to ensure all buffered messages are flushed
func (p *Producer) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}
	return nil
}

// Message represents a single message to send to Kafka
type Message struct {
	Key   []byte // Partition key
	Value []byte // Message payload
}
