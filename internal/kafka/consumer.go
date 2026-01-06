package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/prathamesh/log-aggregation-system/internal/config"
	"github.com/rs/zerolog/log"
)

// Consumer wraps the Kafka consumer group
// Consumer groups allow multiple instances to share the work
// Each partition is consumed by exactly one consumer in the group
type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	topics        []string
}

// MessageHandler is called for each message consumed
// You implement this function to process messages
type MessageHandler func(ctx context.Context, message *sarama.ConsumerMessage) error

// NewConsumer creates a new Kafka consumer
func NewConsumer(cfg *config.Config) (*Consumer, error) {
	config := sarama.NewConfig()

	// Consumer settings
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // Start from beginning if no offset exists
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.AutoCommit.Enable = false // We'll commit manually after processing

	// Session timeout - if consumer doesn't send heartbeat within this time, it's considered dead
	config.Consumer.Group.Session.Timeout = cfg.Kafka.SessionTimeout

	// Create consumer group
	// All consumers with same group ID will share the partitions
	consumerGroup, err := sarama.NewConsumerGroup(cfg.Kafka.Brokers, cfg.Kafka.ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	log.Info().
		Strs("brokers", cfg.Kafka.Brokers).
		Str("group", cfg.Kafka.ConsumerGroup).
		Str("topic", cfg.Kafka.Topic).
		Msg("Kafka consumer created successfully")

	return &Consumer{
		consumerGroup: consumerGroup,
		topics:        []string{cfg.Kafka.Topic},
	}, nil
}

// Consume starts consuming messages and calls handler for each
// This is a blocking call - run it in a goroutine if needed
func (c *Consumer) Consume(ctx context.Context, handler MessageHandler) error {
	// Create a consumer group handler
	h := &consumerGroupHandler{
		handler: handler,
	}

	// Consume is a blocking loop
	// It handles rebalancing automatically when consumers join/leave
	for {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Consume starts a consumer loop
		// It returns when context is cancelled or an error occurs
		err := c.consumerGroup.Consume(ctx, c.topics, h)
		if err != nil {
			log.Error().Err(err).Msg("Error from consumer")
			// Continue consuming unless context is cancelled
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Wait a bit before retrying
			time.Sleep(time.Second)
		}
	}
}

// Close shuts down the consumer gracefully
func (c *Consumer) Close() error {
	if c.consumerGroup != nil {
		return c.consumerGroup.Close()
	}
	return nil
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler interface
// This is the interface Kafka expects for handling messages
type consumerGroupHandler struct {
	handler MessageHandler
}

// Setup is called when a new session begins (e.g., after rebalancing)
func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Info().Msg("Consumer group session started")
	return nil
}

// Cleanup is called when a session ends
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Info().Msg("Consumer group session ended")
	return nil
}

// ConsumeClaim processes messages from a partition
// This is called once per partition assigned to this consumer
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Process messages from this partition
	for {
		select {
		case <-session.Context().Done():
			// Session is closing
			return nil

		case message, ok := <-claim.Messages():
			if !ok {
				// Channel closed, claim is finished
				return nil
			}

			// Call the handler function
			err := h.handler(session.Context(), message)
			if err != nil {
				log.Error().
					Err(err).
					Int32("partition", message.Partition).
					Int64("offset", message.Offset).
					Msg("Error processing message")
				// In production, you might want to send this to a DLQ here
				// For now, we just log and continue
			}

			// Mark the message as processed
			// This doesn't commit yet - just marks it for the next commit
			session.MarkMessage(message, "")

			// Commit the offset
			// This tells Kafka we've successfully processed up to this point
			// If we crash, we'll restart from the last committed offset
			session.Commit()
		}
	}
}
