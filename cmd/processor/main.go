package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/prathamesh/log-aggregation-system/internal/chunker"
	"github.com/prathamesh/log-aggregation-system/internal/config"
	"github.com/prathamesh/log-aggregation-system/internal/kafka"
	"github.com/prathamesh/log-aggregation-system/internal/models"
	"github.com/prathamesh/log-aggregation-system/internal/observability"
	"github.com/prathamesh/log-aggregation-system/internal/redis"
	"github.com/prathamesh/log-aggregation-system/internal/s3"
	"github.com/rs/zerolog/log"
)

// Processor consumes log events from Kafka, processes them, and stores them
func main() {
	// Load configuration
	cfg := config.LoadConfig("processor")

	// Initialize logging
	observability.InitLogger(cfg.LogLevel)

	log.Info().
		Str("service", cfg.ServiceName).
		Str("environment", cfg.Environment).
		Msg("Starting Processor Service")

	// Register metrics
	observability.RegisterMetrics()
	observability.StartMetricsServer(cfg.MetricsPort)

	// Create clients
	s3Client, err := s3.NewClient(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create S3 client")
	}

	redisClient, err := redis.NewClient(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Redis client")
	}
	defer redisClient.Close()

	kafkaConsumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Kafka consumer")
	}
	defer kafkaConsumer.Close()

	// Create the processor
	processor := NewProcessor(s3Client, redisClient, cfg)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consuming in a goroutine
	go func() {
		log.Info().Msg("Starting Kafka consumer...")

		// The messageHandler is called for each Kafka message
		err := kafkaConsumer.Consume(ctx, func(ctx context.Context, message *sarama.ConsumerMessage) error {
			return processor.ProcessMessage(ctx, message)
		})

		if err != nil {
			log.Error().Err(err).Msg("Consumer stopped with error")
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan

	log.Info().Msg("Shutdown signal received, stopping processor...")

	// Cancel context to stop consumer
	cancel()

	// Give it a moment to finish processing current batch
	time.Sleep(2 * time.Second)

	log.Info().Msg("Processor shutdown complete")
}

// Processor handles the processing logic
type Processor struct {
	s3Client    *s3.Client
	redisClient *redis.Client
	chunker     *chunker.Chunker
	batchSize   int
	batchTimer  time.Duration
	batch       []*models.LogEvent
	lastFlush   time.Time
}

// NewProcessor creates a new processor
func NewProcessor(s3Client *s3.Client, redisClient *redis.Client, cfg *config.Config) *Processor {
	return &Processor{
		s3Client:    s3Client,
		redisClient: redisClient,
		chunker:     chunker.NewChunker(5 * time.Minute), // 5-minute windows
		batchSize:   cfg.Kafka.BatchSize,
		batchTimer:  cfg.Kafka.BatchTimeout,
		batch:       make([]*models.LogEvent, 0, cfg.Kafka.BatchSize),
		lastFlush:   time.Now(),
	}
}

// ProcessMessage processes a single message from Kafka
// This is called for each message consumed
func (p *Processor) ProcessMessage(ctx context.Context, message *sarama.ConsumerMessage) error {
	observability.ProcessorEventsConsumed.Inc()

	// Deserialize the log event
	var event models.LogEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		log.Error().
			Err(err).
			Str("partition", string(message.Partition)).
			Int64("offset", message.Offset).
			Msg("Failed to unmarshal event")
		// This event is corrupted - skip it
		// In production, you'd send to DLQ here
		return nil
	}

	// Add to batch
	p.batch = append(p.batch, &event)

	// Check if we should flush the batch
	shouldFlush := len(p.batch) >= p.batchSize ||
		time.Since(p.lastFlush) >= p.batchTimer

	if shouldFlush {
		return p.flushBatch(ctx)
	}

	return nil
}

// flushBatch processes and stores the current batch
// This is where the main work happens
func (p *Processor) flushBatch(ctx context.Context) error {
	if len(p.batch) == 0 {
		return nil
	}

	start := time.Now()

	log.Debug().
		Int("batch_size", len(p.batch)).
		Msg("Flushing batch")

	// Step 1: Group events into chunks by tenant/service/time-window
	chunks := p.chunker.GroupEvents(p.batch)

	log.Debug().
		Int("num_chunks", len(chunks)).
		Msg("Grouped events into chunks")

	// Step 2: Process each chunk
	for _, chunk := range chunks {
		if err := p.processChunk(ctx, chunk); err != nil {
			log.Error().
				Err(err).
				Str("tenant", chunk.TenantID).
				Str("service", chunk.Service).
				Msg("Failed to process chunk")
			// Continue with other chunks
			// In production, you might want to retry or send to DLQ
		}
	}

	// Clear the batch
	p.batch = p.batch[:0]
	p.lastFlush = time.Now()

	// Record metrics
	latency := time.Since(start).Milliseconds()
	observability.ProcessorProcessingLatency.Observe(float64(latency))

	log.Debug().
		Int64("latency_ms", latency).
		Int("chunks_processed", len(chunks)).
		Msg("Batch flushed successfully")

	return nil
}

// processChunk processes a single chunk:
// 1. Serialize to JSON
// 2. Upload to S3 (with compression)
// 3. Update Redis index
func (p *Processor) processChunk(ctx context.Context, chunk *chunker.Chunk) error {
	// Get statistics for logging
	stats := p.chunker.GetStats(chunk)

	log.Debug().
		Str("tenant", chunk.TenantID).
		Str("service", chunk.Service).
		Int("events", stats.EventCount).
		Int("size_bytes", stats.SizeBytes).
		Msg("Processing chunk")

	// Step 1: Serialize chunk to JSON (newline-delimited)
	data, err := p.chunker.SerializeChunk(chunk)
	if err != nil {
		return err
	}

	// Step 2: Generate S3 key
	s3Key := p.chunker.GenerateS3Key(chunk)

	// Step 3: Upload to S3 (S3 client will compress it)
	if err := p.s3Client.PutObject(ctx, s3Key, data); err != nil {
		observability.ProcessorS3WriteErrors.Inc()
		return err
	}

	observability.ProcessorChunksWritten.Inc()

	log.Info().
		Str("s3_key", s3Key).
		Str("tenant", chunk.TenantID).
		Str("service", chunk.Service).
		Int("events", stats.EventCount).
		Msg("Uploaded chunk to S3")

	// Step 4: Update Redis index
	// This allows fast lookups by time range
	if err := p.redisClient.IndexChunk(ctx, chunk.TenantID, chunk.Service, chunk.WindowStart, s3Key); err != nil {
		observability.ProcessorRedisIndexErrors.Inc()
		log.Error().
			Err(err).
			Str("s3_key", s3Key).
			Msg("Failed to update Redis index")
		// This is not critical - we can still query by scanning S3
		// But it will be slower
		// In production, you might want to write to a reconciliation queue
	}

	return nil
}
