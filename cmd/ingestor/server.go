package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	ingestorv1 "github.com/prathamesh/log-aggregation-system/api/ingestor/v1"
	"github.com/prathamesh/log-aggregation-system/internal/kafka"
	"github.com/prathamesh/log-aggregation-system/internal/models"
	"github.com/prathamesh/log-aggregation-system/internal/observability"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IngestorServer implements the gRPC IngestorService
// This is the main entry point for log ingestion
type IngestorServer struct {
	// Embed the unimplemented server for forward compatibility
	// This ensures we implement all required methods
	ingestorv1.UnimplementedIngestorServiceServer

	producer    *kafka.Producer
	batchSize   int
	batchTimer  time.Duration
	bufferSize  int
	eventBuffer chan *models.LogEvent // Buffered channel for backpressure
	wg          sync.WaitGroup         // Wait for goroutines to finish
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewIngestorServer creates a new ingestor server
func NewIngestorServer(producer *kafka.Producer, batchSize int, batchTimer time.Duration) *IngestorServer {
	ctx, cancel := context.WithCancel(context.Background())

	server := &IngestorServer{
		producer:    producer,
		batchSize:   batchSize,
		batchTimer:  batchTimer,
		bufferSize:  batchSize * 10, // Buffer holds 10 batches worth of events
		eventBuffer: make(chan *models.LogEvent, batchSize*10),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Start the batch processor in background
	// This goroutine batches events and sends to Kafka
	server.wg.Add(1)
	go server.processBatches()

	return server
}

// StreamLogs handles streaming log ingestion from clients
// The client sends logs one by one, we batch them and send to Kafka
func (s *IngestorServer) StreamLogs(stream ingestorv1.IngestorService_StreamLogsServer) error {
	var acceptedCount, rejectedCount int32
	var lastError string

	// Receive logs from the stream
	for {
		// Receive one log event
		event, err := stream.Recv()

		// Check if stream is done
		if err == io.EOF {
			// Client closed the stream - send final ack
			return stream.SendAndClose(&ingestorv1.StreamAck{
				AcceptedCount: acceptedCount,
				RejectedCount: rejectedCount,
				LastError:     lastError,
			})
		}

		if err != nil {
			log.Error().Err(err).Msg("Error receiving from stream")
			return status.Errorf(codes.Internal, "failed to receive: %v", err)
		}

		// Convert protobuf to our domain model
		logEvent := protoToModel(event)

		// Validate the event
		if err := logEvent.Validate(); err != nil {
			rejectedCount++
			lastError = err.Error()
			observability.IngestorEventsRejected.WithLabelValues("validation_error").Inc()
			log.Warn().
				Err(err).
				Str("tenant", logEvent.TenantID).
				Msg("Invalid log event rejected")
			continue
		}

		// Normalize the event (trim spaces, set defaults, etc.)
		logEvent.Normalize()

		// Try to add to buffer
		select {
		case s.eventBuffer <- logEvent:
			// Successfully buffered
			acceptedCount++
			observability.IngestorEventsReceived.Inc()

		default:
			// Buffer is full - apply backpressure
			rejectedCount++
			lastError = "buffer full"
			observability.IngestorEventsRejected.WithLabelValues("buffer_full").Inc()
			log.Warn().
				Str("tenant", logEvent.TenantID).
				Msg("Event rejected: buffer full")
		}

		// Update buffer depth metric
		observability.IngestorBufferDepth.Set(float64(len(s.eventBuffer)))
	}
}

// IngestBatch handles batch ingestion (for high-throughput clients)
func (s *IngestorServer) IngestBatch(ctx context.Context, req *ingestorv1.IngestRequest) (*ingestorv1.IngestResponse, error) {
	observability.IngestorRequestsTotal.WithLabelValues("batch", "received").Inc()

	var acceptedCount, rejectedCount int32
	var lastError string

	for _, event := range req.Events {
		// Convert and validate
		logEvent := protoToModel(event)

		if err := logEvent.Validate(); err != nil {
			rejectedCount++
			lastError = err.Error()
			observability.IngestorEventsRejected.WithLabelValues("validation_error").Inc()
			continue
		}

		logEvent.Normalize()

		// Try to buffer
		select {
		case s.eventBuffer <- logEvent:
			acceptedCount++
			observability.IngestorEventsReceived.Inc()

		case <-ctx.Done():
			// Request cancelled
			return nil, status.Error(codes.Canceled, "request cancelled")

		default:
			rejectedCount++
			lastError = "buffer full"
			observability.IngestorEventsRejected.WithLabelValues("buffer_full").Inc()
		}
	}

	observability.IngestorBufferDepth.Set(float64(len(s.eventBuffer)))

	if rejectedCount > 0 {
		observability.IngestorRequestsTotal.WithLabelValues("batch", "partial").Inc()
	} else {
		observability.IngestorRequestsTotal.WithLabelValues("batch", "success").Inc()
	}

	return &ingestorv1.IngestResponse{
		AcceptedCount: acceptedCount,
		RejectedCount: rejectedCount,
		LastError:     lastError,
	}, nil
}

// HealthCheck returns the server health status
func (s *IngestorServer) HealthCheck(ctx context.Context, req *ingestorv1.HealthCheckRequest) (*ingestorv1.HealthCheckResponse, error) {
	// Check if buffer is getting too full
	bufferUsage := float64(len(s.eventBuffer)) / float64(s.bufferSize)

	if bufferUsage > 0.9 {
		return &ingestorv1.HealthCheckResponse{
			Status:  "unhealthy",
			Message: fmt.Sprintf("buffer is %.0f%% full", bufferUsage*100),
		}, nil
	}

	return &ingestorv1.HealthCheckResponse{
		Status:  "healthy",
		Message: "OK",
	}, nil
}

// processBatches runs in a goroutine and batches events before sending to Kafka
// This is more efficient than sending one event at a time
func (s *IngestorServer) processBatches() {
	defer s.wg.Done()

	// Create a timer for batch timeout
	timer := time.NewTimer(s.batchTimer)
	defer timer.Stop()

	batch := make([]*models.LogEvent, 0, s.batchSize)

	// Helper function to send current batch
	sendBatch := func() {
		if len(batch) == 0 {
			return
		}

		start := time.Now()

		// Convert batch to Kafka messages
		messages := make([]kafka.Message, len(batch))
		for i, event := range batch {
			// Serialize to JSON
			value, err := json.Marshal(event)
			if err != nil {
				log.Error().Err(err).Msg("Failed to marshal event")
				continue
			}

			// Use tenant_id + service as the key
			// This ensures logs from same tenant/service go to same partition
			// This maintains ordering within a tenant/service
			key := []byte(fmt.Sprintf("%s:%s", event.TenantID, event.Service))

			messages[i] = kafka.Message{
				Key:   key,
				Value: value,
			}
		}

		// Send to Kafka
		if err := s.producer.SendBatch(s.ctx, messages); err != nil {
			log.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to send batch to Kafka")
			// In production, you might want to retry or send to a DLQ
		} else {
			// Record latency
			latency := time.Since(start).Milliseconds()
			observability.IngestorKafkaProduceLatency.Observe(float64(latency))

			log.Debug().
				Int("batch_size", len(batch)).
				Int64("latency_ms", latency).
				Msg("Sent batch to Kafka")
		}

		// Clear the batch
		batch = batch[:0]

		// Reset timer
		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(s.batchTimer)
	}

	for {
		select {
		case <-s.ctx.Done():
			// Server is shutting down - send any remaining batch
			sendBatch()
			return

		case event := <-s.eventBuffer:
			// Add to batch
			batch = append(batch, event)

			// If batch is full, send it immediately
			if len(batch) >= s.batchSize {
				sendBatch()
			}

		case <-timer.C:
			// Timer expired - send whatever we have
			sendBatch()
			timer.Reset(s.batchTimer)
		}
	}
}

// Shutdown gracefully shuts down the server
// Call this when the application is terminating
func (s *IngestorServer) Shutdown() {
	log.Info().Msg("Shutting down ingestor server...")

	// Cancel context to stop batch processor
	s.cancel()

	// Wait for batch processor to finish
	s.wg.Wait()

	log.Info().Msg("Ingestor server shutdown complete")
}

// protoToModel converts protobuf LogEvent to our domain model
func protoToModel(pb *ingestorv1.LogEvent) *models.LogEvent {
	return &models.LogEvent{
		TenantID:      pb.TenantId,
		Service:       pb.Service,
		Host:          pb.Host,
		Timestamp:     pb.Timestamp,
		Level:         models.LogLevel(pb.Level),
		Message:       pb.Message,
		Labels:        pb.Labels,
		TraceID:       pb.TraceId,
		IngestionTime: pb.IngestionTime,
	}
}
