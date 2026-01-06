package main

import (
	"context"
	"flag"
	"math/rand"
	"time"

	ingestorv1 "github.com/prathamesh/log-aggregation-system/api/ingestor/v1"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Simple log generator that sends synthetic logs to the ingestor
// This is useful for testing and demonstration
func main() {
	// Command line flags
	var (
		ingestorAddr = flag.String("addr", "localhost:50051", "Ingestor gRPC address")
		tenantID     = flag.String("tenant", "demo-tenant", "Tenant ID")
		service      = flag.String("service", "demo-service", "Service name")
		rate         = flag.Int("rate", 10, "Events per second")
		duration     = flag.Int("duration", 60, "Duration in seconds (0 for unlimited)")
	)
	flag.Parse()

	log.Info().
		Str("addr", *ingestorAddr).
		Str("tenant", *tenantID).
		Str("service", *service).
		Int("rate", *rate).
		Msg("Starting log generator")

	// Connect to ingestor
	conn, err := grpc.NewClient(*ingestorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to ingestor")
	}
	defer conn.Close()

	client := ingestorv1.NewIngestorServiceClient(conn)

	// Create a streaming connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create stream")
	}

	// Generate and send logs
	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()

	var totalSent int
	startTime := time.Now()

	// Sample log messages
	messages := []string{
		"User login successful",
		"Database query executed",
		"Cache hit for key user:123",
		"API request processed",
		"Background job completed",
		"Connection timeout to external service",
		"Payment transaction processed",
		"Email sent successfully",
		"File uploaded to storage",
		"Authentication failed",
	}

	levels := []string{"DEBUG", "INFO", "WARN", "ERROR"}

	log.Info().Msg("Generating logs... (Press Ctrl+C to stop)")

	for {
		select {
		case <-ticker.C:
			// Generate a random log event
			event := &ingestorv1.LogEvent{
				TenantId:  *tenantID,
				Service:   *service,
				Host:      "generator-1",
				Timestamp: time.Now().UnixMilli(),
				Level:     levels[rand.Intn(len(levels))],
				Message:   messages[rand.Intn(len(messages))],
				Labels: map[string]string{
					"environment": "development",
					"version":     "1.0.0",
				},
			}

			// Send the event
			if err := stream.Send(event); err != nil {
				log.Error().Err(err).Msg("Failed to send event")
				// Try to close and exit
				stream.CloseSend()
				return
			}

			totalSent++

			// Log progress every 100 events
			if totalSent%100 == 0 {
				elapsed := time.Since(startTime)
				actualRate := float64(totalSent) / elapsed.Seconds()
				log.Info().
					Int("total_sent", totalSent).
					Float64("actual_rate", actualRate).
					Msg("Progress")
			}

			// Check if we've reached the duration limit
			if *duration > 0 && time.Since(startTime) >= time.Duration(*duration)*time.Second {
				log.Info().
					Int("total_sent", totalSent).
					Dur("elapsed", time.Since(startTime)).
					Msg("Duration limit reached, stopping")

				// Close the stream and get acknowledgement
				ack, err := stream.CloseAndRecv()
				if err != nil {
					log.Error().Err(err).Msg("Failed to receive ack")
				} else {
					log.Info().
						Int32("accepted", ack.AcceptedCount).
						Int32("rejected", ack.RejectedCount).
						Str("last_error", ack.LastError).
						Msg("Final acknowledgement")
				}
				return
			}
		}
	}
}
