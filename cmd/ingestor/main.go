package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	ingestorv1 "github.com/prathamesh/log-aggregation-system/api/ingestor/v1"
	"github.com/prathamesh/log-aggregation-system/internal/config"
	"github.com/prathamesh/log-aggregation-system/internal/kafka"
	"github.com/prathamesh/log-aggregation-system/internal/observability"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Load configuration from environment variables
	cfg := config.LoadConfig("ingestor")

	// Initialize structured logging
	observability.InitLogger(cfg.LogLevel)

	log.Info().
		Str("service", cfg.ServiceName).
		Str("environment", cfg.Environment).
		Msg("Starting Ingestor Service")

	// Register Prometheus metrics
	observability.RegisterMetrics()

	// Start metrics server in background
	// This exposes /metrics endpoint for Prometheus to scrape
	observability.StartMetricsServer(cfg.MetricsPort)

	// Create Kafka producer
	// This connects to Kafka and sets up the producer
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Kafka producer")
	}
	defer producer.Close()

	// Create the ingestor server
	// batchSize: 100 events per batch
	// batchTimer: Send batch every 1 second even if not full
	ingestorServer := NewIngestorServer(producer, cfg.Kafka.BatchSize, cfg.Kafka.BatchTimeout)
	defer ingestorServer.Shutdown()

	// Create gRPC server
	grpcServer := grpc.NewServer(
		// Add interceptors for logging, metrics, etc.
		grpc.UnaryInterceptor(unaryLoggingInterceptor),
		grpc.StreamInterceptor(streamLoggingInterceptor),
	)

	// Register our ingestor service
	ingestorv1.RegisterIngestorServiceServer(grpcServer, ingestorServer)

	// Enable gRPC reflection for tools like grpcurl
	// This makes it easier to test the service
	reflection.Register(grpcServer)

	// Start listening on the gRPC port
	addr := fmt.Sprintf(":%d", cfg.Server.GRPCPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal().Err(err).Str("addr", addr).Msg("Failed to listen")
	}

	// Start gRPC server in a goroutine
	go func() {
		log.Info().
			Str("addr", addr).
			Msg("gRPC server listening")

		if err := grpcServer.Serve(listener); err != nil {
			log.Fatal().Err(err).Msg("Failed to serve gRPC")
		}
	}()

	// Wait for interrupt signal (Ctrl+C or SIGTERM)
	// This is the graceful shutdown pattern
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan // Block until signal received

	log.Info().Msg("Shutdown signal received, initiating graceful shutdown...")

	// Graceful shutdown
	// 1. Stop accepting new requests
	grpcServer.GracefulStop()

	// 2. Close Kafka producer (flushes any buffered messages)
	if err := producer.Close(); err != nil {
		log.Error().Err(err).Msg("Error closing Kafka producer")
	}

	log.Info().Msg("Shutdown complete")
}

// unaryLoggingInterceptor logs all unary RPC calls
// This runs before your handler function
func unaryLoggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()

	// Call the actual handler
	resp, err := handler(ctx, req)

	// Log the call
	duration := time.Since(start)
	log.Info().
		Str("method", info.FullMethod).
		Dur("duration", duration).
		Err(err).
		Msg("gRPC unary call")

	return resp, err
}

// streamLoggingInterceptor logs streaming RPC calls
func streamLoggingInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()

	// Call the actual handler
	err := handler(srv, stream)

	// Log the call
	duration := time.Since(start)
	log.Info().
		Str("method", info.FullMethod).
		Dur("duration", duration).
		Err(err).
		Msg("gRPC stream call")

	return err
}
