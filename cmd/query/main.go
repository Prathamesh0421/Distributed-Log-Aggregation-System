package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prathamesh/log-aggregation-system/internal/chunker"
	"github.com/prathamesh/log-aggregation-system/internal/config"
	"github.com/prathamesh/log-aggregation-system/internal/models"
	"github.com/prathamesh/log-aggregation-system/internal/observability"
	"github.com/prathamesh/log-aggregation-system/internal/redis"
	"github.com/prathamesh/log-aggregation-system/internal/s3"
	"github.com/rs/zerolog/log"
)

func main() {
	// Load configuration
	cfg := config.LoadConfig("query")

	// Initialize logging
	observability.InitLogger(cfg.LogLevel)

	log.Info().
		Str("service", cfg.ServiceName).
		Str("environment", cfg.Environment).
		Msg("Starting Query Service")

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

	// Create the query handler
	queryHandler := NewQueryHandler(s3Client, redisClient)

	// Create HTTP router
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	// Routes
	r.Get("/healthz", healthHandler)
	r.Get("/readyz", readyHandler)
	r.Post("/v1/query", queryHandler.HandleQuery)

	// Start HTTP server
	addr := fmt.Sprintf(":%d", cfg.Server.HTTPPort)
	server := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	// Start server in a goroutine
	go func() {
		log.Info().Str("addr", addr).Msg("HTTP server listening")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server failed")
		}
	}()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan

	log.Info().Msg("Shutdown signal received, stopping query service...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("Server shutdown error")
	}

	log.Info().Msg("Query service shutdown complete")
}

// QueryHandler handles query requests
type QueryHandler struct {
	s3Client    *s3.Client
	redisClient *redis.Client
	chunker     *chunker.Chunker
}

// NewQueryHandler creates a new query handler
func NewQueryHandler(s3Client *s3.Client, redisClient *redis.Client) *QueryHandler {
	return &QueryHandler{
		s3Client:    s3Client,
		redisClient: redisClient,
		chunker:     chunker.NewChunker(5 * time.Minute), // Same window as processor
	}
}

// QueryRequest represents a log query
type QueryRequest struct {
	TenantID string `json:"tenant_id"`
	Service  string `json:"service,omitempty"`
	FromTS   int64  `json:"from_ts"` // epoch ms
	ToTS     int64  `json:"to_ts"`   // epoch ms
	Level    string `json:"level,omitempty"`
	Contains string `json:"contains,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

// QueryResponse represents the query result
type QueryResponse struct {
	Items []*models.LogEvent `json:"items"`
	Stats QueryStats         `json:"stats"`
}

// QueryStats provides metadata about the query execution
type QueryStats struct {
	ObjectsRead    int   `json:"objects_read"`
	EventsScanned  int   `json:"events_scanned"`
	EventsReturned int   `json:"events_returned"`
	DurationMs     int64 `json:"duration_ms"`
}

// HandleQuery handles the /v1/query endpoint
func (h *QueryHandler) HandleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Parse request
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		observability.QueryRequestsTotal.WithLabelValues("error").Inc()
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if req.TenantID == "" {
		observability.QueryRequestsTotal.WithLabelValues("error").Inc()
		http.Error(w, "tenant_id is required", http.StatusBadRequest)
		return
	}

	if req.FromTS <= 0 || req.ToTS <= 0 {
		observability.QueryRequestsTotal.WithLabelValues("error").Inc()
		http.Error(w, "from_ts and to_ts are required", http.StatusBadRequest)
		return
	}

	if req.Limit == 0 {
		req.Limit = 100 // Default limit
	}
	if req.Limit > 1000 {
		req.Limit = 1000 // Max limit
	}

	log.Info().
		Str("tenant", req.TenantID).
		Str("service", req.Service).
		Int64("from", req.FromTS).
		Int64("to", req.ToTS).
		Msg("Query request received")

	// Execute query
	result, err := h.executeQuery(r.Context(), &req)
	if err != nil {
		observability.QueryRequestsTotal.WithLabelValues("error").Inc()
		log.Error().Err(err).Msg("Query failed")
		http.Error(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Record metrics
	duration := time.Since(start)
	observability.QueryLatency.Observe(float64(duration.Milliseconds()))
	observability.QueryRequestsTotal.WithLabelValues("success").Inc()

	result.Stats.DurationMs = duration.Milliseconds()

	// Return response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// executeQuery executes the actual query logic
func (h *QueryHandler) executeQuery(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
	stats := QueryStats{}

	// Step 1: Find relevant S3 objects using Redis index
	// If service is specified, query that specific index
	// Otherwise, we'd need to query all services (not implemented in MVP)
	if req.Service == "" {
		return nil, fmt.Errorf("service filter is required in MVP")
	}

	s3Keys, err := h.redisClient.FindChunks(ctx, req.TenantID, req.Service, req.FromTS, req.ToTS)
	if err != nil {
		return nil, fmt.Errorf("failed to find chunks: %w", err)
	}

	log.Debug().
		Int("num_objects", len(s3Keys)).
		Msg("Found S3 objects from index")

	// Step 2: Fetch and filter logs from S3
	var allEvents []*models.LogEvent

	for _, s3Key := range s3Keys {
		// Download and decompress the chunk
		data, err := h.s3Client.GetObject(ctx, s3Key)
		if err != nil {
			log.Error().Err(err).Str("s3_key", s3Key).Msg("Failed to fetch S3 object")
			continue // Skip this chunk
		}

		stats.ObjectsRead++
		observability.QueryS3ObjectsRead.Inc()

		// Deserialize events
		events, err := h.chunker.DeserializeChunk(data)
		if err != nil {
			log.Error().Err(err).Str("s3_key", s3Key).Msg("Failed to deserialize chunk")
			continue
		}

		// Filter events
		for _, event := range events {
			stats.EventsScanned++
			observability.QueryEventsScanned.Inc()

			// Apply filters
			if !h.matchesFilters(event, req) {
				continue
			}

			allEvents = append(allEvents, event)

			// Stop if we've hit the limit
			if len(allEvents) >= req.Limit {
				break
			}
		}

		// Stop fetching if we have enough events
		if len(allEvents) >= req.Limit {
			break
		}
	}

	stats.EventsReturned = len(allEvents)

	return &QueryResponse{
		Items: allEvents,
		Stats: stats,
	}, nil
}

// matchesFilters checks if an event matches the query filters
func (h *QueryHandler) matchesFilters(event *models.LogEvent, req *QueryRequest) bool {
	// Time range filter (already filtered by Redis, but double-check)
	if event.Timestamp < req.FromTS || event.Timestamp > req.ToTS {
		return false
	}

	// Tenant filter (required)
	if event.TenantID != req.TenantID {
		return false
	}

	// Service filter (required in MVP)
	if req.Service != "" && event.Service != req.Service {
		return false
	}

	// Level filter (optional)
	if req.Level != "" && string(event.Level) != req.Level {
		return false
	}

	// Text search filter (optional)
	if req.Contains != "" {
		if !strings.Contains(strings.ToLower(event.Message), strings.ToLower(req.Contains)) {
			return false
		}
	}

	return true
}

// healthHandler returns health status
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
}

// readyHandler returns readiness status
func readyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ready",
	})
}
