package chunker

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/prathamesh/log-aggregation-system/internal/models"
)

// Chunk represents a group of log events
// Events are grouped by: tenant + service + time window
type Chunk struct {
	TenantID    string             `json:"tenant_id"`
	Service     string             `json:"service"`
	WindowStart int64              `json:"window_start"` // epoch ms
	WindowEnd   int64              `json:"window_end"`   // epoch ms
	Events      []*models.LogEvent `json:"events"`
	CreatedAt   int64              `json:"created_at"` // epoch ms
}

// Chunker groups log events into chunks based on time windows
// This makes storage and retrieval more efficient
type Chunker struct {
	windowSize time.Duration // How long each time window is (e.g., 5 minutes)
}

// NewChunker creates a new chunker with the specified window size
func NewChunker(windowSize time.Duration) *Chunker {
	return &Chunker{
		windowSize: windowSize,
	}
}

// GroupEvents groups a batch of events into chunks
// Each chunk contains events from the same tenant, service, and time window
//
// Example:
//   Input: 100 events from 2 tenants, 3 services, over 10 minutes
//   Window: 5 minutes
//   Output: ~6 chunks (2 tenants × 3 services × 2 time windows)
func (c *Chunker) GroupEvents(events []*models.LogEvent) []*Chunk {
	// Map to group events: key = "tenant:service:window", value = events
	groups := make(map[string][]*models.LogEvent)

	for _, event := range events {
		// Calculate which time window this event belongs to
		windowStart := c.getWindowStart(event.Timestamp)

		// Create a key to group events
		key := fmt.Sprintf("%s:%s:%d", event.TenantID, event.Service, windowStart)

		// Add event to the group
		groups[key] = append(groups[key], event)
	}

	// Convert groups to chunks
	chunks := make([]*Chunk, 0, len(groups))
	now := time.Now().UnixMilli()

	for _, groupEvents := range groups {
		if len(groupEvents) == 0 {
			continue
		}

		// Parse the key to get tenant, service, and window
		// (We could also store these separately, but this works)
		firstEvent := groupEvents[0]
		windowStart := c.getWindowStart(firstEvent.Timestamp)
		windowEnd := windowStart + c.windowSize.Milliseconds()

		chunk := &Chunk{
			TenantID:    firstEvent.TenantID,
			Service:     firstEvent.Service,
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
			Events:      groupEvents,
			CreatedAt:   now,
		}

		chunks = append(chunks, chunk)
	}

	return chunks
}

// getWindowStart calculates the start of the time window for a given timestamp
// Example with 5-minute windows:
//   timestamp = 10:23:45 → windowStart = 10:20:00
//   timestamp = 10:27:30 → windowStart = 10:25:00
func (c *Chunker) getWindowStart(timestampMs int64) int64 {
	windowSizeMs := c.windowSize.Milliseconds()

	// Round down to the nearest window boundary
	return (timestampMs / windowSizeMs) * windowSizeMs
}

// GenerateS3Key creates the S3 object key for a chunk
// Format: tenant/{tenant_id}/service/{service}/yyyy={YYYY}/mm={MM}/dd={DD}/hh={HH}/min={MIN}/chunk_{uuid}.json.gz
//
// This hierarchical structure enables:
// 1. Easy browsing by tenant/service
// 2. Time-based partitioning (efficient for range queries)
// 3. Parallel processing (different workers can handle different partitions)
func (c *Chunker) GenerateS3Key(chunk *Chunk) string {
	// Parse the window start time
	t := time.UnixMilli(chunk.WindowStart).UTC()

	// Generate a unique ID for this chunk
	chunkID := uuid.New().String()

	// Build the S3 key with time partitioning
	return fmt.Sprintf(
		"tenant/%s/service/%s/yyyy=%04d/mm=%02d/dd=%02d/hh=%02d/min=%02d/chunk_%s.json.gz",
		chunk.TenantID,
		chunk.Service,
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		chunkID,
	)
}

// SerializeChunk converts a chunk to JSON bytes
// This is what we'll store in S3 (after compression)
func (c *Chunker) SerializeChunk(chunk *Chunk) ([]byte, error) {
	// Use newline-delimited JSON for easier streaming
	// Each line is one log event
	// This makes it easy to process large files line by line
	var result []byte

	for _, event := range chunk.Events {
		eventJSON, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal event: %w", err)
		}

		// Add the event JSON and a newline
		result = append(result, eventJSON...)
		result = append(result, '\n')
	}

	return result, nil
}

// DeserializeChunk parses JSON bytes back into a chunk
// This is used when reading from S3
func (c *Chunker) DeserializeChunk(data []byte) ([]*models.LogEvent, error) {
	// Split by newlines
	lines := splitLines(data)

	events := make([]*models.LogEvent, 0, len(lines))

	for _, line := range lines {
		if len(line) == 0 {
			continue // Skip empty lines
		}

		var event models.LogEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal event: %w", err)
		}

		events = append(events, &event)
	}

	return events, nil
}

// splitLines splits data by newlines
func splitLines(data []byte) [][]byte {
	var lines [][]byte
	var currentLine []byte

	for _, b := range data {
		if b == '\n' {
			if len(currentLine) > 0 {
				lines = append(lines, currentLine)
				currentLine = nil
			}
		} else {
			currentLine = append(currentLine, b)
		}
	}

	// Add last line if not empty
	if len(currentLine) > 0 {
		lines = append(lines, currentLine)
	}

	return lines
}

// ChunkStats returns statistics about a chunk
// Useful for logging and monitoring
type ChunkStats struct {
	EventCount  int
	SizeBytes   int
	MinTime     int64
	MaxTime     int64
	TimeSpanSec float64
}

// GetStats calculates statistics for a chunk
func (c *Chunker) GetStats(chunk *Chunk) ChunkStats {
	if len(chunk.Events) == 0 {
		return ChunkStats{}
	}

	var minTime, maxTime int64
	minTime = chunk.Events[0].Timestamp
	maxTime = chunk.Events[0].Timestamp

	for _, event := range chunk.Events {
		if event.Timestamp < minTime {
			minTime = event.Timestamp
		}
		if event.Timestamp > maxTime {
			maxTime = event.Timestamp
		}
	}

	// Serialize to get size
	data, _ := c.SerializeChunk(chunk)

	return ChunkStats{
		EventCount:  len(chunk.Events),
		SizeBytes:   len(data),
		MinTime:     minTime,
		MaxTime:     maxTime,
		TimeSpanSec: float64(maxTime-minTime) / 1000.0,
	}
}
