package models

import (
	"fmt"
	"strings"
	"time"
)

// LogLevel represents the severity level of a log entry
type LogLevel string

const (
	LevelDebug LogLevel = "DEBUG"
	LevelInfo  LogLevel = "INFO"
	LevelWarn  LogLevel = "WARN"
	LevelError LogLevel = "ERROR"
)

// ValidLogLevels contains all valid log levels
var ValidLogLevels = map[LogLevel]bool{
	LevelDebug: true,
	LevelInfo:  true,
	LevelWarn:  true,
	LevelError: true,
}

// LogEvent represents a single log entry in the system
type LogEvent struct {
	TenantID      string            `json:"tenant_id"`
	Service       string            `json:"service"`
	Host          string            `json:"host"`
	Timestamp     int64             `json:"timestamp"`      // epoch milliseconds
	Level         LogLevel          `json:"level"`
	Message       string            `json:"message"`
	Labels        map[string]string `json:"labels,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
	IngestionTime int64             `json:"ingestion_time"` // epoch milliseconds
}

// Validate checks if the LogEvent has all required fields and valid values
func (e *LogEvent) Validate() error {
	if strings.TrimSpace(e.TenantID) == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if strings.TrimSpace(e.Service) == "" {
		return fmt.Errorf("service is required")
	}
	if strings.TrimSpace(e.Host) == "" {
		return fmt.Errorf("host is required")
	}
	if strings.TrimSpace(e.Message) == "" {
		return fmt.Errorf("message is required")
	}

	// Validate timestamp (must be within reasonable bounds)
	if e.Timestamp <= 0 {
		return fmt.Errorf("timestamp must be positive")
	}

	// Check timestamp is not too far in the future (allow 1 hour clock skew)
	maxFutureTime := time.Now().Add(1 * time.Hour).UnixMilli()
	if e.Timestamp > maxFutureTime {
		return fmt.Errorf("timestamp is too far in the future")
	}

	// Validate log level
	if !ValidLogLevels[e.Level] {
		// Default to INFO if invalid
		e.Level = LevelInfo
	}

	return nil
}

// Normalize ensures consistent formatting of the log event
func (e *LogEvent) Normalize() {
	e.TenantID = strings.TrimSpace(e.TenantID)
	e.Service = strings.TrimSpace(e.Service)
	e.Host = strings.TrimSpace(e.Host)
	e.Message = strings.TrimSpace(e.Message)
	e.Level = LogLevel(strings.ToUpper(string(e.Level)))

	// Set ingestion time if not set
	if e.IngestionTime == 0 {
		e.IngestionTime = time.Now().UnixMilli()
	}
}
