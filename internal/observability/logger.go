package observability

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// InitLogger initializes the global logger with the specified log level
func InitLogger(level string) {
	// Set up pretty console logging for development
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// Parse and set log level
	logLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		logLevel = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(logLevel)
}

// GetLogger returns a logger with common fields pre-populated
func GetLogger(serviceName string) zerolog.Logger {
	return log.With().
		Str("service", serviceName).
		Logger()
}
