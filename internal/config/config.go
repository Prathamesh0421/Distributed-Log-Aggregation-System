package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration for the services
type Config struct {
	// Service identification
	ServiceName string
	Environment string

	// Kafka configuration
	Kafka KafkaConfig

	// Redis configuration
	Redis RedisConfig

	// S3 configuration
	S3 S3Config

	// Server configuration
	Server ServerConfig

	// Observability
	MetricsPort int
	LogLevel    string
}

type KafkaConfig struct {
	Brokers        []string
	Topic          string
	DLQTopic       string
	ConsumerGroup  string
	BatchSize      int
	BatchTimeout   time.Duration
	ProducerFlush  time.Duration
	SessionTimeout time.Duration
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type S3Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
}

type ServerConfig struct {
	GRPCPort int
	HTTPPort int
}

// LoadConfig loads configuration from environment variables with defaults
func LoadConfig(serviceName string) *Config {
	return &Config{
		ServiceName: serviceName,
		Environment: getEnv("ENVIRONMENT", "development"),

		Kafka: KafkaConfig{
			Brokers:        parseStringSlice(getEnv("KAFKA_BROKERS", "localhost:9092")),
			Topic:          getEnv("KAFKA_TOPIC", "logs.raw"),
			DLQTopic:       getEnv("KAFKA_DLQ_TOPIC", "logs.dlq"),
			ConsumerGroup:  getEnv("KAFKA_CONSUMER_GROUP", "processor-group"),
			BatchSize:      getEnvInt("KAFKA_BATCH_SIZE", 100),
			BatchTimeout:   getEnvDuration("KAFKA_BATCH_TIMEOUT", 1*time.Second),
			ProducerFlush:  getEnvDuration("KAFKA_PRODUCER_FLUSH", 500*time.Millisecond),
			SessionTimeout: getEnvDuration("KAFKA_SESSION_TIMEOUT", 10*time.Second),
		},

		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getEnvInt("REDIS_DB", 0),
		},

		S3: S3Config{
			Endpoint:        getEnv("S3_ENDPOINT", "localhost:9000"),
			Region:          getEnv("S3_REGION", "us-east-1"),
			Bucket:          getEnv("S3_BUCKET", "logs-bucket"),
			AccessKeyID:     getEnv("S3_ACCESS_KEY_ID", "minioadmin"),
			SecretAccessKey: getEnv("S3_SECRET_ACCESS_KEY", "minioadmin"),
			UseSSL:          getEnvBool("S3_USE_SSL", false),
		},

		Server: ServerConfig{
			GRPCPort: getEnvInt("GRPC_PORT", 50051),
			HTTPPort: getEnvInt("HTTP_PORT", 8080),
		},

		MetricsPort: getEnvInt("METRICS_PORT", 8081),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
	}
}

// Helper functions to read environment variables

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func parseStringSlice(s string) []string {
	if s == "" {
		return []string{}
	}
	// Split by comma
	result := []string{}
	for _, v := range splitString(s, ',') {
		if trimmed := trimSpace(v); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func splitString(s string, sep rune) []string {
	var result []string
	var current string
	for _, char := range s {
		if char == sep {
			result = append(result, current)
			current = ""
		} else {
			current += string(char)
		}
	}
	result = append(result, current)
	return result
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n') {
		end--
	}
	return s[start:end]
}
