package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prathamesh/log-aggregation-system/internal/config"
	"github.com/rs/zerolog/log"
)

// Client wraps the Redis client with our domain-specific operations
type Client struct {
	client *redis.Client
}

// NewClient creates a new Redis client
func NewClient(cfg *config.Config) (*Client, error) {
	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,

		// Connection pool settings
		PoolSize:     10,
		MinIdleConns: 5,

		// Timeouts
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	log.Info().
		Str("addr", cfg.Redis.Addr).
		Msg("Redis client connected successfully")

	return &Client{client: client}, nil
}

// IndexChunk adds a chunk to the time-based index
// This allows fast lookups by time range
//
// Parameters:
//   - ctx: Context for cancellation
//   - tenantID: Tenant identifier
//   - service: Service name
//   - windowStartMs: Start of the time window (epoch ms)
//   - s3Key: S3 object key for this chunk
func (c *Client) IndexChunk(ctx context.Context, tenantID, service string, windowStartMs int64, s3Key string) error {
	// Redis sorted set key: idx:{tenant}:{service}
	// Score: window start timestamp
	// Value: S3 object key
	key := fmt.Sprintf("idx:%s:%s", tenantID, service)

	// Add to sorted set
	// ZADD is atomic - safe for concurrent writes
	err := c.client.ZAdd(ctx, key, &redis.Z{
		Score:  float64(windowStartMs),
		Member: s3Key,
	}).Err()

	if err != nil {
		return fmt.Errorf("failed to index chunk: %w", err)
	}

	// Also store metadata about this chunk
	// This helps with debugging and statistics
	metaKey := fmt.Sprintf("meta:%s", s3Key)
	err = c.client.HSet(ctx, metaKey, map[string]interface{}{
		"tenant_id": tenantID,
		"service":   service,
		"window_ms": windowStartMs,
		"created_at": time.Now().Unix(),
	}).Err()

	if err != nil {
		// This is not critical - we can still query without metadata
		log.Warn().Err(err).Str("s3_key", s3Key).Msg("Failed to store chunk metadata")
	}

	return nil
}

// FindChunks returns S3 keys for chunks in the given time range
//
// Parameters:
//   - ctx: Context
//   - tenantID: Tenant to query
//   - service: Service to query
//   - fromMs: Start of time range (inclusive)
//   - toMs: End of time range (inclusive)
//
// Returns:
//   - []string: S3 object keys to fetch
//   - error: Any error during query
func (c *Client) FindChunks(ctx context.Context, tenantID, service string, fromMs, toMs int64) ([]string, error) {
	key := fmt.Sprintf("idx:%s:%s", tenantID, service)

	// ZRANGEBYSCORE returns members with scores in the given range
	// This is very fast - O(log(N) + M) where M is the number of results
	result, err := c.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", fromMs),
		Max: fmt.Sprintf("%d", toMs),
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to find chunks: %w", err)
	}

	log.Debug().
		Str("tenant", tenantID).
		Str("service", service).
		Int("chunks_found", len(result)).
		Msg("Found chunks for query")

	return result, nil
}

// SetCache stores a value in cache with TTL
// This is useful for caching query results
func (c *Client) SetCache(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}

// GetCache retrieves a value from cache
// Returns redis.Nil if key doesn't exist
func (c *Client) GetCache(ctx context.Context, key string) ([]byte, error) {
	result, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		// Key doesn't exist - this is not an error
		return nil, nil
	}
	return result, err
}

// Close shuts down the Redis client
func (c *Client) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// GetClient returns the underlying Redis client
// Use this for operations not covered by our wrapper
func (c *Client) GetClient() *redis.Client {
	return c.client
}
