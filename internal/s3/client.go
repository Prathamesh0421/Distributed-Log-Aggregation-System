package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/prathamesh/log-aggregation-system/internal/config"
	"github.com/rs/zerolog/log"
)

// Client wraps the S3 client for object storage operations
type Client struct {
	s3Client   *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	bucket     string
}

// NewClient creates a new S3 client
// Works with both AWS S3 and MinIO (S3-compatible)
func NewClient(cfg *config.Config) (*Client, error) {
	// Create AWS session
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(cfg.S3.Region),
		Endpoint:         aws.String(cfg.S3.Endpoint), // For MinIO
		DisableSSL:       aws.Bool(!cfg.S3.UseSSL),
		S3ForcePathStyle: aws.Bool(true), // Required for MinIO
		Credentials: credentials.NewStaticCredentials(
			cfg.S3.AccessKeyID,
			cfg.S3.SecretAccessKey,
			"", // token (not needed for MinIO)
		),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create S3 session: %w", err)
	}

	// Create S3 service client
	s3Client := s3.New(sess)

	// Create uploader and downloader for efficient transfers
	uploader := s3manager.NewUploader(sess)
	downloader := s3manager.NewDownloader(sess)

	log.Info().
		Str("endpoint", cfg.S3.Endpoint).
		Str("bucket", cfg.S3.Bucket).
		Msg("S3 client created successfully")

	return &Client{
		s3Client:   s3Client,
		uploader:   uploader,
		downloader: downloader,
		bucket:     cfg.S3.Bucket,
	}, nil
}

// PutObject uploads data to S3
// The data is compressed with gzip before upload
//
// Parameters:
//   - ctx: Context for cancellation
//   - key: S3 object key (path)
//   - data: Uncompressed data to upload
//
// Returns:
//   - error: Any error during upload
func (c *Client) PutObject(ctx context.Context, key string, data []byte) error {
	// Compress the data with gzip
	// This significantly reduces storage costs and transfer time
	compressed, err := compressGzip(data)
	if err != nil {
		return fmt.Errorf("failed to compress data: %w", err)
	}

	// Upload to S3
	_, err = c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(compressed),
		ContentType: aws.String("application/gzip"),
		// Could add metadata here:
		// Metadata: map[string]*string{
		//     "tenant": aws.String(tenantID),
		// },
	})

	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	log.Debug().
		Str("key", key).
		Int("original_size", len(data)).
		Int("compressed_size", len(compressed)).
		Float64("compression_ratio", float64(len(data))/float64(len(compressed))).
		Msg("Uploaded object to S3")

	return nil
}

// GetObject downloads and decompresses data from S3
//
// Parameters:
//   - ctx: Context
//   - key: S3 object key
//
// Returns:
//   - []byte: Decompressed data
//   - error: Any error during download/decompression
func (c *Client) GetObject(ctx context.Context, key string) ([]byte, error) {
	// Download from S3 into a buffer
	buf := &aws.WriteAtBuffer{}

	_, err := c.downloader.DownloadWithContext(ctx, buf, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to download from S3: %w", err)
	}

	// Decompress the data
	decompressed, err := decompressGzip(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to decompress data: %w", err)
	}

	log.Debug().
		Str("key", key).
		Int("compressed_size", len(buf.Bytes())).
		Int("decompressed_size", len(decompressed)).
		Msg("Downloaded object from S3")

	return decompressed, nil
}

// DeleteObject deletes an object from S3
func (c *Client) DeleteObject(ctx context.Context, key string) error {
	_, err := c.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return fmt.Errorf("failed to delete from S3: %w", err)
	}

	return nil
}

// ListObjects lists objects with a given prefix
// Useful for debugging or manual cleanup
func (c *Client) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	result, err := c.s3Client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	keys := make([]string, 0, len(result.Contents))
	for _, obj := range result.Contents {
		if obj.Key != nil {
			keys = append(keys, *obj.Key)
		}
	}

	return keys, nil
}

// compressGzip compresses data using gzip
func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	// Create gzip writer
	// Use best compression for maximum space savings
	gzWriter, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}

	// Write data
	_, err = gzWriter.Write(data)
	if err != nil {
		return nil, err
	}

	// Must close to flush all data
	err = gzWriter.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// decompressGzip decompresses gzip data
func decompressGzip(data []byte) ([]byte, error) {
	// Create gzip reader
	gzReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()

	// Read all decompressed data
	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		return nil, err
	}

	return decompressed, nil
}
