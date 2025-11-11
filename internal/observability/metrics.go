package observability

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var (
	// Ingestor metrics
	IngestorRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ingestor_requests_total",
			Help: "Total number of ingest requests",
		},
		[]string{"method", "status"},
	)

	IngestorEventsReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "ingestor_events_received_total",
			Help: "Total number of log events received",
		},
	)

	IngestorEventsRejected = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ingestor_events_rejected_total",
			Help: "Total number of log events rejected",
		},
		[]string{"reason"},
	)

	IngestorKafkaProduceLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ingestor_kafka_produce_latency_ms",
			Help:    "Kafka produce latency in milliseconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1ms to ~1s
		},
	)

	IngestorBufferDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "ingestor_buffer_depth",
			Help: "Current depth of the ingestor buffer",
		},
	)

	// Processor metrics
	ProcessorEventsConsumed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "processor_events_consumed_total",
			Help: "Total number of events consumed from Kafka",
		},
	)

	ProcessorChunksWritten = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "processor_chunks_written_total",
			Help: "Total number of chunks written to S3",
		},
	)

	ProcessorS3WriteErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "processor_s3_write_errors_total",
			Help: "Total number of S3 write errors",
		},
	)

	ProcessorRedisIndexErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "processor_redis_index_errors_total",
			Help: "Total number of Redis index errors",
		},
	)

	ProcessorProcessingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "processor_processing_latency_ms",
			Help:    "Processing latency in milliseconds",
			Buckets: prometheus.ExponentialBuckets(10, 2, 10), // 10ms to ~10s
		},
	)

	// Query metrics
	QueryRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "query_requests_total",
			Help: "Total number of query requests",
		},
		[]string{"status"},
	)

	QueryLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "query_latency_ms",
			Help:    "Query latency in milliseconds",
			Buckets: prometheus.ExponentialBuckets(10, 2, 12), // 10ms to ~40s
		},
	)

	QueryCacheHits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "query_cache_hits_total",
			Help: "Total number of query cache hits",
		},
	)

	QueryCacheMisses = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "query_cache_misses_total",
			Help: "Total number of query cache misses",
		},
	)

	QueryS3ObjectsRead = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "query_s3_objects_read_total",
			Help: "Total number of S3 objects read",
		},
	)

	QueryEventsScanned = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "query_events_scanned_total",
			Help: "Total number of events scanned",
		},
	)
)

// RegisterMetrics registers all Prometheus metrics
func RegisterMetrics() {
	// Ingestor
	prometheus.MustRegister(IngestorRequestsTotal)
	prometheus.MustRegister(IngestorEventsReceived)
	prometheus.MustRegister(IngestorEventsRejected)
	prometheus.MustRegister(IngestorKafkaProduceLatency)
	prometheus.MustRegister(IngestorBufferDepth)

	// Processor
	prometheus.MustRegister(ProcessorEventsConsumed)
	prometheus.MustRegister(ProcessorChunksWritten)
	prometheus.MustRegister(ProcessorS3WriteErrors)
	prometheus.MustRegister(ProcessorRedisIndexErrors)
	prometheus.MustRegister(ProcessorProcessingLatency)

	// Query
	prometheus.MustRegister(QueryRequestsTotal)
	prometheus.MustRegister(QueryLatency)
	prometheus.MustRegister(QueryCacheHits)
	prometheus.MustRegister(QueryCacheMisses)
	prometheus.MustRegister(QueryS3ObjectsRead)
	prometheus.MustRegister(QueryEventsScanned)
}

// StartMetricsServer starts the Prometheus metrics HTTP server
func StartMetricsServer(port int) {
	http.Handle("/metrics", promhttp.Handler())

	addr := fmt.Sprintf(":%d", port)
	log.Info().Int("port", port).Msg("Starting metrics server")

	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatal().Err(err).Msg("Failed to start metrics server")
		}
	}()
}
