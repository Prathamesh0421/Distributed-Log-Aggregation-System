# Distributed Log Aggregation System

A production-grade, cloud-native distributed log aggregation platform built with Go, Kafka, Redis, and S3.

## Architecture Overview

```
┌─────────────┐      gRPC        ┌─────────────┐      Kafka      ┌─────────────┐
│             │ ──────────────▶  │             │ ──────────────▶ │             │
│ Log Agent   │                  │  Ingestor   │                 │  Processor  │
│  (Producer) │                  │   Service   │                 │   Service   │
└─────────────┘                  └─────────────┘                 └──────┬──────┘
                                        │                                │
                                        │                                ├──▶ S3 (MinIO)
                                        │                                │
                                        ▼                                └──▶ Redis Index
                                   Prometheus
                                    Metrics                          ┌─────────────┐
                                                                     │             │
                                                         REST API    │    Query    │
                                                         ◀───────────│   Service   │
                                                                     │             │
                                                                     └─────────────┘
```

## Features

- **High-throughput ingestion** via gRPC streaming
- **Horizontal scalability** via Kafka partitions
- **Efficient storage** with compressed chunks in S3
- **Fast queries** using time-based Redis indexing
- **Full observability** with Prometheus metrics and Grafana dashboards
- **Multi-tenancy** support with tenant isolation
- **Distributed tracing** support (trace_id propagation)

## Tech Stack

- **Language**: Go 1.21+
- **Messaging**: Apache Kafka (KRaft mode)
- **Indexing**: Redis
- **Storage**: S3-compatible (MinIO for local dev)
- **RPC**: gRPC with Protocol Buffers
- **External API**: REST (using chi router)
- **Metrics**: Prometheus
- **Visualization**: Grafana
- **Container**: Docker & Docker Compose
- **Orchestration**: Kubernetes (manifests included)

## Prerequisites

- Go 1.21 or higher
- Docker and Docker Compose
- Protocol Buffers compiler (`protoc`)
- `protoc-gen-go` and `protoc-gen-go-grpc` plugins

### Install Protocol Buffers Tools

```bash
# Install protoc (macOS)
brew install protobuf

# Install Go plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Ensure they're in your PATH
export PATH="$PATH:$(go env GOPATH)/bin"
```

## Quick Start

### 1. Clone and Setup

```bash
cd "Log Aggregation Systems"
make deps
```

### 2. Generate Protobuf Code

```bash
make proto
```

### 3. Start Infrastructure

```bash
make up
```

This starts:
- Kafka (port 9092)
- Kafka UI (port 8080)
- Redis (port 6379)
- MinIO (port 9000, console 9001)
- Prometheus (port 9090)
- Grafana (port 3000)

Wait ~30 seconds for all services to be healthy.

### 4. Verify Infrastructure

```bash
# Check Kafka topics
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Access UIs:
# - Kafka UI: http://localhost:8080
# - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
# - Prometheus: http://localhost:9090
# - Grafana: http://localhost:3000 (admin/admin)
```

### 5. Build Services

```bash
make build
```

### 6. Run Services

Open three terminals:

**Terminal 1 - Ingestor:**
```bash
make run-ingestor
```

**Terminal 2 - Processor:**
```bash
make run-processor
```

**Terminal 3 - Query Service:**
```bash
make run-query
```

### 7. Generate Test Logs

**Terminal 4 - Agent:**
```bash
make generate-logs
```

## Development

### Project Structure

```
.
├── api/                    # Protocol buffer definitions
│   └── ingestor/v1/
├── cmd/                    # Service entry points
│   ├── agent/             # Log generator
│   ├── ingestor/          # Ingestor service
│   ├── processor/         # Processor service
│   └── query/             # Query service
├── internal/              # Internal packages
│   ├── config/           # Configuration management
│   ├── kafka/            # Kafka producer/consumer
│   ├── redis/            # Redis index operations
│   ├── s3/               # S3 storage operations
│   ├── models/           # Data models
│   ├── index/            # Indexing logic
│   ├── chunker/          # Log chunking
│   └── observability/    # Metrics and logging
├── deploy/
│   ├── compose/          # Docker Compose files
│   ├── k8s/              # Kubernetes manifests
│   └── grafana/          # Grafana dashboards
├── test/
│   ├── integration/      # Integration tests
│   └── load/             # Load tests
└── README.md
```

### Available Make Targets

```bash
make help           # Show all available targets
make proto          # Generate protobuf code
make build          # Build all services
make up             # Start docker-compose stack
make down           # Stop docker-compose stack
make test           # Run unit tests
make lint           # Run golangci-lint
make clean          # Clean build artifacts
```

### Configuration

All services use environment variables for configuration. See `internal/config/config.go` for all options.

Example:
```bash
export KAFKA_BROKERS=localhost:9092
export REDIS_ADDR=localhost:6379
export S3_ENDPOINT=localhost:9000
export LOG_LEVEL=debug
```

## Services

### Ingestor Service

- **gRPC Port**: 50051
- **Metrics Port**: 8081
- Receives logs via gRPC streaming
- Validates and batches events
- Produces to Kafka with backpressure handling

### Processor Service

- **Metrics Port**: 8082
- Consumes from Kafka
- Chunks logs by tenant/service/time window
- Compresses with gzip
- Stores in S3 and updates Redis index

### Query Service

- **HTTP Port**: 8080
- **Metrics Port**: 8083
- REST API for log queries
- Uses Redis index to locate S3 objects
- Supports pagination and filtering

## API Reference

### Query API

**POST /v1/query**

Request:
```json
{
  "tenant_id": "tenant-1",
  "service": "api-service",
  "from_ts": 1700000000000,
  "to_ts": 1700003600000,
  "level": "ERROR",
  "contains": "timeout",
  "limit": 100
}
```

Response:
```json
{
  "items": [...],
  "next_cursor": "base64_encoded_cursor",
  "stats": {
    "objects_read": 5,
    "events_scanned": 1000,
    "duration_ms": 45
  }
}
```

## Testing

```bash
# Unit tests
make test

# Integration tests (requires docker-compose up)
make test-integration

# Load tests
cd test/load && go run main.go
```

## Monitoring

### Prometheus Metrics

All services expose metrics on their respective metrics ports:
- Ingestor: http://localhost:8081/metrics
- Processor: http://localhost:8082/metrics
- Query: http://localhost:8083/metrics

### Grafana Dashboards

Access Grafana at http://localhost:3000 (admin/admin) for pre-built dashboards.

## Deployment

### Local (Docker Compose)

```bash
make up
```

### Kubernetes

```bash
kubectl apply -f deploy/k8s/
```

Manifests include:
- Deployments for each service
- Services for networking
- ConfigMaps for configuration
- HorizontalPodAutoscalers for scaling

## Roadmap

- [x] Phase 1: MVP End-to-End
  - [x] Infrastructure setup
  - [x] Basic service implementations
  - [ ] Integration testing
- [ ] Phase 2: Hardening
  - [ ] DLQ implementation
  - [ ] Rate limiting per tenant
  - [ ] Graceful shutdown
  - [ ] Advanced pagination
- [ ] Phase 3: Advanced Search (Optional)
  - [ ] OpenSearch integration
  - [ ] Full-text search

## Contributing

This is a learning/portfolio project. Feel free to explore and modify!

## License

MIT License - See LICENSE file for details.
