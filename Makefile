.PHONY: help proto up down test lint run-ingestor run-processor run-query generate-logs clean build

# Default target
help:
	@echo "Available targets:"
	@echo "  make proto           - Generate protobuf and gRPC code"
	@echo "  make build           - Build all services"
	@echo "  make up              - Start docker-compose stack"
	@echo "  make down            - Stop docker-compose stack"
	@echo "  make test            - Run all tests"
	@echo "  make lint            - Run golangci-lint"
	@echo "  make run-ingestor    - Run ingestor service"
	@echo "  make run-processor   - Run processor service"
	@echo "  make run-query       - Run query service"
	@echo "  make generate-logs   - Run log generator"
	@echo "  make clean           - Clean build artifacts"

# Generate protobuf code
proto:
	@echo "Generating protobuf code..."
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/ingestor/v1/ingestor.proto

# Build all services
build:
	@echo "Building all services..."
	@mkdir -p bin
	go build -o bin/ingestor ./cmd/ingestor
	go build -o bin/processor ./cmd/processor
	go build -o bin/query ./cmd/query
	go build -o bin/agent ./cmd/agent

# Docker compose
up:
	docker-compose -f deploy/compose/docker-compose.yml up -d

down:
	docker-compose -f deploy/compose/docker-compose.yml down

# Testing
test:
	go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

test-integration:
	go test -v -tags=integration ./test/integration/...

# Linting
lint:
	golangci-lint run ./...

# Run services locally (assumes infrastructure is up)
run-ingestor:
	go run ./cmd/ingestor

run-processor:
	go run ./cmd/processor

run-query:
	go run ./cmd/query

generate-logs:
	go run ./cmd/agent

# Clean
clean:
	rm -rf bin/
	rm -f coverage.txt
	docker-compose -f deploy/compose/docker-compose.yml down -v

# Development helpers
deps:
	go mod download
	go mod tidy

fmt:
	go fmt ./...
	goimports -w .
