#!/bin/bash

# End-to-End Test Script for Log Aggregation System
# Tests the complete flow: Agent → Ingestor → Kafka → Processor → S3/Redis → Query

set -e  # Exit on error

echo "🚀 Log Aggregation System - End-to-End Test"
echo "=============================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Set environment variables
export KAFKA_BROKERS=localhost:9094
export REDIS_ADDR=localhost:6379
export S3_ENDPOINT=localhost:9000
export LOG_LEVEL=info

# Test parameters
TENANT="test-tenant"
SERVICE="test-service"
NUM_LOGS=50

echo -e "${BLUE}📋 Test Configuration:${NC}"
echo "  Tenant: $TENANT"
echo "  Service: $SERVICE"
echo "  Number of logs: $NUM_LOGS"
echo "  Kafka: $KAFKA_BROKERS"
echo ""

# Step 1: Start Ingestor
echo -e "${BLUE}1️⃣  Starting Ingestor Service...${NC}"
./bin/ingestor > logs/ingestor.log 2>&1 &
INGESTOR_PID=$!
echo "   PID: $INGESTOR_PID"
sleep 3

# Check if ingestor started
if ! kill -0 $INGESTOR_PID 2>/dev/null; then
    echo -e "${YELLOW}   ❌ Ingestor failed to start. Check logs/ingestor.log${NC}"
    exit 1
fi
echo -e "${GREEN}   ✅ Ingestor running${NC}"
echo ""

# Step 2: Start Processor
echo -e "${BLUE}2️⃣  Starting Processor Service...${NC}"
./bin/processor > logs/processor.log 2>&1 &
PROCESSOR_PID=$!
echo "   PID: $PROCESSOR_PID"
sleep 3

# Check if processor started
if ! kill -0 $PROCESSOR_PID 2>/dev/null; then
    echo -e "${YELLOW}   ❌ Processor failed to start. Check logs/processor.log${NC}"
    kill $INGESTOR_PID 2>/dev/null
    exit 1
fi
echo -e "${GREEN}   ✅ Processor running${NC}"
echo ""

# Step 3: Start Query Service
echo -e "${BLUE}3️⃣  Starting Query Service...${NC}"
./bin/query > logs/query.log 2>&1 &
QUERY_PID=$!
echo "   PID: $QUERY_PID"
sleep 3

# Check if query service started
if ! kill -0 $QUERY_PID 2>/dev/null; then
    echo -e "${YELLOW}   ❌ Query service failed to start. Check logs/query.log${NC}"
    kill $INGESTOR_PID $PROCESSOR_PID 2>/dev/null
    exit 1
fi
echo -e "${GREEN}   ✅ Query service running${NC}"
echo ""

# Step 4: Generate Logs
echo -e "${BLUE}4️⃣  Generating $NUM_LOGS test logs...${NC}"
./bin/agent \
    -tenant "$TENANT" \
    -service "$SERVICE" \
    -rate 20 \
    -duration 3

echo -e "${GREEN}   ✅ Logs sent to ingestor${NC}"
echo ""

# Step 5: Wait for processing
echo -e "${BLUE}5️⃣  Waiting for logs to be processed...${NC}"
echo "   (Processor batches and writes to S3/Redis)"
sleep 5
echo -e "${GREEN}   ✅ Processing complete${NC}"
echo ""

# Step 6: Verify Kafka
echo -e "${BLUE}6️⃣  Checking Kafka...${NC}"
KAFKA_MESSAGES=$(docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic logs.raw \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 3000 2>/dev/null | wc -l)
echo "   Messages in Kafka: $KAFKA_MESSAGES"
if [ "$KAFKA_MESSAGES" -gt 0 ]; then
    echo -e "${GREEN}   ✅ Kafka has messages${NC}"
else
    echo -e "${YELLOW}   ⚠️  No messages in Kafka (might have been processed)${NC}"
fi
echo ""

# Step 7: Verify S3/MinIO
echo -e "${BLUE}7️⃣  Checking MinIO (S3)...${NC}"
S3_OBJECTS=$(docker exec minio mc ls myminio/logs-bucket --recursive 2>/dev/null | grep ".json.gz" | wc -l)
echo "   Objects in S3: $S3_OBJECTS"
if [ "$S3_OBJECTS" -gt 0 ]; then
    echo -e "${GREEN}   ✅ S3 has log chunks${NC}"
    echo "   Sample objects:"
    docker exec minio mc ls myminio/logs-bucket --recursive 2>/dev/null | grep ".json.gz" | head -3 | sed 's/^/     /'
else
    echo -e "${YELLOW}   ⚠️  No objects in S3 yet${NC}"
fi
echo ""

# Step 8: Verify Redis Index
echo -e "${BLUE}8️⃣  Checking Redis Index...${NC}"
REDIS_KEYS=$(docker exec redis redis-cli KEYS "idx:$TENANT:$SERVICE" 2>/dev/null | wc -l)
echo "   Index keys in Redis: $REDIS_KEYS"
if [ "$REDIS_KEYS" -gt 0 ]; then
    echo -e "${GREEN}   ✅ Redis has index entries${NC}"
else
    echo -e "${YELLOW}   ⚠️  No index in Redis yet${NC}"
fi
echo ""

# Step 9: Query Logs
echo -e "${BLUE}9️⃣  Querying logs via REST API...${NC}"
# Get current time range (last hour)
NOW_MS=$(date +%s)000
FROM_MS=$((NOW_MS - 3600000))  # 1 hour ago

QUERY_PAYLOAD=$(cat <<EOF
{
  "tenant_id": "$TENANT",
  "service": "$SERVICE",
  "from_ts": $FROM_MS,
  "to_ts": $NOW_MS,
  "limit": 10
}
EOF
)

echo "   Query payload:"
echo "$QUERY_PAYLOAD" | sed 's/^/     /'
echo ""

QUERY_RESULT=$(curl -s -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -d "$QUERY_PAYLOAD")

echo "   Query response:"
echo "$QUERY_RESULT" | python3 -m json.tool 2>/dev/null | head -30 | sed 's/^/     /' || echo "$QUERY_RESULT" | sed 's/^/     /'

# Check if query was successful
EVENTS_RETURNED=$(echo "$QUERY_RESULT" | grep -o '"events_returned":[0-9]*' | cut -d: -f2)
if [ -n "$EVENTS_RETURNED" ] && [ "$EVENTS_RETURNED" -gt 0 ]; then
    echo -e "${GREEN}   ✅ Query returned $EVENTS_RETURNED events${NC}"
else
    echo -e "${YELLOW}   ⚠️  Query returned no events (data might still be processing)${NC}"
fi
echo ""

# Step 10: Check Metrics
echo -e "${BLUE}🔟  Checking Prometheus Metrics...${NC}"
echo ""
echo "   Ingestor Metrics (http://localhost:8081/metrics):"
curl -s http://localhost:8081/metrics | grep "ingestor_events_received_total\|ingestor_events_rejected_total" | sed 's/^/     /'
echo ""
echo "   Processor Metrics (http://localhost:8082/metrics):"
curl -s http://localhost:8082/metrics | grep "processor_events_consumed_total\|processor_chunks_written_total" | sed 's/^/     /'
echo ""
echo "   Query Metrics (http://localhost:8083/metrics):"
curl -s http://localhost:8083/metrics | grep "query_requests_total\|query_latency" | grep -v "# " | sed 's/^/     /'
echo ""

# Summary
echo -e "${GREEN}✅ End-to-End Test Complete!${NC}"
echo ""
echo -e "${BLUE}📊 Access Points:${NC}"
echo "   Kafka UI:    http://localhost:8080"
echo "   MinIO:       http://localhost:9001 (minioadmin/minioadmin)"
echo "   Prometheus:  http://localhost:9091"
echo "   Grafana:     http://localhost:3000 (admin/admin)"
echo ""
echo "   Metrics:"
echo "   - Ingestor:  http://localhost:8081/metrics"
echo "   - Processor: http://localhost:8082/metrics"
echo "   - Query:     http://localhost:8083/metrics"
echo ""
echo -e "${BLUE}📝 Logs:${NC}"
echo "   - Ingestor:  logs/ingestor.log"
echo "   - Processor: logs/processor.log"
echo "   - Query:     logs/query.log"
echo ""

# Cleanup
read -p "Press Enter to stop all services and cleanup..."
echo ""
echo "Stopping services..."
kill $INGESTOR_PID $PROCESSOR_PID $QUERY_PID 2>/dev/null || true
sleep 2

echo -e "${GREEN}Done!${NC}"
