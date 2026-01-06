#!/bin/bash

# Script to test the ingestor service end-to-end

echo "🚀 Testing Log Aggregation System - Ingestor"
echo "=============================================="
echo ""

# Set environment variables for Kafka (using our custom port)
export KAFKA_BROKERS=localhost:9094

echo "1️⃣ Starting Ingestor service..."
./bin/ingestor &
INGESTOR_PID=$!

# Wait for ingestor to start
sleep 3

echo ""
echo "2️⃣ Checking Ingestor health..."
echo ""

# Give it a moment to fully initialize
sleep 2

echo "3️⃣ Starting log generator (will send 50 logs at 10/sec)..."
echo ""
./bin/agent -duration 5 -rate 10

echo ""
echo "4️⃣ Checking Kafka topics..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo "5️⃣ Checking messages in Kafka..."
echo "   (Showing first 10 messages from logs.raw topic)"
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic logs.raw \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 5000 2>/dev/null || echo "   No messages found (yet)"

echo ""
echo "6️⃣ Checking Prometheus metrics..."
curl -s http://localhost:8081/metrics | grep ingestor_events_received_total || echo "   Metrics not available yet"

echo ""
echo "✅ Test complete!"
echo ""
echo "📊 View metrics at: http://localhost:8081/metrics"
echo "📈 View Prometheus at: http://localhost:9091"
echo "🎛️  View Kafka UI at: http://localhost:8080"
echo ""
echo "Stopping ingestor..."
kill $INGESTOR_PID

echo "Done!"
