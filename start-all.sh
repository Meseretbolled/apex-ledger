#!/bin/bash
# ═══════════════════════════════════════════════════════════════
#  start-all.sh — Start the full Apex AI Platform
#  Run from: ~/Documents/TRP1/week-5/apex-ledger
#
#  Starts:
#    1. Kafka + Zookeeper (Docker)
#    2. Kafka REST Proxy (Docker)
#    3. LangGraph dev server
#    4. Instructions for the Next.js website
# ═══════════════════════════════════════════════════════════════

set -e
LEDGER_DIR="$HOME/Documents/TRP1/week-5/apex-ledger"
UI_DIR="$HOME/Documents/TRP1/week-5/apex-ui"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║         Apex Financial Services — AI Platform           ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── 1. Check we're in the right place ───────────────────────────
if [ ! -f "$LEDGER_DIR/langgraph.json" ]; then
  echo "✗ langgraph.json not found in $LEDGER_DIR"
  echo "  Run this script from the apex-ledger directory."
  exit 1
fi

# ── 2. Kill any existing processes ──────────────────────────────
echo "→ Cleaning up old processes..."
pkill -f "langgraph dev" 2>/dev/null || true
pkill -f "next dev"      2>/dev/null || true
sleep 1
echo "  ✓ Done"

# ── 3. Start Kafka + Zookeeper via Docker ───────────────────────
echo ""
echo "→ Starting Kafka (Docker)..."

# Check if kafka container already running
if docker ps --format '{{.Names}}' | grep -q "apex-kafka"; then
  echo "  ✓ Kafka already running"
else
  # Create docker-compose file
  mkdir -p /tmp/apex-kafka
  cat > /tmp/apex-kafka/docker-compose.yml << 'COMPOSE'
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: apex-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: apex-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'

  kafka-rest:
    image: confluentinc/cp-kafka-rest:7.5.0
    container_name: apex-kafka-rest
    depends_on:
      - kafka
    ports:
      - "8082:8082"
    environment:
      KAFKA_REST_HOST_NAME: kafka-rest
      KAFKA_REST_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_REST_LISTENERS: http://0.0.0.0:8082
      KAFKA_REST_CORS_ALLOWED_ORIGINS: "*"
      KAFKA_REST_ACCESS_CONTROL_ALLOW_ORIGIN: "*"
      KAFKA_REST_ACCESS_CONTROL_ALLOW_METHODS: "GET,POST,PUT,DELETE,OPTIONS"
      KAFKA_REST_ACCESS_CONTROL_ALLOW_HEADERS: "origin,x-requested-with,content-type"
COMPOSE

  docker compose -f /tmp/apex-kafka/docker-compose.yml up -d
  echo "  ✓ Kafka starting... (takes ~15 seconds)"
  echo "  Waiting for Kafka to be ready..."
  sleep 18

  # Create the topics we need
  echo "  Creating Kafka topics..."
  docker exec apex-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 --partitions 3 \
    --topic documents.uploaded 2>/dev/null || true

  docker exec apex-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 --partitions 3 \
    --topic llm.requests 2>/dev/null || true

  docker exec apex-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 --partitions 6 \
    --topic agent.events 2>/dev/null || true

  docker exec apex-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 --partitions 3 \
    --topic documents.processed 2>/dev/null || true

  echo "  ✓ Kafka ready on port 9092"
  echo "  ✓ Kafka REST Proxy ready on port 8082"
fi

# ── 4. Update .env.local with Kafka URL ─────────────────────────
if [ -f "$UI_DIR/.env.local" ]; then
  if grep -q "^KAFKA_REST_URL=$" "$UI_DIR/.env.local" || grep -q "^KAFKA_REST_URL=http" "$UI_DIR/.env.local"; then
    sed -i 's|^KAFKA_REST_URL=.*|KAFKA_REST_URL=http://localhost:8082|' "$UI_DIR/.env.local"
    echo "  ✓ Updated KAFKA_REST_URL in apex-ui/.env.local"
  fi
fi

# Update apex-ledger .env too
if [ -f "$LEDGER_DIR/.env" ]; then
  if ! grep -q "KAFKA_REST_URL" "$LEDGER_DIR/.env"; then
    echo "KAFKA_REST_URL=http://localhost:8082" >> "$LEDGER_DIR/.env"
    echo "  ✓ Added KAFKA_REST_URL to apex-ledger/.env"
  else
    sed -i 's|^KAFKA_REST_URL=.*|KAFKA_REST_URL=http://localhost:8082|' "$LEDGER_DIR/.env"
  fi
fi

# ── 5. Start LangGraph in background ────────────────────────────
echo ""
echo "→ Starting LangGraph dev server..."
cd "$LEDGER_DIR"
source .venv/bin/activate

# Start LangGraph in background, log to file
nohup langgraph dev > /tmp/langgraph.log 2>&1 &
LANGGRAPH_PID=$!
echo "  ✓ LangGraph starting (PID $LANGGRAPH_PID)..."
sleep 5

# Check it's actually running
if kill -0 $LANGGRAPH_PID 2>/dev/null; then
  echo "  ✓ LangGraph running on http://127.0.0.1:2024"
  echo "  ✓ Studio UI: https://smith.langchain.com/studio/?baseUrl=http://127.0.0.1:2024"
else
  echo "  ✗ LangGraph failed to start. Check: cat /tmp/langgraph.log"
  cat /tmp/langgraph.log | tail -20
  exit 1
fi

# ── 6. Summary ───────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                   ✅  ALL SYSTEMS UP                    ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Kafka Broker       →  localhost:9092                   ║"
echo "║  Kafka REST Proxy   →  http://localhost:8082            ║"
echo "║  Kafka Dashboard    →  open kafka-dashboard.html        ║"
echo "║  LangGraph Server   →  http://127.0.0.1:2024            ║"
echo "║  LangSmith Studio   →  https://smith.langchain.com/...  ║"
echo "║  LangGraph logs     →  tail -f /tmp/langgraph.log       ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  NEXT STEP: Start the website in a new terminal:        ║"
echo "║                                                          ║"
echo "║  cd ~/Documents/TRP1/week-5/apex-ui                     ║"
echo "║  npm run dev                                             ║"
echo "║  → http://localhost:3000                                 ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  AGENTS AVAILABLE:                                       ║"
echo "║  • CreditAnalysisAgent    (running via LangGraph)        ║"
echo "║  • FraudDetectionAgent    (stub — events only)           ║"
echo "║  • ComplianceAgent        (stub — events only)           ║"
echo "║  • DecisionOrchestrator   (stub — events only)           ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  TO STOP EVERYTHING:                                     ║"
echo "║  ./stop-all.sh                                           ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── 7. Verify Kafka REST is responding ───────────────────────────
echo "→ Verifying Kafka REST Proxy..."
sleep 3
KAFKA_RESP=$(curl -s http://localhost:8082/topics 2>/dev/null || echo "not ready")
if echo "$KAFKA_RESP" | grep -q "\["; then
  echo "  ✓ Kafka REST responding — topics available"
else
  echo "  ⚠ Kafka REST not yet ready — it may take another 10-15 seconds"
  echo "    Check: curl http://localhost:8082/topics"
fi

echo ""
echo "→ Verifying LangGraph..."
LG_RESP=$(curl -s http://127.0.0.1:2024/info 2>/dev/null || echo "not ready")
if echo "$LG_RESP" | grep -q "version"; then
  echo "  ✓ LangGraph responding"
else
  echo "  ⚠ LangGraph not yet ready — check: tail -f /tmp/langgraph.log"
fi

echo ""
echo "Ready! Open http://localhost:3000 after starting the website."