#!/bin/bash
# stop-all.sh — Stop all Apex platform services
echo "Stopping Apex platform..."

pkill -f "langgraph dev" 2>/dev/null && echo "✓ LangGraph stopped" || echo "  LangGraph was not running"
pkill -f "next dev"      2>/dev/null && echo "✓ Next.js stopped"   || echo "  Next.js was not running"

docker compose -f /tmp/apex-kafka/docker-compose.yml down 2>/dev/null && \
  echo "✓ Kafka stopped" || echo "  Kafka was not running"

echo "Done."