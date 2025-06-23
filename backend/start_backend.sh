#!/bin/bash

# Multi-Agent Labeling System - Backend Starter
echo "🚀 Multi-Agent Labeling System - Backend Starter"
echo "=================================================="

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check Redis connection
echo "🔍 Checking Redis connection..."
if ! docker exec redis redis-cli ping > /dev/null 2>&1; then
    echo "❌ Redis not running! Starting Redis..."
    docker run -d --name redis -p 6379:6379 redis:latest
    sleep 3
fi
echo "✅ Redis connection verified"

# Activate virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "✅ Virtual environment activated"
else
    echo "❌ Virtual environment not found!"
    exit 1
fi

echo "🚀 Starting all backend services..."

# Start API Gateway
echo "📡 Starting API Gateway..."
cd api_gateway
python main.py &
API_GATEWAY_PID=$!
cd ..
sleep 2

# Start Mother AI
echo "🧠 Starting Mother AI..."
cd mother_ai
python main.py &
MOTHER_AI_PID=$!
cd ..
sleep 2

# Start Text Agent
echo "🤖 Starting Text Agent..."
cd agents/text_agent
python main.py &
TEXT_AGENT_PID=$!
cd ../..

echo "============================================================"
echo "✅ All services started successfully!"
echo "📡 API Gateway: http://localhost:8000"
echo "🎯 Frontend: http://localhost:5173 or http://localhost:5174"
echo "📊 Health Check: http://localhost:8000/health"
echo "============================================================"
echo "Press Ctrl+C to stop all services"

# Function to handle cleanup
cleanup() {
    echo ""
    echo "🛑 Shutting down backend services..."
    kill $API_GATEWAY_PID $MOTHER_AI_PID $TEXT_AGENT_PID 2>/dev/null || true
    echo "✅ Backend services stopped"
    exit 0
}

# Set up signal handler
trap cleanup SIGINT SIGTERM

# Keep script running
wait 