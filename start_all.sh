#!/bin/bash

# Multi-Agent Labeling System - Complete System Starter
# Starts both backend and frontend services

echo "🚀 Multi-Agent Labeling System - Complete System Starter"
echo "========================================================="

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Function to check if a port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null; then
        return 0  # Port is in use
    else
        return 1  # Port is available
    fi
}

# Function to kill existing services
cleanup_existing() {
    echo "🧹 Cleaning up existing processes..."
    
    # Kill existing Python services
    pkill -f "python3.*main.py" 2>/dev/null || true
    
    # Kill existing npm dev servers
    pkill -f "npm run dev" 2>/dev/null || true
    
    # Kill any processes using port 8000
    lsof -ti:8000 | xargs kill -9 2>/dev/null || true
    
    # Kill any processes using port 5173
    lsof -ti:5173 | xargs kill -9 2>/dev/null || true
    
    sleep 3
}

# Cleanup existing services
cleanup_existing

# Start backend services
echo "🔧 Starting backend services..."
cd backend
./start_backend.sh &
BACKEND_PID=$!

# Wait a bit for backend to start
echo "⏳ Waiting for backend services to initialize..."
sleep 8

# Check if backend is running
if ! kill -0 $BACKEND_PID 2>/dev/null; then
    echo "❌ Backend failed to start!"
    exit 1
fi

# Start frontend
echo "🎨 Starting frontend..."
cd ../frontend

# Check if npm is available
if ! command -v npm &> /dev/null; then
    echo "❌ npm not found! Please install Node.js and npm"
    kill $BACKEND_PID 2>/dev/null || true
    exit 1
fi

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "📦 Installing frontend dependencies..."
    npm install
fi

# Start frontend development server
npm run dev &
FRONTEND_PID=$!

# Wait a bit for frontend to start
sleep 3

echo "=============================================="
echo "✅ All services started successfully!"
echo ""
echo "🔗 Access URLs:"
echo "   📡 API Gateway:  http://localhost:8000"
echo "   🎯 Frontend:     http://localhost:5173"
echo "   📊 Health Check: http://localhost:8000/health"
echo ""
echo "📋 Service Management:"
echo "   Backend PID:  $BACKEND_PID"
echo "   Frontend PID: $FRONTEND_PID"
echo ""
echo "🛑 Press Ctrl+C to stop all services"
echo "=============================================="

# Function to handle cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Shutting down all services..."
    
    # Kill frontend
    if kill -0 $FRONTEND_PID 2>/dev/null; then
        echo "🎨 Stopping frontend..."
        kill $FRONTEND_PID 2>/dev/null || true
    fi
    
    # Kill backend
    if kill -0 $BACKEND_PID 2>/dev/null; then
        echo "🔧 Stopping backend services..."
        kill $BACKEND_PID 2>/dev/null || true
    fi
    
    # Additional cleanup
    pkill -f "python3.*main.py" 2>/dev/null || true
    pkill -f "npm run dev" 2>/dev/null || true
    
    echo "✅ All services stopped"
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Keep the script running
while true; do
    # Check if services are still running
    if ! kill -0 $BACKEND_PID 2>/dev/null; then
        echo "❌ Backend process died!"
        cleanup
    fi
    
    if ! kill -0 $FRONTEND_PID 2>/dev/null; then
        echo "❌ Frontend process died!"
        cleanup
    fi
    
    sleep 5
done 