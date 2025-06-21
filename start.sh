#!/bin/bash

echo "🚀 Starting CMDB Analyzer..."

# Start Flask backend in the background
echo "📡 Starting Flask backend..."
cd backend
python3 app.py &
BACKEND_PID=$!

# Wait a moment for backend to start
sleep 2

# Start React frontend
echo "🎨 Starting React frontend..."
cd ../cmdb-analyzer
npm run dev &
FRONTEND_PID=$!

echo "✅ Both servers started!"
echo "📱 Frontend: http://localhost:5173"
echo "🔧 Backend: http://localhost:5000"
echo ""
echo "Press Ctrl+C to stop both servers"

# Function to cleanup processes when script exits
cleanup() {
    echo ""
    echo "🛑 Stopping servers..."
    kill $BACKEND_PID 2>/dev/null
    kill $FRONTEND_PID 2>/dev/null
    echo "✅ Servers stopped"
    exit
}

# Trap Ctrl+C and cleanup
trap cleanup INT

# Wait for either process to exit
wait 