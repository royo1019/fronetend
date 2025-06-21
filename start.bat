@echo off
echo 🚀 Starting CMDB Analyzer...

echo 📡 Starting Flask backend...
cd backend
start /B python app.py

echo Waiting for backend to start...
timeout /t 3 /nobreak >nul

echo 🎨 Starting React frontend...
cd ..\cmdb-analyzer
start /B npm run dev

echo ✅ Both servers started!
echo 📱 Frontend: http://localhost:5173
echo 🔧 Backend: http://localhost:5000
echo.
echo Press any key to stop both servers...
pause >nul

echo 🛑 Stopping servers...
taskkill /f /im python.exe >nul 2>&1
taskkill /f /im node.exe >nul 2>&1
echo ✅ Servers stopped 