@echo off
setlocal enabledelayedexpansion

echo Starting AI Hedge Fund Development Environment...
echo.

:: Check if directories exist
if not exist "app\backend" (
    echo Error: app\backend directory not found!
    pause
    exit /b 1
)

if not exist "app\frontend" (
    echo Error: app\frontend directory not found!
    pause
    exit /b 1
)

:: Set up cleanup function
set "cleanup_done=false"

:: Start backend in a new window
echo Starting Backend...
start "AI-HedgeFund-Backend" cmd /k "cd /d app\backend && poetry run uvicorn main:app --reload"

:: Wait a moment for backend to start
timeout /t 2 /nobreak >nul

:: Start frontend in a new window
echo Starting Frontend...
start "AI-HedgeFund-Frontend" cmd /k "cd /d app\frontend && npm run dev"

echo.
echo Both services are starting in separate windows:
echo - Backend: http://localhost:8000 (FastAPI with auto-reload)
echo - Frontend: http://localhost:5173 (or check the frontend window for the actual port)
echo.
echo Choose an option:
echo [1] Keep services running and exit launcher
echo [2] Stop all services and close windows
echo [3] Just wait here (services keep running)
echo.
echo Or press Ctrl+C to force stop all services...

choice /c 123 /n /m "Enter your choice (1, 2, or 3): "

if errorlevel 3 goto :wait_mode
if errorlevel 2 goto :cleanup
if errorlevel 1 goto :exit_only

:wait_mode
echo.
echo Services are running. Press Ctrl+C to stop all services, or close this window to keep them running.
pause >nul
goto :cleanup

:cleanup
if "!cleanup_done!"=="true" goto :eof
set "cleanup_done=true"
echo.
echo Stopping all services...

:: Kill backend window
taskkill /fi "WindowTitle eq AI-HedgeFund-Backend*" /t /f >nul 2>&1

:: Kill frontend window  
taskkill /fi "WindowTitle eq AI-HedgeFund-Frontend*" /t /f >nul 2>&1

:: Also kill processes by name as backup
taskkill /im "uvicorn.exe" /f >nul 2>&1
taskkill /im "node.exe" /f >nul 2>&1

echo All services stopped and windows closed.
timeout /t 2 /nobreak >nul
goto :eof

:exit_only
echo Launcher closed. Services continue running in their windows.
goto :eof 