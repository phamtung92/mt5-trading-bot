@echo off
setlocal
cd /d "%~dp0"

echo Installing MT5 Telegram bot...

python --version >nul 2>&1
if errorlevel 1 (
    echo Python is not installed or not in PATH.
    echo Install Python 3.10+ from https://www.python.org/downloads/
    pause
    exit /b 1
)

if not exist ".venv" (
    echo Creating virtual environment...
    python -m venv .venv
    if errorlevel 1 (
        echo Cannot create virtual environment.
        pause
        exit /b 1
    )
)

echo Upgrading pip...
".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 (
    echo Cannot upgrade pip.
    pause
    exit /b 1
)

echo Installing requirements...
".venv\Scripts\pip.exe" install -r requirements.txt
if errorlevel 1 (
    echo Cannot install requirements.
    pause
    exit /b 1
)

echo Installing MetaTrader MCP server...
".venv\Scripts\pip.exe" install metatrader-mcp-server
if errorlevel 1 (
    echo Cannot install metatrader-mcp-server.
    pause
    exit /b 1
)

if not exist ".env" (
    copy ".env.example" ".env" >nul
    echo Created .env from .env.example. Run CONFIG.bat to fill values.
)

echo Install completed.
pause
