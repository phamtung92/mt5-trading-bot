@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

set "PYTHON_EXE=.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"

if not exist ".env" (
    echo Missing .env. Run CONFIG.bat first.
    pause
    exit /b 1
)

for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
    if /i "%%A"=="MT5_LOGIN" set "MT5_LOGIN=%%B"
    if /i "%%A"=="MT5_PASSWORD" set "MT5_PASSWORD=%%B"
    if /i "%%A"=="MT5_SERVER" set "MT5_SERVER=%%B"
    if /i "%%A"=="MT5_PATH" set "MT5_PATH=%%B"
    if /i "%%A"=="MCP_PORT" set "MCP_PORT=%%B"
)
if "%MCP_PORT%"=="" set "MCP_PORT=8080"

if "%MT5_LOGIN%"=="" (
    echo Missing MT5_LOGIN in .env. Run CONFIG.bat first.
    pause
    exit /b 1
)
if "%MT5_PASSWORD%"=="" (
    echo Missing MT5_PASSWORD in .env. Run CONFIG.bat first.
    pause
    exit /b 1
)
if "%MT5_SERVER%"=="" (
    echo Missing MT5_SERVER in .env. Run CONFIG.bat first.
    pause
    exit /b 1
)

if exist "bot.pid" (
    set /p OLD_PID=<bot.pid
    if "!OLD_PID!"=="" (
        del /q "bot.pid" >nul 2>&1
    ) else (
        powershell -NoProfile -ExecutionPolicy Bypass -Command "if (Get-Process -Id !OLD_PID! -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }"
        if not errorlevel 1 (
            echo Bot is already running. PID=!OLD_PID!
            pause
            exit /b 0
        )
        del /q "bot.pid" >nul 2>&1
    )
)

set "MCP_RUNNING="
powershell -NoProfile -ExecutionPolicy Bypass -Command "if (Test-NetConnection -ComputerName 127.0.0.1 -Port %MCP_PORT% -InformationLevel Quiet) { exit 0 } else { exit 1 }" >nul 2>&1
if not errorlevel 1 set "MCP_RUNNING=1"

if "%MCP_RUNNING%"=="1" (
    echo MCP server already running on port %MCP_PORT%.
) else (
    echo Starting MT5 MCP server on port %MCP_PORT%...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "$args=@('--login','%MT5_LOGIN%','--password','%MT5_PASSWORD%','--server','%MT5_SERVER%','--transport','sse','--host','127.0.0.1','--port','%MCP_PORT%'); if ('%MT5_PATH%' -ne '') { $args += @('--path','%MT5_PATH%') }; $p=Start-Process -FilePath 'metatrader-mcp-server' -ArgumentList $args -WorkingDirectory '%~dp0' -RedirectStandardOutput '%~dp0mcp_stdout.log' -RedirectStandardError '%~dp0mcp_stderr.log' -WindowStyle Hidden -PassThru; Set-Content -Path '%~dp0mcp.pid' -Value $p.Id; Write-Host ('MCP server started. PID=' + $p.Id)"
    if errorlevel 1 (
        echo Cannot start MT5 MCP server.
        pause
        exit /b 1
    )
    timeout /t 3 /nobreak >nul
)

echo Starting MT5 Telegram bot...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$p=Start-Process -FilePath '%PYTHON_EXE%' -ArgumentList 'bot.py' -WorkingDirectory '%~dp0' -RedirectStandardOutput '%~dp0bot_stdout.log' -RedirectStandardError '%~dp0bot_stderr.log' -WindowStyle Hidden -PassThru; Set-Content -Path '%~dp0bot.pid' -Value $p.Id; Write-Host ('Bot started. PID=' + $p.Id)"

if errorlevel 1 (
    echo Cannot start bot.
    pause
    exit /b 1
)

echo Logs: bot_stdout.log, bot_stderr.log
echo MCP Logs: mcp_stdout.log, mcp_stderr.log
pause
