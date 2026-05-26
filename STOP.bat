@echo off
setlocal
cd /d "%~dp0"

if not exist "bot.pid" (
    echo bot.pid not found. Trying to stop bot.py processes in this folder...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "$cwd=(Resolve-Path '%~dp0').Path.TrimEnd('\'); Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*bot.py*' -and $_.CommandLine -like ('*' + $cwd + '*') } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Host ('Stopped PID=' + $_.ProcessId) }"
    pause
    exit /b 0
)

set /p BOT_PID=<bot.pid
if "%BOT_PID%"=="" (
    del /q "bot.pid" >nul 2>&1
    echo Empty bot.pid removed.
    pause
    exit /b 0
)

powershell -NoProfile -ExecutionPolicy Bypass -Command "$p=Get-Process -Id %BOT_PID% -ErrorAction SilentlyContinue; if ($p) { Stop-Process -Id %BOT_PID% -Force; Write-Host 'Bot stopped.' } else { Write-Host 'Bot process is not running.' }"
del /q "bot.pid" >nul 2>&1
pause
