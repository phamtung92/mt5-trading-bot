@echo off
setlocal
cd /d "%~dp0"

echo Configure MT5 Telegram bot
echo Leave optional values blank to use defaults.
echo.

set /p TELEGRAM_BOT_TOKEN=Telegram bot token: 
set /p ACTIVATION_CODE=Activation code: 
set /p MCP_URL=MCP URL [http://127.0.0.1:8080/sse]: 
if "%MCP_URL%"=="" set "MCP_URL=http://127.0.0.1:8080/sse"
set /p MCP_PORT=MCP port [8080]: 
if "%MCP_PORT%"=="" set "MCP_PORT=8080"

set /p MT5_LOGIN=MT5 login: 
set /p MT5_PASSWORD=MT5 password: 
set /p MT5_SERVER=MT5 server: 
set /p MT5_PATH=MT5 terminal path [blank = auto-detect]: 

set /p TRADE_SYMBOL=Trade symbol [XAUUSD]: 
if "%TRADE_SYMBOL%"=="" set "TRADE_SYMBOL=XAUUSD"

set /p DEFAULT_TIMEFRAME=Default timeframe [M5]: 
if "%DEFAULT_TIMEFRAME%"=="" set "DEFAULT_TIMEFRAME=M5"

set /p FIXED_LOT=Fixed lot [0.01]: 
if "%FIXED_LOT%"=="" set "FIXED_LOT=0.01"

set /p MAX_SPREAD_POINTS=Max spread points [80]: 
if "%MAX_SPREAD_POINTS%"=="" set "MAX_SPREAD_POINTS=80"

set /p MACHINE_ID=Machine ID override [blank = auto]: 
set /p ACTIVATION_ALERT_CHAT_ID=Telegram chat ID for activation errors [blank = none]: 

(
echo TELEGRAM_BOT_TOKEN=%TELEGRAM_BOT_TOKEN%
echo MCP_URL=%MCP_URL%
echo MCP_PORT=%MCP_PORT%
echo.
echo MT5_LOGIN=%MT5_LOGIN%
echo MT5_PASSWORD=%MT5_PASSWORD%
echo MT5_SERVER=%MT5_SERVER%
echo MT5_PATH=%MT5_PATH%
echo.
echo ACTIVATION_URL=http://110.172.29.4:3005/api/activate
echo ACTIVATION_CODE=%ACTIVATION_CODE%
echo MACHINE_ID=%MACHINE_ID%
echo ACTIVATION_TIMEOUT=15
echo ACTIVATION_ALERT_CHAT_ID=%ACTIVATION_ALERT_CHAT_ID%
echo.
echo TRADE_SYMBOL=%TRADE_SYMBOL%
echo TRADE_SYMBOL_ALIASES=XAUUSD,XAUUSDc,XAUUSDm,GOLD
echo XAUUSD_ALIASES=XAUUSD,XAUUSDc,XAUUSDm,GOLD
echo BTCUSD_ALIASES=BTCUSD,BTCUSDc,BTCUSDm,BITCOIN
echo DEFAULT_TIMEFRAME=%DEFAULT_TIMEFRAME%
echo.
echo FIXED_LOT=%FIXED_LOT%
echo RISK_PERCENT=0.5
echo MAX_SPREAD_POINTS=%MAX_SPREAD_POINTS%
echo MAX_DAILY_LOSS_PERCENT=3
echo EMERGENCY_SL_ATR_MULT=1.5
echo MIN_CONFIDENCE=60
echo FORCE_SIGNAL=true
echo ENFORCE_ENTRY_WINDOW=false
echo.
echo MAGIC_NUMBER=20260523
echo ORDER_COMMENT_PREFIX=XAU_CANDLE_AUTO
echo POLL_SECONDS=5
) > ".env"

echo Configuration saved to .env
pause
