# XAUUSD Candle Auto Bot

Telegram bot dieu khien MT5 qua MCP de auto trade vang theo nen M1, M5, M15, H1.

## Chuc nang

- Inline menu giong signal bot: Analyze, chon symbol XAUUSD/BTCUSD, chon timeframe, chon lot, Auto ON/OFF, Status, Close Bot Positions.
- Auto trade theo dau nen va tu dong dong lenh khi nen ket thuc.
- Entry-window theo yeu cau:
  - M1: khong vao neu nen da chay qua 30 giay.
  - M5: khong vao neu nen da chay qua 3 phut.
  - M15: khong vao neu nen da chay qua 10 phut.
  - H1: khong vao neu nen da chay qua 40 phut.
- Mac dinh `ENFORCE_ENTRY_WINDOW=false`, nen cac moc tren chi la canh bao de bot luon co BUY/SELL khi analyze. Doi thanh `true` neu muon chan lenh tre dau nen.
- Loc bat buoc: spread, confidence, max daily loss, 1 bot position tai mot thoi diem.
- Moi lenh co `magic number` va `comment` rieng.
- Co emergency SL theo ATR.
- Chi dong lenh co magic/comment cua bot.

## MCP tools can co

Bot se tu thu nhieu ten tool pho bien, nhung MCP server nen co cac capability tuong duong:

- `get_account_info`
- `get_symbol_price` hoac `get_tick`
- `get_ohlcv` hoac `get_rates`
- `get_all_positions` hoac `get_open_positions`
- `place_market_order` hoac `open_position`/`order_send`
- `close_position`
- `get_deals` hoac `get_order_history`

Co the test danh sach tool:

```bash
python mcp_helper.py http://127.0.0.1:8080/sse list_tools
```

## Cai dat

```bash
INSTALL.bat
CONFIG.bat
START.bat
```

Dung `STOP.bat` de dung bot dang chay nen.

## Cau hinh quan trong

```env
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
MCP_URL=http://127.0.0.1:8080/sse
ACTIVATION_URL=http://110.172.29.4:3005/api/activate
ACTIVATION_CODE=your_activation_code_here
MACHINE_ID=
ACTIVATION_TIMEOUT=15
TRADE_SYMBOL=XAUUSD
TRADE_SYMBOL_ALIASES=XAUUSD,XAUUSDm,GOLD
XAUUSD_ALIASES=XAUUSD,XAUUSDm,GOLD
BTCUSD_ALIASES=BTCUSD,BTCUSDm,BITCOIN
DEFAULT_TIMEFRAME=M5
FIXED_LOT=0.01
MAX_SPREAD_POINTS=80
MAX_DAILY_LOSS_PERCENT=3
FORCE_SIGNAL=true
ENFORCE_ENTRY_WINDOW=false
MAGIC_NUMBER=20260523
ORDER_COMMENT_PREFIX=XAU_CANDLE_AUTO
```

Neu activation server tra ve `valid=false`, hoac khong co `ACTIVATION_CODE`, bot se log loi va thoat truoc khi polling Telegram. `MACHINE_ID` de trong thi bot tu tao id on dinh tu may hien tai.

Neu broker dung symbol `XAUUSDm`, de alias dau tien thanh `XAUUSDm`:

```env
TRADE_SYMBOL_ALIASES=XAUUSDm,XAUUSD,GOLD
```

Neu broker dung symbol crypto khac, sua alias BTC:

```env
BTCUSD_ALIASES=BTCUSDm,BTCUSD,BITCOIN
```

## Lenh Telegram

- `/start` hoac `/menu`: mo menu.
- `/analyze`: phan tich tin hieu hien tai.
- `/auto_on`: bat auto trade.
- `/auto_off`: tat auto trade.
- `/lot 0.03`: chon lot trade tuy y.
- `/taikhoan`: xem tai khoan.
- `/lenhmo`: xem lenh dang mo.

## Luu y

Day la bot dat lenh that neu MCP server co tool execution. Hay test tren demo truoc. Vang M1/M5 co spread va slippage cao, nen bat dau voi lot nho.
