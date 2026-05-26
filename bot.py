#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram XAUUSD candle auto-trading bot for an MT5 MCP server.

The bot is intentionally conservative:
- Auto trade is explicit per Telegram chat.
- Every managed order uses a magic number and comment.
- Entries are blocked when the current candle is older than the configured limit.
- Positions are closed when the candle that opened them has finished.
"""

import asyncio
import atexit
import csv
import io
import json
import logging
import math
import os
import re
import socket
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from mcp import ClientSession
from mcp.client.sse import sse_client
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import NetworkError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MCP_URL = os.getenv("MCP_URL") or os.getenv("MCP_SERVER_URL", "http://127.0.0.1:8080/sse")
ACTIVATION_URL = os.getenv("ACTIVATION_URL", "http://110.172.29.4:3005/api/activate")
ACTIVATION_CODE = os.getenv("ACTIVATION_CODE", "").strip()
ACTIVATION_TIMEOUT = float(os.getenv("ACTIVATION_TIMEOUT", "15"))
MACHINE_ID = os.getenv("MACHINE_ID", "").strip()
ACTIVATION_ALERT_CHAT_ID = os.getenv("ACTIVATION_ALERT_CHAT_ID", "").strip()

SYMBOL = os.getenv("TRADE_SYMBOL", "XAUUSD").strip()
SYMBOL_ALIASES = [s.strip() for s in os.getenv("TRADE_SYMBOL_ALIASES", "XAUUSD,XAUUSDc,XAUUSDm,GOLD").split(",") if s.strip()]
SYMBOL_PROFILES = {
    "XAUUSD": [s.strip() for s in os.getenv("XAUUSD_ALIASES", "XAUUSD,XAUUSDc,XAUUSDm,GOLD,GOLDUSD").split(",") if s.strip()],
    "BTCUSD": [s.strip() for s in os.getenv("BTCUSD_ALIASES", "BTCUSD,BTCUSDc,BTCUSDm,BITCOIN,MBTUSD").split(",") if s.strip()],
    "EURUSD": ["EURUSD"],
    "GBPUSD": ["GBPUSD"],
    "USDJPY": ["USDJPY"],
    "USDCHF": ["USDCHF"],
    "USDCAD": ["USDCAD"],
    "AUDUSD": ["AUDUSD"],
    "NZDUSD": ["NZDUSD"],
    "EURJPY": ["EURJPY"],
    "GBPJPY": ["GBPJPY"],
    "EURGBP": ["EURGBP"],
    "AUDJPY": ["AUDJPY"],
    "CHFJPY": ["CHFJPY"],
    "CADJPY": ["CADJPY"],
    "EURAUD": ["EURAUD"],
    "GBPAUD": ["GBPAUD"],
    "AUDCAD": ["AUDCAD"],
}
SYMBOL_LABELS: dict[str, str] = {}
FOREX_CURRENCIES = (
    "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD",
    "CNH", "CNY", "HKD", "SGD", "NOK", "SEK", "DKK", "MXN",
    "ZAR", "TRY", "PLN", "CZK", "HUF",
)
DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME", "M5").upper()
FIXED_LOT = float(os.getenv("FIXED_LOT", "0.01"))
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "0.5"))
MAX_DAILY_LOSS_PERCENT = float(os.getenv("MAX_DAILY_LOSS_PERCENT", "0"))
MAX_SPREAD_POINTS = float(os.getenv("MAX_SPREAD_POINTS", "80"))
EMERGENCY_SL_ATR_MULT = float(os.getenv("EMERGENCY_SL_ATR_MULT", "1.5"))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "60"))
FORCE_SIGNAL = os.getenv("FORCE_SIGNAL", "true").lower() in ("1", "true", "yes", "on")
ENFORCE_ENTRY_WINDOW = os.getenv("ENFORCE_ENTRY_WINDOW", "false").lower() in ("1", "true", "yes", "on")
MAGIC_NUMBER = int(os.getenv("MAGIC_NUMBER", "20260523"))
ORDER_COMMENT_PREFIX = os.getenv("ORDER_COMMENT_PREFIX", "XAU_CANDLE_AUTO")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "5"))
TELEGRAM_CONNECT_TIMEOUT = float(os.getenv("TELEGRAM_CONNECT_TIMEOUT", "20"))
TELEGRAM_READ_TIMEOUT = float(os.getenv("TELEGRAM_READ_TIMEOUT", "60"))
TELEGRAM_WRITE_TIMEOUT = float(os.getenv("TELEGRAM_WRITE_TIMEOUT", "60"))
TELEGRAM_POOL_TIMEOUT = float(os.getenv("TELEGRAM_POOL_TIMEOUT", "20"))
TELEGRAM_GET_UPDATES_TIMEOUT = int(os.getenv("TELEGRAM_GET_UPDATES_TIMEOUT", "30"))
TELEGRAM_GET_UPDATES_READ_TIMEOUT = float(os.getenv("TELEGRAM_GET_UPDATES_READ_TIMEOUT", "75"))

TIMEFRAMES = {
    "M1": {"seconds": 60, "max_age": 30},
    "M5": {"seconds": 300, "max_age": 180},
    "M15": {"seconds": 900, "max_age": 600},
    "H1": {"seconds": 3600, "max_age": 2400},
}

AUTO_TASKS: dict[int, asyncio.Task] = {}
TRADE_MONITOR_TASKS: dict[int, asyncio.Task] = {}
CHAT_SETTINGS: dict[int, dict[str, Any]] = {}
ACTIVE_TRADES: dict[int, dict[str, Any]] = {}
STATE_FILE = os.path.join(os.path.dirname(__file__), "active_trades.json")
INSTANCE_LOCK_FILE = os.path.join(os.path.dirname(__file__), ".bot_instance.lock")
INSTANCE_LOCK_HANDLE = None


@dataclass
class SignalResult:
    direction: str
    confidence: int
    reasons: list[str]
    trend: str
    momentum: str
    volatility: str
    rsi: float
    atr: float
    spread_points: float
    candle_age: int
    candle_remaining: int
    candle_open_ts: int


class MT5MCPClient:
    """Async MT5 MCP client dùng raw httpx SSE calls"""

    def __init__(self, mcp_url: str):
        self.mcp_url = mcp_url.rstrip('/')
        # Extract base URL (remove /sse if present)
        if self.mcp_url.endswith('/sse'):
            self.base_url = self.mcp_url[:-4]  # Remove '/sse'
        else:
            self.base_url = self.mcp_url
        self._client: Optional[httpx.AsyncClient] = None
        self._session_id: Optional[str] = None
        self._lock = asyncio.Lock()
        self._message_id = 0

    async def _ensure_client(self):
        """Đảm bảo có httpx client"""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=60.0)

    async def _disconnect(self):
        """Đóng connection"""
        if self._client:
            await self._client.aclose()
            self._client = None
        self._session_id = None

    async def _establish_session(self):
        """Establish SSE session và lấy session_id"""
        await self._ensure_client()

        try:
            # GET /sse để lấy session_id
            async with self._client.stream('GET', self.mcp_url) as response:
                response.raise_for_status()

                # Parse SSE events để lấy endpoint
                async for line in response.aiter_lines():
                    if line.startswith('data: '):
                        data = line[6:]  # Remove 'data: ' prefix
                        try:
                            event_data = json.loads(data)
                            if 'endpoint' in event_data:
                                # Extract session_id from endpoint URL
                                endpoint = event_data['endpoint']
                                if '?sessionId=' in endpoint:
                                    self._session_id = endpoint.split('?sessionId=')[1].split('&')[0]
                                elif '/sse/' in endpoint:
                                    self._session_id = endpoint.split('/sse/')[1].split('?')[0]
                                else:
                                    # Generate session ID if not provided
                                    self._session_id = str(uuid.uuid4())
                                logger.info(f"MCP session established: {self._session_id}")
                                return
                        except json.JSONDecodeError:
                            continue

                # Fallback: generate session ID if can't parse from response
                self._session_id = str(uuid.uuid4())
                logger.warning(f"Generated fallback session ID: {self._session_id}")

        except Exception as e:
            logger.error(f"Failed to establish MCP session: {e}")
            # Fallback: use generated session ID
            self._session_id = str(uuid.uuid4())
            logger.warning(f"Using fallback session ID after error: {self._session_id}")

    def _parse_csv_to_json(self, csv_text: str) -> list[dict]:
        """Parse CSV to JSON list"""
        if not csv_text or csv_text.strip() == "":
            return []
        try:
            lines = csv_text.strip().split('\n')
            if len(lines) <= 1:
                return []
            reader = csv.DictReader(io.StringIO(csv_text))
            rows = list(reader)
            if not rows:
                return []
            result = []
            for row in rows:
                if '' in row:
                    del row['']
                if not any(row.values()):
                    continue
                for key in row:
                    if row[key] and row[key].strip():
                        try:
                            if '.' in str(row[key]):
                                row[key] = float(row[key])
                            else:
                                row[key] = int(row[key])
                        except (ValueError, TypeError):
                            pass
                result.append(row)
            return result
        except Exception:
            return []

    def _parse_tool_result(self, result: Any) -> Any:
        """Normalize MCP SDK tool results into dict/list/text."""
        if hasattr(result, "content") and result.content:
            if len(result.content) > 1:
                texts = [content.text for content in result.content if hasattr(content, "text")]
                if texts:
                    return texts
            content = result.content[0]
            if hasattr(content, "text"):
                text = content.text
                if not text or text.strip() == "":
                    return []
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    pass
                if "," in text:
                    return self._parse_csv_to_json(text)
                return text
            return str(content)
        return []

    async def _is_mcp_reachable(self) -> bool:
        parsed = urlparse(self.mcp_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)

        def can_connect() -> bool:
            try:
                with socket.create_connection((host, port), timeout=1.5):
                    return True
            except OSError:
                return False

        return await asyncio.to_thread(can_connect)

    async def _call_tool(self, tool_name: str, arguments: Optional[dict] = None, max_retries: int = 3) -> Any:
        """Call MCP tool with retry logic using the SDK SSE transport."""
        async with self._lock:
            if not await self._is_mcp_reachable():
                return {"error": f"MCP server is not reachable at {self.mcp_url}. Start metatrader-mcp-server on port 8080 first."}

            for attempt in range(max_retries):
                try:
                    async with sse_client(self.mcp_url) as (read, write):
                        async with ClientSession(read, write) as session:
                            await session.initialize()
                            result = await session.call_tool(tool_name, arguments or {})
                            return self._parse_tool_result(result)

                except Exception as e:
                    error_str = str(e).lower()
                    if "connection" in error_str or "timeout" in error_str or "taskgroup" in error_str or "unhandled" in error_str:
                        if attempt < max_retries - 1:
                            logger.warning(f"MCP connection error (attempt {attempt + 1}/{max_retries}), reconnecting: {e}")
                            await asyncio.sleep(2)
                            continue
                    logger.error(f"MCP tool call failed: {tool_name} - {e}")
                    return {"error": str(e)}

            return {"error": "Max retries exceeded"}

    async def _call_first(self, candidates: list[tuple[str, dict]]) -> Any:
        """Try multiple tool names until one succeeds"""
        errors = []
        for tool_name, args in candidates:
            result = await self._call_tool(tool_name, args)
            if not self._is_error(result):
                return result
            errors.append(f"{tool_name}: {result.get('error')}")
        return {"error": " | ".join(errors)}

    @staticmethod
    def _is_error(result: Any) -> bool:
        return isinstance(result, dict) and result.get("error")

    @staticmethod
    def _error_message(result: dict) -> Any:
        error = result.get("error")
        return result.get("message") or result.get("retcode") or (error if error not in (True, False) else "Unknown MCP error")

    async def get_account_info(self) -> dict:
        result = await self._call_first([
            ("get_account_info", {}),
            ("account_info", {}),
        ])
        return result if isinstance(result, dict) else {}

    async def get_symbol_price(self, symbol: str) -> dict:
        result = await self._call_tool("get_symbol_price", {"symbol_name": symbol})
        return result if isinstance(result, dict) else {}

    async def get_symbols(self, group: str = "*") -> list[str]:
        result = await self._call_first([
            ("get_symbols", {"group": group}),
            ("get_all_symbols", {}),
        ])
        if isinstance(result, list):
            symbols = []
            for item in result:
                if isinstance(item, str):
                    symbols.append(item.strip())
                elif isinstance(item, dict):
                    value = item.get("symbol") or item.get("name") or item.get("Symbol") or item.get("Name")
                    if value:
                        symbols.append(str(value).strip())
            return [s for s in symbols if s]
        if isinstance(result, str):
            return [line.strip() for line in result.replace(",", "\n").splitlines() if line.strip()]
        return []

    async def get_ohlcv(self, symbol: str, timeframe: str, count: int = 120) -> list[dict]:
        result = await self._call_tool(
            "get_candles_latest",
            {"symbol_name": symbol, "timeframe": normalize_timeframe(timeframe), "count": count},
        )
        if isinstance(result, list):
            return [r for r in result if isinstance(r, dict)]
        if isinstance(result, dict):
            for key in ("rates", "ohlcv", "candles", "data"):
                if isinstance(result.get(key), list):
                    return [r for r in result[key] if isinstance(r, dict)]
        return []

    async def get_all_positions(self) -> list[dict]:
        result = await self._call_first([
            ("get_all_positions", {}),
            ("get_open_positions", {}),
            ("positions_get", {}),
        ])
        if isinstance(result, list):
            return [p for p in result if isinstance(p, dict)]
        if isinstance(result, dict):
            for key in ("positions", "data"):
                if isinstance(result.get(key), list):
                    return [p for p in result[key] if isinstance(p, dict)]
        return []

    async def get_deals(
        self,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> list[dict]:
        args = {"from_date": from_date, "to_date": to_date, "symbol": symbol}
        result = await self._call_first([
            ("get_deals", args),
            ("get_order_history", args),
            ("history_deals_get", args),
        ])
        if isinstance(result, list):
            return [d for d in result if isinstance(d, dict)]
        if isinstance(result, dict):
            for key in ("deals", "history", "data"):
                if isinstance(result.get(key), list):
                    return [d for d in result[key] if isinstance(d, dict)]
        return []

    async def place_market_order(self, symbol: str, direction: str, lot: float, sl: float, comment: str) -> dict:
        side = direction.upper()
        result = await self._call_tool(
            "place_market_order",
            {"symbol": symbol, "volume": lot, "type": side},
        )
        if result is True:
            return {"success": True}
        if result is False:
            return {"error": "MCP returned False"}
        return result if isinstance(result, dict) else {"result": result}

    async def close_position(self, ticket: int) -> dict:
        result = await self._call_tool("close_position", {"id": ticket})
        return result if isinstance(result, dict) else {"result": result}


mt5 = MT5MCPClient(MCP_URL)


def money(value: Any) -> str:
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return "$0.00"


def fmt_num(value: Any, digits: int = 2) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "N/A"


def first_value(data: dict, keys: tuple[str, ...], default: Any = None) -> Any:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return default


def load_active_trades() -> None:
    global ACTIVE_TRADES
    try:
        with open(STATE_FILE, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        ACTIVE_TRADES = {int(k): v for k, v in data.items()}
    except FileNotFoundError:
        ACTIVE_TRADES = {}
    except Exception as exc:
        logger.warning("Cannot load active trade state: %s", exc)
        ACTIVE_TRADES = {}


def save_active_trades() -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(ACTIVE_TRADES, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        logger.warning("Cannot save active trade state: %s", exc)


def active_trade_items(chat_id: int) -> list[dict]:
    record = ACTIVE_TRADES.get(chat_id)
    if not record:
        return []
    if isinstance(record, dict) and isinstance(record.get("trades"), list):
        return [trade for trade in record["trades"] if isinstance(trade, dict)]
    if isinstance(record, dict):
        return [record]
    return []


def set_active_trade_items(chat_id: int, trades: list[dict]) -> None:
    if trades:
        ACTIVE_TRADES[chat_id] = {"trades": trades}
    else:
        ACTIVE_TRADES.pop(chat_id, None)


def add_active_trade(chat_id: int, trade: dict) -> None:
    trades = active_trade_items(chat_id)
    trades.append(trade)
    set_active_trade_items(chat_id, trades)


def latest_active_trade(chat_id: int) -> Optional[dict]:
    trades = active_trade_items(chat_id)
    return trades[-1] if trades else None


def release_instance_lock() -> None:
    global INSTANCE_LOCK_HANDLE
    if INSTANCE_LOCK_HANDLE is None:
        return
    try:
        if os.name == "nt":
            import msvcrt

            INSTANCE_LOCK_HANDLE.seek(0)
            msvcrt.locking(INSTANCE_LOCK_HANDLE.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(INSTANCE_LOCK_HANDLE.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        INSTANCE_LOCK_HANDLE.close()
    except Exception:
        pass
    INSTANCE_LOCK_HANDLE = None


def acquire_instance_lock() -> bool:
    """Allow only one polling bot process per working directory."""
    global INSTANCE_LOCK_HANDLE
    handle = open(INSTANCE_LOCK_FILE, "a+", encoding="utf-8")

    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        return False

    handle.seek(0)
    handle.truncate()
    handle.write(f"pid={os.getpid()}\nstarted={datetime.now().isoformat()}\n")
    handle.flush()
    INSTANCE_LOCK_HANDLE = handle
    atexit.register(release_instance_lock)
    return True


def get_machine_id() -> str:
    """Return a stable activation machine id unless MACHINE_ID overrides it."""
    if MACHINE_ID:
        return MACHINE_ID
    raw = f"{socket.gethostname()}-{uuid.getnode()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, raw))


def notify_activation_error(message: str) -> None:
    if not TELEGRAM_TOKEN or not ACTIVATION_ALERT_CHAT_ID:
        return
    try:
        with httpx.Client(timeout=10.0) as client:
            client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": ACTIVATION_ALERT_CHAT_ID, "text": message},
            )
    except Exception as exc:
        logger.warning("Cannot send activation error to Telegram: %s", exc)


def check_activation() -> bool:
    if not ACTIVATION_CODE:
        message = "Activation invalid: missing ACTIVATION_CODE in .env"
        logger.error(message)
        notify_activation_error(message)
        return False

    machine_id = get_machine_id()
    payload = {
        "code": ACTIVATION_CODE,
        "machine_id": machine_id,
        "bot_token": TELEGRAM_TOKEN,
    }

    try:
        with httpx.Client(timeout=ACTIVATION_TIMEOUT) as client:
            response = client.post(ACTIVATION_URL, json=payload)
            response.raise_for_status()
            data = response.json()
    except Exception as exc:
        message = f"Activation invalid: cannot contact activation server ({exc})"
        logger.error(message)
        notify_activation_error(message)
        return False

    if data.get("valid") is True:
        logger.info(
            "Activation valid. Plan=%s Expire=%s Machine=%s",
            data.get("plan", "unknown"),
            data.get("expire", "unknown"),
            machine_id,
        )
        return True

    message = f"Activation invalid. Machine={machine_id}"
    logger.error(message)
    notify_activation_error(message)
    return False


def now_utc_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def normalize_timeframe(tf: str) -> str:
    tf = (tf or DEFAULT_TIMEFRAME).upper()
    return tf if tf in TIMEFRAMES else "M5"


def candle_open_ts(timestamp: int, timeframe: str) -> int:
    seconds = TIMEFRAMES[timeframe]["seconds"]
    return timestamp - (timestamp % seconds)


def candle_age(timestamp: int, timeframe: str) -> int:
    return timestamp - candle_open_ts(timestamp, timeframe)


def candle_remaining(timestamp: int, timeframe: str) -> int:
    return TIMEFRAMES[timeframe]["seconds"] - candle_age(timestamp, timeframe)


def in_entry_window(timeframe: str) -> tuple[bool, int, int]:
    ts = now_utc_ts()
    age = candle_age(ts, timeframe)
    max_age = TIMEFRAMES[timeframe]["max_age"]
    return age <= max_age, age, candle_remaining(ts, timeframe)


def parse_float(row: dict, keys: tuple[str, ...], default: float = 0.0) -> float:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            try:
                return float(row[key])
            except Exception:
                continue
    return default


def extract_closes(rates: list[dict]) -> list[float]:
    return [parse_float(r, ("close", "Close", "c")) for r in rates if parse_float(r, ("close", "Close", "c")) > 0]


def ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
    alpha = 2 / (period + 1)
    out = values[0]
    for value in values[1:]:
        out = value * alpha + out * (1 - alpha)
    return out


def rsi(values: list[float], period: int = 14) -> float:
    if len(values) <= period:
        return 50.0
    gains, losses = [], []
    recent = values[-(period + 1):]
    for prev, cur in zip(recent, recent[1:]):
        change = cur - prev
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))


def atr(rates: list[dict], period: int = 14) -> float:
    if len(rates) < 2:
        return 0.0
    trs = []
    recent = rates[-(period + 1):]
    prev_close = parse_float(recent[0], ("close", "Close", "c"))
    for row in recent[1:]:
        high = parse_float(row, ("high", "High", "h"))
        low = parse_float(row, ("low", "Low", "l"))
        close = parse_float(row, ("close", "Close", "c"))
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        prev_close = close
    return sum(trs) / len(trs) if trs else 0.0


def macd_hist(values: list[float]) -> float:
    if len(values) < 35:
        return 0.0
    macd_values = []
    for idx in range(26, len(values) + 1):
        subset = values[:idx]
        macd_values.append(ema(subset, 12) - ema(subset, 26))
    signal = ema(macd_values, 9)
    return macd_values[-1] - signal


def spread_points(price: dict) -> float:
    bid = float(price.get("bid") or price.get("Bid") or 0)
    ask = float(price.get("ask") or price.get("Ask") or 0)
    point = float(price.get("point") or price.get("Point") or 0.01)
    if ask <= 0 or bid <= 0:
        return math.inf
    return (ask - bid) / point if point > 0 else ask - bid


def normalize_symbol_text(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def symbol_candidates(profile: str, aliases: list[str]) -> list[str]:
    base = normalize_symbol_text(profile)
    values = [base, *[normalize_symbol_text(alias) for alias in aliases]]
    if base == "XAUUSD":
        values.extend(["XAUUSD", "GOLD", "GOLDUSD"])
    elif base == "BTCUSD":
        values.extend(["BTCUSD", "BTCUSDT", "BITCOIN", "MBTUSD"])
    seen = set()
    result = []
    for item in values:
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def choose_detected_symbol(profile: str, aliases: list[str], available: list[str]) -> Optional[str]:
    available_set = set(available)
    for alias in [profile, *aliases]:
        if alias in available_set:
            return alias

    by_normalized = {normalize_symbol_text(symbol): symbol for symbol in available}
    candidates = symbol_candidates(profile, aliases)
    for candidate in candidates:
        if candidate in by_normalized:
            return by_normalized[candidate]

    scored: list[tuple[int, int, str]] = []
    for symbol in available:
        normalized = normalize_symbol_text(symbol)
        for candidate in candidates:
            if normalized.startswith(candidate):
                scored.append((0, len(normalized) - len(candidate), symbol))
            elif normalized.endswith(candidate):
                scored.append((1, len(normalized) - len(candidate), symbol))
            elif candidate in normalized:
                scored.append((2, len(normalized) - len(candidate), symbol))
    if not scored:
        return None
    scored.sort(key=lambda item: (item[0], item[1], item[2]))
    return scored[0][2]


def forex_profile_from_symbol(symbol: str) -> Optional[str]:
    normalized = normalize_symbol_text(symbol)
    for base in FOREX_CURRENCIES:
        for quote in FOREX_CURRENCIES:
            if base == quote:
                continue
            pair = f"{base}{quote}"
            if normalized == pair or normalized.startswith(pair) or normalized.endswith(pair):
                return pair
    return None


async def auto_detect_symbols() -> None:
    global SYMBOL, SYMBOL_ALIASES

    available = await mt5.get_symbols("*")
    if not available:
        logger.warning("Symbol auto-detect skipped: MCP returned no symbols")
        return

    for symbol in available:
        profile = forex_profile_from_symbol(symbol)
        if profile and profile not in SYMBOL_PROFILES:
            SYMBOL_PROFILES[profile] = [profile]

    detected: dict[str, str] = {}
    for profile, aliases in list(SYMBOL_PROFILES.items()):
        symbol = choose_detected_symbol(profile, aliases, available)
        if symbol:
            detected[profile] = symbol
            SYMBOL_PROFILES[profile] = [symbol, *[alias for alias in aliases if alias != symbol]]
            SYMBOL_LABELS[profile] = symbol

    default_profile = normalize_symbol_text(SYMBOL)
    default_symbol = choose_detected_symbol(default_profile, [SYMBOL, *SYMBOL_ALIASES], available)
    if default_symbol:
        SYMBOL_PROFILES[default_profile] = [default_symbol, *[alias for alias in SYMBOL_ALIASES if alias != default_symbol]]
        SYMBOL_LABELS[default_profile] = default_symbol
        SYMBOL_ALIASES = SYMBOL_PROFILES[default_profile]
        detected.setdefault(default_profile, default_symbol)
    if default_profile in SYMBOL_PROFILES and SYMBOL_PROFILES[default_profile]:
        SYMBOL_ALIASES = SYMBOL_PROFILES[default_profile]
    elif default_profile in {normalize_symbol_text(symbol) for symbol in available}:
        by_normalized = {normalize_symbol_text(symbol): symbol for symbol in available}
        SYMBOL_ALIASES = [by_normalized[default_profile]]

    logger.info(
        "Symbol auto-detect complete: %s",
        ", ".join(f"{profile}->{symbol}" for profile, symbol in sorted(detected.items())) or "no matches",
    )


def current_trade_symbol() -> str:
    profile = normalize_symbol_text(str(CHAT_SETTINGS.get("_global_symbol", SYMBOL)))
    aliases = SYMBOL_PROFILES.get(profile) or SYMBOL_ALIASES
    return aliases[0] if aliases else profile


def chat_symbol(chat_id: int) -> str:
    settings = CHAT_SETTINGS.setdefault(chat_id, {"timeframe": DEFAULT_TIMEFRAME, "symbol": SYMBOL})
    profile = normalize_symbol_text(str(settings.get("symbol", SYMBOL)))
    aliases = SYMBOL_PROFILES.get(profile) or [profile]
    return aliases[0]


def chat_lot(chat_id: int) -> float:
    settings = CHAT_SETTINGS.setdefault(chat_id, {"timeframe": DEFAULT_TIMEFRAME, "symbol": SYMBOL, "lot": FIXED_LOT})
    try:
        lot = float(settings.get("lot", FIXED_LOT))
    except Exception:
        lot = FIXED_LOT
    return max(0.01, round(lot, 2))


def resolve_symbol_name(value: str) -> str:
    profile = normalize_symbol_text(value or SYMBOL)
    aliases = SYMBOL_PROFILES.get(profile)
    return aliases[0] if aliases else (value or SYMBOL)


def position_ticket(pos: dict) -> Optional[int]:
    for key in ("id", "ID", "ticket", "Ticket", "identifier", "position", "order"):
        if pos.get(key) is not None:
            try:
                return int(pos[key])
            except Exception:
                continue
    return None


def order_ticket(result: dict) -> Optional[int]:
    for key in ("ticket", "order", "position", "deal", "id", "ID"):
        value = result.get(key)
        if value is not None:
            try:
                return int(value)
            except Exception:
                continue
    data = result.get("data")
    if isinstance(data, list):
        for index in (2, 1):
            if len(data) > index:
                try:
                    return int(data[index])
                except Exception:
                    pass
    message = str(result.get("message") or "")
    match = re.search(r"Position ID:\s*(\d+)", message, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def is_bot_position(pos: dict) -> bool:
    comment = str(pos.get("comment") or pos.get("Comment") or "")
    magic = str(pos.get("magic") or pos.get("Magic") or "")
    return str(MAGIC_NUMBER) == magic or comment.startswith(ORDER_COMMENT_PREFIX)


def bot_symbols() -> set[str]:
    symbols = {SYMBOL.upper()}
    for aliases in SYMBOL_PROFILES.values():
        symbols.update(a.upper() for a in aliases)
    return symbols


async def bot_positions(symbol: Optional[str] = None) -> list[dict]:
    positions = await mt5.get_all_positions()
    if not positions:
        return []

    if symbol:
        wanted = {symbol.upper()}
    else:
        wanted = bot_symbols()

    managed_ids = {
        int(trade["ticket"])
        for chat_id in ACTIVE_TRADES
        for trade in active_trade_items(chat_id)
        if str(trade.get("ticket", "N/A")).isdigit()
    }

    result = []
    for pos in positions:
        ticket = position_ticket(pos)
        pos_symbol = str(pos.get("symbol") or pos.get("Symbol") or "").upper()
        if is_bot_position(pos):
            result.append(pos)
        elif ticket in managed_ids:
            result.append(pos)
        elif pos_symbol in wanted:
            result.append(pos)
    return result


def parse_position_open_ts(pos: dict) -> Optional[int]:
    value = pos.get("time") or pos.get("Time") or pos.get("time_open")
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value)
    try:
        if "+07:00" in text:
            dt = datetime.fromisoformat(text)
        else:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        try:
            dt = datetime.strptime(text.split("+")[0], "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=timezone(timedelta(hours=7)))
            return int(dt.timestamp())
        except Exception:
            return None


async def analyze_symbol(symbol: str, timeframe: str) -> Optional[SignalResult]:
    logger.info(f"[ANALYZE] analyze_symbol started: {symbol} {timeframe}")
    timeframe = normalize_timeframe(timeframe)
    allowed, age, remaining = in_entry_window(timeframe)
    entry_warning = ""
    if not allowed:
        entry_warning = f"Nen {timeframe} da chay {age}s, vuot moc dau nen {TIMEFRAMES[timeframe]['max_age']}s"

    logger.info(f"[ANALYZE] calling mt5.get_ohlcv({symbol}, {timeframe}, 150)")
    rates = await mt5.get_ohlcv(symbol, timeframe, 150)
    logger.info(f"[ANALYZE] get_ohlcv returned {len(rates)} rates")
    closes = extract_closes(rates)
    logger.info(f"[ANALYZE] calling mt5.get_symbol_price({symbol})")
    price = await mt5.get_symbol_price(symbol)
    spr = spread_points(price)

    if len(closes) < 3:
        if FORCE_SIGNAL:
            direction = "BUY" if (now_utc_ts() // TIMEFRAMES[timeframe]["seconds"]) % 2 == 0 else "SELL"
            reasons = [
                "Force signal: MCP khong tra du lieu nen, van tao tin hieu de test",
                "Can kiem tra symbol broker/Market Watch neu muon tin hieu theo data that",
            ]
            if entry_warning:
                reasons.append(entry_warning)
            return SignalResult(direction, 51, reasons, "No candle data", "Fallback", "Unknown", 50, 0, spr, age, remaining, candle_open_ts(now_utc_ts(), timeframe))
        return SignalResult("NO_TRADE", 0, ["Khong du du lieu nen de phan tich"], "N/A", "N/A", "N/A", 50, 0, spr, age, remaining, candle_open_ts(now_utc_ts(), timeframe))
    spread_warning = spr > MAX_SPREAD_POINTS

    fast = ema(closes, 9)
    slow = ema(closes, 21)
    long = ema(closes, 50)
    rsi_value = rsi(closes)
    hist = macd_hist(closes)
    atr_value = atr(rates)

    buy_score = 0
    sell_score = 0
    reasons = []

    if fast > slow > long:
        buy_score += 30
        reasons.append("EMA trend bullish")
    elif fast < slow < long:
        sell_score += 30
        reasons.append("EMA trend bearish")

    if hist > 0:
        buy_score += 25
        reasons.append("MACD momentum bullish")
    elif hist < 0:
        sell_score += 25
        reasons.append("MACD momentum bearish")

    if 50 <= rsi_value <= 70:
        buy_score += 20
        reasons.append(f"RSI buy zone {rsi_value:.1f}")
    elif 30 <= rsi_value <= 50:
        sell_score += 20
        reasons.append(f"RSI sell zone {rsi_value:.1f}")
    elif rsi_value > 75:
        sell_score += 10
        reasons.append(f"RSI overbought {rsi_value:.1f}")
    elif rsi_value < 25:
        buy_score += 10
        reasons.append(f"RSI oversold {rsi_value:.1f}")

    if closes[-1] > fast:
        buy_score += 15
        reasons.append("Price above fast EMA")
    elif closes[-1] < fast:
        sell_score += 15
        reasons.append("Price below fast EMA")

    if atr_value > 0:
        buy_score += 10
        sell_score += 10

    if buy_score > sell_score and (buy_score >= MIN_CONFIDENCE or FORCE_SIGNAL):
        direction = "BUY"
        confidence = min(95, max(buy_score, 51))
    elif sell_score > buy_score and (sell_score >= MIN_CONFIDENCE or FORCE_SIGNAL):
        direction = "SELL"
        confidence = min(95, max(sell_score, 51))
    elif FORCE_SIGNAL:
        last_close = closes[-1]
        prev_close = closes[-2]
        direction = "BUY" if last_close >= prev_close else "SELL"
        confidence = max(51, min(60, max(buy_score, sell_score)))
        reasons.append("Force signal: chon huong theo nen gan nhat vi score chua du manh")
    else:
        direction = "NO_TRADE"
        confidence = max(buy_score, sell_score)
        reasons.append("Tin hieu chua du manh")

    if spread_warning:
        reasons.append(f"Canh bao spread cao: {spr:.1f} points")
    if entry_warning:
        reasons.append(entry_warning)

    trend = "Bullish" if fast > slow > long else "Bearish" if fast < slow < long else "Mixed"
    momentum = "Bullish" if hist > 0 else "Bearish" if hist < 0 else "Flat"
    volatility = "Normal" if atr_value > 0 else "Unknown"

    logger.info(f"[ANALYZE] analyze_symbol done: {direction} {confidence}%")
    return SignalResult(direction, confidence, reasons, trend, momentum, volatility, rsi_value, atr_value, spr, age, remaining, candle_open_ts(now_utc_ts(), timeframe))


async def emergency_sl(symbol: str, direction: str, atr_value: float) -> float:
    price = await mt5.get_symbol_price(symbol)
    bid = float(price.get("bid") or 0)
    ask = float(price.get("ask") or 0)
    distance = max(atr_value * EMERGENCY_SL_ATR_MULT, 1.0)
    if direction == "BUY":
        return round((ask or bid) - distance, 2)
    return round((bid or ask) + distance, 2)



def format_signal(symbol: str, timeframe: str, signal: SignalResult) -> str:
    reasons = "\n".join(f"- {r}" for r in signal.reasons[:8])
    return "\n".join([
        f"*{symbol} {timeframe} Analysis*",
        "",
        f"Signal: *{signal.direction}*",
        f"Confidence: *{signal.confidence}%*",
        f"Entry window: {signal.candle_age}s / max {TIMEFRAMES[timeframe]['max_age']}s",
        f"Exit: candle close in {signal.candle_remaining}s",
        f"Spread: {signal.spread_points:.1f} points",
        "",
        "*Market Info:*",
        f"Trend: {signal.trend}",
        f"Momentum: {signal.momentum}",
        f"Volatility: {signal.volatility}",
        "",
        "*Technical Overview:*",
        f"RSI: {signal.rsi:.1f}",
        f"ATR: {signal.atr:.2f}",
        "",
        "*Reason:*",
        reasons or "- No reason",
    ])


def main_keyboard() -> InlineKeyboardMarkup:
    xau_label = SYMBOL_LABELS.get("XAUUSD", resolve_symbol_name("XAUUSD"))
    btc_label = SYMBOL_LABELS.get("BTCUSD", resolve_symbol_name("BTCUSD"))
    eur_label = SYMBOL_LABELS.get("EURUSD", resolve_symbol_name("EURUSD"))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Analyze Signal", callback_data="analyze")],
        [
            InlineKeyboardButton(f"Gold {xau_label}", callback_data="sym:XAUUSD"),
            InlineKeyboardButton(f"BTC {btc_label}", callback_data="sym:BTCUSD"),
        ],
        [InlineKeyboardButton(eur_label, callback_data="sym:EURUSD")],
        [
            InlineKeyboardButton("M1", callback_data="tf:M1"),
            InlineKeyboardButton("M5", callback_data="tf:M5"),
            InlineKeyboardButton("M15", callback_data="tf:M15"),
            InlineKeyboardButton("H1", callback_data="tf:H1"),
        ],
        [
            InlineKeyboardButton("Lot 0.01", callback_data="lot:0.01"),
            InlineKeyboardButton("Lot 0.02", callback_data="lot:0.02"),
            InlineKeyboardButton("Lot 0.05", callback_data="lot:0.05"),
            InlineKeyboardButton("Lot 0.10", callback_data="lot:0.10"),
        ],
        [
            InlineKeyboardButton("Lot 0.20", callback_data="lot:0.20"),
            InlineKeyboardButton("Lot 0.30", callback_data="lot:0.30"),
            InlineKeyboardButton("Lot 0.50", callback_data="lot:0.50"),
            InlineKeyboardButton("Lot 1.00", callback_data="lot:1.00"),
        ],
        [
            InlineKeyboardButton("Auto ON", callback_data="auto:on"),
            InlineKeyboardButton("Auto OFF", callback_data="auto:off"),
            InlineKeyboardButton("Status", callback_data="status"),
        ],
        [InlineKeyboardButton("Close Bot Positions", callback_data="close_all")],
    ])


def trade_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Place BUY", callback_data="place:BUY"),
            InlineKeyboardButton("Place SELL", callback_data="place:SELL"),
        ],
        [InlineKeyboardButton("Auto ON", callback_data="auto:on"), InlineKeyboardButton("Cancel", callback_data="cancel")],
    ])


def set_active_trade_message(chat_id: int, message_id: Optional[int]) -> None:
    if message_id is None or chat_id not in ACTIVE_TRADES:
        return
    trade = latest_active_trade(chat_id)
    if not trade:
        return
    trade["message_id"] = message_id
    save_active_trades()


def close_result_line(ticket: int, result: dict, position: Optional[dict] = None) -> str:
    position = position or {}
    profit = first_value(
        result,
        ("profit", "Profit", "closed_profit", "ClosedProfit", "pnl", "PnL"),
        first_value(position, ("profit", "Profit")),
    )
    close_price = first_value(
        result,
        ("price", "Price", "close_price", "ClosePrice", "price_close", "PriceClose"),
        first_value(position, ("price_current", "PriceCurrent", "current_price", "CurrentPrice", "price", "Price")),
    )
    symbol = first_value(result, ("symbol", "Symbol"), first_value(position, ("symbol", "Symbol"), ""))
    symbol_part = f" `{symbol}`" if symbol else ""
    return f"- Ticket `{ticket}`{symbol_part} | Close: `{fmt_num(close_price)}` | Profit: `{money(profit or 0)}`"


async def telegram_call_with_retry(action, *args, attempts: int = 3, **kwargs):
    for attempt in range(attempts):
        try:
            return await action(*args, **kwargs)
        except (TimedOut, NetworkError) as exc:
            if attempt >= attempts - 1:
                raise
            delay = 1.5 * (attempt + 1)
            logger.warning("Telegram API timeout/network issue, retrying in %.1fs: %s", delay, exc)
            await asyncio.sleep(delay)


async def edit_trade_result_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    fallback_message_id: Optional[int] = None,
) -> None:
    trade = latest_active_trade(chat_id) or {}
    message_id = trade.get("message_id") or fallback_message_id
    if not message_id:
        logger.warning("No message_id available to edit close result for chat %s", chat_id)
        return
    try:
        await telegram_call_with_retry(
            context.bot.edit_message_text,
            chat_id=chat_id,
            message_id=int(message_id),
            text=text,
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )
    except Exception:
        logger.exception("Cannot edit trade result message for chat %s message %s", chat_id, message_id)


async def send_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, text: Optional[str] = None) -> None:
    chat_id = update.effective_chat.id
    settings = CHAT_SETTINGS.setdefault(chat_id, {"timeframe": DEFAULT_TIMEFRAME, "symbol": SYMBOL})
    tf = normalize_timeframe(settings.get("timeframe"))
    symbol = chat_symbol(chat_id)
    lot = chat_lot(chat_id)
    status = "ON" if chat_id in AUTO_TASKS and not AUTO_TASKS[chat_id].done() else "OFF"
    body = text or "\n".join([
        "*Candle Auto Bot*",
        "",
        f"Symbol: `{symbol}`",
        f"Timeframe: *{tf}*",
        f"Mode: *Auto Trade*",
        f"Lot: `{lot}` + emergency SL",
        f"Auto: *{status}*",
        "",
        "Entry age limit:",
        "M1 <= 30s | M5 <= 3m | M15 <= 10m | H1 <= 40m",
    ])
    if update.callback_query:
        await telegram_call_with_retry(update.callback_query.edit_message_text, body, parse_mode="Markdown", reply_markup=main_keyboard())
    else:
        await telegram_call_with_retry(update.message.reply_text, body, parse_mode="Markdown", reply_markup=main_keyboard())


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"[DEBUG] start called by {update.effective_chat.id}", flush=True)
    logger.info(f"[COMMAND] start called by {update.effective_chat.id}")
    await send_menu(update, context)
    print(f"[DEBUG] start completed", flush=True)


async def account_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"[DEBUG] account_cmd called by {update.effective_chat.id}", flush=True)
    logger.info(f"[COMMAND] account_cmd called by {update.effective_chat.id}")
    account = await mt5.get_account_info()
    print(f"[DEBUG] account_cmd got response", flush=True)
    logger.info(f"[COMMAND] account_cmd got account info: {account}")
    msg = "\n".join([
        "*THONG TIN TAI KHOAN*",
        "",
        f"Balance: {money(account.get('balance') or account.get('Balance') or 0)}",
        f"Equity: {money(account.get('equity') or account.get('Equity') or 0)}",
        f"Profit: {money(account.get('profit') or account.get('Profit') or 0)}",
        f"Free margin: {money(account.get('free_margin') or account.get('margin_free') or 0)}",
    ])
    await update.message.reply_text(msg, parse_mode="Markdown")


async def positions_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    positions = await mt5.get_all_positions()
    if not positions:
        await update.message.reply_text("Khong co lenh dang mo.")
        return
    lines = [f"*{len(positions)} lenh dang mo*", ""]
    for pos in positions[:20]:
        lines.append(f"#{position_ticket(pos)} {pos.get('symbol', '')} {pos.get('type', pos.get('side', ''))} lot={pos.get('volume', pos.get('lot', ''))} P/L={money(pos.get('profit', 0))}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def auto_on_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await enable_auto_trade(update.effective_chat.id, context)
    await update.message.reply_text("Auto trade da BAT.", reply_markup=main_keyboard())


async def auto_off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await disable_auto_trade(update.effective_chat.id, context)
    await update.message.reply_text("Auto trade da TAT.", reply_markup=main_keyboard())


async def lot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text(f"Lot hien tai: {chat_lot(chat_id)}\nDung: /lot 0.03")
        return
    try:
        lot = float(context.args[0])
    except Exception:
        await update.message.reply_text("Lot khong hop le. Vi du: /lot 0.03")
        return
    if lot <= 0:
        await update.message.reply_text("Lot phai lon hon 0.")
        return
    CHAT_SETTINGS.setdefault(chat_id, {"timeframe": DEFAULT_TIMEFRAME, "symbol": SYMBOL})["lot"] = round(lot, 2)
    await update.message.reply_text(f"Da chon lot: {chat_lot(chat_id)}", reply_markup=main_keyboard())


async def analyze_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    logger.info(f"[ANALYZE] analyze_cmd started for chat {chat_id}")
    settings = CHAT_SETTINGS.setdefault(chat_id, {"timeframe": DEFAULT_TIMEFRAME, "symbol": SYMBOL})
    tf = normalize_timeframe(settings.get("timeframe"))
    symbol = chat_symbol(chat_id)

    if context.args:
        for arg in context.args:
            upper = arg.upper()
            if upper in TIMEFRAMES:
                tf = normalize_timeframe(upper)
                settings["timeframe"] = tf
            else:
                settings["symbol"] = upper
                symbol = resolve_symbol_name(upper)

    signal = await analyze_symbol( symbol, tf)
    if not signal:
        await update.message.reply_text("Khong phan tich duoc tin hieu.")
        return
    await update.message.reply_text(format_signal(symbol, tf, signal), parse_mode="Markdown", reply_markup=trade_keyboard())


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    data = query.data
    settings = CHAT_SETTINGS.setdefault(chat_id, {"timeframe": DEFAULT_TIMEFRAME})

    if data.startswith("tf:"):
        settings["timeframe"] = normalize_timeframe(data.split(":", 1)[1])
        await send_menu(update, context, f"Da chon timeframe *{settings['timeframe']}*.")
        return

    if data.startswith("sym:"):
        selected = data.split(":", 1)[1].upper()
        settings["symbol"] = selected
        await send_menu(update, context, f"Da chon symbol *{chat_symbol(chat_id)}*.")
        return

    if data.startswith("lot:"):
        lot = float(data.split(":", 1)[1])
        settings["lot"] = lot
        await send_menu(update, context, f"Da chon lot *{chat_lot(chat_id)}*.")
        return

    if data == "analyze":
        tf = normalize_timeframe(settings.get("timeframe"))
        symbol = chat_symbol(chat_id)
        signal = await analyze_symbol( symbol, tf)
        await query.edit_message_text(format_signal(symbol, tf, signal), parse_mode="Markdown", reply_markup=trade_keyboard())
        return

    if data.startswith("place:"):
        direction = data.split(":", 1)[1].upper()
        tf = normalize_timeframe(settings.get("timeframe"))
        symbol = chat_symbol(chat_id)
        signal = await analyze_symbol( symbol, tf)
        if signal:
            signal.direction = direction
            signal.confidence = max(signal.confidence, 51)
        else:
            signal = SignalResult(direction, 51, ["Manual place from Telegram"], "Manual", "Manual", "Unknown", 50, 0, math.inf, candle_age(now_utc_ts(), tf), candle_remaining(now_utc_ts(), tf), candle_open_ts(now_utc_ts(), tf))
        result = await open_auto_trade( chat_id, symbol, tf, signal)
        if chat_id in ACTIVE_TRADES:
            ensure_trade_monitor(chat_id, context)
        edited = await query.edit_message_text(result, parse_mode="Markdown", reply_markup=main_keyboard())
        set_active_trade_message(chat_id, getattr(edited, "message_id", query.message.message_id))
        return

    if data == "auto:on":
        await enable_auto_trade(chat_id, context)
        await send_menu(update, context, "Auto trade da BAT.")
        return

    if data == "auto:off":
        await disable_auto_trade(chat_id, context)
        await send_menu(update, context, "Auto trade da TAT.")
        return

    if data == "status":
        await send_status(query, chat_id)
        return

    if data == "close_all":
        msg = await close_bot_positions_result()
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=main_keyboard())
        return

    if data == "cancel":
        await send_menu(update, context, "Da huy.")


async def enable_auto_trade(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    task = AUTO_TASKS.get(chat_id)
    if task and not task.done():
        return
    monitor = TRADE_MONITOR_TASKS.pop(chat_id, None)
    if monitor and not monitor.done():
        monitor.cancel()
    AUTO_TASKS[chat_id] = context.application.create_task(auto_trade_loop(chat_id, context))


async def disable_auto_trade(chat_id: int, context: Optional[ContextTypes.DEFAULT_TYPE] = None):
    task = AUTO_TASKS.pop(chat_id, None)
    if task and not task.done():
        task.cancel()
    if context and chat_id in ACTIVE_TRADES:
        ensure_trade_monitor(chat_id, context)


def auto_task_running(chat_id: int) -> bool:
    task = AUTO_TASKS.get(chat_id)
    return bool(task and not task.done())


def ensure_trade_monitor(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    if auto_task_running(chat_id):
        return
    task = TRADE_MONITOR_TASKS.get(chat_id)
    if task and not task.done():
        return
    TRADE_MONITOR_TASKS[chat_id] = context.application.create_task(trade_monitor_loop(chat_id, context))


async def trade_monitor_loop(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    while chat_id in ACTIVE_TRADES:
        if auto_task_running(chat_id):
            break
        try:
            await manage_active_trade(chat_id, context)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("trade monitor error")
            try:
                await context.bot.send_message(chat_id, f"Trade monitor error: {exc}")
            except Exception:
                logger.exception("failed to notify trade monitor error")
        await asyncio.sleep(POLL_SECONDS)
    TRADE_MONITOR_TASKS.pop(chat_id, None)


async def auto_trade_loop(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    while True:
        try:
            settings = CHAT_SETTINGS.setdefault(chat_id, {"timeframe": DEFAULT_TIMEFRAME, "symbol": SYMBOL})
            timeframe = normalize_timeframe(settings.get("timeframe"))
            symbol = chat_symbol(chat_id)

            await manage_active_trade(chat_id, context)

            current_open = candle_open_ts(now_utc_ts(), timeframe)
            allowed, age, remaining = in_entry_window(timeframe)
            if ENFORCE_ENTRY_WINDOW and not allowed:
                await asyncio.sleep(POLL_SECONDS)
                continue

            signal = await analyze_symbol( symbol, timeframe)
            if not signal or signal.direction == "NO_TRADE":
                await context.bot.send_message(chat_id, format_signal(symbol, timeframe, signal), parse_mode="Markdown")
                await asyncio.sleep(POLL_SECONDS)
                continue

            result = await open_auto_trade( chat_id, symbol, timeframe, signal)
            message = await context.bot.send_message(chat_id, result, parse_mode="Markdown", reply_markup=main_keyboard())
            set_active_trade_message(chat_id, message.message_id)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("auto loop error")
            await context.bot.send_message(chat_id, f"Auto trade error: {exc}")
        await asyncio.sleep(POLL_SECONDS)


async def daily_loss_guard(symbol: str) -> tuple[bool, str]:
    if MAX_DAILY_LOSS_PERCENT <= 0:
        return True, ""

    account = await mt5.get_account_info()
    balance = parse_float(
        account,
        ("balance", "Balance", "equity", "Equity"),
        0.0,
    )
    if balance <= 0:
        logger.warning("Daily loss guard skipped: invalid account balance in %s", account)
        return True, ""

    today = datetime.now(timezone.utc).date().isoformat()
    deals = await mt5.get_deals(from_date=today, to_date=today, symbol=symbol)
    if not deals:
        return True, ""

    realized_profit = sum(
        parse_float(deal, ("profit", "Profit", "pnl", "PnL", "profit_loss", "ProfitLoss"), 0.0)
        for deal in deals
    )
    if realized_profit >= 0:
        return True, ""

    loss_pct = abs(realized_profit) / balance * 100
    if loss_pct < MAX_DAILY_LOSS_PERCENT:
        return True, ""

    return False, (
        f"Daily loss limit reached: {money(realized_profit)} "
        f"({loss_pct:.2f}% / {MAX_DAILY_LOSS_PERCENT:.2f}%)."
    )


async def open_auto_trade(chat_id: int, symbol: str, timeframe: str, signal: SignalResult) -> str:
    allowed, age, remaining = in_entry_window(timeframe)
    if ENFORCE_ENTRY_WINDOW and not allowed:
        return f"Entry rejected: nen {timeframe} da chay {age}s."
    daily_ok, daily_message = await daily_loss_guard(symbol)
    if not daily_ok:
        return daily_message
    existing_positions = await bot_positions(symbol)
    before_tickets = {position_ticket(pos) for pos in existing_positions if position_ticket(pos)}

    lot = chat_lot(chat_id)
    sl = await emergency_sl(symbol, signal.direction, signal.atr)
    comment = f"{ORDER_COMMENT_PREFIX}_{timeframe}_{signal.direction}"
    result = await mt5.place_market_order(symbol, signal.direction, lot, sl, comment)
    if mt5._is_error(result):
        return f"Khong dat duoc lenh: `{mt5._error_message(result)}`"

    ticket = order_ticket(result)
    if ticket is None:
        for attempt in range(3):
            if attempt:
                await asyncio.sleep(0.5)
            opened_positions = await bot_positions(symbol)
            new_tickets = [
                pos_ticket
                for pos in opened_positions
                if (pos_ticket := position_ticket(pos)) and pos_ticket not in before_tickets
            ]
            if new_tickets:
                ticket = new_tickets[-1]
                break
    ticket = ticket or "N/A"
    add_active_trade(chat_id, {
        "ticket": ticket,
        "symbol": symbol,
        "timeframe": timeframe,
        "direction": signal.direction,
        "candle_open_ts": signal.candle_open_ts,
        "opened_at": now_utc_ts(),
    })
    save_active_trades()
    return "\n".join([
        "*Trade Active*",
        "",
        f"Symbol: `{symbol}`",
        f"Direction: *{signal.direction}*",
        f"Timeframe: *{timeframe}*",
        f"Lot: `{lot}`",
        f"Emergency SL: `{sl}`",
        f"Close in: `{remaining}s`",
        f"Ticket: `{ticket}`",
    ])


async def manage_active_trade(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    trades = active_trade_items(chat_id)
    settings = CHAT_SETTINGS.setdefault(chat_id, {"timeframe": DEFAULT_TIMEFRAME, "symbol": SYMBOL})
    timeframe = normalize_timeframe((trades[-1] if trades else {}).get("timeframe") or settings.get("timeframe"))
    symbol = (trades[-1] if trades else {}).get("symbol") or chat_symbol(chat_id)

    current_candle_open = candle_open_ts(now_utc_ts(), timeframe)
    positions = await bot_positions(symbol)

    logger.info("=== MANAGE_ACTIVE_TRADE DEBUG ===")
    logger.info(f"Chat ID: {chat_id}")
    logger.info(f"Timeframe: {timeframe}")
    logger.info(f"Symbol: {symbol}")
    logger.info(f"Now UTC: {now_utc_ts()}")
    logger.info(f"Current candle_open_ts: {current_candle_open}")
    logger.info(f"Active trades: {trades}")
    logger.info(f"Bot positions count: {len(positions)}")

    closed_trade_message_id = None
    remaining_trades = []
    if trades:
        tickets = []
        for trade in trades:
            trade_timeframe = normalize_timeframe(trade.get("timeframe") or timeframe)
            trade_current_candle = candle_open_ts(now_utc_ts(), trade_timeframe)
            trade_candle_open = int(trade["candle_open_ts"])
            logger.info(f"Trade candle_open_ts: {trade_candle_open}")
            logger.info(f"Should close? {trade_current_candle > trade_candle_open}")
            if trade_current_candle <= trade_candle_open:
                remaining_trades.append(trade)
                continue
            ticket = trade.get("ticket")
            if ticket != "N/A" and str(ticket).isdigit():
                tickets.append(int(ticket))
                closed_trade_message_id = closed_trade_message_id or trade.get("message_id")
            else:
                tickets.extend(position_ticket(p) for p in positions if position_ticket(p))
                closed_trade_message_id = closed_trade_message_id or trade.get("message_id")
    else:
        tickets = []
        for pos in positions:
            opened_at = parse_position_open_ts(pos)
            if opened_at is None:
                continue
            if current_candle_open > candle_open_ts(opened_at, timeframe):
                ticket = position_ticket(pos)
                if ticket:
                    tickets.append(ticket)

    if not tickets:
        set_active_trade_items(chat_id, remaining_trades)
        save_active_trades()
        return

    tickets = list(dict.fromkeys(ticket for ticket in tickets if ticket))
    position_by_ticket = {position_ticket(pos): pos for pos in positions if position_ticket(pos)}
    closed_lines = []
    error_lines = []
    for item in tickets:
        result = await mt5.close_position(item)
        if mt5._is_error(result):
            error_lines.append(f"- Ticket `{item}`: `{mt5._error_message(result)}`")
        else:
            closed_lines.append(close_result_line(item, result, position_by_ticket.get(item)))

    result_message = [
        "*Trade Closed - Candle Close*",
        "",
    ]
    if closed_lines:
        result_message.extend(closed_lines)
    if error_lines:
        result_message.extend(["", "*Close Errors:*", *error_lines])
    result_message.extend(["", "*Menu:*"])

    await edit_trade_result_message(context, chat_id, "\n".join(result_message), closed_trade_message_id)
    set_active_trade_items(chat_id, remaining_trades)
    save_active_trades()


async def close_bot_positions() -> tuple[int, list[str]]:
    errors = []
    count = 0
    for pos in await bot_positions():
        ticket = position_ticket(pos)
        if not ticket:
            continue
        result = await mt5.close_position(ticket)
        if mt5._is_error(result):
            errors.append(f"{ticket}: {mt5._error_message(result)}")
        else:
            count += 1
    if count:
        ACTIVE_TRADES.clear()
        save_active_trades()
    return count, errors


async def close_bot_positions_result() -> str:
    positions = await bot_positions()
    if not positions:
        ACTIVE_TRADES.clear()
        save_active_trades()
        return "\n".join(["*No Bot Positions*", "", "*Menu:*"])

    closed_lines = []
    error_lines = []
    for pos in positions:
        ticket = position_ticket(pos)
        if not ticket:
            continue
        result = await mt5.close_position(ticket)
        if mt5._is_error(result):
            error_lines.append(f"- Ticket `{ticket}`: `{mt5._error_message(result)}`")
        else:
            closed_lines.append(close_result_line(ticket, result, pos))

    if closed_lines:
        ACTIVE_TRADES.clear()
        save_active_trades()

    message = ["*Trade Closed - Manual Close*", ""]
    if closed_lines:
        message.extend(closed_lines)
    if error_lines:
        message.extend(["", "*Close Errors:*", *error_lines])
    if not closed_lines and not error_lines:
        message.append("No closeable bot positions found.")
    message.extend(["", "*Menu:*"])
    return "\n".join(message)


async def send_status(query, chat_id: int):
    settings = CHAT_SETTINGS.setdefault(chat_id, {"timeframe": DEFAULT_TIMEFRAME, "symbol": SYMBOL})
    tf = normalize_timeframe(settings.get("timeframe"))
    symbol = chat_symbol(chat_id)
    lot = chat_lot(chat_id)
    status = "ON" if chat_id in AUTO_TASKS and not AUTO_TASKS[chat_id].done() else "OFF"
    positions = await bot_positions()
    allowed, age, remaining = in_entry_window(tf)
    msg = "\n".join([
        "*Bot Status*",
        "",
        f"Auto: *{status}*",
        f"Symbol: `{symbol}`",
        f"Timeframe: *{tf}*",
        f"Lot: `{lot}`",
        f"Candle age: `{age}s`",
        f"Remaining: `{remaining}s`",
        f"Entry allowed: *{'YES' if allowed else 'NO'}*",
        f"Bot positions: *{len(positions)}*",
    ])
    await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=main_keyboard())


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").lower()
    if "auto on" in text or "bat auto" in text:
        await enable_auto_trade(update.effective_chat.id, context)
        await update.message.reply_text("Auto trade da BAT.", reply_markup=main_keyboard())
    elif "auto off" in text or "tat auto" in text:
        await disable_auto_trade(update.effective_chat.id, context)
        await update.message.reply_text("Auto trade da TAT.", reply_markup=main_keyboard())
    elif any(word in text for word in ["analyze", "phan tich", "signal", "tin hieu"]):
        await analyze_cmd(update, context)
    elif text.startswith("lot "):
        context.args = text.split()[1:]
        await lot_cmd(update, context)
    elif any(word in text for word in ["tai khoan", "balance", "account"]):
        await account_cmd(update, context)
    elif any(word in text for word in ["lenh", "position", "open"]):
        await positions_cmd(update, context)
    else:
        await send_menu(update, context)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(context.error, (TimedOut, NetworkError)):
        logger.warning("Telegram API/network issue: %s", context.error)
        return
    logger.error("Update %s caused error %s", update, context.error)


def main():
    if not TELEGRAM_TOKEN:
        logger.error("Missing TELEGRAM_BOT_TOKEN in .env")
        return

    if not check_activation():
        return

    if not acquire_instance_lock():
        logger.error("Another bot instance is already running. Exit before Telegram polling to avoid 409 Conflict.")
        return

    load_active_trades()
    logger.info("MCP Server: %s", MCP_URL)
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(auto_detect_symbols())
    except Exception as exc:
        logger.warning("Symbol auto-detect failed: %s", exc)

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .connect_timeout(TELEGRAM_CONNECT_TIMEOUT)
        .read_timeout(TELEGRAM_READ_TIMEOUT)
        .write_timeout(TELEGRAM_WRITE_TIMEOUT)
        .pool_timeout(TELEGRAM_POOL_TIMEOUT)
        .get_updates_connect_timeout(TELEGRAM_CONNECT_TIMEOUT)
        .get_updates_read_timeout(TELEGRAM_GET_UPDATES_READ_TIMEOUT)
        .get_updates_write_timeout(TELEGRAM_WRITE_TIMEOUT)
        .get_updates_pool_timeout(TELEGRAM_POOL_TIMEOUT)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", start))
    app.add_handler(CommandHandler("auto_on", auto_on_cmd))
    app.add_handler(CommandHandler("auto_off", auto_off_cmd))
    app.add_handler(CommandHandler("analyze", analyze_cmd))
    app.add_handler(CommandHandler("lot", lot_cmd))
    app.add_handler(CommandHandler("taikhoan", account_cmd))
    app.add_handler(CommandHandler("lenhmo", positions_cmd))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    logger.info("Bot is running. Press Ctrl+C to stop.")
    print("[DEBUG] Bot starting to poll updates", flush=True)
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        timeout=TELEGRAM_GET_UPDATES_TIMEOUT,
        read_timeout=TELEGRAM_GET_UPDATES_READ_TIMEOUT,
        write_timeout=TELEGRAM_WRITE_TIMEOUT,
        connect_timeout=TELEGRAM_CONNECT_TIMEOUT,
        pool_timeout=TELEGRAM_POOL_TIMEOUT,
        bootstrap_retries=-1,
    )


if __name__ == "__main__":
    main()
