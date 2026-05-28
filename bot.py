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
from telegram.helpers import escape_markdown
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
MAX_SPREAD_POINTS = float(os.getenv("MAX_SPREAD_POINTS", "80"))
EMERGENCY_SL_ATR_MULT = float(os.getenv("EMERGENCY_SL_ATR_MULT", "1.5"))
SL_ATR_MULT = float(os.getenv("SL_ATR_MULT", "1.0"))
TP_RR = float(os.getenv("TP_RR", "1.2"))
BREAKEVEN_R_MULT = float(os.getenv("BREAKEVEN_R_MULT", "0.7"))
TRAILING_R_MULT = float(os.getenv("TRAILING_R_MULT", "1.1"))
TRAILING_ATR_MULT = float(os.getenv("TRAILING_ATR_MULT", "0.8"))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "60"))
FORCE_SIGNAL = os.getenv("FORCE_SIGNAL", "true").lower() in ("1", "true", "yes", "on")
ENFORCE_ENTRY_WINDOW = os.getenv("ENFORCE_ENTRY_WINDOW", "true").lower() in ("1", "true", "yes", "on")
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
    "M1": {"seconds": 60, "max_age": 20},
    "M5": {"seconds": 300, "max_age": 90},
    "M15": {"seconds": 900, "max_age": 240},
    "H1": {"seconds": 3600, "max_age": 900},
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


@dataclass
class V3Candle:
    open: float
    high: float
    low: float
    close: float
    body: float
    range: float
    upper_wick: float
    lower_wick: float
    close_position: float
    direction: str


@dataclass
class V3Context:
    symbol: str
    timeframe: str
    rates: list[dict]
    closes: list[float]
    last: V3Candle
    ema20: float
    ema50: float
    atr: float
    median_atr: float
    spread_points: float
    range_high: float
    range_low: float
    range_position: float
    bullish_streak: int
    bearish_streak: int
    micro_bias: str
    exhaustion: str
    candle_age: int
    candle_remaining: int
    candle_open_ts: int
    trend: str
    common_blocks: list[str]


@dataclass
class V3Setup:
    name: str
    direction: str
    score: int
    valid: bool
    reasons: list[str]
    blocks: list[str]


@dataclass
class V3Decision:
    direction: str
    confidence: int
    setup: str
    reasons: list[str]
    blocks: list[str]
    context: V3Context
    setups: list[V3Setup]


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
            return sort_rates_oldest_first([r for r in result if isinstance(r, dict)])
        if isinstance(result, dict):
            for key in ("rates", "ohlcv", "candles", "data"):
                if isinstance(result.get(key), list):
                    return sort_rates_oldest_first([r for r in result[key] if isinstance(r, dict)])
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

    async def get_deals(self, limit: int = 10) -> list[dict]:
        result = await self._call_first([
            ("get_deals", {"limit": limit}),
            ("get_order_history", {"limit": limit}),
            ("history_deals_get", {"limit": limit}),
        ])
        if isinstance(result, list):
            return [d for d in result if isinstance(d, dict)]
        if isinstance(result, dict):
            for key in ("deals", "history", "data"):
                if isinstance(result.get(key), list):
                    return [d for d in result[key] if isinstance(d, dict)]
        return []

    async def place_market_order(self, symbol: str, direction: str, lot: float, sl: float, tp: float, comment: str) -> dict:
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

    async def modify_position(self, ticket: int, sl: Optional[float] = None, tp: Optional[float] = None) -> dict:
        args: dict[str, Any] = {"id": ticket}
        if sl is not None:
            args["stop_loss"] = sl
        if tp is not None:
            args["take_profit"] = tp
        result = await self._call_tool("modify_position", args)
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
        with open(STATE_FILE, "r", encoding="utf-8") as f:
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


def parse_rate_ts(row: dict) -> int:
    value = first_value(row, ("time", "Time", "datetime", "Datetime", "date", "Date"), "")
    if isinstance(value, (int, float)):
        return int(value)
    if not value:
        return 0
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp())
        except Exception:
            continue
    return 0


def sort_rates_oldest_first(rates: list[dict]) -> list[dict]:
    return sorted(rates, key=parse_rate_ts)


def closed_rates_only(rates: list[dict], timeframe: str) -> list[dict]:
    current_open = candle_open_ts(now_utc_ts(), timeframe)
    closed = [row for row in rates if parse_rate_ts(row) and parse_rate_ts(row) < current_open]
    return closed if len(closed) >= 60 else rates


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


def symbol_digits(symbol: str) -> int:
    normalized = normalize_symbol_text(symbol)
    if "BTC" in normalized:
        return 2
    if "XAU" in normalized or "GOLD" in normalized:
        return 3
    if normalized.endswith("JPY"):
        return 3
    if len(normalized) >= 6 and normalized[:3] in FOREX_CURRENCIES and normalized[3:6] in FOREX_CURRENCIES:
        return 5
    return 2


def position_open_price(pos: dict) -> float:
    return float(first_value(pos, ("open", "Open", "price_open", "PriceOpen", "price", "Price"), 0) or 0)


def position_current_price(pos: dict, price: dict) -> float:
    value = first_value(pos, ("price_current", "PriceCurrent", "current_price", "CurrentPrice"), 0)
    try:
        current = float(value or 0)
        if current > 0:
            return current
    except Exception:
        pass
    pos_type = str(first_value(pos, ("type", "Type"), "")).upper()
    bid = float(price.get("bid") or price.get("Bid") or 0)
    ask = float(price.get("ask") or price.get("Ask") or 0)
    return bid if pos_type == "BUY" else ask or bid


def calculate_sl_tp(symbol: str, direction: str, price: dict, atr_value: float) -> tuple[float, float, float]:
    bid = float(price.get("bid") or price.get("Bid") or 0)
    ask = float(price.get("ask") or price.get("Ask") or 0)
    entry = ask if direction == "BUY" else bid
    if entry <= 0:
        entry = bid or ask
    distance = max(float(atr_value or 0) * SL_ATR_MULT, 0.0001)
    digits = symbol_digits(symbol)
    if direction == "BUY":
        sl = round(entry - distance, digits)
        tp = round(entry + distance * TP_RR, digits)
    else:
        sl = round(entry + distance, digits)
        tp = round(entry - distance * TP_RR, digits)
    return sl, tp, distance


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
    exact_candidates = [profile, *aliases]
    available_exact = {symbol: symbol for symbol in available}
    for candidate in exact_candidates:
        if candidate in available_exact:
            return available_exact[candidate]

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
    selected = str(CHAT_SETTINGS.get("_global_symbol", SYMBOL))
    profile = normalize_symbol_text(selected)
    aliases = SYMBOL_PROFILES.get(profile) or SYMBOL_ALIASES
    return aliases[0] if aliases else selected


def chat_symbol(chat_id: int) -> str:
    settings = CHAT_SETTINGS.setdefault(chat_id, {"timeframe": DEFAULT_TIMEFRAME, "symbol": SYMBOL})
    selected = str(settings.get("symbol", SYMBOL))
    profile = normalize_symbol_text(selected)
    aliases = SYMBOL_PROFILES.get(profile) or [selected]
    return aliases[0] if aliases else selected


def chat_lot(chat_id: int) -> float:
    settings = CHAT_SETTINGS.setdefault(chat_id, {"timeframe": DEFAULT_TIMEFRAME, "symbol": SYMBOL, "lot": FIXED_LOT})
    try:
        lot = float(settings.get("lot", FIXED_LOT))
    except Exception:
        lot = FIXED_LOT
    return max(0.01, round(lot, 2))


def resolve_symbol_name(value: str) -> str:
    selected = value or SYMBOL
    profile = normalize_symbol_text(selected)
    aliases = SYMBOL_PROFILES.get(profile)
    return aliases[0] if aliases else selected


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
    symbols = {normalize_symbol_text(SYMBOL)}
    for aliases in SYMBOL_PROFILES.values():
        symbols.update(normalize_symbol_text(a) for a in aliases)
    return symbols


async def bot_positions(symbol: Optional[str] = None) -> list[dict]:
    positions = await mt5.get_all_positions()
    if not positions:
        return []

    if symbol:
        wanted = {normalize_symbol_text(symbol)}
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
        pos_symbol = normalize_symbol_text(str(pos.get("symbol") or pos.get("Symbol") or ""))
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


def v3_value(row: dict, *keys: str, default: float = 0.0) -> float:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            try:
                return float(row[key])
            except Exception:
                pass
    return default


def v3_candle(row: dict) -> V3Candle:
    open_price = v3_value(row, "open", "Open")
    high = v3_value(row, "high", "High")
    low = v3_value(row, "low", "Low")
    close = v3_value(row, "close", "Close")
    body = abs(close - open_price)
    candle_range = max(high - low, 0.0)
    upper_wick = high - max(open_price, close)
    lower_wick = min(open_price, close) - low
    close_position = (close - low) / candle_range if candle_range > 0 else 0.5
    direction = "BULL" if close > open_price else "BEAR" if close < open_price else "DOJI"
    return V3Candle(open_price, high, low, close, body, candle_range, upper_wick, lower_wick, close_position, direction)


def v3_atr_median(rates: list[dict], count: int = 40) -> float:
    values = []
    sample = rates[-count:] if len(rates) >= count else rates
    for prev, cur in zip(sample, sample[1:]):
        high = v3_value(cur, "high", "High")
        low = v3_value(cur, "low", "Low")
        prev_close = v3_value(prev, "close", "Close")
        values.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if not values:
        return 0.0
    values.sort()
    return values[len(values) // 2]


def v3_candle_streak(rates: list[dict], direction: str, max_count: int = 8) -> int:
    count = 0
    for row in reversed(rates[-max_count:]):
        candle = v3_candle(row)
        if candle.direction == direction:
            count += 1
        else:
            break
    return count


def v3_micro_bias(rates: list[dict]) -> str:
    candles = [v3_candle(row) for row in rates[-3:]]
    if len(candles) < 3:
        return "MIXED"
    bull_count = sum(1 for candle in candles if candle.direction == "BULL")
    bear_count = sum(1 for candle in candles if candle.direction == "BEAR")
    closes_up = candles[-1].close > candles[-2].close > candles[-3].close
    closes_down = candles[-1].close < candles[-2].close < candles[-3].close
    if bull_count >= 2 and (closes_up or candles[-1].close > candles[0].open):
        return "BULLISH"
    if bear_count >= 2 and (closes_down or candles[-1].close < candles[0].open):
        return "BEARISH"
    return "MIXED"


def v3_exhaustion(rates: list[dict], ema20: float, atr_value: float) -> str:
    last = v3_candle(rates[-1])
    distance = abs(last.close - ema20)
    if v3_candle_streak(rates, "BULL") >= 3 and distance > atr_value:
        return "BULLISH_EXHAUSTION"
    if v3_candle_streak(rates, "BEAR") >= 3 and distance > atr_value:
        return "BEARISH_EXHAUSTION"
    return "NONE"


def v3_near_ema(candle: V3Candle, ema_value: float, atr_value: float, max_distance_atr: float) -> bool:
    if candle.low <= ema_value <= candle.high:
        return True
    return abs(candle.close - ema_value) <= max(atr_value * max_distance_atr, 0.01)


def v3_build_context(symbol: str, timeframe: str, rates: list[dict], price: dict) -> Optional[V3Context]:
    if len(rates) < 60:
        return None
    closes = extract_closes(rates)
    if len(closes) < 60:
        return None
    last = v3_candle(rates[-1])
    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)
    atr_value = atr(rates)
    median_atr = v3_atr_median(rates)
    spr = spread_points(price)
    recent = rates[-30:]
    range_high = max(v3_value(row, "high", "High") for row in recent)
    range_low = min(v3_value(row, "low", "Low") for row in recent)
    range_size = max(range_high - range_low, 0.01)
    range_position = (last.close - range_low) / range_size
    allowed, age, remaining = in_entry_window(timeframe)

    if ema20 > ema50 and last.close > ema50:
        trend = "BULLISH"
    elif ema20 < ema50 and last.close < ema50:
        trend = "BEARISH"
    else:
        trend = "MIXED"

    common_blocks = []
    if not math.isfinite(spr) or spr > MAX_SPREAD_POINTS:
        common_blocks.append(f"Spread too high/unavailable: {spr:.1f}")
    if atr_value <= 0:
        common_blocks.append("ATR unavailable")
    if last.range <= 0:
        common_blocks.append("Invalid candle range")
    if median_atr > 0 and last.range > median_atr * 2.8:
        common_blocks.append("Last candle range is abnormally large")

    return V3Context(
        symbol=symbol,
        timeframe=timeframe,
        rates=rates,
        closes=closes,
        last=last,
        ema20=ema20,
        ema50=ema50,
        atr=atr_value,
        median_atr=median_atr,
        spread_points=spr,
        range_high=range_high,
        range_low=range_low,
        range_position=range_position,
        bullish_streak=v3_candle_streak(rates, "BULL"),
        bearish_streak=v3_candle_streak(rates, "BEAR"),
        micro_bias=v3_micro_bias(rates),
        exhaustion=v3_exhaustion(rates, ema20, atr_value),
        candle_age=age,
        candle_remaining=remaining,
        candle_open_ts=candle_open_ts(now_utc_ts(), timeframe),
        trend=trend,
        common_blocks=common_blocks,
    )


def v3_timing_penalty(ctx: V3Context, score: int, reasons: list[str]) -> int:
    if ctx.candle_age > TIMEFRAMES[ctx.timeframe]["max_age"]:
        score -= 8
        reasons.append(f"Entry is late in candle: {ctx.candle_age}s")
    return score


def v3_micro_adjustment(ctx: V3Context, direction: str, score: int, reasons: list[str]) -> int:
    if direction == "BUY":
        if ctx.micro_bias == "BULLISH":
            score += 8
            reasons.append("3-candle micro bias supports BUY")
        elif ctx.micro_bias == "BEARISH":
            score -= 8
            reasons.append("3-candle micro bias against BUY")
        if ctx.exhaustion == "BULLISH_EXHAUSTION":
            score -= 12
            reasons.append("Bullish exhaustion penalty")
    elif direction == "SELL":
        if ctx.micro_bias == "BEARISH":
            score += 8
            reasons.append("3-candle micro bias supports SELL")
        elif ctx.micro_bias == "BULLISH":
            score -= 8
            reasons.append("3-candle micro bias against SELL")
        if ctx.exhaustion == "BEARISH_EXHAUSTION":
            score -= 12
            reasons.append("Bearish exhaustion penalty")
    return score


def v3_force_direction_allowed(ctx: V3Context, direction: str) -> tuple[bool, str]:
    if direction == "BUY" and ctx.range_position >= 0.88:
        return False, "Do not force BUY near range high"
    if direction == "SELL" and ctx.range_position <= 0.12:
        return False, "Do not force SELL near range low"
    if direction == "BUY" and ctx.exhaustion == "BULLISH_EXHAUSTION":
        return False, "Do not force BUY after bullish exhaustion"
    if direction == "SELL" and ctx.exhaustion == "BEARISH_EXHAUSTION":
        return False, "Do not force SELL after bearish exhaustion"
    return True, ""


def v3_trend_pullback(ctx: V3Context) -> V3Setup:
    reasons = []
    blocks = []
    score = 0
    direction = "NO_TRADE"
    last = ctx.last
    weak_body = last.range > 0 and last.body < last.range * 0.18
    pullback = any(
        v3_near_ema(v3_candle(row), ctx.ema20, ctx.atr, 0.8) or v3_near_ema(v3_candle(row), ctx.ema50, ctx.atr, 0.8)
        for row in ctx.rates[-3:]
    )

    if ctx.trend == "BULLISH":
        direction = "BUY"
        score += 25
        reasons.append("Setup Trend Pullback: EMA trend bullish")
        if pullback:
            score += 25
            reasons.append("Recent pullback near EMA20/EMA50")
        else:
            blocks.append("No recent EMA pullback")
        if last.direction == "BULL" or last.lower_wick >= last.body * 0.8:
            score += 25
            reasons.append("Bullish candle/rejection confirmation")
        else:
            blocks.append("No bullish candle confirmation")
        if abs(last.close - ctx.ema20) <= ctx.atr * 0.9:
            score += 10
            reasons.append("Not chasing far from EMA20")
        else:
            blocks.append("Price too far from EMA20 for pullback")
    elif ctx.trend == "BEARISH":
        direction = "SELL"
        score += 25
        reasons.append("Setup Trend Pullback: EMA trend bearish")
        if pullback:
            score += 25
            reasons.append("Recent pullback near EMA20/EMA50")
        else:
            blocks.append("No recent EMA pullback")
        if last.direction == "BEAR" or last.upper_wick >= last.body * 0.8:
            score += 25
            reasons.append("Bearish candle/rejection confirmation")
        else:
            blocks.append("No bearish candle confirmation")
        if abs(last.close - ctx.ema20) <= ctx.atr * 0.9:
            score += 10
            reasons.append("Not chasing far from EMA20")
        else:
            blocks.append("Price too far from EMA20 for pullback")
    else:
        blocks.append("EMA trend is mixed")

    if weak_body and direction != "NO_TRADE":
        score -= 10
        reasons.append("Weak/doji candle penalty")
    score = v3_micro_adjustment(ctx, direction, score, reasons)
    score = v3_timing_penalty(ctx, score, reasons)
    return V3Setup("Trend Pullback", direction, score, score >= 55 and not blocks, reasons, blocks)


def v3_range_reversal(ctx: V3Context) -> V3Setup:
    reasons = []
    blocks = []
    score = 0
    direction = "NO_TRADE"
    last = ctx.last
    weak_body = last.range > 0 and last.body < last.range * 0.18
    ema_gap = abs(ctx.ema20 - ctx.ema50)
    range_size = max(ctx.range_high - ctx.range_low, 0.01)
    range_like = ctx.trend == "MIXED" or ema_gap <= max(ctx.atr * 0.8, range_size * 0.18)

    if range_like:
        score += 20
        reasons.append("Setup Range Reversal: market is range-like")
    else:
        blocks.append("Market is not range-like")

    if ctx.range_position <= 0.25:
        direction = "BUY"
        score += 25
        reasons.append("Price near 30-candle range low")
        if last.lower_wick >= last.body * 0.8 or (last.direction == "BULL" and last.close_position >= 0.55):
            score += 30
            reasons.append("Lower rejection from range low")
        else:
            blocks.append("No lower rejection")
        if last.close >= ctx.range_low:
            score += 10
            reasons.append("Close stayed inside/above range low")
        else:
            blocks.append("Closed below range low")
    elif ctx.range_position >= 0.75:
        direction = "SELL"
        score += 25
        reasons.append("Price near 30-candle range high")
        if last.upper_wick >= last.body * 0.8 or (last.direction == "BEAR" and last.close_position <= 0.45):
            score += 30
            reasons.append("Upper rejection from range high")
        else:
            blocks.append("No upper rejection")
        if last.close <= ctx.range_high:
            score += 10
            reasons.append("Close stayed inside/below range high")
        else:
            blocks.append("Closed above range high")
    else:
        blocks.append("Price is not near range edge")

    if weak_body and direction != "NO_TRADE":
        if last.upper_wick >= last.range * 0.35 or last.lower_wick >= last.range * 0.35:
            score += 5
            reasons.append("Doji accepted because wick rejection is visible")
        else:
            score -= 10
            reasons.append("Weak/doji candle penalty")
    score = v3_micro_adjustment(ctx, direction, score, reasons)
    score = v3_timing_penalty(ctx, score, reasons)
    return V3Setup("Range Reversal", direction, score, score >= 55 and not blocks, reasons, blocks)


def v3_momentum_continuation(ctx: V3Context) -> V3Setup:
    reasons = []
    blocks = []
    score = 0
    direction = "NO_TRADE"
    last = ctx.last
    lookback = ctx.rates[-11:-1]
    prior_high = max(v3_value(row, "high", "High") for row in lookback)
    prior_low = min(v3_value(row, "low", "Low") for row in lookback)
    body_ok = last.range > 0 and last.body >= last.range * 0.4
    weak_body = last.range > 0 and last.body < last.range * 0.18

    if last.close > prior_high:
        direction = "BUY"
        score += 30
        reasons.append("Setup Momentum Continuation: breakout above prior 10-candle high")
        if weak_body:
            blocks.append("Doji candle cannot confirm momentum breakout")
        if last.close_position >= 0.72 and body_ok:
            score += 25
            reasons.append("Strong bullish breakout candle")
        else:
            blocks.append("Breakout candle not strong enough")
        if ctx.ema20 >= ctx.ema50 or last.close > ctx.ema20:
            score += 15
            reasons.append("Momentum aligns with EMA context")
        else:
            blocks.append("Momentum conflicts with EMA context")
        if ctx.bullish_streak <= 3:
            score += 10
            reasons.append("Not overextended by bullish streak")
        else:
            blocks.append("Too many bullish candles in a row")
        if abs(last.close - ctx.ema20) <= ctx.atr * 1.25:
            score += 10
            reasons.append("Breakout not too far from EMA20")
        else:
            blocks.append("Breakout is too far from EMA20")
    elif last.close < prior_low:
        direction = "SELL"
        score += 30
        reasons.append("Setup Momentum Continuation: breakdown below prior 10-candle low")
        if weak_body:
            blocks.append("Doji candle cannot confirm momentum breakdown")
        if last.close_position <= 0.28 and body_ok:
            score += 25
            reasons.append("Strong bearish breakdown candle")
        else:
            blocks.append("Breakdown candle not strong enough")
        if ctx.ema20 <= ctx.ema50 or last.close < ctx.ema20:
            score += 15
            reasons.append("Momentum aligns with EMA context")
        else:
            blocks.append("Momentum conflicts with EMA context")
        if ctx.bearish_streak <= 3:
            score += 10
            reasons.append("Not overextended by bearish streak")
        else:
            blocks.append("Too many bearish candles in a row")
        if abs(last.close - ctx.ema20) <= ctx.atr * 1.25:
            score += 10
            reasons.append("Breakdown not too far from EMA20")
        else:
            blocks.append("Breakdown is too far from EMA20")
    else:
        blocks.append("No 10-candle breakout/breakdown")

    score = v3_micro_adjustment(ctx, direction, score, reasons)
    score = v3_timing_penalty(ctx, score, reasons)
    return V3Setup("Momentum Continuation", direction, score, score >= 55 and not blocks, reasons, blocks)


def v3_choose_setup(ctx: V3Context, setups: list[V3Setup]) -> V3Decision:
    valid = [setup for setup in setups if setup.valid and setup.direction != "NO_TRADE"]
    if ctx.common_blocks:
        return V3Decision("NO_TRADE", max((setup.score for setup in setups), default=0), "Blocked", [], ctx.common_blocks, ctx, setups)
    if valid:
        def setup_rank(setup: V3Setup) -> tuple[int, int]:
            edge_bonus = 8 if setup.name == "Range Reversal" and (ctx.range_position <= 0.28 or ctx.range_position >= 0.72) else 0
            if setup.name == "Momentum Continuation" and 0.28 < ctx.range_position < 0.72:
                edge_bonus = 4
            return (setup.score + edge_bonus, 1 if setup.name == "Range Reversal" else 0)

        selected = max(valid, key=setup_rank)
        return V3Decision(selected.direction, min(95, selected.score), selected.name, selected.reasons, selected.blocks, ctx, setups)

    best = max(setups, key=lambda item: item.score)
    if FORCE_SIGNAL and best.direction != "NO_TRADE" and best.score >= 35:
        allowed, block = v3_force_direction_allowed(ctx, best.direction)
        weak_trend_pullback = best.name == "Trend Pullback" and any(
            key in block_text
            for block_text in best.blocks
            for key in ("No recent EMA pullback", "Price too far from EMA20")
        )
        if not allowed or weak_trend_pullback:
            best.blocks.append(block or "Trend Pullback force blocked because entry would chase price")
            return V3Decision("NO_TRADE", min(best.score, 45), "No valid setup", best.reasons, best.blocks, ctx, setups)
        reasons = [*best.reasons, "FORCE_SIGNAL fallback from best partial setup"]
        confidence_cap = 54
        if best.name == "Range Reversal" and any("rejection" in block.lower() for block in best.blocks):
            confidence_cap = 52
            reasons.append("Range force capped because rejection is missing")
        return V3Decision(best.direction, max(51, min(confidence_cap, best.score)), f"{best.name} (force)", reasons, best.blocks, ctx, setups)
    if FORCE_SIGNAL and ctx.micro_bias in ("BULLISH", "BEARISH"):
        direction = "BUY" if ctx.micro_bias == "BULLISH" else "SELL"
        allowed, block = v3_force_direction_allowed(ctx, direction)
        if not allowed:
            return V3Decision("NO_TRADE", 45, "No valid setup", [], [block], ctx, setups)
        reasons = [
            f"FORCE_SIGNAL fallback from 3-candle micro bias: {ctx.micro_bias}",
            "No full setup is valid, using weakest directional fallback",
        ]
        if ctx.exhaustion != "NONE":
            reasons.append(f"Exhaustion warning: {ctx.exhaustion}")
        return V3Decision(direction, 51, "Micro Bias (force)", reasons, best.blocks, ctx, setups)
    return V3Decision("NO_TRADE", best.score, "No valid setup", best.reasons, best.blocks, ctx, setups)


async def analyze_symbol(symbol: str, timeframe: str) -> Optional[SignalResult]:
    logger.info(f"[ANALYZE] analyze_symbol started: {symbol} {timeframe}")
    timeframe = normalize_timeframe(timeframe)
    logger.info(f"[ANALYZE] calling mt5.get_ohlcv({symbol}, {timeframe}, 150)")
    rates = await mt5.get_ohlcv(symbol, timeframe, 150)
    logger.info(f"[ANALYZE] get_ohlcv returned {len(rates)} rates")
    logger.info(f"[ANALYZE] calling mt5.get_symbol_price({symbol})")
    price = await mt5.get_symbol_price(symbol)
    spr = spread_points(price)
    allowed, age, remaining = in_entry_window(timeframe)
    analysis_rates = closed_rates_only(rates, timeframe)

    ctx = v3_build_context(symbol, timeframe, analysis_rates, price)
    if ctx is None:
        if FORCE_SIGNAL:
            direction = "BUY" if (now_utc_ts() // TIMEFRAMES[timeframe]["seconds"]) % 2 == 0 else "SELL"
            reasons = [
                "Force signal: MCP khong tra du lieu nen, van tao tin hieu de test",
                "Can kiem tra symbol broker/Market Watch neu muon tin hieu theo data that",
            ]
            return SignalResult(direction, 51, reasons, "No candle data", "Fallback", "Unknown", 50, 0, spr, age, remaining, candle_open_ts(now_utc_ts(), timeframe))
        return SignalResult("NO_TRADE", 0, ["Khong du du lieu nen de phan tich"], "N/A", "N/A", "N/A", 50, 0, spr, age, remaining, candle_open_ts(now_utc_ts(), timeframe))

    setups = [
        v3_trend_pullback(ctx),
        v3_range_reversal(ctx),
        v3_momentum_continuation(ctx),
    ]
    decision = v3_choose_setup(ctx, setups)
    rsi_value = rsi(ctx.closes)

    reasons = [f"Analyzer v3: {decision.setup}", *decision.reasons]
    if decision.blocks:
        reasons.extend(f"Block: {block}" for block in decision.blocks[:3])
    if ctx.common_blocks:
        reasons.extend(f"Hard block: {block}" for block in ctx.common_blocks[:3])
    reasons.extend([
        f"Context: trend={ctx.trend}, micro={ctx.micro_bias}, range_pos={ctx.range_position:.2f}",
        f"Streak: bull={ctx.bullish_streak}, bear={ctx.bearish_streak}, exhaustion={ctx.exhaustion}",
    ])
    if not allowed:
        reasons.append(f"Nen {timeframe} da chay {age}s, vuot moc dau nen {TIMEFRAMES[timeframe]['max_age']}s")
        decision.direction = "NO_TRADE"
        decision.confidence = min(decision.confidence, 40)

    if decision.direction in ("BUY", "SELL"):
        direction = decision.direction
        confidence = decision.confidence
    else:
        direction = "NO_TRADE"
        confidence = decision.confidence
        reasons.append("No valid v3 setup")

    trend = "Bullish" if ctx.trend == "BULLISH" else "Bearish" if ctx.trend == "BEARISH" else "Mixed"
    momentum = "Bullish" if ctx.micro_bias == "BULLISH" else "Bearish" if ctx.micro_bias == "BEARISH" else "Mixed"
    volatility = "High" if ctx.median_atr > 0 and ctx.atr > ctx.median_atr * 1.6 else "Normal" if ctx.atr > 0 else "Unknown"

    logger.info(f"[ANALYZE] analyze_symbol done: {direction} {confidence}%")
    return SignalResult(direction, confidence, reasons, trend, momentum, volatility, rsi_value, ctx.atr, spr, age, remaining, ctx.candle_open_ts)


async def emergency_sl(symbol: str, direction: str, atr_value: float) -> float:
    price = await mt5.get_symbol_price(symbol)
    bid = float(price.get("bid") or 0)
    ask = float(price.get("ask") or 0)
    distance = max(atr_value * EMERGENCY_SL_ATR_MULT, 1.0)
    if direction == "BUY":
        return round((ask or bid) - distance, 2)
    return round((bid or ask) + distance, 2)



def format_signal(symbol: str, timeframe: str, signal: SignalResult) -> str:
    reasons = "\n".join(f"- {escape_markdown(str(r), version=1)}" for r in signal.reasons[:8])
    return "\n".join([
        f"*{symbol} {timeframe} Analysis*",
        "",
        f"Signal: *{signal.direction}*",
        f"Confidence: *{signal.confidence}%*",
        f"Entry window: {signal.candle_age}s / max {TIMEFRAMES[timeframe]['max_age']}s",
        f"Trade management: SL/TP, no candle-close exit",
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
        f"Lot: `{lot}` | SL: `{SL_ATR_MULT} ATR` | TP: `{TP_RR}R`",
        f"Auto: *{status}*",
        "",
        "Entry age limit:",
        "M1 <= 20s | M5 <= 90s | M15 <= 4m | H1 <= 15m",
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
                settings["symbol"] = arg
                symbol = resolve_symbol_name(arg)

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
        selected = data.split(":", 1)[1]
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
        result = await open_auto_trade(chat_id, symbol, tf, signal, manual=True)
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
            if not allowed:
                await asyncio.sleep(POLL_SECONDS)
                continue

            signal = await analyze_symbol( symbol, timeframe)
            if not signal or signal.direction == "NO_TRADE":
                await context.bot.send_message(chat_id, format_signal(symbol, timeframe, signal), parse_mode="Markdown")
                await asyncio.sleep(POLL_SECONDS)
                continue
            if signal.confidence < MIN_CONFIDENCE:
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


async def open_auto_trade(chat_id: int, symbol: str, timeframe: str, signal: SignalResult, manual: bool = False) -> str:
    allowed, age, remaining = in_entry_window(timeframe)
    if not manual and not allowed:
        return f"Entry rejected: nen {timeframe} da chay {age}s."
    if not manual and signal.confidence < MIN_CONFIDENCE:
        return f"Entry rejected: confidence {signal.confidence}% < {MIN_CONFIDENCE}%."
    existing_positions = await bot_positions(symbol)
    before_tickets = {position_ticket(pos) for pos in existing_positions if position_ticket(pos)}

    lot = chat_lot(chat_id)
    price = await mt5.get_symbol_price(symbol)
    sl, tp, risk_distance = calculate_sl_tp(symbol, signal.direction, price, signal.atr)
    comment = f"{ORDER_COMMENT_PREFIX}_{timeframe}_{signal.direction}"
    result = await mt5.place_market_order(symbol, signal.direction, lot, sl, tp, comment)
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
    modify_error = ""
    if ticket is not None:
        modify_result = await mt5.modify_position(int(ticket), sl, tp)
        if mt5._is_error(modify_result):
            modify_error = f"\nSL/TP modify error: `{mt5._error_message(modify_result)}`"
    ticket = ticket or "N/A"
    add_active_trade(chat_id, {
        "ticket": ticket,
        "symbol": symbol,
        "timeframe": timeframe,
        "direction": signal.direction,
        "candle_open_ts": signal.candle_open_ts,
        "opened_at": now_utc_ts(),
        "sl": sl,
        "tp": tp,
        "risk_distance": risk_distance,
        "atr": signal.atr,
        "breakeven_done": False,
        "setup": signal.reasons[0] if signal.reasons else "",
        "confidence": signal.confidence,
    })
    save_active_trades()
    return "\n".join([
        "*Trade Active*",
        "",
        f"Symbol: `{symbol}`",
        f"Direction: *{signal.direction}*",
        f"Timeframe: *{timeframe}*",
        f"Lot: `{lot}`",
        f"SL: `{sl}`",
        f"TP: `{tp}`",
        f"Management: `SL/TP + breakeven/trailing`",
        f"Ticket: `{ticket}`",
        modify_error,
    ])


async def manage_active_trade(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    trades = active_trade_items(chat_id)
    if not trades:
        return

    positions = await bot_positions()
    position_by_ticket = {position_ticket(pos): pos for pos in positions if position_ticket(pos)}
    remaining_trades = []
    finished_lines = []

    for trade in trades:
        ticket = trade.get("ticket")
        if ticket == "N/A" or not str(ticket).isdigit():
            remaining_trades.append(trade)
            continue

        ticket_int = int(ticket)
        pos = position_by_ticket.get(ticket_int)
        if not pos:
            finished_lines.append(f"- Ticket `{ticket_int}` finished by SL/TP/manual close")
            continue

        symbol = str(trade.get("symbol") or first_value(pos, ("symbol", "Symbol"), SYMBOL))
        direction = str(trade.get("direction") or first_value(pos, ("type", "Type"), "")).upper()
        entry = position_open_price(pos)
        risk_distance = float(trade.get("risk_distance") or 0)
        sl = float(trade.get("sl") or first_value(pos, ("stop_loss", "StopLoss", "sl", "SL"), 0) or 0)
        tp = float(trade.get("tp") or first_value(pos, ("take_profit", "TakeProfit", "tp", "TP"), 0) or 0)

        if entry > 0 and risk_distance > 0 and direction in ("BUY", "SELL"):
            price = await mt5.get_symbol_price(symbol)
            current = position_current_price(pos, price)
            move = current - entry if direction == "BUY" else entry - current
            new_sl = None

            if not trade.get("breakeven_done") and move >= risk_distance * BREAKEVEN_R_MULT:
                new_sl = round(entry, symbol_digits(symbol))
                trade["breakeven_done"] = True
            elif move >= risk_distance * TRAILING_R_MULT:
                atr_value = trade.get("atr")
                if not atr_value:
                    rates = closed_rates_only(await mt5.get_ohlcv(symbol, normalize_timeframe(trade.get("timeframe") or DEFAULT_TIMEFRAME), 80), normalize_timeframe(trade.get("timeframe") or DEFAULT_TIMEFRAME))
                    atr_value = atr(rates)
                trail_distance = max(float(atr_value or 0) * TRAILING_ATR_MULT, risk_distance * 0.5)
                if direction == "BUY":
                    candidate = round(current - trail_distance, symbol_digits(symbol))
                    if candidate > sl:
                        new_sl = candidate
                else:
                    candidate = round(current + trail_distance, symbol_digits(symbol))
                    if sl <= 0 or candidate < sl:
                        new_sl = candidate

            if new_sl is not None:
                modify_result = await mt5.modify_position(ticket_int, new_sl, tp if tp > 0 else None)
                if mt5._is_error(modify_result):
                    logger.warning("Cannot update SL for %s: %s", ticket_int, mt5._error_message(modify_result))
                else:
                    trade["sl"] = new_sl

        remaining_trades.append(trade)

    if finished_lines:
        await context.bot.send_message(
            chat_id,
            "\n".join(["*Trade Finished*", "", *finished_lines, "", "*Menu:*"]),
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )
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
