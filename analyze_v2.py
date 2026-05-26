#!/usr/bin/env python3
"""Standalone analyzer v2 test script.

This file does not change or start the Telegram bot. It only calls MCP data
through bot.mt5 and prints a signal candidate for manual testing.
"""

import argparse
import asyncio
import math
from dataclasses import dataclass
from typing import Any

import bot


@dataclass
class V2Result:
    direction: str
    confidence: int
    reasons: list[str]
    trend: str
    pullback: str
    candle: str
    atr: float
    spread_points: float


def value(row: dict, *keys: str, default: float = 0.0) -> float:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            try:
                return float(row[key])
            except Exception:
                pass
    return default


def candle_parts(row: dict) -> dict[str, float]:
    open_price = value(row, "open", "Open")
    high = value(row, "high", "High")
    low = value(row, "low", "Low")
    close = value(row, "close", "Close")
    body = abs(close - open_price)
    candle_range = max(high - low, 0.0)
    upper_wick = high - max(open_price, close)
    lower_wick = min(open_price, close) - low
    close_position = (close - low) / candle_range if candle_range > 0 else 0.5
    return {
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "body": body,
        "range": candle_range,
        "upper_wick": upper_wick,
        "lower_wick": lower_wick,
        "close_position": close_position,
    }


def atr_median(rates: list[dict], count: int = 40) -> float:
    values = []
    sample = rates[-count:] if len(rates) >= count else rates
    for prev, cur in zip(sample, sample[1:]):
        high = value(cur, "high", "High")
        low = value(cur, "low", "Low")
        prev_close = value(prev, "close", "Close")
        values.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if not values:
        return 0.0
    values.sort()
    return values[len(values) // 2]


async def analyze_symbol_v2(symbol: str, timeframe: str = "M5") -> V2Result:
    timeframe = bot.normalize_timeframe(timeframe)
    rates = await bot.mt5.get_ohlcv(symbol, timeframe, 150)
    price = await bot.mt5.get_symbol_price(symbol)
    spr = bot.spread_points(price)

    if len(rates) < 60:
        return V2Result(
            "NO_TRADE",
            0,
            [f"Not enough candles: {len(rates)}"],
            "N/A",
            "N/A",
            "N/A",
            0.0,
            spr,
        )

    closes = bot.extract_closes(rates)
    ema20 = bot.ema(closes, 20)
    ema50 = bot.ema(closes, 50)
    atr_value = bot.atr(rates)
    median_atr = atr_median(rates)
    last = candle_parts(rates[-1])

    reasons: list[str] = []
    buy_score = 0
    sell_score = 0

    if ema20 > ema50 and last["close"] > ema50:
        buy_score += 35
        trend = "Bullish"
        reasons.append("EMA20 > EMA50 and close above EMA50")
    elif ema20 < ema50 and last["close"] < ema50:
        sell_score += 35
        trend = "Bearish"
        reasons.append("EMA20 < EMA50 and close below EMA50")
    else:
        trend = "Mixed"
        reasons.append("EMA trend mixed")

    pullback_distance = abs(last["close"] - ema20)
    pullback_limit = max(atr_value * 0.6, 0.01)
    touched_ema20 = last["low"] <= ema20 <= last["high"]
    near_ema20 = pullback_distance <= pullback_limit

    if trend == "Bullish" and (touched_ema20 or near_ema20):
        buy_score += 25
        pullback = "Bullish pullback"
        reasons.append("Pullback near/touch EMA20")
    elif trend == "Bearish" and (touched_ema20 or near_ema20):
        sell_score += 25
        pullback = "Bearish pullback"
        reasons.append("Pullback near/touch EMA20")
    else:
        pullback = "No clean pullback"
        reasons.append("No clean EMA20 pullback")

    body_ok = last["range"] > 0 and last["body"] >= last["range"] * 0.25
    buy_candle = last["close"] > last["open"] and last["close_position"] >= 0.6 and body_ok
    sell_candle = last["close"] < last["open"] and last["close_position"] <= 0.4 and body_ok

    if buy_candle:
        buy_score += 25
        candle = "Bullish confirmation"
        reasons.append("Bullish candle confirmation")
    elif sell_candle:
        sell_score += 25
        candle = "Bearish confirmation"
        reasons.append("Bearish candle confirmation")
    else:
        candle = "Weak candle"
        reasons.append("Weak/unclear candle confirmation")

    if median_atr > 0 and 0.5 * median_atr <= atr_value <= 2.2 * median_atr:
        buy_score += 10
        sell_score += 10
        reasons.append("ATR normal")
    else:
        reasons.append(f"ATR abnormal: current={atr_value:.2f}, median={median_atr:.2f}")

    if spr > bot.MAX_SPREAD_POINTS:
        reasons.append(f"Spread too high: {spr:.1f} points")
        return V2Result("NO_TRADE", max(buy_score, sell_score), reasons, trend, pullback, candle, atr_value, spr)

    if buy_score > sell_score and buy_score >= 60:
        direction = "BUY"
        confidence = min(95, buy_score)
    elif sell_score > buy_score and sell_score >= 60:
        direction = "SELL"
        confidence = min(95, sell_score)
    elif bot.FORCE_SIGNAL:
        direction = "BUY" if buy_score >= sell_score else "SELL"
        confidence = max(51, min(59, max(buy_score, sell_score)))
        reasons.append("FORCE_SIGNAL fallback")
    else:
        direction = "NO_TRADE"
        confidence = max(buy_score, sell_score)

    if not math.isfinite(spr):
        reasons.append("Spread unavailable")

    return V2Result(direction, confidence, reasons, trend, pullback, candle, atr_value, spr)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("symbol", nargs="?", default="XAUUSD")
    parser.add_argument("timeframe", nargs="?", default="M5")
    args = parser.parse_args()

    await bot.auto_detect_symbols()
    symbol = bot.resolve_symbol_name(args.symbol)
    result = await analyze_symbol_v2(symbol, args.timeframe)

    print(f"Symbol: {symbol}")
    print(f"Timeframe: {bot.normalize_timeframe(args.timeframe)}")
    print(f"Signal: {result.direction}")
    print(f"Confidence: {result.confidence}%")
    print(f"Trend: {result.trend}")
    print(f"Pullback: {result.pullback}")
    print(f"Candle: {result.candle}")
    print(f"ATR: {result.atr:.2f}")
    print(f"Spread: {result.spread_points:.1f} points")
    print("Reasons:")
    for reason in result.reasons:
        print(f"- {reason}")


if __name__ == "__main__":
    asyncio.run(main())
