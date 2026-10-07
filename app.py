import math
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

KALSHI_BASE = "https://external-api.kalshi.com/trade-api/v2"
COINBASE_SPOT = "https://api.coinbase.com/v2/prices/{asset}-USD/spot"
COINBASE_EXCHANGE = "https://api.exchange.coinbase.com"
ASSETS = {
    "BTC": "KXBTC15M",
    "ETH": "KXETH15M",
    "SOL": "KXSOL15M",
    "XRP": "KXXRP15M",
    "DOGE": "KXDOGE15M",
    "BNB": "KXBNB15M",
    "HYPE": "KXHYPE15M",
}

app = FastAPI(title="Kalshi 15M Market Command Center", version="4.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

session = requests.Session()
session.headers.update({"User-Agent": "KNKB-15M-Scanner/4.0"})

_cache_lock = threading.Lock()
_scan_cache = {"ts": 0.0, "payload": None}
_candle_cache = {}


def get_json(url, params=None, timeout=6):
    r = session.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def kalshi_json(path, params=None, timeout=6):
    return get_json(KALSHI_BASE + path, params=params, timeout=timeout)


def num(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def sigmoid(x):
    return 1 / (1 + math.exp(-clamp(x, -20, 20)))


def parse_close(market):
    close = market.get("close_time") or market.get("expiration_time")
    if not close:
        return None
    try:
        return datetime.fromisoformat(close.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def select_open_markets(markets):
    now = time.time()
    selected = {}
    for asset, series in ASSETS.items():
        candidates = []
        for m in markets:
            if not str(m.get("ticker", "")).startswith(series):
                continue
            close_ts = parse_close(m)
            if close_ts is None:
                continue
            seconds_left = close_ts - now
            # Keep the currently active contract and a tiny grace period around close.
            if -5 <= seconds_left <= 16 * 60 + 45:
                candidates.append((seconds_left < 0, abs(seconds_left), m))
        if candidates:
            candidates.sort(key=lambda x: (x[0], x[1]))
            selected[asset] = candidates[0][2]
    return selected


def parse_target(market):
    text = " ".join(str(market.get(k, "")) for k in (
        "title", "subtitle", "yes_sub_title", "no_sub_title", "rules_primary", "rules_secondary"
    ))
    for pattern in (r"\$([0-9][0-9,]*(?:\.[0-9]+)?)", r"([0-9][0-9,]*(?:\.[0-9]+)?)"):
        for match in re.finditer(pattern, text):
            value = num(match.group(1).replace(",", ""), None)
            if value and value > 1:
                return value
    return None


def market_prob(market):
    ask = num(market.get("yes_ask_dollars"), None)
    bid = num(market.get("yes_bid_dollars"), None)
    last = num(market.get("last_price_dollars"), None)

    if ask is None and market.get("yes_ask") is not None:
        ask = num(market.get("yes_ask")) / 100
    if bid is None and market.get("yes_bid") is not None:
        bid = num(market.get("yes_bid")) / 100
    if last is None and market.get("last_price") is not None:
        last = num(market.get("last_price")) / 100

    implied = ask if ask not in (None, 0) else (last if last not in (None, 0) else (bid or 0))
    return bid or 0, ask or 0, last or 0, implied


def orderbook(ticker):
    data = kalshi_json(f"/markets/{ticker}/orderbook", timeout=4)
    ob = data.get("orderbook_fp") or data.get("orderbook") or {}
    yes_levels = ob.get("yes_dollars") or ob.get("yes") or []
    no_levels = ob.get("no_dollars") or ob.get("no") or []
    yes_levels = yes_levels[:10] if isinstance(yes_levels, list) else []
    no_levels = no_levels[:10] if isinstance(no_levels, list) else []

    def qty_sum(levels):
        total = 0.0
        for level in levels:
            try:
                total += num(level[1])
            except Exception:
                pass
        return total

    yes_qty = qty_sum(yes_levels)
    no_qty = qty_sum(no_levels)
    total = yes_qty + no_qty
    imbalance = (yes_qty - no_qty) / total if total else 0.0
    return {"yes_qty": yes_qty, "no_qty": no_qty, "imbalance": imbalance}


def spot_price(asset):
    try:
        data = get_json(COINBASE_SPOT.format(asset=asset), timeout=3)
        amount = ((data or {}).get("data") or {}).get("amount")
        if amount is not None:
            return num(amount, None)
    except Exception:
        pass

    try:
        data = get_json("https://api.binance.com/api/v3/ticker/price", {"symbol": asset + "USDT"}, timeout=3)
        return num((data or {}).get("price"), None)
    except Exception:
        return None


def candle_stats(asset):
    """Return short-horizon momentum + realized volatility from Coinbase 1m candles.

    Coinbase candle rows are [time, low, high, open, close, volume], newest first.
    Cached because these features do not need 2.5-second refreshes.
    """
    now = time.time()
    with _cache_lock:
        cached = _candle_cache.get(asset)
        if cached and now - cached["ts"] < 20:
            return cached["data"]

    stats = {
        "momentum_3m": None,
        "momentum_5m": None,
        "momentum_10m": None,
        "volatility_10m": None,
        "range_5m": None,
        "trend": "UNKNOWN",
    }

    try:
        candles = get_json(
            f"{COINBASE_EXCHANGE}/products/{asset}-USD/candles",
            params={"granularity": 60},
            timeout=4,
        )
        if not isinstance(candles, list) or len(candles) < 6:
            raise ValueError("not enough candle data")

        candles = sorted(candles[:16], key=lambda x: x[0])
        closes = [num(c[4], None) for c in candles if len(c) >= 5]
        highs = [num(c[2], None) for c in candles if len(c) >= 5]
        lows = [num(c[1], None) for c in candles if len(c) >= 5]
        closes = [x for x in closes if x and x > 0]
        highs = [x for x in highs if x and x > 0]
        lows = [x for x in lows if x and x > 0]

        def momentum(minutes):
            if len(closes) <= minutes:
                return None
            start = closes[-(minutes + 1)]
            end = closes[-1]
            return (end / start - 1) * 100 if start else None

        returns = []
        for a, b in zip(closes[-11:-1], closes[-10:]):
            if a and b:
                returns.append((b / a - 1) * 100)
        vol = None
        if len(returns) >= 2:
            mean = sum(returns) / len(returns)
            variance = sum((x - mean) ** 2 for x in returns) / (len(returns) - 1)
            vol = math.sqrt(variance)

        range_5m = None
        if len(highs) >= 5 and len(lows) >= 5 and closes:
            hi = max(highs[-5:])
            lo = min(lows[-5:])
            mid = closes[-1]
            range_5m = (hi - lo) / mid * 100 if mid else None

        m3 = momentum(3)
        m5 = momentum(5)
        m10 = momentum(10)
        trend_score = (m3 or 0) * 0.55 + (m5 or 0) * 0.30 + (m10 or 0) * 0.15
        if trend_score > 0.06:
            trend = "UP"
        elif trend_score < -0.06:
            trend = "DOWN"
        else:
            trend = "FLAT"

        stats = {
            "momentum_3m": m3,
            "momentum_5m": m5,
            "momentum_10m": m10,
            "volatility_10m": vol,
            "range_5m": range_5m,
            "trend": trend,
        }
    except Exception:
        pass

    with _cache_lock:
        _candle_cache[asset] = {"ts": now, "data": stats}
    return stats


def analyze(asset, market):
    close_ts = parse_close(market)
    seconds_left = max(0.0, (close_ts or time.time()) - time.time())
    bid, ask, last, market_p = market_prob(market)

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_ob = pool.submit(orderbook, market["ticker"])
        f_spot = pool.submit(spot_price, asset)
        f_stats = pool.submit(candle_stats, asset)
        ob = f_ob.result()
        spot = f_spot.result()
        stats = f_stats.result()

    target = parse_target(market)
    distance_pct = None
    distance_z = 0.0
    if spot and target:
        distance_pct = (spot - target) / target * 100
        # The closer to expiry, the more target distance matters.
        expected_move_scale = max(0.012, 0.050 * math.sqrt(max(seconds_left, 1) / 900))
        distance_z = distance_pct / expected_move_scale

    m3 = stats.get("momentum_3m") or 0.0
    m5 = stats.get("momentum_5m") or 0.0
    vol = stats.get("volatility_10m") or 0.08
    momentum_score = clamp((0.7 * m3 + 0.3 * m5) / max(vol, 0.03), -3.0, 3.0)
    time_weight = clamp((900 - seconds_left) / 900, 0.12, 1.0)

    # Conservative heuristic. This is deliberately not labelled as a trained AI model.
    score = (
        0.92 * distance_z * time_weight
        + 0.38 * momentum_score
        + 0.45 * ob["imbalance"]
    )
    model_p = sigmoid(score)
    edge = model_p - market_p if market_p else 0.0

    # Setup quality blends magnitude of edge, time relevance, and feature agreement.
    direction = 1 if edge >= 0 else -1
    agreement = 0
    if distance_pct is not None and distance_pct * direction > 0:
        agreement += 1
    if m3 * direction > 0:
        agreement += 1
    if ob["imbalance"] * direction > 0:
        agreement += 1
    setup_score = clamp(abs(edge) * 520 + agreement * 8 + time_weight * 8, 0, 100)

    action = "PASS"
    confidence = "LOW"
    if market_p and seconds_left > 12:
        if abs(edge) >= 0.14 and setup_score >= 72:
            action = "LEAN YES" if edge > 0 else "LEAN NO"
            confidence = "HIGH"
        elif abs(edge) >= 0.08 and setup_score >= 52:
            action = "WATCH YES" if edge > 0 else "WATCH NO"
            confidence = "MEDIUM"

    return {
        "asset": asset,
        "series": ASSETS[asset],
        "ticker": market["ticker"],
        "title": market.get("title"),
        "target": target,
        "spot": spot,
        "seconds_left": round(seconds_left, 1),
        "yes_bid": bid,
        "yes_ask": ask,
        "last": last,
        "market_probability": market_p,
        "model_probability": model_p,
        "edge": edge,
        "distance_pct": distance_pct,
        "orderbook_imbalance": ob["imbalance"],
        "yes_qty": ob["yes_qty"],
        "no_qty": ob["no_qty"],
        "momentum_3m": stats.get("momentum_3m"),
        "momentum_5m": stats.get("momentum_5m"),
        "momentum_10m": stats.get("momentum_10m"),
        "volatility_10m": stats.get("volatility_10m"),
        "range_5m": stats.get("range_5m"),
        "trend": stats.get("trend"),
        "setup_score": round(setup_score, 1),
        "action": action,
        "confidence": confidence,
        "model_status": "HEURISTIC — PAPER MODE / CALIBRATION REQUIRED",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_scan():
    started = time.time()
    errors = []
    try:
        payload = kalshi_json("/markets", {"status": "open", "limit": 1000}, timeout=7)
        markets = payload.get("markets", [])
    except Exception as exc:
        return {
            "markets": [],
            "errors": [{"asset": "KALSHI", "error": str(exc)}],
            "live": False,
            "elapsed_ms": round((time.time() - started) * 1000),
            "server_time": datetime.now(timezone.utc).isoformat(),
        }

    chosen = select_open_markets(markets)
    rows = []
    if chosen:
        with ThreadPoolExecutor(max_workers=min(7, len(chosen))) as pool:
            futures = {pool.submit(analyze, asset, market): asset for asset, market in chosen.items()}
            for future in as_completed(futures):
                asset = futures[future]
                try:
                    rows.append(future.result())
                except Exception as exc:
                    errors.append({"asset": asset, "error": str(exc)})

    rows.sort(key=lambda x: (x["setup_score"], abs(x["edge"])), reverse=True)
    return {
        "markets": rows,
        "errors": errors,
        "live": True,
        "found": list(chosen.keys()),
        "elapsed_ms": round((time.time() - started) * 1000),
        "server_time": datetime.now(timezone.utc).isoformat(),
        "model": "KNKB heuristic v4",
    }


@app.get("/api/health")
def health():
    return {"ok": True, "version": "4.0", "time": datetime.now(timezone.utc).isoformat()}


@app.get("/api/scan")
def scan():
    # Prevent every phone refresh from hammering upstream APIs. All users share a ~2s snapshot.
    now = time.time()
    with _cache_lock:
        if _scan_cache["payload"] is not None and now - _scan_cache["ts"] < 2.0:
            return _scan_cache["payload"]

    payload = build_scan()
    with _cache_lock:
        _scan_cache["ts"] = time.time()
        _scan_cache["payload"] = payload
    return payload


@app.head("/")
def head_index():
    return {"ok": True}


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "index.html", headers={"Cache-Control": "no-store"})


@app.get("/{path:path}")
def static(path: str):
    p = Path(__file__).parent / path
    if p.exists() and p.is_file():
        return FileResponse(p, headers={"Cache-Control": "no-store"})
    return FileResponse(Path(__file__).parent / "index.html", headers={"Cache-Control": "no-store"})
