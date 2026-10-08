import math
import re
import time
import threading
import os
import asyncio
import logging
import csv
import io
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
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

VERSION = "9.0"
SCAN_INTERVAL = max(3.0, float(os.getenv("KNKB_SCAN_INTERVAL", "5")))
STALE_AFTER = max(20.0, SCAN_INTERVAL * 3)
logger = logging.getLogger("knkb")

@asynccontextmanager
async def lifespan(app):
    stop = threading.Event()
    worker = threading.Thread(target=collector, args=(stop,), daemon=True, name="knkb-collector")
    if os.getenv("KNKB_BACKGROUND", "1") != "0":
        worker.start()
    yield
    stop.set()
    if worker.is_alive():
        await asyncio.to_thread(worker.join, timeout=2)

app = FastAPI(title="Kalshi 15M Market Command Center", version=VERSION, lifespan=lifespan)

session = requests.Session()
session.headers.update({"User-Agent": "KNKB-15M-Scanner/9.0"})

_cache_lock = threading.Lock()
_scan_cache = {"ts": 0.0, "payload": None}
_candle_cache = {}
_scan_lock = threading.Lock()
_settlement_lock = threading.Lock()
_thread_sessions = threading.local()
_collector_status = {"last_success": None, "last_error": None}

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+psycopg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

if DATABASE_URL:
    DB_URL = DATABASE_URL
    DB_STORAGE = "postgres-persistent"
else:
    DB_PATH = os.getenv("KNKB_DB_PATH", str(Path(__file__).parent / "knkb_history.db"))
    DB_URL = f"sqlite:///{DB_PATH}"
    DB_STORAGE = "sqlite-local"

from sqlalchemy import create_engine, text, inspect

_engine_kwargs = {"pool_pre_ping": True}
if DB_URL.startswith("sqlite"):
    _engine_kwargs["connect_args"] = {"check_same_thread": False}
engine = create_engine(DB_URL, **_engine_kwargs)
_db_lock = threading.Lock()
_last_settlement_sweep = 0.0


def init_db():
    with _db_lock, engine.begin() as con:
        if DB_STORAGE == "postgres-persistent":
            con.execute(text("""
                CREATE TABLE IF NOT EXISTS signals (
                    id BIGSERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    series TEXT,
                    captured_at TEXT NOT NULL,
                    seconds_left DOUBLE PRECISION,
                    time_bucket INTEGER NOT NULL,
                    target DOUBLE PRECISION,
                    spot DOUBLE PRECISION,
                    market_probability DOUBLE PRECISION,
                    model_probability DOUBLE PRECISION,
                    edge DOUBLE PRECISION,
                    distance_pct DOUBLE PRECISION,
                    orderbook_imbalance DOUBLE PRECISION,
                    momentum_3m DOUBLE PRECISION,
                    momentum_5m DOUBLE PRECISION,
                    momentum_10m DOUBLE PRECISION,
                    volatility_10m DOUBLE PRECISION,
                    range_5m DOUBLE PRECISION,
                    trend TEXT,
                    setup_score DOUBLE PRECISION,
                    action TEXT,
                    confidence TEXT,
                    outcome TEXT,
                    correct INTEGER,
                    UNIQUE(ticker, time_bucket)
                )
            """))
        else:
            con.execute(text("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    series TEXT,
                    captured_at TEXT NOT NULL,
                    seconds_left REAL,
                    time_bucket INTEGER NOT NULL,
                    target REAL,
                    spot REAL,
                    market_probability REAL,
                    model_probability REAL,
                    edge REAL,
                    distance_pct REAL,
                    orderbook_imbalance REAL,
                    momentum_3m REAL,
                    momentum_5m REAL,
                    momentum_10m REAL,
                    volatility_10m REAL,
                    range_5m REAL,
                    trend TEXT,
                    setup_score REAL,
                    action TEXT,
                    confidence TEXT,
                    outcome TEXT,
                    correct INTEGER,
                    UNIQUE(ticker, time_bucket)
                )
            """))
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS tracked_markets (
                ticker TEXT PRIMARY KEY,
                asset TEXT NOT NULL,
                target DOUBLE PRECISION,
                close_time DOUBLE PRECISION,
                outcome TEXT,
                settled_at TEXT,
                last_checked DOUBLE PRECISION DEFAULT 0
            )
        """))
        con.execute(text("CREATE INDEX IF NOT EXISTS idx_signals_asset ON signals(asset)"))
        con.execute(text("CREATE INDEX IF NOT EXISTS idx_signals_outcome ON signals(outcome)"))
        if "signal_version" not in {c["name"] for c in inspect(con).get_columns("signals")}:
            con.execute(text("ALTER TABLE signals ADD COLUMN signal_version TEXT DEFAULT '8.0'"))


def log_signals(rows):
    if not rows:
        return
    now_iso = datetime.now(timezone.utc).isoformat()
    with _db_lock, engine.begin() as con:
        for x in rows:
            seconds_left = max(0.0, num(x.get("seconds_left")))
            bucket = int(seconds_left // 30) * 30
            close_time = x.get("close_time") or time.time() + seconds_left
            con.execute(text("""
                INSERT INTO tracked_markets (ticker, asset, target, close_time)
                VALUES (:ticker, :asset, :target, :close_time)
                ON CONFLICT (ticker) DO NOTHING
            """), {"ticker": x.get("ticker"), "asset": x.get("asset"), "target": x.get("target"), "close_time": close_time})
            con.execute(text("""
                INSERT INTO signals (
                    ticker, asset, series, captured_at, seconds_left, time_bucket,
                    target, spot, market_probability, model_probability, edge,
                    distance_pct, orderbook_imbalance, momentum_3m, momentum_5m,
                    momentum_10m, volatility_10m, range_5m, trend, setup_score,
                    action, confidence, signal_version
                ) VALUES (
                    :ticker,:asset,:series,:captured_at,:seconds_left,:time_bucket,
                    :target,:spot,:market_probability,:model_probability,:edge,
                    :distance_pct,:orderbook_imbalance,:momentum_3m,:momentum_5m,
                    :momentum_10m,:volatility_10m,:range_5m,:trend,:setup_score,
                    :action,:confidence,:signal_version
                ) ON CONFLICT (ticker, time_bucket) DO NOTHING
            """), {
                "ticker": x.get("ticker"), "asset": x.get("asset"), "series": x.get("series"),
                "captured_at": now_iso, "seconds_left": seconds_left, "time_bucket": bucket,
                "target": x.get("target"), "spot": x.get("spot"), "market_probability": x.get("market_probability"),
                "model_probability": x.get("model_probability"), "edge": x.get("edge"), "distance_pct": x.get("distance_pct"),
                "orderbook_imbalance": x.get("orderbook_imbalance"), "momentum_3m": x.get("momentum_3m"),
                "momentum_5m": x.get("momentum_5m"), "momentum_10m": x.get("momentum_10m"),
                "volatility_10m": x.get("volatility_10m"), "range_5m": x.get("range_5m"),
                "trend": x.get("trend"), "setup_score": x.get("setup_score"), "action": x.get("action"),
                "confidence": x.get("confidence"), "signal_version": VERSION
            })


def extract_outcome(payload):
    market = (payload or {}).get("market") or payload or {}
    if market.get("is_provisional"):
        return None
    result = str(market.get("result") or market.get("settlement_result") or "").lower()
    if result in ("yes", "no"):
        return result.upper()
    if market.get("is_provisional"):
        return None
    value = market.get("settlement_value_dollars")
    if value is None and market.get("settlement_value") is not None:
        # Legacy settlement_value is cents, not dollars.
        value = num(market.get("settlement_value"), None)
        value = value / 100 if value is not None else None
    if market.get("status") not in ("settled", "finalized"):
        return None
    if value is not None:
        try:
            value = float(value)
            if value == 1: return "YES"
            if value == 0: return "NO"
        except Exception:
            pass
    return None


def settle_pending(limit=12):
    now = time.time()
    with _db_lock, engine.begin() as con:
        pending = con.execute(text("""
            SELECT ticker, asset FROM tracked_markets
            WHERE outcome IS NULL AND close_time < :cutoff AND (:now - last_checked) > 30
            ORDER BY close_time ASC LIMIT :limit
        """), {"cutoff": now - 3, "now": now, "limit": limit}).fetchall()
        for ticker, asset in pending:
            con.execute(text("UPDATE tracked_markets SET last_checked=:now WHERE ticker=:ticker"), {"now": now, "ticker": ticker})

    for ticker, asset in pending:
        try:
            payload = kalshi_json(f"/markets/{ticker}", timeout=4)
            outcome = extract_outcome(payload)
            if not outcome:
                continue
            settled_at = datetime.now(timezone.utc).isoformat()
            with _db_lock, engine.begin() as con:
                con.execute(text("UPDATE tracked_markets SET outcome=:outcome, settled_at=:settled_at WHERE ticker=:ticker"),
                            {"outcome": outcome, "settled_at": settled_at, "ticker": ticker})
                rows = con.execute(text("SELECT id, action FROM signals WHERE ticker=:ticker"), {"ticker": ticker}).fetchall()
                for sid, action in rows:
                    direction = "YES" if str(action or "").endswith("YES") else ("NO" if str(action or "").endswith("NO") else None)
                    correct = None if direction is None else int(direction == outcome)
                    con.execute(text("UPDATE signals SET outcome=:outcome, correct=:correct WHERE id=:id"),
                                {"outcome": outcome, "correct": correct, "id": sid})
        except Exception:
            pass


def performance_summary():
    with _db_lock, engine.begin() as con:
        total = con.execute(text("SELECT COUNT(*) FROM signals")).scalar_one()
        settled = con.execute(text("SELECT COUNT(*) FROM signals WHERE outcome IS NOT NULL")).scalar_one()
        actionable = con.execute(text("SELECT COUNT(*) FROM signals WHERE correct IS NOT NULL")).scalar_one()
        wins = con.execute(text("SELECT COUNT(*) FROM signals WHERE correct=1")).scalar_one()
        markets = con.execute(text("SELECT COUNT(*) FROM tracked_markets")).scalar_one()
        settled_markets = con.execute(text("SELECT COUNT(*) FROM tracked_markets WHERE outcome IS NOT NULL")).scalar_one()
        by_action = [dict(r) for r in con.execute(text("""
            SELECT action, COUNT(*) samples,
                   SUM(CASE WHEN correct=1 THEN 1 ELSE 0 END) wins,
                   SUM(CASE WHEN correct IS NOT NULL THEN 1 ELSE 0 END) graded
            FROM signals WHERE outcome IS NOT NULL
            GROUP BY action ORDER BY samples DESC
        """)).mappings().all()]
        latest = [dict(r) for r in con.execute(text("""
            SELECT s.* FROM signals s JOIN (
                SELECT ticker, MAX(id) id FROM signals
                WHERE correct IS NOT NULL AND signal_version=:version GROUP BY ticker
            ) latest ON latest.id=s.id
        """), {"version": VERSION}).mappings().all()]
        predictions = [dict(r) for r in con.execute(text("""
            SELECT s.model_probability, s.outcome FROM signals s JOIN (
                SELECT ticker, MAX(id) id FROM signals WHERE outcome IN ('YES','NO')
                AND model_probability IS NOT NULL AND signal_version=:version GROUP BY ticker
            ) latest ON latest.id=s.id
        """), {"version": VERSION}).mappings().all()]
        unique_wins = sum(r["correct"] == 1 for r in latest)
        brier = sum((r["model_probability"] - int(r["outcome"] == "YES")) ** 2 for r in predictions) / len(predictions) if predictions else None
        return {
            "snapshots": total,
            "settled_snapshots": settled,
            "actionable_graded": actionable,
            "wins": wins,
            "win_rate": unique_wins / len(latest) if latest else None,
            "unique_graded": len(latest), "unique_wins": unique_wins,
            "snapshot_win_rate": (wins / actionable) if actionable else None,
            "brier_score": brier, "brier_samples": len(predictions),
            "grading_basis": "Latest directional v9 snapshot per distinct settled market",
            "markets_tracked": markets,
            "markets_settled": settled_markets,
            "by_action": by_action,
            "storage": DB_STORAGE,
        }


init_db()


def get_json(url, params=None, timeout=6):
    if not hasattr(_thread_sessions, "session"):
        _thread_sessions.session = requests.Session()
        _thread_sessions.session.headers.update(session.headers)
    r = _thread_sessions.session.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def kalshi_json(path, params=None, timeout=6):
    return get_json(KALSHI_BASE + path, params=params, timeout=timeout)


def num(value, default=0.0):
    try:
        value = float(value)
        return value if math.isfinite(value) else default
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
            open_time = m.get("open_time")
            if open_time:
                try:
                    if datetime.fromisoformat(open_time.replace("Z", "+00:00")).timestamp() > now:
                        continue
                except ValueError:
                    continue
            if m.get("status") in ("closed", "settled", "finalized", "initialized"):
                continue
            if 0 < seconds_left <= 15 * 60 + 5:
                candidates.append((seconds_left < 0, abs(seconds_left), m))
        if candidates:
            candidates.sort(key=lambda x: (x[0], x[1]))
            selected[asset] = candidates[0][2]
    return selected


def parse_target(market, spot=None):
    """Extract the contract reference price without confusing the '15 min' title text for a strike.

    Prefer structured strike fields when Kalshi provides them. Otherwise collect numeric
    candidates from descriptive fields and, when spot is known, choose the value closest
    to the live asset price. This is important for sub-$1 assets such as DOGE.
    """
    candidates = []

    # Kalshi responses may expose a strike/reference level as a structured field.
    for key in ("floor_strike", "cap_strike", "strike", "strike_value", "reference_price"):
        value = num(market.get(key), None)
        if value is not None and value > 0:
            if spot is None or 0.25 <= value / spot <= 4:
                return value
            return None

    # Search the descriptive fields before the generic title. Targets can be < $1.
    text_fields = (
        "yes_sub_title", "no_sub_title", "subtitle",
        "rules_primary", "rules_secondary", "title"
    )
    for priority, key in enumerate(text_fields, start=1):
        text = str(market.get(key, "") or "")
        if not text:
            continue

        patterns = (
            r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
            r"([0-9][0-9,]*(?:\.[0-9]+)?)\s*(?:or\s+above|or\s+below|or\s+higher|or\s+lower)",
            r"(?:above|below|over|under|at|price(?:\s+of)?)\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
        )
        seen = set()
        for pattern in patterns:
            for match in re.finditer(pattern, text, flags=re.I):
                raw = match.group(1).replace(",", "")
                value = num(raw, None)
                if value is None or value <= 0 or value in seen:
                    continue
                seen.add(value)
                # Never treat the generic '15 min' wording as the target.
                tail = text[match.end():match.end()+12].lower()
                head = text[max(0, match.start()-4):match.start()].lower()
                if abs(value - 15.0) < 1e-12 and ("min" in tail or "min" in head):
                    continue
                candidates.append((priority, value, key))

    if not candidates:
        return None

    if spot is not None and spot > 0:
        # A 15-minute reference level should be near the current market. Pick the
        # candidate nearest to spot and reject obviously unrelated numbers (dates,
        # '15 minutes', etc.).
        plausible = [c for c in candidates if 0.25 <= c[1] / spot <= 4.0]
        if plausible:
            plausible.sort(key=lambda c: (abs(math.log(c[1] / spot)), c[0]))
            return plausible[0][1]
        return None

    candidates.sort(key=lambda c: c[0])
    return candidates[0][1]


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
    def clean(levels, dollars):
        valid = []
        for level in levels if isinstance(levels, list) else []:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            price, qty = num(level[0], None), num(level[1], None)
            if price is None or qty is None:
                continue
            price = price if dollars else price / 100
            if 0 < price < 1 and qty > 0:
                valid.append((price, qty))
        return sorted(valid, reverse=True)[:10]

    yes_levels = clean(yes_levels, "yes_dollars" in ob)
    no_levels = clean(no_levels, "no_dollars" in ob)

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
    yes_bid = yes_levels[0][0] if yes_levels else None
    no_bid = no_levels[0][0] if no_levels else None
    return {"yes_qty": yes_qty, "no_qty": no_qty, "imbalance": imbalance,
            "yes_bid": yes_bid, "no_bid": no_bid,
            "yes_ask": 1 - no_bid if no_bid is not None else None,
            "no_ask": 1 - yes_bid if yes_bid is not None else None}


def spot_price(asset):
    try:
        data = get_json(COINBASE_SPOT.format(asset=asset), timeout=6)
        amount = ((data or {}).get("data") or {}).get("amount")
        if amount is not None:
            price = num(amount, None)
            if price and price > 0:
                return price
    except Exception:
        pass

    try:
        pair = "XBTUSD" if asset == "BTC" else asset + "USD"
        data = get_json("https://api.kraken.com/0/public/Ticker", {"pair": pair}, timeout=4)
        result = data.get("result") or {}
        value = next(iter(result.values()))["c"][0] if result else None
        price = num(value, None)
        return price if price and price > 0 else None
    except Exception:
        return None


def valid_candles(candles, now):
    if not isinstance(candles, list):
        raise ValueError("invalid candle response")
    candles = sorted([c for c in candles if isinstance(c, list) and len(c) >= 6
                      and 0 < num(c[0], 0) <= now
                      and all(num(c[i], 0) > 0 for i in (1, 2, 3, 4))], key=lambda x: x[0])[-16:]
    if len(candles) < 11 or now - num(candles[-1][0]) > 150:
        raise ValueError("missing or stale candles")
    if any(b[0] - a[0] != 60 for a, b in zip(candles[-11:-1], candles[-10:])):
        raise ValueError("non-contiguous candles")
    return candles


def load_candles(asset, now):
    try:
        data = get_json(f"{COINBASE_EXCHANGE}/products/{asset}-USD/candles",
                        params={"granularity": 60}, timeout=6)
        return valid_candles(data, now)
    except Exception:
        pair = "XBTUSD" if asset == "BTC" else asset + "USD"
        data = get_json("https://api.kraken.com/0/public/OHLC", {"pair": pair, "interval": 1}, timeout=6)
        result = data.get("result") or {}
        rows = next((v for k, v in result.items() if k != "last"), [])
        # Kraken: time, open, high, low, close, VWAP, volume, count.
        converted = [[num(c[0]), num(c[3]), num(c[2]), num(c[1]), num(c[4]), num(c[6])]
                     for c in rows if isinstance(c, list) and len(c) >= 7]
        return valid_candles(converted, now)


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
        "source_time": None,
    }

    try:
        candles = load_candles(asset, now)
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
            "source_time": candles[-1][0],
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

    seconds_left = max(0.0, (close_ts or time.time()) - time.time())
    target = parse_target(market, spot)
    bid, ask = ob["yes_bid"], ob["yes_ask"]
    no_ask = ob["no_ask"]
    market_p = (bid + ask) / 2 if bid is not None and ask is not None else None
    quality = []
    if not spot or spot <= 0:
        quality.append("Spot feed unavailable")
    if target is None:
        quality.append("Reference price unavailable")
    if stats.get("source_time") is None:
        quality.append("Fresh contiguous candle data unavailable")
    if bid is None or ask is None or no_ask is None:
        quality.append("Two-sided order book unavailable")
    elif ask < bid:
        quality.append("Crossed order book; refresh required")
    if seconds_left <= 12:
        quality.append("Closing or expired contract")
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
    model_p = sigmoid(score) if not quality else None
    yes_edge = model_p - ask if model_p is not None and ask is not None else None
    no_edge = (1 - model_p) - no_ask if model_p is not None and no_ask is not None else None
    side = "YES" if (yes_edge or 0) >= (no_edge or 0) else "NO"
    best_edge = max(0.0, yes_edge or 0, no_edge or 0)
    edge = best_edge if side == "YES" else -best_edge

    # Setup quality blends magnitude of edge, time relevance, and feature agreement.
    direction = 1 if edge >= 0 else -1
    agreement = 0
    if distance_pct is not None and distance_pct * direction > 0:
        agreement += 1
    if m3 * direction > 0:
        agreement += 1
    if ob["imbalance"] * direction > 0:
        agreement += 1
    setup_score = clamp(best_edge * 520 + agreement * 8 + time_weight * 8, 0, 100) if not quality else 0

    action = "PASS"
    confidence = "LOW"
    if not quality and market_p is not None and seconds_left > 12:
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
        "no_bid": ob["no_bid"],
        "no_ask": no_ask,
        "yes_edge": yes_edge,
        "no_edge": no_edge,
        "spread": ask - bid if ask is not None and bid is not None else None,
        "close_time": close_ts,
        "data_quality": "COMPLETE" if not quality else "INCOMPLETE",
        "quality_notes": quality,
        "spot_source": "External spot proxy, not settlement index",
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
        "model_status": "UNVALIDATED HEURISTIC - PAPER MODE / GROSS EDGE BEFORE FEES",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_series_markets(asset, series):
    """Fetch open markets for one Kalshi series directly.

    V4 fetched a generic page of open markets and then filtered it locally.
    Kalshi has enough open markets that a 15-minute crypto contract may not be in
    that page. Querying by series_ticker makes discovery deterministic.
    """
    payload = kalshi_json(
        "/markets",
        {"series_ticker": series, "status": "open", "limit": 100},
        timeout=6,
    )
    markets = payload.get("markets", [])
    return asset, markets if isinstance(markets, list) else []


def build_scan():
    started = time.time()
    errors = []
    discovery = {}
    markets = []

    # Query each 15-minute crypto series directly instead of relying on the
    # first generic page of all open Kalshi markets.
    with ThreadPoolExecutor(max_workers=len(ASSETS)) as pool:
        futures = {
            pool.submit(fetch_series_markets, asset, series): asset
            for asset, series in ASSETS.items()
        }
        for future in as_completed(futures):
            asset = futures[future]
            try:
                _, series_markets = future.result()
                discovery[asset] = len(series_markets)
                markets.extend(series_markets)
            except Exception as exc:
                discovery[asset] = 0
                errors.append({"asset": asset, "stage": "market_discovery", "error": str(exc)})

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
                    errors.append({"asset": asset, "stage": "analysis", "error": str(exc)})

    completed_at = time.time()
    for row in rows:
        row["seconds_left"] = round(max(0.0, (row["close_time"] or completed_at) - completed_at), 1)
        if row["seconds_left"] <= 12:
            row.update(action="PASS", confidence="LOW", setup_score=0, data_quality="INCOMPLETE")
            if "Closing or expired contract" not in row["quality_notes"]:
                row["quality_notes"].append("Closing or expired contract")
    rows.sort(key=lambda x: (x["setup_score"], abs(x["edge"])), reverse=True)
    log_signals(rows)

    global _last_settlement_sweep
    if time.time() - _last_settlement_sweep > 30 and _settlement_lock.acquire(blocking=False):
        _last_settlement_sweep = time.time()
        def sweep():
            try:
                settle_pending(limit=4)
            except Exception:
                logger.exception("Settlement sweep failed")
            finally:
                _settlement_lock.release()
        threading.Thread(target=sweep, daemon=True).start()

    perf = performance_summary()
    return {
        "markets": rows,
        "errors": errors,
        "live": bool(rows),
        "found": list(chosen.keys()),
        "discovery": discovery,
        "elapsed_ms": round((time.time() - started) * 1000),
        "server_time": datetime.now(timezone.utc).isoformat(),
        "model": "KNKB heuristic v9 (unvalidated)",
        "version": VERSION,
        "performance": perf,
    }


@app.get("/api/health")
def health():
    return {"ok": True, "version": VERSION, "time": datetime.now(timezone.utc).isoformat(),
            "storage": DB_STORAGE, "feed_fallback": "coinbase-kraken",
            "collector": dict(_collector_status)}


@app.get("/api/scan")
def scan():
    with _cache_lock:
        cached, captured = _scan_cache["payload"], _scan_cache["ts"]
    if cached is None:
        if _scan_lock.acquire(blocking=False):
            try:
                collect_once()
            finally:
                _scan_lock.release()
            with _cache_lock:
                cached, captured = _scan_cache["payload"], _scan_cache["ts"]
        if cached is None:
            return {"markets": [], "errors": [], "live": False, "warming_up": True,
                    "version": VERSION, "server_time": datetime.now(timezone.utc).isoformat()}
    elif time.time() - captured >= SCAN_INTERVAL and _scan_lock.acquire(blocking=False):
        def refresh():
            try:
                collect_once()
            finally:
                _scan_lock.release()
        threading.Thread(target=refresh, daemon=True).start()
    age = max(0, time.time() - captured)
    return {**cached, "age_seconds": round(age, 1), "stale": age > STALE_AFTER,
            "live": cached.get("live", False) and age <= STALE_AFTER,
            "storage": DB_STORAGE, "storage_warning": DB_STORAGE != "postgres-persistent"}


def collect_once():
    try:
        payload = build_scan()
        with _cache_lock:
            _scan_cache.update(ts=time.time(), payload=payload)
        _collector_status.update(last_success=payload["server_time"], last_error=None)
    except Exception:
        logger.exception("Scan collection failed")
        _collector_status["last_error"] = "Collection failed; check server logs"


def collector(stop):
    while not stop.is_set():
        if _scan_lock.acquire(blocking=False):
            try:
                collect_once()
            finally:
                _scan_lock.release()
        stop.wait(SCAN_INTERVAL)


@app.get("/api/performance")
def performance():
    return performance_summary()


@app.get("/api/history")
def history(limit: int = 100):
    limit = max(1, min(limit, 500))
    with _db_lock, engine.begin() as con:
        rows = con.execute(text("""
            SELECT ticker, asset, captured_at, seconds_left, time_bucket, target, spot,
                   market_probability, model_probability, edge, setup_score, action,
                   confidence, outcome, correct, signal_version
            FROM signals ORDER BY id DESC LIMIT :limit
        """), {"limit": limit}).mappings().all()
        rows = [dict(r) for r in rows]
        return {"rows": rows, "count": len(rows), "storage": DB_STORAGE}


@app.get("/api/history.csv")
def export_history(limit: int = 500):
    rows = history(limit)["rows"]
    output = io.StringIO()
    fields = ["ticker", "asset", "captured_at", "seconds_left", "target", "spot",
              "model_probability", "market_probability", "edge", "action", "outcome", "correct", "signal_version"]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        # Prevent spreadsheet formula evaluation from upstream text fields.
        writer.writerow({k: ("'" + v if isinstance(v, str) and v.startswith(("=", "+", "-", "@")) else v)
                         for k, v in row.items()})
    return Response(output.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=knkb-history.csv", "Cache-Control": "no-store"})


@app.head("/")
def head_index():
    return {"ok": True}


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "index.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0"})


@app.get("/index.html", include_in_schema=False)
def static_index():
    return index()

