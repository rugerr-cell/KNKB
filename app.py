import math
import re
import time
import threading
import sqlite3
import os
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

app = FastAPI(title="Kalshi 15M Market Command Center", version="7.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

session = requests.Session()
session.headers.update({"User-Agent": "KNKB-15M-Scanner/7.0"})

_cache_lock = threading.Lock()
_scan_cache = {"ts": 0.0, "payload": None}
_candle_cache = {}

DB_PATH = os.getenv("KNKB_DB_PATH", str(Path(__file__).parent / "knkb_history.db"))
_db_lock = threading.Lock()
_last_settlement_sweep = 0.0



def init_db():
    with _db_lock:
        con = sqlite3.connect(DB_PATH)
        try:
            con.executescript("""
            PRAGMA journal_mode=WAL;
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
            );
            CREATE TABLE IF NOT EXISTS tracked_markets (
                ticker TEXT PRIMARY KEY,
                asset TEXT NOT NULL,
                target REAL,
                close_time REAL,
                outcome TEXT,
                settled_at TEXT,
                last_checked REAL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_signals_asset ON signals(asset);
            CREATE INDEX IF NOT EXISTS idx_signals_outcome ON signals(outcome);
            """)
            con.commit()
        finally:
            con.close()


def log_signals(rows):
    """Store one snapshot per 30-second time-to-close bucket.

    This keeps the history compact while preserving how the signal changed as expiry
    approached. Duplicate scans inside the same bucket are ignored.
    """
    if not rows:
        return
    now_iso = datetime.now(timezone.utc).isoformat()
    with _db_lock:
        con = sqlite3.connect(DB_PATH)
        try:
            for x in rows:
                seconds_left = max(0.0, num(x.get("seconds_left")))
                bucket = int(seconds_left // 30) * 30
                close_time = time.time() + seconds_left
                con.execute("""
                    INSERT OR IGNORE INTO tracked_markets
                    (ticker, asset, target, close_time) VALUES (?, ?, ?, ?)
                """, (x.get("ticker"), x.get("asset"), x.get("target"), close_time))
                con.execute("""
                    INSERT OR IGNORE INTO signals (
                        ticker, asset, series, captured_at, seconds_left, time_bucket,
                        target, spot, market_probability, model_probability, edge,
                        distance_pct, orderbook_imbalance, momentum_3m, momentum_5m,
                        momentum_10m, volatility_10m, range_5m, trend, setup_score,
                        action, confidence
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    x.get("ticker"), x.get("asset"), x.get("series"), now_iso,
                    seconds_left, bucket, x.get("target"), x.get("spot"),
                    x.get("market_probability"), x.get("model_probability"), x.get("edge"),
                    x.get("distance_pct"), x.get("orderbook_imbalance"), x.get("momentum_3m"),
                    x.get("momentum_5m"), x.get("momentum_10m"), x.get("volatility_10m"),
                    x.get("range_5m"), x.get("trend"), x.get("setup_score"),
                    x.get("action"), x.get("confidence")
                ))
            con.commit()
        finally:
            con.close()


def extract_outcome(payload):
    market = (payload or {}).get("market") or payload or {}
    result = str(market.get("result") or market.get("settlement_result") or "").lower()
    if result in ("yes", "no"):
        return result.upper()
    # Some API versions may expose an explicit 1/0 settlement value.
    value = market.get("settlement_value")
    if value is not None:
        try:
            value = float(value)
            if value == 1:
                return "YES"
            if value == 0:
                return "NO"
        except Exception:
            pass
    return None


def settle_pending(limit=12):
    """Use Kalshi's explicit settlement result when available; never infer a result."""
    now = time.time()
    with _db_lock:
        con = sqlite3.connect(DB_PATH)
        try:
            pending = con.execute("""
                SELECT ticker, asset FROM tracked_markets
                WHERE outcome IS NULL AND close_time < ? AND (? - last_checked) > 30
                ORDER BY close_time ASC LIMIT ?
            """, (now - 3, now, limit)).fetchall()
            for ticker, asset in pending:
                con.execute("UPDATE tracked_markets SET last_checked=? WHERE ticker=?", (now, ticker))
            con.commit()
        finally:
            con.close()

    for ticker, asset in pending:
        try:
            payload = kalshi_json(f"/markets/{ticker}", timeout=4)
            outcome = extract_outcome(payload)
            if not outcome:
                continue
            settled_at = datetime.now(timezone.utc).isoformat()
            with _db_lock:
                con = sqlite3.connect(DB_PATH)
                try:
                    con.execute(
                        "UPDATE tracked_markets SET outcome=?, settled_at=? WHERE ticker=?",
                        (outcome, settled_at, ticker),
                    )
                    rows = con.execute("SELECT id, action FROM signals WHERE ticker=?", (ticker,)).fetchall()
                    for sid, action in rows:
                        direction = "YES" if str(action or "").endswith("YES") else ("NO" if str(action or "").endswith("NO") else None)
                        correct = None if direction is None else int(direction == outcome)
                        con.execute("UPDATE signals SET outcome=?, correct=? WHERE id=?", (outcome, correct, sid))
                    con.commit()
                finally:
                    con.close()
        except Exception:
            pass


def performance_summary():
    with _db_lock:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        try:
            total = con.execute("SELECT COUNT(*) n FROM signals").fetchone()["n"]
            settled = con.execute("SELECT COUNT(*) n FROM signals WHERE outcome IS NOT NULL").fetchone()["n"]
            actionable = con.execute("SELECT COUNT(*) n FROM signals WHERE correct IS NOT NULL").fetchone()["n"]
            wins = con.execute("SELECT COUNT(*) n FROM signals WHERE correct=1").fetchone()["n"]
            markets = con.execute("SELECT COUNT(*) n FROM tracked_markets").fetchone()["n"]
            settled_markets = con.execute("SELECT COUNT(*) n FROM tracked_markets WHERE outcome IS NOT NULL").fetchone()["n"]
            by_action = [dict(r) for r in con.execute("""
                SELECT action, COUNT(*) samples, SUM(CASE WHEN correct=1 THEN 1 ELSE 0 END) wins,
                       SUM(CASE WHEN correct IS NOT NULL THEN 1 ELSE 0 END) graded
                FROM signals WHERE outcome IS NOT NULL
                GROUP BY action ORDER BY samples DESC
            """).fetchall()]
            return {
                "snapshots": total,
                "settled_snapshots": settled,
                "actionable_graded": actionable,
                "wins": wins,
                "win_rate": (wins / actionable) if actionable else None,
                "markets_tracked": markets,
                "markets_settled": settled_markets,
                "by_action": by_action,
                "storage": "sqlite-local",
            }
        finally:
            con.close()


init_db()

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
            candidates.append((0, value, key))

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
            r"([0-9][0-9,]*(?:\.[0-9]+)?)",
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

    target = parse_target(market, spot)
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
        "model_status": "HEURISTIC - PAPER MODE / CALIBRATION REQUIRED",
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

    rows.sort(key=lambda x: (x["setup_score"], abs(x["edge"])), reverse=True)
    log_signals(rows)

    global _last_settlement_sweep
    if time.time() - _last_settlement_sweep > 30:
        _last_settlement_sweep = time.time()
        settle_pending()

    perf = performance_summary()
    return {
        "markets": rows,
        "errors": errors,
        "live": True,
        "found": list(chosen.keys()),
        "discovery": discovery,
        "elapsed_ms": round((time.time() - started) * 1000),
        "server_time": datetime.now(timezone.utc).isoformat(),
        "model": "KNKB heuristic v7",
        "performance": perf,
    }


@app.get("/api/health")
def health():
    return {"ok": True, "version": "7.0", "time": datetime.now(timezone.utc).isoformat()}


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


@app.get("/api/performance")
def performance():
    return performance_summary()


@app.get("/api/history")
def history(limit: int = 100):
    limit = max(1, min(limit, 500))
    with _db_lock:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        try:
            rows = con.execute("""
                SELECT ticker, asset, captured_at, seconds_left, time_bucket, target, spot,
                       market_probability, model_probability, edge, setup_score, action,
                       confidence, outcome, correct
                FROM signals ORDER BY id DESC LIMIT ?
            """, (limit,)).fetchall()
            return {"rows": [dict(r) for r in rows], "count": len(rows)}
        finally:
            con.close()


@app.head("/")
def head_index():
    return {"ok": True}


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "index.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0"})


@app.get("/{path:path}")
def static(path: str):
    p = Path(__file__).parent / path
    if p.exists() and p.is_file():
        return FileResponse(p, headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0"})
    return FileResponse(Path(__file__).parent / "index.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0"})
