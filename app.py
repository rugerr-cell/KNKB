import math, re, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

BASE = "https://external-api.kalshi.com/trade-api/v2"
ASSETS = {
    "BTC":"KXBTC15M", "ETH":"KXETH15M", "SOL":"KXSOL15M", "XRP":"KXXRP15M",
    "DOGE":"KXDOGE15M", "BNB":"KXBNB15M", "HYPE":"KXHYPE15M"
}

app = FastAPI(title="Kalshi 15M Live Scanner")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
session = requests.Session()
session.headers.update({"User-Agent":"Kalshi15MScanner/1.1"})


def get_json(path, params=None, timeout=6):
    r = session.get(BASE + path, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def sigmoid(x):
    return 1 / (1 + math.exp(-max(-20, min(20, x))))


def num(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def parse_close(m):
    close = m.get("close_time") or m.get("expiration_time")
    if not close:
        return None
    try:
        return datetime.fromisoformat(close.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def select_open_markets(markets):
    now = datetime.now(timezone.utc).timestamp()
    selected = {}
    for asset, series in ASSETS.items():
        candidates = []
        for m in markets:
            if not str(m.get("ticker", "")).startswith(series):
                continue
            ts = parse_close(m)
            if ts is None:
                continue
            left = ts - now
            if -5 <= left <= 16 * 60 + 30:
                candidates.append((left < 0, abs(left), m))
        if candidates:
            candidates.sort(key=lambda x: (x[0], x[1]))
            selected[asset] = candidates[0][2]
    return selected


def orderbook(ticker):
    d = get_json(f"/markets/{ticker}/orderbook", timeout=4)
    ob = d.get("orderbook_fp") or d.get("orderbook") or {}
    ys = ob.get("yes_dollars") or ob.get("yes") or []
    ns = ob.get("no_dollars") or ob.get("no") or []
    ys = ys[:10] if isinstance(ys, list) else []
    ns = ns[:10] if isinstance(ns, list) else []

    def qty_sum(levels):
        total = 0.0
        for level in levels:
            try:
                total += num(level[1])
            except Exception:
                pass
        return total

    y, n = qty_sum(ys), qty_sum(ns)
    imbalance = (y - n) / (y + n) if y + n else 0.0
    return {"yes_qty": y, "no_qty": n, "imbalance": imbalance}


def parse_target(m):
    text = " ".join(str(m.get(k, "")) for k in [
        "title", "subtitle", "yes_sub_title", "no_sub_title", "rules_primary", "rules_secondary"
    ])
    for p in [r'\$([0-9][0-9,]*(?:\.[0-9]+)?)', r'([0-9][0-9,]*(?:\.[0-9]+)?)']:
        for match in re.finditer(p, text):
            v = float(match.group(1).replace(",", ""))
            if v > 1:
                return v
    return None


def asset_price(asset):
    # Try Coinbase first because it is generally reachable from US-hosted services.
    try:
        r = requests.get(f"https://api.coinbase.com/v2/prices/{asset}-USD/spot", timeout=3)
        if r.ok:
            amount = (((r.json() or {}).get("data") or {}).get("amount"))
            if amount is not None:
                return num(amount, None)
    except Exception:
        pass

    # Fallback only; some hosts/regions may block Binance.
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price",
                         params={"symbol": asset + "USDT"}, timeout=3)
        if r.ok:
            return num((r.json() or {}).get("price"), None)
    except Exception:
        pass
    return None


def market_prob(m):
    ask = num(m.get("yes_ask_dollars"), None)
    bid = num(m.get("yes_bid_dollars"), None)
    last = num(m.get("last_price_dollars"), None)

    # Support integer-cent fields if dollar fields are absent.
    if ask is None and m.get("yes_ask") is not None:
        ask = num(m.get("yes_ask")) / 100
    if bid is None and m.get("yes_bid") is not None:
        bid = num(m.get("yes_bid")) / 100
    if last is None and m.get("last_price") is not None:
        last = num(m.get("last_price")) / 100

    p = ask if ask not in (None, 0) else (last if last not in (None, 0) else (bid or 0))
    return bid or 0, ask or 0, last or 0, p


def analyze(asset, m):
    close_ts = parse_close(m)
    secs = max(0, (close_ts or time.time()) - time.time())
    bid, ask, last, market_p = market_prob(m)
    ob = orderbook(m["ticker"])
    spot = asset_price(asset)
    target = parse_target(m)

    distance_z = 0.0
    distance_pct = None
    if spot and target:
        distance_pct = (spot - target) / target * 100
        scale = max(0.015, 0.04 * math.sqrt(max(secs, 1) / 900))
        distance_z = distance_pct / scale

    time_weight = min(1.0, max(0.15, (900 - secs) / 900))
    score = 1.05 * distance_z * time_weight + 0.55 * ob["imbalance"]
    p = sigmoid(score)
    edge = p - market_p if market_p else 0.0

    action, confidence = "PASS", "LOW"
    if market_p:
        if abs(edge) >= 0.12 and secs > 15:
            action = "BUY YES" if edge > 0 else "BUY NO"
            confidence = "HIGH"
        elif abs(edge) >= 0.07 and secs > 30:
            action = "WATCH"
            confidence = "MEDIUM"

    return {
        "asset": asset, "series": ASSETS[asset], "ticker": m["ticker"],
        "title": m.get("title"), "target": target, "spot": spot,
        "seconds_left": round(secs, 1), "yes_bid": bid, "yes_ask": ask, "last": last,
        "market_probability": market_p, "model_probability": p, "edge": edge,
        "distance_pct": distance_pct, "orderbook_imbalance": ob["imbalance"],
        "yes_qty": ob["yes_qty"], "no_qty": ob["no_qty"],
        "action": action, "confidence": confidence,
        "model_status": "HEURISTIC — CALIBRATION REQUIRED",
        "updated_at": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/health")
def health():
    return {"ok": True, "time": datetime.now(timezone.utc).isoformat()}


@app.get("/api/scan")
def scan():
    started = time.time()
    errors = []
    try:
        payload = get_json("/markets", {"status": "open", "limit": 1000}, timeout=7)
        markets = payload.get("markets", [])
    except Exception as e:
        return {
            "markets": [], "errors": [{"asset": "KALSHI", "error": str(e)}],
            "live": False, "elapsed_ms": round((time.time() - started) * 1000)
        }

    chosen = select_open_markets(markets)
    out = []
    with ThreadPoolExecutor(max_workers=min(7, max(1, len(chosen)))) as pool:
        futures = {pool.submit(analyze, asset, m): asset for asset, m in chosen.items()}
        for f in as_completed(futures):
            asset = futures[f]
            try:
                out.append(f.result())
            except Exception as e:
                errors.append({"asset": asset, "error": str(e)})

    out.sort(key=lambda x: abs(x["edge"]), reverse=True)
    return {
        "markets": out, "errors": errors, "live": True,
        "found": list(chosen.keys()),
        "elapsed_ms": round((time.time() - started) * 1000)
    }


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "index.html")


@app.get("/{path:path}")
def static(path: str):
    p = Path(__file__).parent / path
    if p.exists() and p.is_file():
        return FileResponse(p)
    return FileResponse(Path(__file__).parent / "index.html")
