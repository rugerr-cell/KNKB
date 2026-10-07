
import math, re, time
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import requests

BASE = "https://external-api.kalshi.com/trade-api/v2"
ASSETS = {"BTC":"KXBTC15M","ETH":"KXETH15M","SOL":"KXSOL15M","XRP":"KXXRP15M",
          "DOGE":"KXDOGE15M","BNB":"KXBNB15M","HYPE":"KXHYPE15M"}
app = FastAPI(title="Kalshi 15M Live Scanner")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

session = requests.Session()
cache = {}

def get_json(path, params=None, timeout=5):
    r = session.get(BASE + path, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def sigmoid(x):
    return 1/(1+math.exp(-max(-20,min(20,x))))

def num(x, default=0.0):
    try: return float(x)
    except: return default

def find_open_market(series):
    # Pull open markets and select the nearest closing market belonging to the series.
    data = get_json("/markets", {"status":"open","limit":200})
    now = datetime.now(timezone.utc).timestamp()
    candidates=[]
    for m in data.get("markets",[]):
        if not str(m.get("ticker","")).startswith(series):
            continue
        close=m.get("close_time") or m.get("expiration_time")
        try: ts=datetime.fromisoformat(close.replace("Z","+00:00")).timestamp()
        except: continue
        left=ts-now
        if -5 <= left <= 16*60+30:
            candidates.append((abs(left), left, m))
    if not candidates:
        return None
    # Prefer the market still open with the least time remaining.
    candidates.sort(key=lambda x: (x[1] < 0, abs(x[1])))
    return candidates[0][2]

def orderbook(ticker):
    d=get_json(f"/markets/{ticker}/orderbook")
    ob=d.get("orderbook_fp",{})
    ys=ob.get("yes_dollars",[])[:10]
    ns=ob.get("no_dollars",[])[:10]
    y=sum(num(q) for _,q in ys); n=sum(num(q) for _,q in ns)
    imbalance=(y-n)/(y+n) if y+n else 0
    return {"yes_qty":y,"no_qty":n,"imbalance":imbalance,
            "yes_levels":ys[:5],"no_levels":ns[:5]}

def parse_target(m):
    text=" ".join(str(m.get(k,"")) for k in ["title","subtitle","yes_sub_title","no_sub_title","rules_primary","rules_secondary"])
    patterns=[r'\$([0-9][0-9,]*(?:\.[0-9]+)?)', r'([0-9][0-9,]*(?:\.[0-9]+)?)']
    # Prefer explicit target in title/subtitle.
    for p in patterns:
        for match in re.finditer(p,text):
            v=float(match.group(1).replace(",",""))
            if v>1: return v
    return None

def asset_price(asset):
    # External spot is a confirmation input, not the settlement source.
    symbol=asset+"USDT"
    if asset=="HYPE": symbol="HYPEUSDT"
    try:
        d=requests.get("https://api.binance.com/api/v3/ticker/price",params={"symbol":symbol},timeout=3).json()
        return num(d.get("price"))
    except: return None

def analyze(asset, m):
    now=time.time()
    close=m.get("close_time") or m.get("expiration_time")
    close_ts=datetime.fromisoformat(close.replace("Z","+00:00")).timestamp()
    secs=max(0,close_ts-now)
    bid=num(m.get("yes_bid_dollars")); ask=num(m.get("yes_ask_dollars")); last=num(m.get("last_price_dollars"))
    market_p=ask if ask else (last if last else bid)
    ob=orderbook(m["ticker"])
    spot=asset_price(asset)
    target=parse_target(m)

    # Baseline model: deliberately conservative heuristic until enough labeled data is collected.
    # It combines market momentum proxies, order-book imbalance and distance to target.
    # It is NOT a validated probability model yet.
    distance_z=0
    if spot and target:
        distance_pct=(spot-target)/target*100
        # Scale distance by remaining time; closer to expiry, distance matters more.
        scale=max(0.015, 0.04*math.sqrt(max(secs,1)/900))
        distance_z=distance_pct/scale
    else:
        distance_pct=None

    # Order-book signal and time-decay weighting.
    time_weight=min(1.0,max(0.15,(900-secs)/900))
    score=1.05*distance_z*time_weight + 0.55*ob["imbalance"]
    p=sigmoid(score)
    edge=p-market_p
    action="PASS"
    confidence="LOW"
    if abs(edge)>=0.12 and secs>15:
        action="BUY YES" if edge>0 else "BUY NO"
        confidence="HIGH"
    elif abs(edge)>=0.07 and secs>30:
        action="WATCH"
        confidence="MEDIUM"

    return {
        "asset":asset,"series":ASSETS[asset],"ticker":m["ticker"],
        "title":m.get("title"),"target":target,"spot":spot,
        "seconds_left":round(secs,1),"yes_bid":bid,"yes_ask":ask,"last":last,
        "market_probability":market_p,"model_probability":p,"edge":edge,
        "distance_pct":distance_pct,"orderbook_imbalance":ob["imbalance"],
        "yes_qty":ob["yes_qty"],"no_qty":ob["no_qty"],
        "action":action,"confidence":confidence,
        "model_status":"HEURISTIC — CALIBRATION REQUIRED",
        "updated_at":datetime.now(timezone.utc).isoformat()
    }

@app.get("/api/health")
def health():
    return {"ok":True,"time":datetime.now(timezone.utc).isoformat()}

@app.get("/api/scan")
def scan():
    out=[]
    errors=[]
    for asset,series in ASSETS.items():
        try:
            m=find_open_market(series)
            if m: out.append(analyze(asset,m))
        except Exception as e:
            errors.append({"asset":asset,"error":str(e)})
    out.sort(key=lambda x: abs(x["edge"]), reverse=True)
    return {"markets":out,"errors":errors,"live":True}

@app.get("/")
def index():
    return FileResponse(Path(__file__).parent/"index.html")

@app.get("/{path:path}")
def static(path:str):
    p=Path(__file__).parent/path
    if p.exists() and p.is_file(): return FileResponse(p)
    return FileResponse(Path(__file__).parent/"index.html")
