# KNKB 15M Command Center v4

Mobile-first paper-mode dashboard for Kalshi 15-minute crypto markets.

## Features
- Current Kalshi 15m contract discovery
- Coinbase spot price
- 3m/5m/10m momentum
- 10m realized volatility and 5m range
- Kalshi order-book imbalance
- Target distance
- Kalshi implied probability vs heuristic model probability
- Estimated edge and 0-100 setup ranking
- ALL / TOP SETUPS / BTC / FINAL 5M filters
- API caching to reduce upstream rate pressure
- Render health route and HEAD / support

## Render
Build command:

    pip install -r requirements.txt

Start command:

    uvicorn app:app --host 0.0.0.0 --port $PORT

Keep Render Root Directory blank when these files are at the repository root.

## Important
This is paper mode. The probability score is a heuristic, not a trained/calibrated model and not a guarantee of a profitable trade.
