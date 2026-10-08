# KNKB 15M Command Center V9

Phone dashboard for public Kalshi 15-minute crypto market data, external spot prices, paper signals, and settlement history. No trading credentials or order placement are included.

## V9 changes

- Derives purchase prices from the opposite side's best order-book bid and uses the best ten levels.
- Shows YES/NO purchase prices and gross edge before fees. Signals require a target, spot feed, fresh contiguous candles, and a usable two-sided book; otherwise the card says PASS and explains missing inputs.
- Uses Kraken USD spot/candles as a fallback when Coinbase data is unavailable or stale. Both remain external proxies for settlement.
- Collects in the background while the service is running. Concurrent phones share a scan. Settlement polling runs separately.
- Pauses displayed signals on stale data and expired contracts. Refreshes are serialized and resume when Safari becomes visible.
- Grades one latest directional V9 snapshot per settled market and displays the sample count. Brier score uses one latest valid probability per settled market. Legacy V8 data remains in history but is excluded from V9 headline metrics.
- Adds history, CSV export, and storage status. Removes arbitrary project-file serving.

## Render

Build: `pip install -r requirements.txt`

Start: `uvicorn app:app --host 0.0.0.0 --port $PORT`

Health: `/api/health`

Set `DATABASE_URL` in Render to a PostgreSQL connection string. Health and performance endpoints should report `postgres-persistent`. Without it, SQLite is used for development and history may disappear on Render restarts/redeploys. Existing signals receive a V8 marker in an additive database migration.

Collection stops when the host suspends the service. Always-on hosting is needed for uninterrupted collection. Use one Uvicorn worker to avoid duplicate collectors. Optional `KNKB_SCAN_INTERVAL` sets the interval (default 5 seconds, minimum 3); upstream request duration is additional. `KNKB_BACKGROUND=0` disables the worker for tests.

## iPhone

Open the deployed service in Safari, then Share → Add to Home Screen. Use All, Top Setups, BTC, and Final 5M filters. History displays 50 snapshots. CSV export returns up to 500. Refresh fetches the shared snapshot without bypassing collection limits.

## Model limits

This is an unvalidated heuristic, not a trained AI or a demonstrated trading advantage. External spot feeds are proxies for the settlement index. Edge excludes fees and slippage. HIGH/MEDIUM describe feature agreement, not calibrated confidence. Accuracy is not profit. Brier score evaluates probability error, not financial return. Unsupported upstream assets appear as incomplete data. Profitable trades are not promised.

## Endpoints

- `/api/scan`: shared snapshot, errors, age, and storage status.
- `/api/health`: version, storage, and collector status.
- `/api/performance`: counts and distinct-market V9 evaluation.
- `/api/history?limit=100`: snapshots (limit 1–500).
- `/api/history.csv?limit=500`: downloadable history.

## Verification

Install `pytest httpx` and run `python -m pytest -q`. Tests use temporary SQLite, disable the background worker, and mock feeds. They cover quote conversion, missing inputs, target parsing, settlement units, scan concurrency, distinct-market grading, exports, and file-serving restrictions.

