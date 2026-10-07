# KNKB 15M Command Center V7

Mobile-first paper-mode scanner for Kalshi 15-minute crypto markets.

## V7 changes
- More robust frontend connection state: LOADING / LIVE / OFFLINE / TIMEOUT.
- Frontend errors are shown on-screen instead of silently remaining on CONNECTING.
- Strong no-cache headers for the phone UI.
- Fixed model-status text encoding by using an ASCII-safe label.
- SQLite signal history: one snapshot per ticker per 30-second time-to-expiry bucket.
- Automatic settlement checks using Kalshi's explicit market result only.
- `/api/performance` returns tracked/settled counts and directional paper-signal accuracy.
- `/api/history?limit=100` returns recent logged signal snapshots.
- Dashboard shows tracked markets, settled markets and paper-signal win rate.

## Deploy on Render
Build command:
`pip install -r requirements.txt`

Start command:
`uvicorn app:app --host 0.0.0.0 --port $PORT`

Health check:
`/api/health`

## Important storage note
The default history database is `knkb_history.db` on the app filesystem. On Render instances without persistent disk storage, this history can reset after a redeploy or instance replacement. Set `KNKB_DB_PATH` to a persistent mounted path if you add a Render persistent disk, or migrate the history layer to a managed database for long-term calibration.

## Model note
This remains a heuristic paper-mode research tool. The displayed model probability and setup score are not calibrated guarantees. V7's logging exists specifically so later versions can be evaluated against real settled outcomes before trusting the model more heavily.
