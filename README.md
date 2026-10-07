
# Kalshi 15M Live Phone Scanner

## What this build does
- Pulls current open 15-minute crypto markets from Kalshi's public Trade API.
- Pulls the current market quote and order book.
- Uses an external crypto spot feed as a confirmation input.
- Calculates a conservative heuristic probability and an estimated edge.
- Refreshes the phone dashboard every 2.5 seconds.
- Does NOT place orders.
- Does NOT require your Kalshi private API key for the public REST scanner.

## Important
This is a live DATA scanner, but the probability model is not yet statistically validated. "BUY YES/NO" means the heuristic sees a large discrepancy; it is not a promise of profit.

Kalshi's crypto contracts settle from the applicable CF Benchmarks RTI, using the average of 60 one-second RTI observations in the final minute. This scanner currently uses public market data plus external spot as a proxy, so it should not be treated as settlement-perfect.

## Run on a computer
python -m pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
Then open http://localhost:8000

## Put it on your iPhone
Deploy this folder as a Python web service. Render is configured via render.yaml. Once deployed, open the service URL in Safari and use Share -> Add to Home Screen.

## Next upgrade
Add authenticated CF Benchmarks RTI WebSocket data server-side, store every 1-2 second snapshot, label the eventual settlement outcome, then calibrate the probability model using out-of-sample data. Add Brier score, calibration, edge buckets, and paper P&L before real-money use.
