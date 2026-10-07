# KNKB 15M Command Center V8

V8 adds persistent PostgreSQL storage for signal history and settlements.

## Why V8
V7 used a local SQLite file. Render instances can sleep, restart, or redeploy, and local runtime storage should not be treated as durable history. V8 uses `DATABASE_URL` when provided and falls back to local SQLite only for development.

## Render setup
1. Deploy these files to the existing KNKB service.
2. Create/connect a PostgreSQL database (Render Postgres, Neon, Supabase, etc.).
3. In the KNKB Render service, add environment variable `DATABASE_URL` using that database's connection string.
4. Redeploy.
5. Open `/api/performance`. The `storage` field should say `postgres-persistent`.

Build command:
`pip install -r requirements.txt`

Start command:
`uvicorn app:app --host 0.0.0.0 --port $PORT`

## Persistence
Stored data includes market snapshots, tracked market metadata, settlement outcome, and graded correctness. This survives app sleep/restarts/redeploys as long as the PostgreSQL database remains available.
