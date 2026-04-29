# QFA Prime - Global Render Safe No-502 Build

This build is optimized for Render stability and Yahoo throttling control.

## Key protections
- Max 12 tickers per run by default.
- Async server-side job polling.
- Yahoo batch size 4.
- Short retry/timeout settings.
- Cache enabled.
- QuantStats metrics remain active; full QuantStats HTML is disabled by default to reduce Render memory/timeouts.
- No BIST/Turkey stock universe.
- Yahoo Finance daily-only data.
- No synthetic data and no upload fallback.

## Render build command
```bash
pip install --upgrade pip setuptools wheel && pip install -r requirements.txt
```

## Render start command
```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```
