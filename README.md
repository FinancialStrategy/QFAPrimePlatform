# QFA Prime — BIST Stable FX-Aware Build

This build keeps the institutional daily-return kernel and adds BIST-stable Yahoo fetching.

## Render Settings

Build Command:
```bash
pip install --upgrade pip setuptools wheel && pip install -r requirements.txt
```

Start Command:
```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

Important environment variables:
```text
PYTHON_VERSION=3.11.9
QFA_OUTPUT_DIR=/tmp/qfa_output
QFA_YF_BATCH_SIZE=4
QFA_YF_MAX_ATTEMPTS=2
QFA_YF_TIMEOUT=14
QFA_YF_PAUSE_SECONDS=0.35
```

## Notes

- No synthetic data.
- BIST stocks are converted to USD using Yahoo daily USDTRY=X.
- XU100 USD benchmark is built as XU100.IS / USDTRY=X.
- XU100_USD is an internal benchmark column, not a Yahoo ticker.
