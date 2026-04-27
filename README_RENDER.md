# QFA Prime Correct Charts Final - Render Deploy

This package is FastAPI/HTML/Plotly, optimized for Render.

## Render Settings

Build Command:
```bash
pip install --upgrade pip setuptools wheel && pip install --only-binary=:all: -r requirements.txt
```

Start Command:
```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

Environment variables:
```text
PYTHON_VERSION=3.11.9
QFA_OUTPUT_DIR=/tmp/qfa_output
```

## Data policy
- Yahoo Finance only
- Daily interval only
- No synthetic data
- No benchmark proxy fallback
- All time-series charts use daily return derived point arrays

## Chart fixes in this build
- VaR/CVaR/ES are positive loss figures, matching institutional reporting.
- Dense daily time-series use clean line rendering, not thousands of markers.
- Executive dashboard no longer mixes percentage and ratio metrics on one axis.
- Drawdown remains a negative path-return series and is built from daily returns.
