# QFA Prime — DeepSeek-style Chart Upgrade Render Deployment

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

This package preserves Yahoo Finance daily-only data policy and upgrades charts toward the reference institutional Plotly design: equity annotations, drawdown annotations, tracking-error secondary axis, radar dashboard, benchmark-relative frontier, and FinQuant-style Monte Carlo frontier.
