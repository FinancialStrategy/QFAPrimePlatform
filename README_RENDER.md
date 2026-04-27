# QFA Prime Finance Platform — Render Deploy Fixed

This package pins Python to 3.11.9 and uses wheel-only installs to avoid pandas being compiled from source on Render's newer default Python runtimes.

## Render settings

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

The repo includes both `.python-version` and `runtime.txt`. Render's official current method is `.python-version` or PYTHON_VERSION; runtime.txt is included as a fallback.
