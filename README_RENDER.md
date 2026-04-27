# QFA Prime Finance Platform — Render Deployment

## Files
- `app.py` — FastAPI application
- `requirements.txt` — Python dependencies
- `render.yaml` — optional Render Blueprint config
- `runtime.txt` — Python version hint

## Render settings
Use these settings if deploying manually:

- Environment: Python 3
- Build command:

```bash
pip install --upgrade pip && pip install -r requirements.txt
```

- Start command:

```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

- Environment variable:

```bash
QFA_OUTPUT_DIR=/tmp/qfa_output
```

## Health checks
After deployment, open:

- `/`
- `/health`
- `/api/universe`

## Important
This build is locked to Yahoo Finance daily data logic. It does not use synthetic data. If Yahoo throttles or returns incomplete data, the app should fail explicitly rather than fabricate data.
