# QFA Prime Render Deploy - Fixed Dependencies

Fixes conflict: quantstats==0.0.64 requires yfinance>=0.2.65, so requirements now use yfinance>=0.2.65,<0.3.

Build Command:

pip install --upgrade pip setuptools wheel && pip install -r requirements.txt

Start Command:

uvicorn app:app --host 0.0.0.0 --port $PORT

Environment Variables:
PYTHON_VERSION=3.11.9
QFA_OUTPUT_DIR=/tmp/qfa_output
QFA_AUTO_INSTALL_REQUIRED=0
