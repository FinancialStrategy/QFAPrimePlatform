# QFA Prime Finance Platform

### Institutional Multi-Asset Portfolio Analytics & Optimization Engine

---

## Overview

QFA Prime is a **professional-grade quantitative finance platform** designed for institutional portfolio analysis, risk diagnostics, and advanced optimization.

The platform is built on **real market data only (Yahoo Finance)** and operates strictly on **daily frequency returns**, ensuring high-fidelity analytics and decision-grade outputs.

---

## Key Principles

* **No synthetic data** — all inputs are real market data
* **Daily return-based analytics** — no resampling distortions
* **Institutional risk metrics** — VaR, CVaR, Tracking Error, Drawdown
* **Benchmark-relative analysis** — active risk & performance
* **Robust optimization engine** — multiple portfolio construction methodologies

---

## Core Features

### Portfolio Analytics

* Daily return aggregation engine
* Cumulative performance (equity curves)
* Drawdown analysis (peak-to-trough)
* Rolling volatility & tracking error
* Risk contribution decomposition

### Risk Management

* Historical VaR / CVaR (multi-confidence levels)
* Relative VaR (vs benchmark)
* Stress testing (crisis scenarios)
* Rolling beta analysis

### Optimization Engine

* Maximum Sharpe Ratio
* Minimum Volatility
* Equal Risk Contribution (ERC)
* Hierarchical Risk Parity (HRP)
* Black-Litterman Model
* Tracking Error Constrained Optimization

### Visualization

* Institutional-grade Plotly dashboards
* Efficient Frontier & Capital Market Line
* Benchmark-relative performance curves
* Radar charts (normalized strategy comparison)
* Risk contribution bar charts

---

## Data Integrity Architecture

All analytics are built on a strict pipeline:

```text
Yahoo Finance (Adj Close)
        ↓
Daily Price Alignment
        ↓
Daily Returns (pct_change)
        ↓
Portfolio Aggregation (weights normalized)
        ↓
Risk / Performance Metrics
        ↓
Visualization Layer
```

---

## Technology Stack

* Python 3.11
* FastAPI
* NumPy / Pandas
* SciPy / scikit-learn
* PyPortfolioOpt
* QuantStats
* Plotly

---

## Deployment (Render)

### Build Command

```bash
pip install --upgrade pip setuptools wheel && pip install -r requirements.txt
```

### Start Command

```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

### Environment Variables

```text
PYTHON_VERSION=3.11.9
QFA_OUTPUT_DIR=/tmp/qfa_output
```

---

## Usage

1. Select asset universe
2. Run optimization engine
3. Analyze performance vs benchmark
4. Evaluate risk metrics
5. Review stress scenarios

---

## Data Source

All financial data is sourced from:

* Yahoo Finance (via yfinance API)

No synthetic or simulated data is used in production analytics.

---

## Disclaimer

This platform is for **research and analytical purposes only**.
It does not constitute financial advice or investment recommendation.

---

## Author

**QFA Prime – Institutional Quantitative Platform**
MK Istanbul FinTECH LabGEN @2026

