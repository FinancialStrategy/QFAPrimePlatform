# -*- coding: utf-8 -*-
"""
QFA Prime Finance Platform - Institutional Colab PRO CHARTS UPGRADED Final
Author: MK FinTECH LabGEN@2026

Colab usage:
    from google.colab import files
    uploaded = files.upload()  # upload this .py if needed
    import qfa_institutional_colab_app as qfa
    qfa.launch_colab(public=True)   # creates a public ngrok URL if pyngrok is available

Local usage:
    python qfa_institutional_colab_app.py
"""
from __future__ import annotations

import io
import os
import json
import time
import math
import socket
import threading
import warnings
import datetime as _dt
import decimal as _decimal
import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REQUIRED = {
    "fastapi": "fastapi",
    "uvicorn": "uvicorn",
    "pandas": "pandas",
    "numpy": "numpy",
    "yfinance": "yfinance",
    "openpyxl": "openpyxl",
    "multipart": "python-multipart",
    "sklearn": "scikit-learn",
    "scipy": "scipy",
}

OPTIONAL = {
    "pypfopt": "PyPortfolioOpt",
    "quantstats": "quantstats",
    "finquant": "finquant",
}


def _install_missing() -> None:
    missing = [pip_name for import_name, pip_name in REQUIRED.items() if importlib.util.find_spec(import_name) is None]
    if missing:
        print("Installing missing packages:", ", ".join(missing))
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *missing])


if os.getenv('QFA_AUTO_INSTALL_REQUIRED', '1') == '1':
    _install_missing()

def _install_optional() -> None:
    for import_name, pip_name in OPTIONAL.items():
        if importlib.util.find_spec(import_name) is None:
            try:
                print(f"Installing optional analytics package: {pip_name}")
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pip_name])
            except Exception as exc:
                print(f"Optional package {pip_name} could not be installed. Continuing with fallback analytics. Detail: {exc}")

# Optional packages are never auto-installed during app import; this avoids Colab/runtime hangs.
# Install optional packages manually before import if needed.
if os.getenv('QFA_INSTALL_OPTIONAL', '0') == '1':
    _install_optional()

import numpy as np
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel, Field, validator
from sklearn.covariance import LedoitWolf
from sklearn.decomposition import PCA
try:
    from scipy.optimize import minimize
    from scipy.cluster import hierarchy
    from scipy.spatial.distance import squareform
except Exception:
    minimize = None
    hierarchy = None
    squareform = None
import uvicorn

try:
    import quantstats as qs
except Exception:
    qs = None

try:
    from pypfopt import expected_returns, risk_models
    from pypfopt.efficient_frontier import EfficientFrontier
    from pypfopt.black_litterman import BlackLittermanModel, market_implied_risk_aversion
except Exception:
    expected_returns = None
    risk_models = None
    EfficientFrontier = None
    BlackLittermanModel = None
    market_implied_risk_aversion = None

try:
    from finquant.efficient_frontier import EfficientFrontier as FinQuantEfficientFrontier
except Exception:
    FinQuantEfficientFrontier = None


class QFAJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for Pandas, NumPy, and datetime types."""
    def default(self, obj):
        # Pandas Timestamp
        if isinstance(obj, (pd.Timestamp, np.datetime64, _dt.datetime, _dt.date)):
            if pd.isna(obj):
                return None
            return obj.isoformat()
        # Pandas Timedelta
        if isinstance(obj, (pd.Timedelta, np.timedelta64)):
            return str(obj)
        # Pandas Series -> convert to list
        if isinstance(obj, pd.Series):
            return obj.tolist()
        # Pandas DataFrame -> convert to list of dicts (records)
        if isinstance(obj, pd.DataFrame):
            # Replace NaN/Inf with None before converting
            clean = obj.replace({np.nan: None, np.inf: None, -np.inf: None})
            return clean.to_dict(orient='records')
        # Pandas Index / DatetimeIndex
        if isinstance(obj, pd.Index):
            return obj.tolist()
        # NumPy arrays
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        # NumPy scalars
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        # Decimal
        if isinstance(obj, _decimal.Decimal):
            f = float(obj)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        # Fallback
        return super().default(obj)


warnings.filterwarnings("ignore")

try:
    from google.colab import output as _colab_output  # type: ignore
    IN_COLAB = True
except Exception:
    _colab_output = None
    IN_COLAB = False

OUTPUT_DIR = Path(os.getenv("QFA_OUTPUT_DIR", "/content/qfa_output" if IN_COLAB else "qfa_output"))
CACHE_DIR = OUTPUT_DIR / "cache"
QUANTSTATS_HTML_PATH = OUTPUT_DIR / "quantstats_tearsheet_latest.html"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

TRADING_DAYS = 252
DEFAULT_RF = 0.045
MAX_SINGLE_WEIGHT = 0.12
CASH_LIKE = {"BIL", "SHY", "SGOV", "BILS", "GBIL"}
BENCHMARK_SYMBOL = "^GSPC"
BENCHMARK_LABEL = "S&P 500 Daily (^GSPC)"

# Turkish BIST FX-aware benchmark architecture
# Turkish stocks are quoted in TRY on Yahoo Finance. For USD-consistent
# institutional analytics, every .IS stock and XU100 benchmark level is
# converted into USD by dividing the TRY price level by USDTRY=X on the
# same daily historical date. No synthetic FX data or benchmark proxy is used.
USDTRY_SYMBOL = "USDTRY=X"
XU100_TRY_SYMBOL = "XU100.IS"
XU100_TRY_ALTERNATE_SYMBOLS = ["XU100.IS", "^XU100"]
XU100_USD_BENCHMARK_SYMBOL = "^XU100_USD"
XU100_USD_BENCHMARK_LABEL = "BIST 100 Daily USD (XU100.IS / USDTRY=X)"

# -----------------------------------------------------------------------------
# NON-NEGOTIABLE DATA POLICY
# -----------------------------------------------------------------------------
# All analytics and every time-series chart must be fed only by Yahoo Finance
# daily prices transformed into DAILY returns. No synthetic data, no upload-mode
# price fallback, no weekly/monthly resampling, and no benchmark proxy fallback.
ALLOW_UPLOAD_MODE = False
ALLOW_SYNTHETIC_DATA = False
REQUIRE_YAHOO_DAILY_ONLY = True
REQUIRE_BENCHMARK_FROM_YAHOO = True
DAILY_MEDIAN_GAP_LIMIT_DAYS = 3.5


def normalize_benchmark_symbol(value: Any = None) -> str:
    """Normalize benchmark choices while forbidding benchmark proxy fabrication."""
    v = str(value or BENCHMARK_SYMBOL).strip().upper()
    aliases = {
        "XU100_USD": XU100_USD_BENCHMARK_SYMBOL,
        "^XU100_USD": XU100_USD_BENCHMARK_SYMBOL,
        "XU100 (USD)": XU100_USD_BENCHMARK_SYMBOL,
        "BIST100_USD": XU100_USD_BENCHMARK_SYMBOL,
        "BIST 100 DAILY USD": XU100_USD_BENCHMARK_SYMBOL,
        "XU100": XU100_TRY_SYMBOL,
        "XU100.IS": XU100_TRY_SYMBOL,
        "^XU100": XU100_TRY_SYMBOL,
        "SP500": BENCHMARK_SYMBOL,
        "S&P500": BENCHMARK_SYMBOL,
        "S&P 500": BENCHMARK_SYMBOL,
        "^GSPC": BENCHMARK_SYMBOL,
    }
    return aliases.get(v, BENCHMARK_SYMBOL)


ETF_UNIVERSE = {
    "US Broad Equity": ["IVV", "VOO", "VTI", "SCHB", "DIA", "IWM", "MDY"],
    "US Growth & Value": ["QQQ", "VUG", "IWF", "VTV", "IWD", "SCHG", "SCHV"],
    "US Sectors": ["XLK", "XLF", "XLV", "XLE", "XLI", "XLP", "XLY", "XLU", "XLB", "XLC", "XLRE"],
    "International Developed": ["VEA", "IEFA", "EFA", "VGK", "EWJ", "EWG", "EWU", "EWC"],
    "Emerging Markets": ["VWO", "IEMG", "EEM", "EWZ", "INDA", "FXI", "MCHI", "EWT", "EIDO", "EPOL", "EZA", "EPI"],
    "Turkey & EMEA": ["TUR", "GULF", "QDV5.DE", "DBXK.DE", "DX2J.DE", "IS3N.DE"],
    "Türkiye BIST": [
        "AKBNK.IS", "ARCLK.IS", "ASELS.IS", "ASTOR.IS", "BIMAS.IS",
        "EKGYO.IS", "ENKAI.IS", "EREGL.IS", "FROTO.IS", "GARAN.IS",
        "GUBRF.IS", "HALKB.IS", "ISCTR.IS", "KCHOL.IS", "KRDMD.IS",
        "PETKM.IS", "PGSUS.IS", "SAHOL.IS", "SASA.IS", "SISE.IS",
        "TAVHL.IS", "TCELL.IS", "THYAO.IS", "TOASO.IS", "TUPRS.IS",
        "YKBNK.IS", "AEFES.IS", "MGROS.IS", "TTKOM.IS", "ULKER.IS",
        "ANHYT.IS", "TURSG.IS", "ADEL.IS", "ALKLC.IS", "ALTNY.IS"
    ],
    "EUR UCITS": ["SXR8.DE", "EUNL.DE", "VWCE.DE", "IUSQ.DE", "EXSA.DE", "XDAX.DE", "IQQE.DE", "VUSA.L", "VEVE.L", "VUAA.L", "CSPX.L", "IWDA.AS", "EMIM.L", "AGGH.L", "IEAC.L", "IGLN.L", "EUNA.DE", "XESC.DE", "XG7S.DE", "QDVX.DE", "SPPW.DE", "XDWD.DE", "XDWL.DE", "XMME.DE", "VJPN.DE", "VWCG.DE", "VETY.DE", "VDTA.DE", "VGEA.DE", "EL4C.DE"],
    "Fixed Income": ["AGG", "BND", "TLT", "IEF", "SHY", "LQD", "HYG", "MUB", "TIP", "BIL", "SGOV", "BILS"],
    "Real Assets": ["GLD", "IAU", "SLV", "DBC", "VNQ", "REET", "GSG", "PDBC"],
    "Thematic / Factors": ["MTUM", "QUAL", "USMV", "VLUE", "SIZE", "SCHD", "XBI", "ARKK"],
}

HTML_DOC = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>QFA Prime Finance Platform</title>
<script defer src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
:root{--bg:#eef3f8;--panel:#fff;--ink:#1f2d3d;--muted:#66788a;--line:#d9e4ef;--navy:#10263f;--accent:#2E86AB;--danger:#C44747;--good:#3C7A52;--warn:#B7791F;--shadow:0 10px 30px rgba(15,35,58,.09)}
*{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--ink);font-family:'Segoe UI',Roboto,Arial,sans-serif;font-size:12px}.shell{display:grid;grid-template-columns:340px minmax(0,1fr);min-height:100vh}.sidebar{background:linear-gradient(180deg,#10263f,#173a5c);color:#f6fbff;padding:22px 18px;position:sticky;top:0;height:100vh;overflow:auto}.brand h1{margin:0 0 6px;font-size:25px;line-height:1.1}.brand p{margin:0 0 14px;color:#d7e5f4;line-height:1.5}.side-card{border:1px solid rgba(255,255,255,.13);background:rgba(255,255,255,.055);border-radius:16px;padding:14px;margin-bottom:14px}.side-card h3{margin:0 0 10px;font-size:13px}.side-grid{display:grid;gap:10px}label{display:block;font-size:11px;color:#d8e6f4;margin-bottom:4px}input,select{width:100%;border-radius:10px;border:1px solid rgba(255,255,255,.18);background:rgba(255,255,255,.09);color:#fff;padding:10px 11px}select option{color:#10263f}.side-btn{width:100%;border:none;border-radius:12px;padding:12px;font-weight:800;cursor:pointer}.primary{background:#f8fbff;color:#173a5c}.status{font-size:11px;color:#d7e5f4}.smallnote{color:#d7e5f4;line-height:1.55}.category-box{border:1px solid rgba(255,255,255,.10);border-radius:12px;padding:8px 10px;margin-bottom:8px}.category-title{display:flex;align-items:center;justify-content:space-between;font-weight:800;margin-bottom:6px}.ticker-list{max-height:170px;overflow:auto}.tick-item{display:flex;align-items:center;gap:7px;padding:3px 0;color:#eef6ff}.tick-item input{width:auto}.main{padding:20px 24px 40px}.header{background:linear-gradient(135deg,#16355b,#2E86AB);color:#fff;border-radius:20px;padding:22px 25px;box-shadow:var(--shadow)}.header h2{margin:0;font-size:32px;line-height:1.15}.header p{margin:8px 0 0;color:#e6f1fb}.kpi-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:14px;margin:18px 0}.kpi-card{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:14px 16px;box-shadow:var(--shadow)}.kpi-label{font-size:10px;text-transform:uppercase;letter-spacing:.7px;color:var(--muted);margin-bottom:5px}.kpi-value{font-size:21px;font-weight:850}.kpi-sub{margin-top:7px;color:var(--muted);font-size:11px}.tabs{display:flex;gap:8px;flex-wrap:wrap;margin:4px 0 18px}.tab-btn{background:#e7eef6;border:1px solid #d2deeb;color:#24415e;padding:9px 14px;border-radius:999px;cursor:pointer;font-weight:800;font-size:11px}.tab-btn.active{background:#2E86AB;color:#fff;border-color:#2E86AB}.tab{display:none}.tab.active{display:block}.stack{display:grid;gap:18px}.chart-card{background:var(--panel);border:1px solid var(--line);border-radius:18px;box-shadow:var(--shadow);overflow:hidden}.chart-header{padding:13px 16px;border-bottom:1px solid var(--line);background:#f7fbff}.chart-header h3{margin:0;font-size:15px}.chart-body{padding:14px 16px}.plot-slot{min-height:500px}.plot-slot.short{min-height:390px}.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:14px}.data-table{width:100%;border-collapse:collapse;font-size:11px}.data-table th{background:#16355b;color:#fff;padding:9px 10px;text-align:left;position:sticky;top:0}.data-table td{padding:8px 10px;border-bottom:1px solid #e6edf5;text-align:right}.data-table td:first-child,.data-table th:first-child{text-align:left}.note{color:#617288;line-height:1.6}.callout{padding:13px 15px;background:#f8fbff;border:1px solid #dbe6f1;border-radius:12px;line-height:1.65}.footer{margin-top:20px;background:#143252;color:#e9f2fb;border-radius:16px;padding:16px 18px}.grid2{display:grid;grid-template-columns:1fr 1fr;gap:18px}@media (max-width:1350px){.shell{grid-template-columns:1fr}.sidebar{position:relative;height:auto}.kpi-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.grid2{grid-template-columns:1fr}}@media (max-width:800px){.kpi-grid{grid-template-columns:1fr}.header h2{font-size:24px}}
</style>
</head>
<body>
<noscript><div style="padding:24px;background:#fff3cd;color:#5c4100;font-family:Arial">JavaScript is disabled. Enable JavaScript to use QFA Prime.</div></noscript>
<div id="bootBanner" style="padding:10px 16px;background:#10263f;color:white;font-family:Arial;font-size:13px">QFA Prime loading... If this message stays visible, Plotly CDN or browser JavaScript is being blocked, but backend is running.</div>
<div class="shell"><aside class="sidebar"><div class="brand"><h1>QFA Prime Finance Platform</h1><p>Institutional single-file FastAPI + Plotly build optimized for Google Colab, Yahoo Finance, uploads, and benchmark-relative risk diagnostics.</p></div>
<div class="side-card"><h3>Core Controls</h3><div class="side-grid"><div><label>Benchmark</label><select id="benchmarkSymbol"><option value="^GSPC">S&amp;P 500 Daily USD (^GSPC)</option><option value="^XU100_USD">XU100 Daily USD (XU100.IS / USDTRY=X)</option><option value="^XU100">XU100 Daily TRY (XU100.IS) - currency mismatch warning</option></select></div><div><label>Start Date</label><input type="date" id="startDate" value="2019-01-01"></div><div><label>Initial Capital</label><input type="number" id="initialCapital" value="1000000" step="1000"></div><div><label>Risk-Free Rate</label><input type="number" id="riskFreeRate" value="0.045" step="0.0001"></div><div><label>Rolling Window</label><input type="number" id="rollingWindow" value="63" step="1"></div></div></div>
<div class="side-card"><h3>Portfolio Model Controls</h3><div class="side-grid"><div><label>Expected Return Method</label><select id="expReturnMethod"><option value="historical_mean">Historical Mean</option><option value="ema_historical">EMA Historical</option><option value="capm">CAPM-like Benchmark Beta</option></select></div><div><label>Covariance Method</label><select id="covMethod"><option value="ledoit_wolf">Ledoit-Wolf</option><option value="shrinkage">Shrinkage</option><option value="sample">Sample</option></select></div><div><label>Best Strategy Rule</label><select id="bestStrategyRule"><option value="highest_sharpe">Highest Sharpe</option><option value="lowest_tracking_error">Lowest Tracking Error</option><option value="highest_information_ratio">Highest Information Ratio</option><option value="minimum_volatility">Minimum Volatility</option></select></div></div></div>
<div class="side-card"><h3>Stress Filters</h3><div class="side-grid"><div><label>Stress Family</label><select id="stressFamily"><option value="All">All</option><option value="crisis">crisis</option><option value="inflation">inflation</option><option value="banking stress">banking stress</option><option value="sharp rally">sharp rally</option><option value="sharp selloff">sharp selloff</option></select></div><div><label>Minimum Severity</label><input type="number" id="minSeverity" value="0" step="0.1"></div></div></div>
<div class="side-card"><h3>Data Source Policy</h3><div class="side-grid"><div><label>Mode</label><input type="text" id="dataMode" value="Yahoo Finance Daily Only" readonly></div><div class="smallnote"><b>LOCKED:</b> Yahoo Finance adjusted daily prices only. Upload/synthetic/fallback price modes are disabled. Every chart is generated from portfolio DAILY RETURNS; no weekly/monthly resampling is allowed.</div></div></div>
<div class="side-card"><h3>ETF Universe Drill-Down</h3><div id="categoryDrilldown"></div><button class="side-btn primary" id="recomputeBtn">Run Institutional Analysis</button><div style="height:8px"></div><div class="status" id="statusBox">Ready.</div></div></aside>
<main class="main"><div class="header"><h2>QFA Prime Finance Platform</h2><p id="headerMeta">Benchmark: S&P 500 Daily (^GSPC) • Frequency: DAILY RETURNS LOCKED • Periods/Year: 252 • Generated by MK FinTECH LabGEN@2026</p></div><div class="kpi-grid" id="kpiGrid"></div>
<div class="tabs"><button class="tab-btn active" onclick="showTab('tab-key', this)">Key Metrics</button><button class="tab-btn" onclick="showTab('tab-guide', this)">Best Strategy</button><button class="tab-btn" onclick="showTab('tab-info', this)">Info Hub</button><button class="tab-btn" onclick="showTab('tab-exec', this)">Dashboard</button><button class="tab-btn" onclick="showTab('tab-opt', this)">Optimization</button><button class="tab-btn" onclick="showTab('tab-risk', this)">Risk</button><button class="tab-btn" onclick="showTab('tab-factor', this)">Factor PCA</button><button class="tab-btn" onclick="showTab('tab-stress', this)">Stress</button><button class="tab-btn" onclick="showTab('tab-qs', this)">Quantstats</button><button class="tab-btn" onclick="showTab('tab-data', this)">Data QA</button></div>
<section id="tab-key" class="tab active"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Institutional Key Metrics Summary</h3></div><div class="chart-body"><div class="callout" id="keyMetricsHeader"></div><div style="height:12px"></div><div id="keyMetricsTable"></div></div></div></div></section>
<section id="tab-guide" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Best Strategy Guide</h3></div><div class="chart-body"><div class="callout" id="bestGuideBox"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Portfolio Strategy Explanations</h3></div><div class="chart-body"><div class="callout">Each strategy below is calculated from the same audited daily-return matrix. The descriptions help non-technical users understand why strategy results differ.</div><div style="height:12px"></div><div id="strategyExplanationTable"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Strategy Ranking</h3></div><div class="chart-body"><div id="strategyTable"></div></div></div></div></section>
<section id="tab-info" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Investment Universe Identity Map</h3></div><div class="chart-body"><div id="infoHubPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Asset Metadata</h3></div><div class="chart-body"><div id="assetMetaTable"></div></div></div></div></section>
<section id="tab-exec" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Executive Strategy Dashboard</h3></div><div class="chart-body"><div id="dashboardPlot" class="plot-slot"></div></div></div><div class="grid2"><div class="chart-card"><div class="chart-header"><h3>Rolling Sharpe</h3></div><div class="chart-body"><div id="rollingSharpePlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Portfolio Rolling Beta vs Benchmark</h3></div><div class="chart-body"><div id="rollingBetaPlot" class="plot-slot short"></div></div></div></div><div class="chart-card"><div class="chart-header"><h3>Rolling Asset Betas vs Benchmark</h3></div><div class="chart-body"><div class="callout">Each trace is calculated from the same audited daily-return matrix using rolling covariance versus S&P 500 divided by rolling benchmark variance.</div><div style="height:12px"></div><div id="rollingAssetBetaPlot" class="plot-slot"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Beta Summary Smart Table</h3></div><div class="chart-body"><div id="betaSummaryTable"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Benchmark vs Tracking-Error Dynamic Curve</h3></div><div class="chart-body"><div id="trackingDynamicPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Tracking Error by Strategy</h3></div><div class="chart-body"><div id="trackingErrorByStrategyPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Strategy Performance Radar</h3></div><div class="chart-body"><div id="radarPlot" class="plot-slot"></div></div></div></div></section>
<section id="tab-opt" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Portfolio Allocation</h3></div><div class="chart-body"><div id="allocationPlot" class="plot-slot short"></div><div id="allocationExplain" class="callout"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Efficient Frontier and Capital Market Line</h3></div><div class="chart-body"><div id="efficientFrontierPlot" class="plot-slot"></div><div class="callout" id="optimizerStatusBox"></div></div></div><div class="chart-card"><div class="chart-header"><h3>FinQuant-Style Monte Carlo Efficient Frontier</h3></div><div class="chart-body"><div id="finquantMcPlot" class="plot-slot"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Benchmark-Relative Efficient Frontier</h3></div><div class="chart-body"><div id="relativeFrontierPlot" class="plot-slot"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Strategy Risk / Return Map</h3></div><div class="chart-body"><div id="strategyScatterPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Cumulative Return vs Benchmark</h3></div><div class="chart-body"><div id="equityPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Daily Drawdown — high-resolution trading-day series</h3></div><div class="chart-body"><div id="drawdownPlot" class="plot-slot"></div></div></div></div></section>
<section id="tab-risk" class="tab"><div class="stack"><div class="grid2"><div class="chart-card"><div class="chart-header"><h3>Risk Contribution</h3></div><div class="chart-body"><div id="riskContribPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>VaR / CVaR / Relative Risk</h3></div><div class="chart-body"><div id="riskBarPlot" class="plot-slot short"></div></div></div></div><div class="chart-card"><div class="chart-header"><h3>Absolute VaR Smart Table</h3></div><div class="chart-body"><div class="callout">Historical, Parametric and Monte Carlo VaR are all calculated from the selected portfolio daily returns. Values are positive loss magnitudes.</div><div style="height:12px"></div><div id="advancedVarTable"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Relative VaR Smart Table vs Benchmark</h3></div><div class="chart-body"><div class="callout">Relative VaR is calculated from active daily return = portfolio daily return minus S&P 500 daily return.</div><div style="height:12px"></div><div id="relativeVarTable"></div></div></div><div class="grid2"><div class="chart-card"><div class="chart-header"><h3>3-Month VaR / NAV Ratio Evolution (95%)</h3></div><div class="chart-body"><div class="callout">Rolling 63-trading-day historical VaR at 95% confidence, divided by rolling NAV. This is calculated from the selected portfolio daily return path; no synthetic data or resampling is used.</div><div style="height:12px"></div><div id="varNav95Plot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>3-Month VaR / NAV Ratio Evolution (99%)</h3></div><div class="chart-body"><div class="callout">Rolling 63-trading-day historical VaR at 99% confidence, divided by rolling NAV. This is a high-confidence tail-risk-to-capital monitor.</div><div style="height:12px"></div><div id="varNav99Plot" class="plot-slot short"></div></div></div></div><div class="chart-card"><div class="chart-header"><h3>Risk Contribution Table</h3></div><div class="chart-body"><div id="riskContribTable"></div></div></div></div></section>
<section id="tab-factor" class="tab"><div class="stack"><div class="grid2"><div class="chart-card"><div class="chart-header"><h3>PCA Explained Variance</h3></div><div class="chart-body"><div id="pcaVariancePlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>PC1 Loadings</h3></div><div class="chart-body"><div id="pcaLoadingsPlot" class="plot-slot short"></div></div></div></div><div class="chart-card"><div class="chart-header"><h3>PCA Loadings Table</h3></div><div class="chart-body"><div id="pcaTable"></div></div></div></div></section>
<section id="tab-stress" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Stress Dashboard KPIs</h3></div><div class="chart-body"><div id="stressKpiGrid" class="kpi-grid"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Scenario Impact Ranking</h3></div><div class="chart-body"><div id="stressPlot" class="plot-slot"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Scenario Impact Decomposition Heatmap</h3></div><div class="chart-body"><div id="stressHeatmapPlot" class="plot-slot"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Stress Family Summary</h3></div><div class="chart-body"><div id="stressFamilyTable"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Stress Scenario Table</h3></div><div class="chart-body"><div id="stressTable"></div></div></div></div></section>
<section id="tab-qs" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Quantstats-Style Performance Tearsheet</h3></div><div class="chart-body"><div class="callout" id="qsStatusBox"></div><div style="height:12px"></div><div id="qsHtmlFrameBox"></div><div style="height:12px"></div><div id="qsMetricsTable"></div></div></div><div class="grid2"><div class="chart-card"><div class="chart-header"><h3>Daily Returns Distribution</h3></div><div class="chart-body"><div id="dailyReturnHistPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Daily Returns Time Series</h3></div><div class="chart-body"><div id="dailyReturnTsPlot" class="plot-slot short"></div></div></div></div><div class="grid2"><div class="chart-card"><div class="chart-header"><h3>Rolling Volatility</h3></div><div class="chart-body"><div id="rollingVolPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Active Return vs Benchmark</h3></div><div class="chart-body"><div id="activeReturnPlot" class="plot-slot short"></div></div></div></div></div></section>
<section id="tab-data" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Data Quality Diagnostics</h3></div><div class="chart-body"><div id="dataQualityTable"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Time-Series Chart Audit</h3></div><div class="chart-body"><div class="callout">Every line chart is rendered from explicit daily point arrays produced by the backend. No lower-frequency/downsampled frontend series is used.</div><div style="height:12px"></div><div id="timeSeriesChartAuditTable"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Selected Prices Preview</h3></div><div class="chart-body"><div id="dataTable"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Daily Returns Matrix Preview</h3></div><div class="chart-body"><div id="dailyReturnsMatrixTable"></div></div></div></div></section>
<div class="footer">Institutional Quantitative Platform — MK Istanbul Fintech LabGEN @2026</div></main></div>
<script>
try{const b=document.getElementById('bootBanner'); if(b) b.style.display='none';}catch(e){}
function selectedBenchmarkSymbol(){return document.getElementById('benchmarkSymbol').value || '^GSPC';}
const QFA_BUILD_ID = 'qfa_all_timeseries_daily_point_by_point_v1';
let ETF_UNIVERSE = {}; let CURRENT = null;
function status(msg){document.getElementById('statusBox').textContent=msg} function showTab(tabId,btn){document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));document.getElementById(tabId).classList.add('active');btn.classList.add('active');setTimeout(()=>window.dispatchEvent(new Event('resize')),120)}
function fmtPct(v,d=2){return(v==null||isNaN(v))?'—':(v*100).toFixed(d)+'%'}
function fmtNum(v,d=3){return(v==null||isNaN(v))?'—':Number(v).toFixed(d)}
function fmtMoney(v){return(v==null||isNaN(v))?'—':new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',maximumFractionDigits:0}).format(v)}
function metricName(row,k=''){return String((row&&(row.Metric||row.metric||row.Measure||row.measure))||k||'').toLowerCase()}
function formatCell(v,k='',row=null){
  if(v==null || v==='') return '—';
  if(typeof v==='number'){
    const lk=String(k||'').toLowerCase();
    const mk=metricName(row,k);
    const key=(lk+' '+mk).toLowerCase();
    // Ratios and factor-like diagnostics are plain numbers, never percentages or dollars.
    if(['sharpe','sortino','information ratio','calmar','beta','profit factor','skew','kurtosis','factor','pc1','pc2','pc3'].some(x=>key.includes(x))) return fmtNum(v,3);
    // True money fields only. In generic Metric/Value tables, use currency only when the row metric explicitly names a capital/value/P&L field.
    if(['final value','initial capital','portfolio value','benchmark value','total profit','profit loss','p&l','transaction cost','usd'].some(x=>mk.includes(x))) return fmtMoney(v);
    // Percentage-return/risk fields.
    if(['annual return','total return','excess return','cagr','return pct','best day','worst day','max drawdown','drawdown','var','cvar','es 95','volatility','weight','tracking error','alpha','contribution','win rate','loss','impact','yield','rate','exposure','missing %'].some(x=>key.includes(x))) return fmtPct(v);
    // Generic Value column: use the row Metric name above; fallback as a compact numeric.
    return fmtNum(v,4);
  }
  return String(v);
}
function renderTable(id,rows){const el=document.getElementById(id); if(!rows||!rows.length){el.innerHTML='<div class="note">No data available.</div>';return} const cols=Object.keys(rows[0]);let html='<div class="table-wrap"><table class="data-table"><thead><tr>'+cols.map(c=>`<th>${c}</th>`).join('')+'</tr></thead><tbody>';rows.forEach(r=>{html+='<tr>'+cols.map(c=>`<td>${formatCell(r[c],c,r)}</td>`).join('')+'</tr>'});html+='</tbody></table></div>';el.innerHTML=html}
function dailyTrace(points, valueKey, name, extra={}){
  const pts=(points||[]).filter(p => p && p.Date !== undefined && p[valueKey] !== undefined && p[valueKey] !== null && !isNaN(Number(p[valueKey])));
  return Object.assign({
    type:'scattergl',
    mode:'lines',
    name:name,
    x:pts.map(p=>p.Date),
    y:pts.map(p=>Number(p[valueKey])),
    connectgaps:false,
    line:{width:1.8,shape:'linear',simplify:false},
    hovertemplate:'%{x}<br>'+name+': %{y}<extra></extra>'
  }, extra)
}
function dailyPctTrace(points, valueKey, name, extra={}){const tr=dailyTrace(points,valueKey,name,extra);tr.hovertemplate='%{x}<br>'+name+': %{y:.3%}<extra></extra>';return tr}
function dailyMoneyTrace(points, valueKey, name, extra={}){const tr=dailyTrace(points,valueKey,name,extra);tr.hovertemplate='%{x}<br>'+name+': $%{y:,.0f}<extra></extra>';return tr}
function dailyLayout(title, ytitle, yfmt=null){const lay={title:title,xaxis:{title:'Daily trading date',type:'date',rangeslider:{visible:true},tickformat:'%Y-%m-%d',hoverformat:'%Y-%m-%d'},yaxis:{title:ytitle},hovermode:'x unified'}; if(yfmt) lay.yaxis.tickformat=yfmt; return lay}

function plot(id,data,layout){if(typeof Plotly==='undefined'){document.getElementById(id).innerHTML='<div class="callout">Plotly CDN could not load. Backend and UI are running, but charts need internet/CDN access.</div>'; return;} Plotly.newPlot(id,data,Object.assign({paper_bgcolor:'white',plot_bgcolor:'white',font:{family:'Segoe UI, Arial',color:'#213043',size:12},margin:{l:74,r:42,t:70,b:82},legend:{orientation:'h',y:1.08,x:0.5,xanchor:'center'}},layout||{}),{responsive:true,displayModeBar:false})}
function baseChartLayout(title, extra={}){return Object.assign({title:{text:title,x:0.5,font:{size:15}},paper_bgcolor:'white',plot_bgcolor:'white',font:{family:'Segoe UI, Arial',color:'#213043',size:12},margin:{l:78,r:55,t:78,b:88},legend:{orientation:'h',y:1.08,x:0.5,xanchor:'center'},hovermode:'x unified'},extra||{})}
function pointSeries(points,key){return (points||[]).filter(p=>p&&p.Date!==undefined&&p[key]!==undefined&&p[key]!==null&&!isNaN(Number(p[key]))).map(p=>({x:p.Date,y:Number(p[key])}))}
function plotEquityCurve(r){const eq=pointSeries(r.equity_daily_points||[],'Portfolio Equity Value');const beq=pointSeries(r.benchmark_equity_daily_points||[],'Benchmark Equity Value');const s=r.summary||{};let ann=[];if(eq.length){const last=eq[eq.length-1];ann=[{x:0.02,y:0.95,xref:'paper',yref:'paper',text:`Initial: ${fmtMoney(r.meta.initial_capital)} → Final: ${fmtMoney(s.final_value)}<br>Total return: ${fmtPct((s.final_value/r.meta.initial_capital)-1)}`,showarrow:false,bgcolor:'rgba(255,255,255,.86)',bordercolor:'#2E86AB',borderwidth:1,font:{size:11}}]}plot('equityPlot',[{type:'scatter',mode:'lines',name:`${r.meta.best_strategy} Portfolio`,x:eq.map(p=>p.x),y:eq.map(p=>p.y),line:{width:2.4,color:'#2E86AB'},fill:'tozeroy',fillcolor:'rgba(46,134,171,.10)',hovertemplate:'%{x}<br>Portfolio: $%{y:,.0f}<extra></extra>'},{type:'scatter',mode:'lines',name:`Benchmark (${r.meta.benchmark})`,x:beq.map(p=>p.x),y:beq.map(p=>p.y),line:{width:1.7,color:'#E74C3C',dash:'dash'},hovertemplate:'%{x}<br>Benchmark: $%{y:,.0f}<extra></extra>'}],baseChartLayout('Portfolio vs Benchmark Equity Curve — Daily Compounding',{xaxis:{title:'Date',rangeslider:{visible:true}},yaxis:{title:'Portfolio Value',tickprefix:'$'},annotations:ann}))}
function plotDrawdown(r){const dd=pointSeries(r.drawdown_daily_points||[],'Portfolio Daily Drawdown');const bd=pointSeries(r.benchmark_drawdown_daily_points||[],'Benchmark Daily Drawdown');let minP=dd.reduce((a,b)=>b.y<a.y?b:a,{x:null,y:0});let annotations=minP.x?[{x:minP.x,y:minP.y,text:`Max DD: ${fmtPct(minP.y)}`,showarrow:true,arrowhead:2,arrowcolor:'#E74C3C',bgcolor:'rgba(255,255,255,.85)',font:{size:11}}]:[];plot('drawdownPlot',[{type:'scatter',mode:'lines',fill:'tozeroy',name:`${r.meta.best_strategy} Drawdown`,x:dd.map(p=>p.x),y:dd.map(p=>p.y),line:{color:'#E74C3C',width:1.6},fillcolor:'rgba(231,76,60,.30)',hovertemplate:'%{x}<br>Portfolio DD: %{y:.2%}<extra></extra>'},{type:'scatter',mode:'lines',name:'Benchmark Drawdown',x:bd.map(p=>p.x),y:bd.map(p=>p.y),line:{color:'#95A5A6',width:1.3,dash:'dash'},hovertemplate:'%{x}<br>Benchmark DD: %{y:.2%}<extra></extra>'}],baseChartLayout('Drawdown Analysis — Daily Returns, No Resampling',{xaxis:{title:'Date',rangeslider:{visible:true}},yaxis:{title:'Drawdown',tickformat:'.0%'},annotations}))}
function rollingStd(vals,win){let out=[];for(let i=0;i<vals.length;i++){if(i<win-1){out.push(null);continue;}let a=vals.slice(i-win+1,i+1).filter(v=>v!==null&&!isNaN(v));let m=a.reduce((x,y)=>x+y,0)/a.length;let v=a.reduce((x,y)=>x+(y-m)*(y-m),0)/(a.length-1);out.push(Math.sqrt(v)*Math.sqrt(252));}return out}
function plotTrackingDynamic(r){const eq=pointSeries(r.equity_daily_points||[],'Portfolio Equity Value');const beq=pointSeries(r.benchmark_equity_daily_points||[],'Benchmark Equity Value');const te=pointSeries(r.rolling_tracking_error_daily_points||[],'Rolling Tracking Error');const eqMap=new Map(eq.map(p=>[p.x,p.y]));const beqMap=new Map(beq.map(p=>[p.x,p.y]));const common=[...eqMap.keys()].filter(d=>beqMap.has(d)).sort();if(!common.length){document.getElementById('trackingDynamicPlot').innerHTML='<div class="note">No common daily points available for tracking-error chart.</div>';return;}const p0=eqMap.get(common[0]), b0=beqMap.get(common[0]);const xs=common, cp=common.map(d=>eqMap.get(d)/p0-1), cb=common.map(d=>beqMap.get(d)/b0-1);plot('trackingDynamicPlot',[{type:'scatter',mode:'lines',name:`${r.meta.best_strategy} Cum Return`,x:xs,y:cp,line:{color:'#2E86AB',width:2},hovertemplate:'%{x}<br>Portfolio Cum: %{y:.2%}<extra></extra>'},{type:'scatter',mode:'lines',name:'Benchmark Cum Return',x:xs,y:cb,line:{color:'#E74C3C',width:1.5},hovertemplate:'%{x}<br>Benchmark Cum: %{y:.2%}<extra></extra>'},{type:'scatter',mode:'lines',name:'Rolling Tracking Error',x:te.map(p=>p.x),y:te.map(p=>p.y),yaxis:'y2',line:{color:'#F39C12',width:1.5,dash:'dot'},hovertemplate:'%{x}<br>Rolling TE: %{y:.2%}<extra></extra>'}],baseChartLayout('Benchmark vs Tracking-Error Dynamic Curve — daily aligned',{xaxis:{title:'Date'},yaxis:{title:'Cumulative Return',tickformat:'.0%'},yaxis2:{title:'Rolling TE',overlaying:'y',side:'right',tickformat:'.0%',showgrid:false}}))}
function plotRollingAssetBetas(r){const rows=r.rolling_asset_beta_points||[];const assets=r.beta_summary?r.beta_summary.map(x=>x.Asset):[];if(!rows.length||!assets.length){document.getElementById('rollingAssetBetaPlot').innerHTML='<div class="note">No rolling asset beta data available.</div>';return;}const data=assets.map((a,i)=>({type:'scatter',mode:'lines',name:a,x:rows.map(x=>x.Date),y:rows.map(x=>x[a]),line:{width:1.5},opacity:i<8?0.95:0.35,visible:i<12?true:'legendonly',hovertemplate:'%{x}<br>'+a+' beta: %{y:.3f}<extra></extra>'}));plot('rollingAssetBetaPlot',data,baseChartLayout('Rolling Asset Betas vs S&P 500 — daily rolling covariance / variance',{height:620,xaxis:{title:'Date'},yaxis:{title:'Rolling Beta'},shapes:[{type:'line',xref:'paper',x0:0,x1:1,y0:1,y1:1,line:{dash:'dash',color:'gray'}}]}))}
function plotTrackingErrorByStrategy(r){const rows=r.strategy_metrics||[];plot('trackingErrorByStrategyPlot',[{type:'bar',x:rows.map(x=>x.Strategy),y:rows.map(x=>x['Tracking Error']),text:rows.map(x=>fmtPct(x['Tracking Error'])),textposition:'outside',marker:{color:'#F39C12'},hovertemplate:'%{x}<br>Tracking Error: %{y:.2%}<extra></extra>'}],baseChartLayout('Tracking Error by Strategy — annualized active daily return volatility',{height:480,xaxis:{title:'Strategy',tickangle:35},yaxis:{title:'Tracking Error',tickformat:'.0%'},shapes:[{type:'line',xref:'paper',x0:0,x1:1,y0:0.06,y1:0.06,line:{dash:'dash',color:'#E74C3C'}}],annotations:[{xref:'paper',yref:'y',x:1,y:0.06,text:'Target 6%',showarrow:false,xanchor:'right',font:{size:11,color:'#E74C3C'}}]}))}
function plotExecutiveDashboard(rows){const names=rows.map(x=>x.Strategy);const specs=[['annual_return','Annual Return','.2%','#2E86AB'],['sharpe_ratio','Sharpe Ratio','.2f','#6A994E'],['sortino_ratio','Sortino Ratio','.2f','#A23B72'],['max_drawdown','Max Drawdown','.2%','#E74C3C'],['information_ratio','Information Ratio','.2f','#F18F01'],['tracking_error','Tracking Error','.2%','#5D576B']];const keyMap={'Annual Return':'annual_return','Sharpe Ratio':'sharpe_ratio','Sortino Ratio':'sortino_ratio','Max Drawdown':'max_drawdown','Information Ratio':'information_ratio','Tracking Error':'tracking_error'};let data=[];let layout=baseChartLayout('Executive Strategy Dashboard — Separate Axes, Correct Units',{grid:{rows:2,columns:3,pattern:'independent'},showlegend:false,height:760,margin:{l:70,r:40,t:95,b:120}});specs.forEach((sp,i)=>{let axis=i+1;let xaxis='x'+(axis===1?'':axis),yaxis='y'+(axis===1?'':axis);let metric=sp[0];let vals=rows.map(r=> metric==='annual_return'?r['Annual Return']:metric==='sharpe_ratio'?r['Sharpe Ratio']:metric==='sortino_ratio'?r['Sortino Ratio']:metric==='max_drawdown'?r['Max Drawdown']:metric==='information_ratio'?r['Information Ratio']:r['Tracking Error']);data.push({type:'bar',x:names,y:vals,text:vals.map(v=>sp[2].includes('%')?fmtPct(v):fmtNum(v,2)),textposition:'outside',marker:{color:sp[3]},xaxis:xaxis,yaxis:yaxis,hovertemplate:'%{x}<br>'+sp[1]+': %{text}<extra></extra>'});layout['xaxis'+(axis===1?'':axis)]={title:'',tickangle:45,tickfont:{size:9}};layout['yaxis'+(axis===1?'':axis)]={title:sp[1],tickformat:sp[2].includes('%')?'.0%':undefined};});Plotly.newPlot('dashboardPlot',data,layout,{responsive:true,displayModeBar:false})}
function plotRadar(rows){const metrics=[['Sharpe Ratio','Sharpe'],['Sortino Ratio','Sortino'],['Information Ratio','Info Ratio'],['Max Drawdown','Max DD'],['Annual Return','Annual Return'],['Tracking Error','Tracking Error']];let top=rows.slice(0,8);let mins={},maxs={};metrics.forEach(([k])=>{let vals=top.map(r=>k==='Max Drawdown'?-r[k]:k==='Tracking Error'?-r[k]:r[k]).filter(v=>isFinite(v));mins[k]=Math.min(...vals);maxs[k]=Math.max(...vals);});let data=top.map((r,i)=>{let theta=metrics.map(x=>x[1]);let vals=metrics.map(([k])=>{let raw=k==='Max Drawdown'?-r[k]:k==='Tracking Error'?-r[k]:r[k];let den=maxs[k]-mins[k];return den>0?(raw-mins[k])/den:.5;});vals.push(vals[0]);theta.push(theta[0]);return {type:'scatterpolar',r:vals,theta:theta,fill:'toself',name:r.Strategy,hovertemplate:'%{theta}: %{r:.2f}<extra>'+r.Strategy+'</extra>'};});plot('radarPlot',data,{title:'Strategy Performance Radar (Normalized)',polar:{radialaxis:{visible:true,range:[0,1],tickformat:'.0%'}},height:650,legend:{orientation:'v',x:1.02,y:1}})}
function plotRelativeFrontier(r){const rel=r.relative_frontier||[];const rows=r.strategy_metrics||[];plot('relativeFrontierPlot',[{type:'scatter',mode:'markers',name:'Feasible Relative Portfolios',x:rel.map(x=>x.ActiveVolatility),y:rel.map(x=>x.ExcessReturn),marker:{size:4,color:'lightgray'},opacity:.35,hovertemplate:'Active Risk: %{x:.2%}<br>Excess Return: %{y:.2%}<extra></extra>'},{type:'scatter',mode:'markers+text',name:'Strategies',x:rows.map(x=>x['Tracking Error']),y:rows.map(x=>x['Annual Return']-(r.summary.annual_return-r.summary.alpha||0)),text:rows.map(x=>x.Strategy),textposition:'top center',marker:{size:11,symbol:'star',color:'#E74C3C'},hovertemplate:'%{text}<br>TE: %{x:.2%}<br>Excess Ret Proxy: %{y:.2%}<extra></extra>'},{type:'scatter',mode:'markers+text',name:'Benchmark',x:[0],y:[0],text:['Benchmark'],textposition:'bottom center',marker:{size:10,color:'#2E86AB'}}],baseChartLayout('Benchmark-Relative Efficient Frontier — Active Risk vs Excess Return',{xaxis:{title:'Active Risk / Tracking Error',tickformat:'.0%'},yaxis:{title:'Excess Annual Return vs Benchmark',tickformat:'.0%'},shapes:[{type:'line',xref:'paper',x0:0,x1:1,y0:0,y1:0,line:{dash:'dash',color:'gray'}},{type:'line',yref:'paper',y0:0,y1:1,x0:0,x1:0,line:{dash:'dash',color:'gray'}}]}))}
function plotFinquantMc(r){const cloud=r.monte_carlo_frontier||[];const frontier=r.efficient_frontier||[];const assets=r.asset_risk_return||[];plot('finquantMcPlot',[{type:'scatter',mode:'markers',name:`Random Portfolios (${cloud.length})`,x:cloud.map(x=>x.Volatility),y:cloud.map(x=>x.Return),marker:{size:3,color:cloud.map(x=>x.Sharpe),colorscale:'Viridis',showscale:true,colorbar:{title:'Sharpe'}},opacity:.75,hovertemplate:'Risk: %{x:.2%}<br>Return: %{y:.2%}<br>Sharpe: %{marker.color:.2f}<extra></extra>'},{type:'scatter',mode:'lines',name:'Efficient Frontier',x:frontier.map(x=>x.Volatility),y:frontier.map(x=>x.Return),line:{color:'#2E86AB',width:3},hovertemplate:'Risk: %{x:.2%}<br>Return: %{y:.2%}<extra></extra>'},{type:'scatter',mode:'markers+text',name:'Individual Assets',x:assets.map(x=>x.Volatility),y:assets.map(x=>x.Return),text:assets.map(x=>x.Asset),textposition:'top center',marker:{size:9,color:'#95A5A6'},textfont:{size:8}}],baseChartLayout('FinQuant-Style Efficient Frontier & Monte Carlo',{xaxis:{title:'Annual Volatility (Risk)',tickformat:'.0%'},yaxis:{title:'Annual Return',tickformat:'.0%'},height:650,legend:{orientation:'h',x:.01,y:.99,xanchor:'left',yanchor:'top'}}))}
function plotRiskBars(r){const rows=r.strategy_metrics||[];plot('riskBarPlot',[{type:'bar',name:'VaR 95',x:rows.map(x=>x.Strategy),y:rows.map(x=>x['VaR 95']),text:rows.map(x=>fmtPct(x['VaR 95'])),textposition:'auto',marker:{color:'#E74C3C'}},{type:'bar',name:'CVaR 95',x:rows.map(x=>x.Strategy),y:rows.map(x=>x['CVaR 95']),text:rows.map(x=>fmtPct(x['CVaR 95'])),textposition:'auto',marker:{color:'#F39C12'}},{type:'bar',name:'Relative CVaR 95',x:rows.map(x=>x.Strategy),y:rows.map(x=>x['Relative CVaR 95']),text:rows.map(x=>fmtPct(x['Relative CVaR 95'])),textposition:'auto',marker:{color:'#9B59B6'}}],baseChartLayout('VaR / CVaR Figures — Positive Loss Magnitudes',{barmode:'group',xaxis:{tickangle:35},yaxis:{title:'Loss Magnitude',tickformat:'.0%'}}))}
function plotVarNavRatio(r, level){const key=level===99?'rolling_var_nav_99_points':'rolling_var_nav_95_points';const valueKey=level===99?'VaR NAV Ratio 99':'VaR NAV Ratio 95';const id=level===99?'varNav99Plot':'varNav95Plot';const pts=pointSeries(r[key]||[], valueKey);if(!pts.length){document.getElementById(id).innerHTML='<div class="note">No rolling VaR/NAV ratio points available. Need at least 63 daily observations.</div>';return;}const maxPt=pts.reduce((a,b)=>b.y>a.y?b:a,{x:null,y:-Infinity});const annotations=maxPt.x?[{x:maxPt.x,y:maxPt.y,text:`Peak: ${fmtPct(maxPt.y)}`,showarrow:true,arrowhead:2,bgcolor:'rgba(255,255,255,.88)',font:{size:11}}]:[];plot(id,[{type:'scatter',mode:'lines',name:`3M VaR / NAV ${level}%`,x:pts.map(p=>p.x),y:pts.map(p=>p.y),line:{width:2.1,color:level===99?'#A23B72':'#E74C3C'},fill:'tozeroy',fillcolor:level===99?'rgba(162,59,114,.14)':'rgba(231,76,60,.14)',hovertemplate:'%{x}<br>VaR/NAV: %{y:.3%}<extra></extra>'}],baseChartLayout(`3-Month VaR / NAV Ratio Evolution (${level}%) — daily rolling 63D`,{height:430,xaxis:{title:'Date',rangeslider:{visible:true}},yaxis:{title:'VaR / NAV Ratio',tickformat:'.1%'},annotations}))}

async function fetchUniverse(){const res=await fetch('/api/universe');const js=await res.json();ETF_UNIVERSE=js.universe||{};buildDrilldown()} function buildDrilldown(){const host=document.getElementById('categoryDrilldown');host.innerHTML='';Object.entries(ETF_UNIVERSE).forEach(([cat,tickers],idx)=>{const box=document.createElement('div');box.className='category-box';box.innerHTML=`<div class="category-title"><span>${cat}</span><label><input type="checkbox" data-cat="${idx}" class="cat-toggle"> all</label></div>`;const list=document.createElement('div');list.className='ticker-list';tickers.forEach(t=>{const row=document.createElement('label');row.className='tick-item';row.innerHTML=`<input type="checkbox" class="ticker-check" data-category="${cat}" value="${t}"><span>${t}</span>`;list.appendChild(row)});box.appendChild(list);host.appendChild(box)});document.querySelectorAll('.cat-toggle').forEach(toggle=>{toggle.addEventListener('change',e=>{const cat=Object.keys(ETF_UNIVERSE)[Number(e.target.dataset.cat)];document.querySelectorAll(`.ticker-check[data-category="${cat}"]`).forEach(cb=>cb.checked=e.target.checked)})});['US Broad Equity','US Growth & Value','Emerging Markets','Fixed Income','Real Assets'].forEach(cat=>{document.querySelectorAll(`.ticker-check[data-category="${cat}"]`).forEach((cb,i)=>{if(i<Math.min(3,(ETF_UNIVERSE[cat]||[]).length))cb.checked=true})})}
function selectedTickers(){return[...document.querySelectorAll('.ticker-check:checked')].map(x=>x.value)} function selectedCategories(){return[...new Set([...document.querySelectorAll('.ticker-check:checked')].map(x=>x.dataset.category))]}
async function uploadAndParseFiles(){throw new Error('Upload mode is disabled: Yahoo Finance daily-only policy is locked.')}
async function getYahooRows(tickers){const payload={tickers,start_date:document.getElementById('startDate').value,benchmark_symbol:selectedBenchmarkSymbol(),use_cache:false};const res=await fetch('/api/yahoo-prices',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});if(!res.ok)throw new Error(await res.text());return await res.json()}
async function recompute(){try{status('Working...');const tickers=selectedTickers();if(tickers.length<3){alert('Please select at least 3 ETFs.');status('Ready.');return}let metadata=[];const yh=await getYahooRows(tickers);const rows=yh.rows;const payload={rows,data_source:'yahoo',source_interval:'1d',synthetic_data_allowed:false,lower_frequency_aggregate_allowed:false,benchmark_symbol:selectedBenchmarkSymbol(),initial_capital:Number(document.getElementById('initialCapital').value||1000000),risk_free_rate:Number(document.getElementById('riskFreeRate').value||0.045),rolling_window:Number(document.getElementById('rollingWindow').value||63),expected_return_method:document.getElementById('expReturnMethod').value,covariance_method:document.getElementById('covMethod').value,best_strategy_rule:document.getElementById('bestStrategyRule').value,stress_family:document.getElementById('stressFamily').value,min_severity:Number(document.getElementById('minSeverity').value||0)};const res=await fetch('/api/compute-report',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});if(!res.ok)throw new Error(await res.text());const js=await res.json();CURRENT={report:js.report,metadata};renderAll();status('Done.')}catch(err){console.error(err);status('Failed.');alert(String(err))}}

function getKpiToneForImpact(v){if(v<=-0.20)return 'border-left:6px solid #C73E1D'; if(v< -0.05)return 'border-left:6px solid #F39C12'; return 'border-left:6px solid #6A994E'}
function plotStressRanking(r){
  const rows=(r.stress_table||[]).slice().sort((a,b)=>(a['Portfolio Impact']||0)-(b['Portfolio Impact']||0));
  const h=Math.max(520, rows.length*58+170);
  plot('stressPlot',[{type:'bar',orientation:'h',y:rows.map(x=>x.Scenario),x:rows.map(x=>x['Portfolio Impact']),text:rows.map(x=>fmtPct(x['Portfolio Impact'])),textposition:'outside',cliponaxis:false,marker:{color:rows.map(x=>(x['Portfolio Impact']||0)<=-0.20?'#C73E1D':((x['Portfolio Impact']||0)<-0.05?'#F39C12':'#6A994E'))},hovertemplate:'%{y}<br>Impact: %{x:.2%}<extra></extra>'}],baseChartLayout('Scenario Impact Ranking — readable label layout',{height:h,margin:{l:260,r:110,t:70,b:65},xaxis:{tickformat:'.0%',title:'Portfolio impact'},yaxis:{automargin:true,categoryorder:'array',categoryarray:rows.map(x=>x.Scenario)}}))
}
function plotStressHeatmap(r){
  const rows=(r.stress_table||[]);
  const buckets=['Equity Impact','Rates Impact','Gold Impact','Commodity Impact','EM Impact','Turkey Impact'];
  if(!rows.length){document.getElementById('stressHeatmapPlot').innerHTML='<div class="note">No stress heatmap available.</div>'; return}
  plot('stressHeatmapPlot',[{type:'heatmap',x:buckets.map(x=>x.replace(' Impact','')),y:rows.map(x=>x.Scenario),z:rows.map(row=>buckets.map(b=>Number(row[b]||0))),colorscale:[[0,'#C73E1D'],[0.5,'#FFFFFF'],[1,'#27AE60']],zmid:0,text:rows.map(row=>buckets.map(b=>fmtPct(row[b]||0))),texttemplate:'%{text}',textfont:{size:10},hovertemplate:'%{y}<br>%{x}: %{z:.2%}<extra></extra>'}],baseChartLayout('Scenario Impact Decomposition by Risk Bucket',{height:Math.max(520, rows.length*55+160),margin:{l:260,r:40,t:70,b:80},xaxis:{title:'Risk bucket'},yaxis:{automargin:true}}))
}
function buildStressFamilySummary(rows){
  const by={};
  (rows||[]).forEach(x=>{const f=x.Family||'Unknown'; if(!by[f]) by[f]={Family:f,Count:0,'Worst Impact':0,'Average Impact':0,'Average Severity':0}; by[f].Count+=1; by[f]['Worst Impact']=Math.min(by[f]['Worst Impact'], Number(x['Portfolio Impact']||0)); by[f]['Average Impact']+=Number(x['Portfolio Impact']||0); by[f]['Average Severity']+=Number(x['Severity']||0)});
  return Object.values(by).map(x=>{x['Average Impact']/=x.Count||1; x['Average Severity']/=x.Count||1; return x}).sort((a,b)=>a['Worst Impact']-b['Worst Impact']);
}

function renderAll(){const r=CURRENT.report,s=r.summary;document.getElementById('headerMeta').textContent=`Benchmark: ${r.meta.benchmark_label || r.meta.benchmark} • Frequency: DAILY RETURNS LOCKED • Periods/Year: 252 • RF: ${fmtPct(r.meta.risk_free_rate)} | Generated by MK FinTECH LabGEN@2026`;document.getElementById('kpiGrid').innerHTML=`<div class="kpi-card"><div class="kpi-label">Assets</div><div class="kpi-value">${r.meta.selected_count}</div><div class="kpi-sub">${selectedCategories().join(' • ')}</div></div><div class="kpi-card"><div class="kpi-label">Best Strategy</div><div class="kpi-value">${r.meta.best_strategy}</div><div class="kpi-sub">Rule: ${r.meta.best_strategy_rule}</div></div><div class="kpi-card"><div class="kpi-label">Annual Return</div><div class="kpi-value">${fmtPct(s.annual_return)}</div><div class="kpi-sub">Vol: ${fmtPct(s.volatility)}</div></div><div class="kpi-card"><div class="kpi-label">Sharpe</div><div class="kpi-value">${fmtNum(s.sharpe_ratio)}</div><div class="kpi-sub">IR: ${fmtNum(s.information_ratio)}</div></div><div class="kpi-card"><div class="kpi-label">Max Drawdown</div><div class="kpi-value">${fmtPct(s.max_drawdown)}</div><div class="kpi-sub">CVaR 95: ${fmtPct(s.cvar_95)}</div></div><div class="kpi-card"><div class="kpi-label">Final Value</div><div class="kpi-value">${fmtMoney(s.final_value)}</div><div class="kpi-sub">Initial: ${fmtMoney(r.meta.initial_capital)}</div></div>`;
document.getElementById('keyMetricsHeader').innerHTML=`<b>Benchmark:</b> ${r.meta.benchmark_label || r.meta.benchmark} &nbsp; • &nbsp; <b>Frequency:</b> Yahoo Finance 1D -> Portfolio Daily Return Series -> all time-series charts &nbsp; • &nbsp; <b>Periods/Year:</b> 252 &nbsp; • &nbsp; <b>RF:</b> ${fmtPct(r.meta.risk_free_rate)} &nbsp; | &nbsp; Generated by MK FinTECH LabGEN@2026`;renderTable('keyMetricsTable',r.key_metrics);renderTable('strategyExplanationTable',r.strategy_explanations||[]);renderTable('strategyTable',r.strategy_metrics);document.getElementById('bestGuideBox').innerHTML=`<b>Selected strategy:</b> ${r.meta.best_strategy}<br><br>${r.meta.best_strategy_explanation}<br><br><b>Institutional guardrails:</b> long-only weights, single-name cap, cash-like ETF cap, Ledoit-Wolf covariance fallback, benchmark-relative TE/IR/alpha/beta diagnostics, VaR/CVaR/ES, PCA and stress scenario ranking.`;
const metaRows=(CURRENT.metadata&&CURRENT.metadata.length)?CURRENT.metadata:r.weights.map(w=>({category:[...Object.entries(ETF_UNIVERSE)].find(([k,v])=>v.includes(w.asset))?.[0]||'',ticker:w.asset,ISINCODE:'',name:w.asset,exchange:'',currency:'',type:'ETF'}));renderTable('assetMetaTable',metaRows);renderTable('riskContribTable',r.risk_contrib);renderTable('advancedVarTable',r.advanced_var_table||[]);renderTable('relativeVarTable',r.relative_var_table||[]);renderTable('dataTable',r.prices_preview);renderTable('dataQualityTable',r.data_quality);renderTable('timeSeriesChartAuditTable',r.time_series_chart_audit||[]);renderTable('dailyReturnsMatrixTable',(r.daily_returns_matrix||[]).slice(0,30));renderTable('pcaTable',r.pca_loadings);renderTable('stressFamilyTable',buildStressFamilySummary(r.stress_table||[]));renderTable('stressTable',r.stress_table);
document.getElementById('allocationExplain').innerHTML=`<b>Allocation Methodology</b><br>Available strategies are computed and ranked: equal weight, inverse volatility, minimum variance, max Sharpe approximation, and tracking-error aware blend. The chosen strategy is selected by the rule in the sidebar.`;
const catCounts={};metaRows.forEach(x=>{catCounts[x.category]=(catCounts[x.category]||0)+1});
plot('infoHubPlot',[{type:'bar',x:Object.keys(catCounts),y:Object.values(catCounts),text:Object.values(catCounts),textposition:'outside',marker:{color:'#2E86AB'}}],baseChartLayout('Investment Universe Identity Map',{yaxis:{title:'Instrument Count'}}));
plotExecutiveDashboard(r.strategy_metrics||[]);
plotRadar(r.strategy_metrics||[]);
plot('allocationPlot',[{type:'bar',x:r.weights.map(x=>x.asset),y:r.weights.map(x=>x.weight),text:r.weights.map(x=>fmtPct(x.weight)),textposition:'outside',marker:{color:'#2E86AB'}}],baseChartLayout('Top Strategy Allocation',{yaxis:{title:'Weight',tickformat:'.0%'},xaxis:{tickangle:35}}));
plotEquityCurve(r);
plotDrawdown(r);
plotTrackingDynamic(r);
plot('riskContribPlot',[{type:'bar',x:r.risk_contrib.map(x=>x.Asset),y:r.risk_contrib.map(x=>x['Contribution %']),text:r.risk_contrib.map(x=>fmtPct(x['Contribution %'])),textposition:'outside',marker:{color:'#9B59B6'}}],baseChartLayout('Risk Contribution (% of Total Portfolio Volatility)',{yaxis:{title:'Contribution to Risk',tickformat:'.0%'},xaxis:{tickangle:35},shapes:[{type:'line',xref:'paper',x0:0,x1:1,y0:0,y1:0,line:{dash:'dash',color:'gray'}}]}));
plotRiskBars(r);
plotVarNavRatio(r,95);
plotVarNavRatio(r,99);
const rsPts=r.rolling_sharpe_daily_points||[];const rbPts=r.rolling_beta_daily_points||[];
plot('rollingSharpePlot',[dailyTrace(rsPts,'Rolling Sharpe','Rolling Sharpe',{line:{width:2,color:'#2E86AB'}})],baseChartLayout(`Rolling Sharpe — daily returns, rolling window (${rsPts.length} observations)`,{xaxis:{title:'Date'},yaxis:{title:'Sharpe Ratio'}}));
plot('rollingBetaPlot',[dailyTrace(rbPts,'Rolling Beta','Rolling Beta vs S&P 500',{line:{width:2,color:'#2E86AB'}})],baseChartLayout(`Rolling Beta — daily returns vs S&P 500 (${rbPts.length} observations)`,{xaxis:{title:'Date'},yaxis:{title:'Beta'},shapes:[{type:'line',xref:'paper',x0:0,x1:1,y0:1,y1:1,line:{dash:'dash',color:'gray'}}]}));
plotRollingAssetBetas(r);renderTable('betaSummaryTable',r.beta_summary||[]);plotTrackingErrorByStrategy(r);
plot('pcaVariancePlot',[{type:'bar',x:r.pca_variance.map(x=>x.Component),y:r.pca_variance.map(x=>x['Explained Variance']),marker:{color:'#2E86AB'}}],baseChartLayout('PCA Explained Variance',{yaxis:{tickformat:'.0%'}}));
plot('pcaLoadingsPlot',[{type:'bar',x:r.pca_loadings.map(x=>x.Asset),y:r.pca_loadings.map(x=>x.PC1),marker:{color:'#6A994E'}}],baseChartLayout('PC1 Loadings',{xaxis:{tickangle:35}}));
const cml=r.capital_market_line||[];const rows=r.strategy_metrics||[];plot('efficientFrontierPlot',[{type:'scatter',mode:'markers',name:'Efficient Frontier',x:(r.efficient_frontier||[]).map(x=>x.Volatility),y:(r.efficient_frontier||[]).map(x=>x.Return),marker:{size:7,color:(r.efficient_frontier||[]).map(x=>x.Sharpe),colorscale:'Viridis',showscale:true,colorbar:{title:'Sharpe'}},hovertemplate:'Volatility: %{x:.2%}<br>Return: %{y:.2%}<br>Sharpe: %{marker.color:.2f}<extra></extra>'},{type:'scatter',mode:'lines',name:'Capital Market Line',x:cml.map(x=>x.Volatility),y:cml.map(x=>x.Return),line:{dash:'dash',width:2,color:'#27AE60'}},{type:'scatter',mode:'markers+text',name:'Strategies',x:rows.map(x=>x.Volatility),y:rows.map(x=>x['Annual Return']),text:rows.map(x=>x.Strategy),textposition:'top center',marker:{size:12,symbol:'star',color:'#E74C3C'},textfont:{size:9}}],baseChartLayout('Portfolio Optimization & Efficient Frontier',{xaxis:{title:'Annualized Volatility',tickformat:'.0%'},yaxis:{title:'Annualized Return',tickformat:'.0%'},height:620}));
plotFinquantMc(r);
plotRelativeFrontier(r);
plot('strategyScatterPlot',[{type:'scatter',mode:'markers+text',x:rows.map(x=>x.Volatility),y:rows.map(x=>x['Annual Return']),text:rows.map(x=>x.Strategy),textposition:'top center',marker:{size:rows.map(x=>Math.max(10,Math.abs(x['Sharpe Ratio']||0)*12)),color:rows.map(x=>x['Sharpe Ratio']),colorscale:'Viridis',showscale:true,colorbar:{title:'Sharpe'}}}],baseChartLayout('Optimization Strategy Map',{xaxis:{title:'Volatility',tickformat:'.0%'},yaxis:{title:'Annual Return',tickformat:'.0%'}}));
document.getElementById('optimizerStatusBox').innerHTML=`<b>Optimizer engine:</b> ${r.meta.optimizer_engine||'Internal'}<br><b>PyPortfolioOpt status:</b> ${r.meta.pypfopt_status||'not reported'}<br><b>Input frequency:</b> daily returns only / daily-only calculation<br><b>Capital Market Line:</b> slope is based on selected portfolio Sharpe ratio and configured risk-free rate.`;
renderTable('qsMetricsTable',r.quantstats_metrics);document.getElementById('qsStatusBox').innerHTML=`<b>Quantstats package status:</b> ${r.meta.quantstats_status||'not reported'}<br><b>Data alignment:</b> ${r.meta.data_alignment||'common sample'}<br><b>Daily audit:</b> ${(r.meta.daily_return_audit&&r.meta.daily_return_audit.return_observations)||'—'} observations; median gap ${(r.meta.daily_return_audit&&r.meta.daily_return_audit.median_gap_days)||'—'} days; lower-frequency aggregate used: false<br>Below: real quantstats HTML tearsheet when available, plus Plotly mirrors. All Quantstats and PyPortfolioOpt inputs are audited daily-return series; daily-only calculation is used.`;document.getElementById('qsHtmlFrameBox').innerHTML=(r.meta.quantstats_html_url?`<iframe src="${r.meta.quantstats_html_url}" style="width:100%;height:900px;border:1px solid #d9e4ef;border-radius:14px;background:white;"></iframe>`:`<div class="note">Full quantstats HTML was not generated in this runtime; using Plotly mirror charts below.</div>`);
plot('dailyReturnHistPlot',[{type:'histogram',x:(r.portfolio_daily_return_points||[]).map(x=>x['Portfolio Daily Return']),nbinsx:80,marker:{color:'#2E86AB'}}],baseChartLayout('Daily Portfolio Return Distribution',{xaxis:{title:'Daily return',tickformat:'.1%'},yaxis:{title:'Frequency'}}));
const prPts=r.portfolio_daily_return_points||[];const brPts=r.benchmark_daily_return_points||[];plot('dailyReturnTsPlot',[dailyPctTrace(prPts,'Portfolio Daily Return','Portfolio Daily Return',{line:{width:1.4,color:'#2E86AB'}}),dailyPctTrace(brPts,'Benchmark Daily Return','Benchmark Daily Return',{line:{width:1.1,color:'#E74C3C'},opacity:0.65})],baseChartLayout(`Portfolio and S&P 500 DAILY Returns — tick-by-tick trading days (${prPts.length} observations)`,{xaxis:{title:'Date'},yaxis:{title:'Daily return',tickformat:'.1%'}}));
const rvPts=r.rolling_volatility_daily_points||[];const arPts=r.active_return_daily_points||[];plot('rollingVolPlot',[dailyPctTrace(rvPts,'Rolling Annualized Volatility','Rolling Annualized Volatility',{line:{width:2,color:'#F39C12'}})],baseChartLayout(`Rolling Annualized Volatility — daily returns (${rvPts.length} observations)`,{xaxis:{title:'Date'},yaxis:{title:'Volatility',tickformat:'.0%'}}));plot('activeReturnPlot',[dailyPctTrace(arPts,'Cumulative Active Return','Cumulative Active Return',{line:{width:2,color:'#27AE60'}})],baseChartLayout(`Cumulative Active Return vs S&P 500 — DAILY points only (${arPts.length} observations)`,{xaxis:{title:'Date'},yaxis:{title:'Cumulative active return',tickformat:'.0%'}}));document.getElementById('stressKpiGrid').innerHTML=`<div class="kpi-card" style="${getKpiToneForImpact(r.stress_kpis.worst_relative_return||0)}"><div class="kpi-label">Worst Scenario</div><div class="kpi-value">${r.stress_kpis.worst_scenario||'—'}</div><div class="kpi-sub">Impact: ${fmtPct(r.stress_kpis.worst_relative_return)}</div></div><div class="kpi-card" style="${(r.stress_kpis.average_severity||0)>=4?'border-left:6px solid #F39C12':'border-left:6px solid #6A994E'}"><div class="kpi-label">Avg Severity</div><div class="kpi-value">${fmtNum(r.stress_kpis.average_severity)}</div><div class="kpi-sub">Filtered scenarios</div></div><div class="kpi-card" style="${getKpiToneForImpact(r.stress_kpis.worst_drawdown||0)}"><div class="kpi-label">Worst Drawdown Proxy</div><div class="kpi-value">${fmtPct(r.stress_kpis.worst_drawdown)}</div><div class="kpi-sub">Scenario loss estimate</div></div><div class="kpi-card"><div class="kpi-label">Scenario Count</div><div class="kpi-value">${r.stress_kpis.count}</div><div class="kpi-sub">Red ${r.stress_kpis.red_count||0} • Amber ${r.stress_kpis.amber_count||0} • Green ${r.stress_kpis.green_count||0}</div></div>`;plotStressRanking(r);plotStressHeatmap(r)}
document.getElementById('recomputeBtn').addEventListener('click',recompute);fetchUniverse();
</script></body></html>'''



class ApiErrorPayload(BaseModel):
    status: str = "error"
    endpoint: str
    message: str
    detail: Optional[str] = None
    hint: Optional[str] = None
    timestamp_utc: str = Field(default_factory=lambda: pd.Timestamp.utcnow().isoformat())


class ApiSuccessPayload(BaseModel):
    status: str = "ok"
    endpoint: str
    saved_to: Optional[str] = None
    data_quality_note: Optional[str] = None


class YahooPricesRequest(BaseModel):
    tickers: List[str] = Field(..., min_items=3)
    start_date: str = Field("2019-01-01")
    benchmark_symbol: str = Field("^GSPC")
    use_cache: bool = Field(False)

    @validator("tickers", pre=True)
    def clean_tickers(cls, value: Any) -> List[str]:
        if value is None:
            raise ValueError("tickers cannot be empty")
        if isinstance(value, str):
            value = [x.strip() for x in value.replace(";", ",").split(",")]
        out: List[str] = []
        for item in value:
            t = str(item).strip().upper()
            if t and t not in out:
                out.append(t)
        if len(out) < 3:
            raise ValueError("Select at least 3 unique tickers.")
        return out

    @validator("start_date")
    def validate_start_date(cls, value: str) -> str:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            raise ValueError("start_date must be a valid date, for example 2019-01-01.")
        if dt >= pd.Timestamp.today().normalize():
            raise ValueError("start_date must be earlier than today.")
        return dt.strftime("%Y-%m-%d")

    @validator("benchmark_symbol", pre=True)
    def clean_benchmark(cls, value: Any) -> str:
        return normalize_benchmark_symbol(value)


class ComputeReportRequest(BaseModel):
    rows: List[Dict[str, Any]] = Field(..., min_items=2)
    data_source: str = Field("yahoo")
    source_interval: str = Field("1d")
    synthetic_data_allowed: bool = Field(False)
    lower_frequency_aggregate_allowed: bool = Field(False)
    benchmark_symbol: str = Field("^GSPC")
    initial_capital: float = Field(1_000_000, gt=0)
    risk_free_rate: float = Field(DEFAULT_RF, ge=-0.05, le=0.30)
    rolling_window: int = Field(63, ge=20, le=504)
    cov_method: str = Field("ledoit_wolf")
    expected_return_method: str = Field("historical_mean")
    best_strategy_rule: str = Field("highest_sharpe")
    stress_family: str = Field("All")
    min_severity: float = Field(1.0, ge=0.0, le=5.0)

    class Config:
        extra = "allow"

    @validator("benchmark_symbol", pre=True)
    def clean_benchmark(cls, value: Any) -> str:
        return normalize_benchmark_symbol(value)

    @validator("data_source")
    def validate_data_source(cls, value: str) -> str:
        v = str(value).strip().lower()
        if REQUIRE_YAHOO_DAILY_ONLY and v != "yahoo":
            raise ValueError("Yahoo Finance daily-only mode is locked. Uploaded/synthetic/fallback data is not allowed.")
        return v

    @validator("source_interval")
    def validate_source_interval(cls, value: str) -> str:
        v = str(value).strip().lower()
        if REQUIRE_YAHOO_DAILY_ONLY and v != "1d":
            raise ValueError("Only Yahoo Finance interval='1d' is allowed. Weekly/monthly/lower-frequency inputs are rejected.")
        return v

    @validator("synthetic_data_allowed")
    def validate_no_synthetic(cls, value: bool) -> bool:
        if REQUIRE_YAHOO_DAILY_ONLY and bool(value):
            raise ValueError("Synthetic data is forbidden. The platform must fail instead of fabricating prices or returns.")
        return False

    @validator("lower_frequency_aggregate_allowed")
    def validate_no_lower_frequency(cls, value: bool) -> bool:
        if REQUIRE_YAHOO_DAILY_ONLY and bool(value):
            raise ValueError("Lower-frequency aggregation is forbidden. Every chart must use daily returns.")
        return False

    @validator("cov_method")
    def validate_cov_method(cls, value: str) -> str:
        allowed = {"ledoit_wolf", "shrinkage", "sample"}
        v = str(value).strip().lower()
        if v not in allowed:
            raise ValueError(f"cov_method must be one of {sorted(allowed)}")
        return v

    @validator("expected_return_method")
    def validate_expected_return_method(cls, value: str) -> str:
        allowed = {"historical_mean", "ema_historical", "capm"}
        v = str(value).strip().lower()
        if v not in allowed:
            raise ValueError(f"expected_return_method must be one of {sorted(allowed)}")
        return v

    @validator("best_strategy_rule")
    def validate_best_rule(cls, value: str) -> str:
        allowed = {"highest_sharpe", "lowest_tracking_error", "highest_information_ratio", "minimum_volatility"}
        v = str(value).strip().lower()
        if v not in allowed:
            raise ValueError(f"best_strategy_rule must be one of {sorted(allowed)}")
        return v


def json_safe(obj: Any) -> Any:
    """Return a fully JSON-native Python object. No pd.Timestamp can survive this function."""
    if obj is None:
        return None
    if isinstance(obj, BaseModel):
        if hasattr(obj, "model_dump"):
            return json_safe(obj.model_dump())
        return json_safe(obj.dict())
    try:
        missing = pd.isna(obj)
        if isinstance(missing, (bool, np.bool_)) and bool(missing):
            return None
    except Exception:
        pass
    if isinstance(obj, (pd.Timestamp, np.datetime64, _dt.datetime, _dt.date)):
        try:
            if pd.isna(obj):
                return None
        except Exception:
            pass
        try:
            return pd.Timestamp(obj).isoformat()
        except Exception:
            return str(obj)
    if isinstance(obj, (pd.Timedelta, np.timedelta64)):
        return None if str(obj) in {"NaT", "nan", "NaN"} else str(obj)
    if isinstance(obj, pd.DataFrame):
        clean = obj.copy().replace([np.inf, -np.inf], np.nan)
        return [json_safe(row) for row in clean.to_dict(orient="records")]
    if isinstance(obj, pd.Series):
        return {"index": [json_safe(x) for x in obj.index.tolist()], "values": [json_safe(x) for x in obj.tolist()]}
    if isinstance(obj, pd.Index):
        return [json_safe(x) for x in obj.tolist()]
    if isinstance(obj, np.ndarray):
        return [json_safe(x) for x in obj.tolist()]
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            sk = json_safe(k)
            if sk is None:
                sk = "null"
            if not isinstance(sk, (str, int, float, bool)):
                sk = str(sk)
            out[str(sk)] = json_safe(v)
        return out
    if isinstance(obj, (list, tuple, set)):
        return [json_safe(x) for x in obj]
    if isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating, float)):
        x = float(obj)
        return None if (math.isnan(x) or math.isinf(x)) else x
    if isinstance(obj, _decimal.Decimal):
        try:
            x = float(obj)
            return None if (math.isnan(x) or math.isinf(x)) else x
        except Exception:
            return str(obj)
    if isinstance(obj, (str, int)):
        return obj
    if hasattr(obj, "item"):
        try:
            return json_safe(obj.item())
        except Exception:
            pass
    try:
        json.dumps(obj, ensure_ascii=False, allow_nan=False)
        return obj
    except Exception:
        return str(obj)

def _native_json_payload(payload: Any, label: str = "payload") -> Any:
    safe = json_safe(payload)
    try:
        text = json.dumps(safe, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    except Exception as exc:
        raise TypeError(f"{label} failed strict JSON conversion: {exc}") from exc
    return json.loads(text)

def assert_json_serializable(payload: Any, label: str = "payload") -> Any:
    return _native_json_payload(payload, label)

def qfa_json_content(payload: Any, label: str = "payload") -> Any:
    return _native_json_payload(payload, label)

class QFAJSONResponse(Response):
    media_type = "application/json"
    def __init__(self, content: Any, status_code: int = 200, **kwargs: Any) -> None:
        native = _native_json_payload(content, "QFAJSONResponse")
        body = json.dumps(native, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        super().__init__(content=body, status_code=status_code, media_type=self.media_type, **kwargs)

def qfa_response(payload: Any, status_code: int = 200, label: str = "payload") -> Response:
    native = _native_json_payload(payload, label)
    body = json.dumps(native, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    return Response(content=body, media_type="application/json", status_code=status_code)

def api_error(endpoint: str, exc: Exception, status_code: int = 400, hint: Optional[str] = None) -> Response:
    payload = {"status": "error", "endpoint": str(endpoint), "error": str(exc), "message": str(exc), "detail": exc.__class__.__name__, "hint": hint}
    return qfa_response(payload, status_code=status_code, label=f"error:{endpoint}")

def api_ok(endpoint: str, payload: Dict[str, Any], saved_to: Optional[str] = None, note: Optional[str] = None) -> Response:
    """Pydantic-free success response boundary.

    Colab can run mixed FastAPI/Pydantic versions. Returning success envelopes
    through BaseModel may raise Pydantic v2 "class-not-fully-defined" errors
    even when the app is healthy. This function intentionally builds a plain
    dict and sends it through the existing strict JSON serializer.
    """
    envelope: Dict[str, Any] = {
        "status": "ok",
        "endpoint": str(endpoint),
        "saved_to": saved_to,
        "data_quality_note": note,
    }
    envelope.update(payload or {})
    return qfa_response(envelope, status_code=200, label=f"ok:{endpoint}")

app = FastAPI(title="QFA Prime Finance Platform - Institutional Colab", default_response_class=QFAJSONResponse)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.exception_handler(Exception)
async def qfa_unhandled_exception_handler(request, exc: Exception):
    return qfa_response({"status": "error", "endpoint": str(getattr(request, "url", "")), "error": str(exc), "message": str(exc), "detail": exc.__class__.__name__, "hint": "Unhandled server error serialized by QFA hard JSON boundary."}, status_code=500, label="unhandled_exception")

try:
    from fastapi.exceptions import RequestValidationError
    @app.exception_handler(RequestValidationError)
    async def qfa_validation_exception_handler(request, exc: Exception):
        return qfa_response({"status": "error", "endpoint": str(getattr(request, "url", "")), "error": "Request validation failed", "message": str(exc), "detail": "RequestValidationError", "hint": "Check request fields, types, and required arrays."}, status_code=422, label="validation_exception")
except Exception:
    pass

def parse_uploaded_file_bytes(filename: str, content: bytes) -> pd.DataFrame:
    suffix = Path(filename).suffix.lower()
    if suffix == ".csv":
        try:
            return pd.read_csv(io.BytesIO(content))
        except UnicodeDecodeError:
            return pd.read_csv(io.BytesIO(content), encoding="latin-1")
    if suffix == ".txt":
        try:
            return pd.read_csv(io.BytesIO(content), sep=None, engine="python")
        except Exception:
            return pd.read_csv(io.BytesIO(content), sep="\t")
    if suffix in [".xlsx", ".xls"]:
        return pd.read_excel(io.BytesIO(content))
    raise ValueError(f"Unsupported file type: {suffix}")


def ensure_wide_price_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    cols_lower = {c.lower(): c for c in df.columns}
    if "date" not in cols_lower:
        first = df.columns[0]
        parsed = pd.to_datetime(df[first], errors="coerce", dayfirst=True)
        if parsed.notna().mean() > 0.75:
            df = df.rename(columns={first: "Date"})
            cols_lower = {c.lower(): c for c in df.columns}
        else:
            raise ValueError("Price file must contain a Date column or a date-like first column.")
    date_col = cols_lower["date"]
    if "ticker" in cols_lower and ("close" in cols_lower or "price" in cols_lower or "adj close" in cols_lower):
        t_col = cols_lower["ticker"]
        p_col = cols_lower.get("adj close", cols_lower.get("close", cols_lower.get("price")))
        tmp = df[[date_col, t_col, p_col]].copy()
        tmp.columns = ["Date", "Ticker", "Close"]
        tmp["Date"] = pd.to_datetime(tmp["Date"], errors="coerce", dayfirst=True)
        tmp["Close"] = pd.to_numeric(tmp["Close"], errors="coerce")
        out = tmp.dropna().pivot_table(index="Date", columns="Ticker", values="Close", aggfunc="last").sort_index()
        return out.reset_index()
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce", dayfirst=True)
    out = out.dropna(subset=[date_col]).sort_values(date_col).rename(columns={date_col: "Date"})
    for c in out.columns:
        if c != "Date":
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _cache_key(tickers: List[str], start_date: str) -> Path:
    safe = "_".join([t.replace("^", "IDX").replace(".", "_") for t in sorted(tickers)])[:180]
    return CACHE_DIR / f"yahoo_1d_fxaware_daily_returns_matrix_v7_{safe}_{start_date}.csv"



def _is_turkish_bist_ticker(ticker: Any) -> bool:
    return str(ticker).upper().endswith(".IS")


def _extract_yahoo_close_series(data: pd.DataFrame, ticker: str, batch: List[str]) -> Optional[pd.Series]:
    """Extract the adjusted/close-like Yahoo series for one ticker from yf.download output."""
    if data is None or data.empty:
        return None
    try:
        if isinstance(data.columns, pd.MultiIndex):
            if ticker not in data.columns.get_level_values(0):
                return None
            sub = data[ticker]
            col = "Close" if "Close" in sub.columns else ("Adj Close" if "Adj Close" in sub.columns else None)
            if col is None:
                return None
            return pd.to_numeric(sub[col], errors="coerce").rename(ticker)
        col = "Close" if "Close" in data.columns else ("Adj Close" if "Adj Close" in data.columns else None)
        if col and len(batch) == 1:
            return pd.to_numeric(data[col], errors="coerce").rename(ticker)
    except Exception:
        return None
    return None


def _apply_bist_usd_conversion(prices: pd.DataFrame, requested: List[str]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Convert BIST TRY prices and XU100 TRY benchmark to USD using daily USDTRY=X."""
    prices = prices.copy().sort_index()
    requested_bist = [c for c in requested if _is_turkish_bist_ticker(c)]
    audit: Dict[str, Any] = {
        "fx_engine_enabled": bool(requested_bist),
        "fx_symbol": USDTRY_SYMBOL,
        "turkish_assets_detected": requested_bist,
        "turkish_assets_converted_to_usd": [],
        "xU100_benchmark_converted_to_usd": False,
        "conversion_formula": "USD price = TRY close / USDTRY=X close",
        "synthetic_fx_used": False,
        "benchmark_proxy_used": False,
    }
    if not requested_bist:
        return prices, audit
    if USDTRY_SYMBOL not in prices.columns:
        raise ValueError("Turkish BIST assets require Yahoo USDTRY=X daily historical FX series. USDTRY=X was not downloaded; synthetic FX fallback is forbidden.")
    if XU100_TRY_SYMBOL not in prices.columns:
        raise ValueError("Turkish BIST assets require Yahoo XU100.IS daily benchmark series. XU100.IS was not downloaded; benchmark proxy fallback is forbidden.")
    fx = pd.to_numeric(prices[USDTRY_SYMBOL], errors="coerce").replace([np.inf, -np.inf], np.nan).ffill(limit=3)
    if fx.dropna().empty:
        raise ValueError("USDTRY=X daily FX series is empty after cleaning. Turkish USD conversion cannot proceed.")
    if float(fx.dropna().median()) <= 0:
        raise ValueError("USDTRY=X daily FX series has non-positive median level. Turkish USD conversion rejected.")
    for col in requested_bist:
        if col in prices.columns:
            prices[col] = pd.to_numeric(prices[col], errors="coerce").div(fx)
            audit["turkish_assets_converted_to_usd"].append(col)
    prices[XU100_USD_BENCHMARK_SYMBOL] = pd.to_numeric(prices[XU100_TRY_SYMBOL], errors="coerce").div(fx)
    audit["xU100_benchmark_converted_to_usd"] = True
    audit["fx_first_date"] = str(fx.dropna().index.min().date()) if len(fx.dropna()) else ""
    audit["fx_last_date"] = str(fx.dropna().index.max().date()) if len(fx.dropna()) else ""
    audit["fx_observations"] = int(fx.notna().sum())
    return prices, audit

def load_yahoo_prices(tickers: List[str], start_date: str, benchmark_symbol: str = BENCHMARK_SYMBOL, use_cache: bool = False) -> pd.DataFrame:
    requested = list(dict.fromkeys([str(t).strip().upper() for t in tickers if str(t).strip()]))
    bench = normalize_benchmark_symbol(benchmark_symbol)
    has_bist = any(_is_turkish_bist_ticker(t) for t in requested)
    required_market_series = [bench]
    if has_bist:
        required_market_series.extend([USDTRY_SYMBOL, XU100_TRY_SYMBOL])
    all_tickers = list(dict.fromkeys(requested + required_market_series))
    if len(requested) < 3:
        raise ValueError("No tickers selected or fewer than 3 tickers selected.")
    cache_path = _cache_key(all_tickers, start_date)
    if use_cache and cache_path.exists() and (time.time() - cache_path.stat().st_mtime) < 12 * 3600:
        cached = pd.read_csv(cache_path)
        if "Date" in cached.columns and cached.shape[1] >= 4:
            try:
                _cached_idx = pd.to_datetime(cached["Date"], errors="coerce").dropna().sort_values()
                _gaps = _cached_idx.diff().dt.days.dropna()
                _median_gap = float(_gaps.median()) if len(_gaps) else 1.0
                if _median_gap <= DAILY_MEDIAN_GAP_LIMIT_DAYS:
                    return cached
            except Exception:
                pass
    frames: List[pd.Series] = []
    errors: List[str] = []
    for batch_start in range(0, len(all_tickers), 12):
        batch = all_tickers[batch_start:batch_start + 12]
        data = pd.DataFrame()
        for attempt in range(3):
            try:
                data = yf.download(batch, start=start_date, interval="1d", auto_adjust=True, actions=False, progress=False, group_by="ticker", threads=False, timeout=25)
                if not data.empty:
                    break
            except Exception as exc:
                errors.append(f"{batch}: {exc}")
                time.sleep(1.5 * (attempt + 1))
        if data.empty:
            continue
        for t in batch:
            s = _extract_yahoo_close_series(data, t, batch)
            if s is not None:
                frames.append(s)
    if not frames:
        raise ValueError("No usable Yahoo Finance daily price series returned. Synthetic/upload fallback is disabled; reduce the universe or retry Yahoo later.")
    prices = pd.concat(frames, axis=1).sort_index()
    prices = prices.loc[:, ~prices.columns.duplicated()]
    if has_bist:
        prices, _fx_audit = _apply_bist_usd_conversion(prices, requested)
    usable_assets = [c for c in prices.columns if c in requested and c in prices.columns]
    if len(usable_assets) < 3:
        raise ValueError(f"Too few usable assets after Yahoo daily cleanup: {usable_assets}. Synthetic/upload fallback is disabled; try a smaller universe or retry Yahoo later.")
    keep = list(usable_assets)
    if has_bist:
        if XU100_USD_BENCHMARK_SYMBOL not in prices.columns:
            raise ValueError("BIST assets are selected but XU100 USD benchmark was not created. Benchmark proxy/fallback is disabled.")
        keep.append(XU100_USD_BENCHMARK_SYMBOL)
        if bench in prices.columns and bench not in keep:
            keep.append(bench)
    else:
        if bench in prices.columns and bench not in keep:
            keep.append(bench)
    prices = prices[list(dict.fromkeys(keep))]
    out = prices.reset_index().rename(columns={prices.index.name or "index": "Date"})
    out.to_csv(cache_path, index=False)
    return out

def enforce_daily_common_sample(df: pd.DataFrame, ffill_limit: int = 10) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Force Yahoo 1D data into one common daily business-day matrix; daily-only calculation."""
    if df.empty:
        raise ValueError("Cannot enforce daily sample on an empty price matrix.")
    df = df.copy().sort_index()
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.loc[~df.index.isna()]
    df = df.loc[~df.index.duplicated(keep="last")]
    raw_index = df.index.copy()
    raw_obs_by_asset = df.notna().sum()
    start_dt, end_dt = df.index.min(), df.index.max()
    bday_index = pd.bdate_range(start_dt, end_dt, freq="B")
    df = df.reindex(bday_index)
    missing_before_ffill = df.isna().mean()
    df = df.ffill(limit=ffill_limit)
    min_actual_obs = max(60, int(len(bday_index) * 0.35))
    min_after_ffill_obs = max(126, int(len(bday_index) * 0.70)) if len(bday_index) > 260 else max(40, int(len(bday_index) * 0.60))
    keep_cols, dropped = [], {}
    for col in df.columns:
        actual = int(raw_obs_by_asset.get(col, 0))
        after = int(df[col].notna().sum())
        if actual < min_actual_obs:
            dropped[str(col)] = f"Dropped: too few actual Yahoo daily observations ({actual} < {min_actual_obs})"
            continue
        if after < min_after_ffill_obs:
            dropped[str(col)] = f"Dropped: insufficient daily coverage after capped ffill ({after} < {min_after_ffill_obs})"
            continue
        keep_cols.append(col)
    df = df[keep_cols].dropna(how="any")
    if len(df) < 60:
        raise ValueError("Insufficient aligned daily observations after capped ffill/common-sample cleaning.")
    gaps = pd.Series(raw_index.sort_values()).diff().dt.days.dropna()
    audit = {
        "source_interval": "Yahoo Finance interval=1d",
        "analytics_frequency": "Daily business-day common sample",
        "lower-frequency_aggregate_used": False,
        "ffill_limit_business_days": int(ffill_limit),
        "raw_first_date": str(raw_index.min().date()) if len(raw_index) else "",
        "raw_last_date": str(raw_index.max().date()) if len(raw_index) else "",
        "common_first_date": str(df.index.min().date()),
        "common_last_date": str(df.index.max().date()),
        "common_daily_observations": int(len(df)),
        "median_raw_gap_days": float(gaps.median()) if len(gaps) else 1.0,
        "max_raw_gap_days": int(gaps.max()) if len(gaps) else 1,
        "assets_after_daily_alignment": int(df.shape[1]),
        "dropped_assets_daily_coverage": dropped,
        "average_missing_before_ffill": float(missing_before_ffill.mean()) if len(missing_before_ffill) else 0.0,
    }
    return df, audit


def assert_daily_return_inputs(returns: pd.DataFrame, bench_ret: pd.Series, label: str) -> Dict[str, Any]:
    """Validate that PyPortfolioOpt/Quantstats inputs are daily, not lower-frequency."""
    if returns.empty or bench_ret.empty:
        raise ValueError(f"{label}: empty returns passed to daily-frequency validator.")
    idx = pd.DatetimeIndex(returns.index)
    gaps = pd.Series(idx).diff().dt.days.dropna()
    median_gap = float(gaps.median()) if len(gaps) else 1.0
    if median_gap > DAILY_MEDIAN_GAP_LIMIT_DAYS:
        raise ValueError(f"{label}: data looks lower-frequency, not daily. Median gap is {median_gap:.2f} days.")
    common = returns.index.intersection(bench_ret.index)
    if len(common) < 60:
        raise ValueError(f"{label}: too few common daily return observations ({len(common)}).")
    return {
        "label": label,
        "validated_daily": True,
        "return_observations": int(len(common)),
        "median_gap_days": median_gap,
        "first_return_date": str(pd.DatetimeIndex(common).min().date()),
        "last_return_date": str(pd.DatetimeIndex(common).max().date()),
    }

def assert_daily_points(points: List[Dict[str, Any]], label: str) -> Dict[str, Any]:
    """Validate frontend chart point arrays are daily/trading-day resolution, not lower-frequency/downsampled."""
    if not points or len(points) < 60:
        raise ValueError(f"{label}: too few daily chart points ({len(points) if points else 0}).")
    idx = pd.to_datetime([p.get("Date") for p in points], errors="coerce")
    idx = pd.DatetimeIndex(idx).dropna().sort_values()
    gaps = pd.Series(idx).diff().dt.days.dropna()
    median_gap = float(gaps.median()) if len(gaps) else 1.0
    p95_gap = float(gaps.quantile(0.95)) if len(gaps) else 1.0
    if median_gap > DAILY_MEDIAN_GAP_LIMIT_DAYS:
        raise ValueError(f"{label}: chart points look lower-frequency/downsampled. Median gap={median_gap:.2f} days.")
    return {"Chart": label, "Point Source": "explicit daily point array", "Frequency": "daily/trading-day", "Observations": int(len(idx)), "Median Gap Days": median_gap, "P95 Gap Days": p95_gap}


def clean_price_frame(price_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Institutional price cleaning: Yahoo daily 1D, capped ffill, common daily sample."""
    df = ensure_wide_price_df(price_df)
    df = df.set_index("Date")
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()].sort_index()
    df = df.loc[~df.index.duplicated(keep="last")]
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.replace([np.inf, -np.inf], np.nan).dropna(axis=1, how="all")
    if df.empty:
        raise ValueError("Price matrix is empty after parsing.")
    aligned, audit = enforce_daily_common_sample(df, ffill_limit=10)
    return aligned, audit

def _extract_close_from_download(data: pd.DataFrame, symbol: str) -> Optional[pd.Series]:
    """Extract a Yahoo Close/Adj Close series from single or MultiIndex download output."""
    if data is None or data.empty:
        return None
    sub = data
    try:
        if isinstance(data.columns, pd.MultiIndex):
            level0 = list(data.columns.get_level_values(0))
            level1 = list(data.columns.get_level_values(1)) if data.columns.nlevels > 1 else []
            if symbol in level0:
                sub = data[symbol]
            elif "Close" in level0:
                s = data["Close"]
                if isinstance(s, pd.DataFrame):
                    col = symbol if symbol in s.columns else s.columns[0]
                    return pd.to_numeric(s[col], errors="coerce").rename(symbol)
                return pd.to_numeric(s, errors="coerce").rename(symbol)
            elif "Adj Close" in level0:
                s = data["Adj Close"]
                if isinstance(s, pd.DataFrame):
                    col = symbol if symbol in s.columns else s.columns[0]
                    return pd.to_numeric(s[col], errors="coerce").rename(symbol)
                return pd.to_numeric(s, errors="coerce").rename(symbol)
            elif "Close" in level1:
                try:
                    s = data.xs("Close", axis=1, level=1)
                    col = symbol if symbol in s.columns else s.columns[0]
                    return pd.to_numeric(s[col], errors="coerce").rename(symbol)
                except Exception:
                    pass
        col = "Close" if "Close" in sub.columns else ("Adj Close" if "Adj Close" in sub.columns else None)
        if col is not None:
            s = sub[col]
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
            return pd.to_numeric(s, errors="coerce").rename(symbol)
    except Exception:
        return None
    return None

def _fetch_yahoo_close_series_for_dates(symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    """Fetch one Yahoo daily Close series for an exact date range. No synthetic fallback."""
    start_s = pd.Timestamp(start).strftime("%Y-%m-%d")
    end_s = (pd.Timestamp(end) + pd.Timedelta(days=5)).strftime("%Y-%m-%d")
    candidates = XU100_TRY_ALTERNATE_SYMBOLS if symbol in {"^XU100", "XU100.IS", XU100_TRY_SYMBOL} else [symbol]
    errors = []
    for sym in candidates:
        try:
            data = yf.download(sym, start=start_s, end=end_s, interval="1d", auto_adjust=True, actions=False, progress=False, threads=False, timeout=25)
            s = _extract_close_from_download(data, sym)
            if s is None or s.dropna().empty:
                errors.append(f"{sym}: no Close/Adj Close data")
                continue
            s = s.dropna().rename(symbol)
            if getattr(s.index, "tz", None) is not None:
                s.index = s.index.tz_localize(None)
            s.index = pd.to_datetime(s.index).normalize()
            if s.empty:
                errors.append(f"{sym}: empty after cleaning")
                continue
            return s
        except Exception as exc:
            errors.append(f"{sym}: {exc}")
    raise ValueError(f"Yahoo Finance did not return required daily series {symbol}. Tried {candidates}. Synthetic fallback is forbidden. Details: {'; '.join(errors[-3:])}")


def _ensure_bist_fx_benchmark_in_clean_prices(df: pd.DataFrame, requested_benchmark: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Guarantee XU100 USD benchmark for BIST analytics even if an older payload omitted it.

    This does not fabricate data. It fetches Yahoo XU100.IS and USDTRY=X daily historical
    series, aligns them to the exact clean daily price index, and creates ^XU100_USD.
    """
    out = df.copy()
    audit: Dict[str, Any] = {
        "bist_fx_engine_compute_guard_enabled": False,
        "xU100_usd_benchmark_added_in_compute": False,
        "benchmark_choice_requested": requested_benchmark,
        "synthetic_fx_used": False,
        "benchmark_proxy_used": False,
    }
    has_bist_assets = any(_is_turkish_bist_ticker(c) for c in out.columns)
    if not has_bist_assets:
        return out, audit
    audit["bist_fx_engine_compute_guard_enabled"] = True
    if XU100_USD_BENCHMARK_SYMBOL in out.columns and out[XU100_USD_BENCHMARK_SYMBOL].notna().sum() >= 60:
        return out, audit
    xu100_try = _fetch_yahoo_close_series_for_dates(XU100_TRY_SYMBOL, out.index.min(), out.index.max())
    usdtry = _fetch_yahoo_close_series_for_dates(USDTRY_SYMBOL, out.index.min(), out.index.max())
    fx_df = pd.concat([xu100_try.rename("XU100_TRY"), usdtry.rename("USDTRY")], axis=1).sort_index()
    fx_df = fx_df.reindex(out.index).ffill(limit=3)
    fx_df = fx_df.replace([np.inf, -np.inf], np.nan).dropna()
    if len(fx_df) < 60:
        raise ValueError("Could not build XU100 USD benchmark from Yahoo XU100.IS and USDTRY=X with sufficient daily observations. No proxy/fallback is allowed.")
    xu100_usd = fx_df["XU100_TRY"].div(fx_df["USDTRY"]).rename(XU100_USD_BENCHMARK_SYMBOL)
    out[XU100_USD_BENCHMARK_SYMBOL] = xu100_usd.reindex(out.index).ffill(limit=3)
    out = out.dropna(subset=[XU100_USD_BENCHMARK_SYMBOL])
    audit.update({
        "xU100_usd_benchmark_added_in_compute": True,
        "xU100_usd_observations": int(out[XU100_USD_BENCHMARK_SYMBOL].notna().sum()),
        "xU100_usd_first_date": str(out[XU100_USD_BENCHMARK_SYMBOL].dropna().index.min().date()),
        "xU100_usd_last_date": str(out[XU100_USD_BENCHMARK_SYMBOL].dropna().index.max().date()),
        "conversion_formula": "XU100_USD = XU100.IS Close / USDTRY=X Close",
    })
    return out, audit


def annualized_return(r: pd.Series) -> float:
    r = r.dropna()
    if len(r) == 0:
        return 0.0
    return float((1 + r).prod() ** (TRADING_DAYS / len(r)) - 1)


def max_drawdown_from_returns(r: pd.Series) -> float:
    eq = (1 + r.fillna(0)).cumprod()
    return float((eq / eq.cummax() - 1).min()) if len(eq) else 0.0


def downside_deviation(r: pd.Series, mar: float = 0.0) -> float:
    down = r[r < mar]
    return float(down.std() * np.sqrt(TRADING_DAYS)) if len(down) > 1 else 0.0


def var_cvar(x: pd.Series, level: float = 0.95) -> Tuple[float, float, float]:
    """Return positive loss figures for VaR/CVaR/ES.

    The old implementation returned negative tail returns. That made the
    VaR/CVaR bar charts look inverted and inconsistent with institutional
    reporting. This follows the reference AnalyticsEngine logic: VaR and
    CVaR are shown as positive loss magnitudes, while drawdown remains
    negative because it is a path-return series.
    """
    x = x.dropna().astype(float)
    if len(x) < 10:
        return 0.0, 0.0, 0.0
    q = float(np.quantile(x, 1 - level))
    tail = x[x <= q]
    var = max(-q, 0.0)
    cvar = max(-float(tail.mean()), 0.0) if len(tail) else var
    es = cvar
    return var, cvar, es




def compute_advanced_var_tables(portfolio_returns: pd.Series, benchmark_returns: pd.Series, initial_capital: float, seed: int = 42) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Institutional VaR engine using one daily-return kernel.

    Absolute VaR table:
      - Historical VaR: empirical left-tail loss.
      - Parametric VaR: Gaussian daily return model using observed daily mean/std.
      - Monte Carlo VaR: simulated daily returns using observed daily mean/std.
    Relative VaR table:
      - Uses active daily return = portfolio daily return - benchmark daily return.
    All figures are positive loss magnitudes in return units and USD units.
    """
    aligned = pd.concat([portfolio_returns.rename("portfolio"), benchmark_returns.rename("benchmark")], axis=1).dropna()
    if aligned.empty or len(aligned) < 30:
        empty_abs = pd.DataFrame(columns=["Confidence", "Historical VaR", "Parametric VaR", "Monte Carlo VaR", "Historical VaR USD", "Parametric VaR USD", "Monte Carlo VaR USD", "Daily Observations"])
        empty_rel = pd.DataFrame(columns=["Confidence", "Historical Relative VaR", "Parametric Relative VaR", "Monte Carlo Relative VaR", "Historical Relative VaR USD", "Parametric Relative VaR USD", "Monte Carlo Relative VaR USD", "Daily Observations"])
        return empty_abs, empty_rel

    active = (aligned["portfolio"] - aligned["benchmark"]).dropna()
    levels = [0.90, 0.95, 0.99]

    def z_score_for_left_tail(level: float) -> float:
        local_rng = np.random.default_rng(seed + int(level * 10_000))
        return abs(float(np.quantile(local_rng.standard_normal(250_000), 1.0 - level)))

    def method_values(series: pd.Series, level: float, local_seed_offset: int = 0) -> Tuple[float, float, float]:
        x = series.dropna().astype(float)
        if len(x) < 30 or float(x.std(ddof=1)) <= 0:
            return 0.0, 0.0, 0.0
        hist = max(-float(np.quantile(x, 1.0 - level)), 0.0)
        z = z_score_for_left_tail(level)
        param = max(-(float(x.mean()) - z * float(x.std(ddof=1))), 0.0)
        local_rng = np.random.default_rng(seed + int(level * 10_000) + local_seed_offset)
        sims = local_rng.normal(float(x.mean()), float(x.std(ddof=1)), 250_000)
        mc = max(-float(np.quantile(sims, 1.0 - level)), 0.0)
        return hist, param, mc

    abs_rows = []
    rel_rows = []
    for level in levels:
        h, p, m = method_values(aligned["portfolio"], level, 0)
        rh, rp, rm = method_values(active, level, 500)
        abs_rows.append({
            "Confidence": f"{int(level * 100)}%",
            "Historical VaR": h,
            "Parametric VaR": p,
            "Monte Carlo VaR": m,
            "Historical VaR USD": h * initial_capital,
            "Parametric VaR USD": p * initial_capital,
            "Monte Carlo VaR USD": m * initial_capital,
            "Daily Observations": int(len(aligned)),
        })
        rel_rows.append({
            "Confidence": f"{int(level * 100)}%",
            "Historical Relative VaR": rh,
            "Parametric Relative VaR": rp,
            "Monte Carlo Relative VaR": rm,
            "Historical Relative VaR USD": rh * initial_capital,
            "Parametric Relative VaR USD": rp * initial_capital,
            "Monte Carlo Relative VaR USD": rm * initial_capital,
            "Daily Observations": int(len(active)),
        })
    return pd.DataFrame(abs_rows), pd.DataFrame(rel_rows)


def portfolio_strategy_explanations_table() -> pd.DataFrame:
    """User-facing explanation of every available portfolio strategy."""
    rows = [
        {"Strategy": "Equal Weight", "Purpose": "Neutral baseline allocation", "Detailed Explanation": "Allocates the same capital weight to every selected asset. It is simple and transparent, but it does not explicitly account for volatility, correlation, drawdown, or benchmark-relative risk.", "Best Used When": "A neutral benchmark portfolio is needed for comparison.", "Main Risk": "High-volatility assets can dominate total portfolio risk even with equal capital weights."},
        {"Strategy": "Inverse Volatility", "Purpose": "Lower-risk asset tilt", "Detailed Explanation": "Allocates more capital to assets with lower historical daily volatility and less capital to assets with higher volatility. This improves risk balance but does not fully account for cross-asset correlations.", "Best Used When": "The objective is defensive allocation with simple volatility control.", "Main Risk": "It can over-allocate to low-volatility assets that are highly correlated with each other."},
        {"Strategy": "Minimum Variance", "Purpose": "Minimize total volatility", "Detailed Explanation": "Uses the covariance matrix of daily returns to construct the portfolio with the lowest estimated annualized volatility subject to long-only and maximum weight constraints.", "Best Used When": "Capital preservation and lower volatility are the primary goals.", "Main Risk": "May underweight high-return assets and can be sensitive to covariance estimation errors."},
        {"Strategy": "Max Sharpe Approx", "Purpose": "Risk-adjusted return optimization", "Detailed Explanation": "Ranks assets using expected return relative to volatility, then normalizes weights under institutional caps. It approximates a maximum Sharpe allocation while remaining robust in cloud/runtime environments.", "Best Used When": "The mandate emphasizes return per unit of risk.", "Main Risk": "Expected return estimates are noisy and can change quickly."},
        {"Strategy": "Equal Risk Contribution (ERC)", "Purpose": "Risk budgeting", "Detailed Explanation": "Solves for weights where each asset contributes approximately equally to total portfolio volatility. This is more institutional than equal weight because it allocates by risk rather than capital.", "Best Used When": "Risk contribution discipline and diversification are important.", "Main Risk": "May still concentrate economically if assets share hidden factor exposures."},
        {"Strategy": "Maximum Diversification", "Purpose": "Diversification ratio maximization", "Detailed Explanation": "Maximizes the ratio between weighted individual asset volatility and total portfolio volatility. It seeks assets that diversify each other rather than simply minimizing risk.", "Best Used When": "The goal is to improve cross-asset diversification efficiency.", "Main Risk": "Can favor assets with attractive correlation properties even when return expectations are weak."},
        {"Strategy": "HRP", "Purpose": "Hierarchical risk allocation", "Detailed Explanation": "Uses correlation clustering to organize assets into risk groups and then allocates across the hierarchy. HRP is designed to be more stable than classical mean-variance optimization when covariance estimates are noisy.", "Best Used When": "The asset universe is broad and correlation structure matters.", "Main Risk": "Cluster structure can shift across regimes."},
        {"Strategy": "Black-Litterman", "Purpose": "Blend equilibrium and views", "Detailed Explanation": "Combines market-implied equilibrium returns with investor views and then optimizes the posterior expected returns and covariance matrix. It is a professional framework for view-based allocation.", "Best Used When": "The portfolio manager has explicit views or scenario assumptions.", "Main Risk": "Results depend strongly on view quality and confidence calibration."},
        {"Strategy": "Tracking Error Optimal", "Purpose": "Benchmark-aware active portfolio", "Detailed Explanation": "Optimizes expected active return while penalizing tracking error against the benchmark. It is designed for mandates where outperforming the benchmark matters but active risk must be controlled.", "Best Used When": "The portfolio is judged against S&P 500 or another formal benchmark.", "Main Risk": "A tight tracking error target can constrain return potential."},
    ]
    return pd.DataFrame(rows)


def covariance_matrix(returns: pd.DataFrame, method: str = "ledoit_wolf") -> pd.DataFrame:
    if method == "sample":
        return returns.cov() * TRADING_DAYS
    if method == "shrinkage":
        sample = returns.cov() * TRADING_DAYS
        diag = pd.DataFrame(np.diag(np.diag(sample)), index=sample.index, columns=sample.columns)
        return sample * 0.85 + diag * 0.15
    try:
        lw = LedoitWolf().fit(returns.dropna().values)
        return pd.DataFrame(lw.covariance_ * TRADING_DAYS, index=returns.columns, columns=returns.columns)
    except Exception:
        return returns.cov() * TRADING_DAYS


def normalize_weights(w: pd.Series, cap: float = MAX_SINGLE_WEIGHT) -> pd.Series:
    w = w.clip(lower=0).replace([np.inf, -np.inf], np.nan).fillna(0)
    if w.sum() <= 0:
        w[:] = 1.0 / len(w)
    w = w / w.sum()
    for _ in range(20):
        over = w > cap
        if not over.any():
            break
        excess = (w[over] - cap).sum()
        w[over] = cap
        under = ~over
        if under.any() and w[under].sum() > 0:
            w[under] += excess * w[under] / w[under].sum()
    cash = [x for x in w.index if x in CASH_LIKE]
    if cash and w.loc[cash].sum() > 0.12:
        scale = 0.12 / w.loc[cash].sum()
        w.loc[cash] *= scale
        non_cash = [x for x in w.index if x not in CASH_LIKE]
        if non_cash:
            w.loc[non_cash] += (1 - w.sum()) * w.loc[non_cash] / w.loc[non_cash].sum()
    return w / w.sum()


def equal_weight_strategy(assets: List[str]) -> pd.Series:
    return pd.Series(1.0 / len(assets), index=assets)


def inverse_volatility_strategy(returns: pd.DataFrame) -> pd.Series:
    vol = returns.std() * np.sqrt(TRADING_DAYS)
    inv = 1.0 / vol.replace(0, np.nan)
    return normalize_weights(inv.fillna(0), cap=MAX_SINGLE_WEIGHT)


def min_variance_strategy(returns: pd.DataFrame, cov_method: str) -> pd.Series:
    cov = covariance_matrix(returns, cov_method)
    assets = returns.columns
    n = len(assets)
    try:
        inv_cov = np.linalg.pinv(cov.values)
        ones = np.ones(n)
        w = inv_cov @ ones / (ones @ inv_cov @ ones)
        return normalize_weights(pd.Series(w, index=assets), cap=MAX_SINGLE_WEIGHT)
    except Exception:
        return inverse_volatility_strategy(returns)


def max_sharpe_approx_strategy(returns: pd.DataFrame, rf: float, cov_method: str) -> pd.Series:
    mu = returns.mean() * TRADING_DAYS
    vol = returns.std() * np.sqrt(TRADING_DAYS)
    score = ((mu - rf) / vol.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
    score = score.clip(lower=0.0001).fillna(0)
    return normalize_weights(score, cap=MAX_SINGLE_WEIGHT)


def erc_strategy(returns: pd.DataFrame, cov_method: str, current_weights: Optional[pd.Series] = None) -> pd.Series:
    if minimize is None:
        raise ImportError("scipy.optimize.minimize is not available")
    cov = covariance_matrix(returns, cov_method).values
    assets = returns.columns
    n = len(assets)
    w0 = np.ones(n) / n
    bounds = [(0.0, MAX_SINGLE_WEIGHT)] * n
    cons = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]

    def obj(w: np.ndarray) -> float:
        port_var = float(w @ cov @ w)
        if port_var <= 0:
            return 1e6
        marginal = cov @ w
        rc = w * marginal / np.sqrt(port_var)
        target = float(np.mean(rc))
        turnover = 0.0
        if current_weights is not None:
            turnover = float(np.sum(np.abs(w - current_weights.reindex(assets).fillna(0).values)))
        return float(np.sum((rc - target) ** 2) + 0.005 * turnover)

    res = minimize(obj, w0, bounds=bounds, constraints=cons, method="SLSQP", options={"maxiter": 500})
    if not res.success:
        raise RuntimeError(f"ERC optimization failed: {res.message}")
    return normalize_weights(pd.Series(res.x, index=assets), cap=MAX_SINGLE_WEIGHT)


def max_diversification_strategy(returns: pd.DataFrame, cov_method: str, current_weights: Optional[pd.Series] = None) -> pd.Series:
    if minimize is None:
        raise ImportError("scipy.optimize.minimize is not available")
    cov = covariance_matrix(returns, cov_method).values
    assets = returns.columns
    n = len(assets)
    vols = np.sqrt(np.maximum(np.diag(cov), 1e-12))
    w0 = np.ones(n) / n
    bounds = [(0.0, MAX_SINGLE_WEIGHT)] * n
    cons = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]

    def obj(w: np.ndarray) -> float:
        port_vol = float(np.sqrt(max(w @ cov @ w, 0.0)))
        if port_vol <= 0:
            return 1e6
        turnover = 0.0
        if current_weights is not None:
            turnover = float(np.sum(np.abs(w - current_weights.reindex(assets).fillna(0).values)))
        return float(-((w @ vols) / port_vol) + 0.005 * turnover)

    res = minimize(obj, w0, bounds=bounds, constraints=cons, method="SLSQP", options={"maxiter": 500})
    if not res.success:
        raise RuntimeError(f"Maximum diversification failed: {res.message}")
    return normalize_weights(pd.Series(res.x, index=assets), cap=MAX_SINGLE_WEIGHT)


def hrp_strategy(returns: pd.DataFrame, cov: pd.DataFrame) -> pd.Series:
    if hierarchy is None or squareform is None:
        raise ImportError("scipy.cluster/squareform is not available")
    corr = returns.corr().clip(-1, 1).fillna(0)
    dist = np.sqrt((1 - corr) / 2).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    condensed = squareform(dist.values, checks=False)
    link = hierarchy.linkage(condensed, method="ward")

    def get_quasi_diag(linkage_matrix: np.ndarray) -> List[int]:
        linkage_matrix = linkage_matrix.astype(int)
        sort_ix = pd.Series([linkage_matrix[-1, 0], linkage_matrix[-1, 1]])
        num_items = int(linkage_matrix[-1, 3])
        while sort_ix.max() >= num_items:
            sort_ix.index = range(0, sort_ix.shape[0] * 2, 2)
            df0 = sort_ix[sort_ix >= num_items]
            i = df0.index
            j = df0.values - num_items
            sort_ix.loc[i] = linkage_matrix[j, 0]
            df1 = pd.Series(linkage_matrix[j, 1], index=i + 1)
            sort_ix = pd.concat([sort_ix, df1]).sort_index()
            sort_ix.index = range(sort_ix.shape[0])
        return [int(x) for x in sort_ix.tolist()]

    ordered_assets = corr.index[get_quasi_diag(link)].tolist()
    weights = pd.Series(1.0, index=ordered_assets)

    def cluster_variance(items: List[str]) -> float:
        cov_slice = cov.loc[items, items]
        inv_diag = 1 / np.maximum(np.diag(cov_slice.values), 1e-12)
        w_par = inv_diag / inv_diag.sum()
        return float(w_par @ cov_slice.values @ w_par)

    def bisect(items: List[str], w: pd.Series) -> pd.Series:
        if len(items) <= 1:
            return w
        mid = len(items) // 2
        left, right = items[:mid], items[mid:]
        v_left, v_right = cluster_variance(left), cluster_variance(right)
        alpha = 1 - v_left / (v_left + v_right) if (v_left + v_right) > 0 else 0.5
        w.loc[left] *= alpha
        w.loc[right] *= 1 - alpha
        return bisect(right, bisect(left, w))

    weights = bisect(ordered_assets, weights)
    weights = weights.reindex(returns.columns).fillna(0)
    return normalize_weights(weights / weights.sum(), cap=MAX_SINGLE_WEIGHT)


def black_litterman_strategy(returns: pd.DataFrame, benchmark_returns: pd.Series, mu_historical: pd.Series, cov: pd.DataFrame, rf: float) -> pd.Series:
    if EfficientFrontier is None or BlackLittermanModel is None or market_implied_risk_aversion is None:
        raise ImportError("PyPortfolioOpt Black-Litterman stack is not available")
    assert_daily_return_inputs(returns, benchmark_returns, "Black-Litterman daily input")
    bench_prices = (1 + benchmark_returns.dropna()).cumprod().to_frame("benchmark")
    delta = market_implied_risk_aversion(bench_prices, frequency=TRADING_DAYS)
    market_weights = pd.Series(1.0 / len(mu_historical), index=mu_historical.index)
    pi = delta * (cov @ market_weights)
    views: Dict[str, float] = {}
    for asset in ["GLD", "QQQ", "IEF"]:
        if asset in mu_historical.index:
            views[asset] = float(max(mu_historical.loc[asset], 0.03))
    bl = BlackLittermanModel(cov, pi=pi, absolute_views=views if views else None, tau=0.05)
    bl_ret = bl.bl_returns()
    bl_cov = bl.bl_cov()
    ef = EfficientFrontier(bl_ret, bl_cov, weight_bounds=(0, MAX_SINGLE_WEIGHT))
    ef.max_sharpe(risk_free_rate=rf)
    return normalize_weights(pd.Series(ef.clean_weights()).reindex(returns.columns).fillna(0), cap=MAX_SINGLE_WEIGHT)


def tracking_error_optimal_strategy(
    returns: pd.DataFrame,
    benchmark_returns: pd.Series,
    mu: pd.Series,
    cov: pd.DataFrame,
    target_te: float = 0.06,
    current_weights: Optional[pd.Series] = None,
) -> pd.Series:
    if minimize is None:
        raise ImportError("scipy.optimize.minimize is not available")
    assert_daily_return_inputs(returns, benchmark_returns, "Tracking-error optimizer daily input")
    assets = returns.columns
    n = len(assets)
    w0 = np.ones(n) / n
    bounds = [(0.0, MAX_SINGLE_WEIGHT)] * n
    cons = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]
    ret_mat = returns.values
    bench_arr = benchmark_returns.reindex(returns.index).values
    mu_arr = mu.reindex(assets).fillna(0).values
    bench_ann = annualized_return(benchmark_returns)

    def obj(w: np.ndarray) -> float:
        port = ret_mat @ w
        te = float(np.std(port - bench_arr) * np.sqrt(TRADING_DAYS))
        active_return = float(np.dot(w, mu_arr) - bench_ann)
        turnover = 0.0
        if current_weights is not None:
            turnover = float(np.sum(np.abs(w - current_weights.reindex(assets).fillna(0).values)))
        te_penalty = 50.0 * max(te - target_te, 0.0) ** 2
        return float(-active_return + te_penalty + 0.005 * turnover)

    res = minimize(obj, w0, bounds=bounds, constraints=cons, method="SLSQP", options={"maxiter": 500})
    if not res.success:
        raise RuntimeError(f"Tracking-error optimal failed: {res.message}")
    return normalize_weights(pd.Series(res.x, index=assets), cap=MAX_SINGLE_WEIGHT)


def build_strategies(returns: pd.DataFrame, bench_ret: pd.Series, rf: float, cov_method: str) -> Dict[str, pd.Series]:
    """Build all strategies from the single validated DAILY return matrix. No synthetic data or benchmark proxy is introduced here."""
    assert_daily_return_inputs(returns, bench_ret, "Strategy engine master daily input")
    assets = list(returns.columns)
    cov = covariance_matrix(returns, cov_method)
    mu = returns.mean() * TRADING_DAYS
    strategies: Dict[str, pd.Series] = {}

    def add(name: str, func) -> None:
        try:
            w = func()
            strategies[name] = normalize_weights(w.reindex(assets).fillna(0), cap=MAX_SINGLE_WEIGHT)
        except Exception as exc:
            print(f"{name} failed: {exc}")

    add("Equal Weight", lambda: equal_weight_strategy(assets))
    add("Inverse Volatility", lambda: inverse_volatility_strategy(returns))
    add("Minimum Variance", lambda: min_variance_strategy(returns, cov_method))
    add("Max Sharpe Approx", lambda: max_sharpe_approx_strategy(returns, rf, cov_method))
    add("Equal Risk Contribution (ERC)", lambda: erc_strategy(returns, cov_method))
    add("Maximum Diversification", lambda: max_diversification_strategy(returns, cov_method))
    add("HRP", lambda: hrp_strategy(returns, cov))
    add("Black-Litterman", lambda: black_litterman_strategy(returns, bench_ret, mu, cov, rf))
    add("Tracking Error Optimal", lambda: tracking_error_optimal_strategy(returns, bench_ret, mu, cov, target_te=0.06))

    if not strategies:
        raise ValueError("All strategies failed. Daily return matrix is valid, but optimization engines returned no usable portfolio.")
    return strategies

def strategy_metrics(name: str, weights: pd.Series, returns: pd.DataFrame, bench_ret: pd.Series, initial_capital: float, rf: float) -> Dict[str, Any]:
    pr = returns.mul(weights, axis=1).sum(axis=1)
    active = pr - bench_ret.reindex(pr.index).fillna(0)
    ann_ret = annualized_return(pr)
    ann_vol = float(pr.std() * np.sqrt(TRADING_DAYS)) if len(pr) else 0.0
    sharpe = float((ann_ret - rf) / ann_vol) if ann_vol else 0.0
    sortino = float((ann_ret - rf) / downside_deviation(pr)) if downside_deviation(pr) else 0.0
    bench_ann = annualized_return(bench_ret)
    te = float(active.std() * np.sqrt(TRADING_DAYS)) if len(active) else 0.0
    ir = float((ann_ret - bench_ann) / te) if te else 0.0
    beta = float(pr.cov(bench_ret) / bench_ret.var()) if bench_ret.var() else 1.0
    alpha = float(ann_ret - (rf + beta * (bench_ann - rf)))
    v95, c95, e95 = var_cvar(pr, 0.95)
    rv95, rc95, _ = var_cvar(active, 0.95)
    eq = (1 + pr).cumprod() * initial_capital
    return {
        "Strategy": name,
        "Annual Return": ann_ret,
        "Volatility": ann_vol,
        "Sharpe Ratio": sharpe,
        "Sortino Ratio": sortino,
        "Max Drawdown": max_drawdown_from_returns(pr),
        "Tracking Error": te,
        "Information Ratio": ir,
        "Beta": beta,
        "Alpha": alpha,
        "VaR 95": v95,
        "CVaR 95": c95,
        "ES 95": e95,
        "Relative VaR 95": rv95,
        "Relative CVaR 95": rc95,
        "Final Value": float(eq.iloc[-1]) if len(eq) else initial_capital,
    }


def choose_strategy(metrics: List[Dict[str, Any]], rule: str) -> str:
    df = pd.DataFrame(metrics)
    if rule == "lowest_tracking_error":
        return str(df.sort_values(["Tracking Error", "Max Drawdown"], ascending=[True, False]).iloc[0]["Strategy"])
    if rule == "highest_information_ratio":
        return str(df.sort_values(["Information Ratio", "Sharpe Ratio"], ascending=[False, False]).iloc[0]["Strategy"])
    if rule == "minimum_volatility":
        return str(df.sort_values(["Volatility", "Max Drawdown"], ascending=[True, False]).iloc[0]["Strategy"])
    return str(df.sort_values(["Sharpe Ratio", "Information Ratio"], ascending=[False, False]).iloc[0]["Strategy"])


def rolling_beta(pr: pd.Series, br: pd.Series, window: int) -> pd.Series:
    cov = pr.rolling(window).cov(br)
    var = br.rolling(window).var()
    return (cov / var).replace([np.inf, -np.inf], np.nan).dropna()


def rolling_asset_betas(returns: pd.DataFrame, bench_ret: pd.Series, window: int) -> pd.DataFrame:
    aligned = returns.join(bench_ret.rename('__BENCHMARK__'), how='inner').dropna()
    if aligned.empty:
        return pd.DataFrame()
    br = aligned['__BENCHMARK__']
    out = {}
    for asset in returns.columns:
        ar = aligned[asset]
        beta = ar.rolling(window).cov(br) / br.rolling(window).var()
        out[asset] = beta.replace([np.inf, -np.inf], np.nan)
    return pd.DataFrame(out).dropna(how='all')


def beta_summary_table(returns: pd.DataFrame, bench_ret: pd.Series, rolling_betas_df: pd.DataFrame) -> pd.DataFrame:
    aligned = returns.join(bench_ret.rename('__BENCHMARK__'), how='inner').dropna()
    if aligned.empty:
        return pd.DataFrame(columns=['Asset','Full Sample Beta','Latest Rolling Beta','Average Rolling Beta','Min Rolling Beta','Max Rolling Beta','Correlation vs Benchmark','R-Squared','Daily Observations'])
    br = aligned['__BENCHMARK__']
    rows = []
    for asset in returns.columns:
        ar = aligned[asset]
        beta = float(ar.cov(br) / br.var()) if br.var() else np.nan
        corr = float(ar.corr(br)) if len(ar.dropna()) > 2 else np.nan
        rb = rolling_betas_df[asset].dropna() if asset in rolling_betas_df.columns else pd.Series(dtype=float)
        rows.append({
            'Asset': asset,
            'Full Sample Beta': beta,
            'Latest Rolling Beta': float(rb.iloc[-1]) if len(rb) else np.nan,
            'Average Rolling Beta': float(rb.mean()) if len(rb) else np.nan,
            'Min Rolling Beta': float(rb.min()) if len(rb) else np.nan,
            'Max Rolling Beta': float(rb.max()) if len(rb) else np.nan,
            'Correlation vs Benchmark': corr,
            'R-Squared': float(corr*corr) if np.isfinite(corr) else np.nan,
            'Daily Observations': int(len(ar.dropna())),
        })
    return pd.DataFrame(rows).sort_values('Full Sample Beta', ascending=False)


def rolling_tracking_error(pr: pd.Series, bench_ret: pd.Series, window: int) -> pd.Series:
    aligned = pd.concat([pr, bench_ret], axis=1).dropna()
    aligned.columns = ['portfolio', 'benchmark']
    active = aligned['portfolio'] - aligned['benchmark']
    return (active.rolling(window).std() * np.sqrt(TRADING_DAYS)).replace([np.inf, -np.inf], np.nan).dropna()


def rolling_var_nav_ratio(pr: pd.Series, initial_capital: float, confidence: float = 0.95, window: int = 63) -> pd.Series:
    """Rolling 3-month historical VaR divided by NAV.

    The calculation uses only the selected portfolio DAILY return path:
    1) NAV_t = initial_capital * cumulative daily compounding.
    2) Rolling VaR_t = max(0, -quantile(window daily returns, 1-confidence)) * NAV_t.
    3) Ratio_t = Rolling VaR_t / NAV_t, equal to the rolling daily return loss magnitude.

    It is kept as a ratio so it remains comparable through time as NAV changes.
    No synthetic data, no benchmark proxy, no lower-frequency aggregation.
    """
    x = pr.dropna().astype(float).sort_index()
    if len(x) < window:
        return pd.Series(dtype=float, name=f"VaR NAV Ratio {int(confidence*100)}")
    nav = (1.0 + x).cumprod() * float(initial_capital)
    rolling_loss = x.rolling(window).quantile(1.0 - confidence).mul(-1.0).clip(lower=0.0)
    var_amount = rolling_loss * nav
    ratio = (var_amount / nav).replace([np.inf, -np.inf], np.nan).dropna()
    ratio.name = f"VaR NAV Ratio {int(confidence*100)}"
    return ratio


def compute_pca(returns: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cols = list(returns.columns)
    if len(cols) < 2 or len(returns) < 30:
        return pd.DataFrame(), pd.DataFrame()
    x = returns.dropna()
    x = (x - x.mean()) / x.std(ddof=0).replace(0, np.nan)
    x = x.dropna(axis=1).dropna()
    if x.shape[1] < 2:
        return pd.DataFrame(), pd.DataFrame()
    k = min(5, x.shape[1])
    pca = PCA(n_components=k).fit(x.values)
    variance = pd.DataFrame({"Component": [f"PC{i+1}" for i in range(k)], "Explained Variance": pca.explained_variance_ratio_})
    load = pd.DataFrame(pca.components_.T, index=x.columns, columns=[f"PC{i+1}" for i in range(k)]).reset_index().rename(columns={"index": "Asset"})
    return variance, load


def stress_scenarios(weights: pd.Series, stress_family: str = "All", min_severity: float = 0.0) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    scenarios = [
        {"Scenario": "Global Financial Crisis Shock", "Family": "crisis", "Severity": 5.0, "Equity": -0.35, "Rates": 0.08, "Gold": 0.12, "Commodity": -0.18, "EM": -0.42, "Turkey": -0.45},
        {"Scenario": "COVID Liquidity Shock", "Family": "crisis", "Severity": 4.5, "Equity": -0.28, "Rates": 0.06, "Gold": 0.08, "Commodity": -0.22, "EM": -0.32, "Turkey": -0.36},
        {"Scenario": "Inflation Re-Acceleration", "Family": "inflation", "Severity": 3.8, "Equity": -0.12, "Rates": -0.10, "Gold": 0.10, "Commodity": 0.16, "EM": -0.16, "Turkey": -0.20},
        {"Scenario": "Banking Stress / Credit Spread Widening", "Family": "banking stress", "Severity": 4.2, "Equity": -0.18, "Rates": 0.05, "Gold": 0.07, "Commodity": -0.08, "EM": -0.22, "Turkey": -0.25},
        {"Scenario": "Sharp Risk-On Rally", "Family": "sharp rally", "Severity": 2.2, "Equity": 0.18, "Rates": -0.04, "Gold": -0.05, "Commodity": 0.08, "EM": 0.22, "Turkey": 0.25},
        {"Scenario": "Sharp Equity Selloff", "Family": "sharp selloff", "Severity": 3.7, "Equity": -0.20, "Rates": 0.04, "Gold": 0.05, "Commodity": -0.09, "EM": -0.24, "Turkey": -0.28},
    ]
    buckets = ["Equity", "Rates", "Gold", "Commodity", "EM", "Turkey"]

    def classify(asset: str) -> str:
        if asset in CASH_LIKE or asset in {"AGG", "BND", "TLT", "IEF", "LQD", "HYG", "MUB", "TIP"}: return "Rates"
        if asset in {"GLD", "IAU", "SLV", "IGLN.L"}: return "Gold"
        if asset in {"DBC", "GSG", "PDBC", "USO", "UNG"}: return "Commodity"
        if asset in {"TUR", "QDV5.DE", "DBXK.DE", "DX2J.DE", "IS3N.DE"}: return "Turkey"
        if asset in {"VWO", "IEMG", "EEM", "EWZ", "INDA", "FXI", "MCHI", "EWT", "EIDO", "EPOL", "EZA", "EPI", "EMIM.L", "XMME.DE"}: return "EM"
        return "Equity"

    rows = []
    for sc in scenarios:
        if stress_family != "All" and sc["Family"] != stress_family:
            continue
        if sc["Severity"] < min_severity:
            continue

        contrib = {f"{b} Impact": 0.0 for b in buckets}
        for asset in weights.index:
            bucket = classify(asset)
            contrib[f"{bucket} Impact"] += float(weights.get(asset, 0.0)) * float(sc[bucket])

        impact = float(sum(contrib.values()))
        row = {
            "Scenario": sc["Scenario"],
            "Family": sc["Family"],
            "Severity": sc["Severity"],
            "Portfolio Impact": impact,
            "Worst Drawdown Proxy": min(impact, 0.0),
            "Interpretation": "loss" if impact < 0 else "gain",
            "Traffic Light": "RED" if impact <= -0.20 else ("AMBER" if impact < -0.05 else "GREEN"),
        }
        row.update(contrib)
        rows.append(row)

    df = pd.DataFrame(rows).sort_values("Portfolio Impact") if rows else pd.DataFrame(columns=["Scenario", "Family", "Severity", "Portfolio Impact", "Worst Drawdown Proxy", "Interpretation", "Traffic Light"] + [f"{b} Impact" for b in buckets])
    kpis = {
        "worst_scenario": df.iloc[0]["Scenario"] if len(df) else None,
        "average_severity": float(df["Severity"].mean()) if len(df) else 0.0,
        "worst_relative_return": float(df["Portfolio Impact"].min()) if len(df) else 0.0,
        "worst_drawdown": float(df["Worst Drawdown Proxy"].min()) if len(df) else 0.0,
        "count": int(len(df)),
        "red_count": int((df["Traffic Light"] == "RED").sum()) if len(df) else 0,
        "amber_count": int((df["Traffic Light"] == "AMBER").sum()) if len(df) else 0,
        "green_count": int((df["Traffic Light"] == "GREEN").sum()) if len(df) else 0,
    }
    return df, kpis



def compute_efficient_frontier_payload(returns: pd.DataFrame, weights: pd.Series, rf: float, cov_method: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """Build efficient-frontier data. Prefer PyPortfolioOpt when available, fallback to deterministic Dirichlet simulation."""
    opt_daily_audit = assert_daily_return_inputs(returns, returns.mean(axis=1), "PyPortfolioOpt efficient frontier")
    assets = list(returns.columns)
    mu = returns.mean() * TRADING_DAYS
    cov = covariance_matrix(returns, cov_method)
    rows: List[Dict[str, Any]] = []
    status = {"optimizer_engine": "Internal simulation fallback", "pypfopt_status": "PyPortfolioOpt not used", "daily_return_audit": opt_daily_audit}
    rng = np.random.default_rng(42)
    if EfficientFrontier is not None:
        status = {"optimizer_engine": "PyPortfolioOpt + internal frontier sampler", "pypfopt_status": f"available; daily inputs validated ({opt_daily_audit['return_observations']} obs, median gap {opt_daily_audit['median_gap_days']:.2f}d)", "daily_return_audit": opt_daily_audit}
        try:
            ef = EfficientFrontier(mu, cov, weight_bounds=(0, MAX_SINGLE_WEIGHT))
            ef.min_volatility()
            r0, v0, sh0 = ef.portfolio_performance(risk_free_rate=rf)
            rows.append({"Portfolio": "PyPortfolioOpt Min Vol", "Return": float(r0), "Volatility": float(v0), "Sharpe": float(sh0)})
        except Exception as exc:
            status["pypfopt_status"] = f"available, min-vol fallback used: {str(exc)[:100]}"
        try:
            ef = EfficientFrontier(mu, cov, weight_bounds=(0, MAX_SINGLE_WEIGHT))
            ef.max_sharpe(risk_free_rate=rf)
            r1, v1, sh1 = ef.portfolio_performance(risk_free_rate=rf)
            rows.append({"Portfolio": "PyPortfolioOpt Max Sharpe", "Return": float(r1), "Volatility": float(v1), "Sharpe": float(sh1)})
        except Exception as exc:
            status["pypfopt_status"] = f"available, max-sharpe fallback used: {str(exc)[:100]}"
    n = len(assets)
    for i in range(700):
        raw = rng.dirichlet(np.ones(n))
        w = normalize_weights(pd.Series(raw, index=assets), cap=MAX_SINGLE_WEIGHT)
        ret = float(w @ mu.loc[w.index])
        vol = float(math.sqrt(max(w.values @ cov.loc[w.index, w.index].values @ w.values, 0)))
        sharpe = float((ret - rf) / vol) if vol else 0.0
        rows.append({"Portfolio": f"Frontier {i+1}", "Return": ret, "Volatility": vol, "Sharpe": sharpe})
    frontier = pd.DataFrame(rows).replace([np.inf, -np.inf], np.nan).dropna(subset=["Return", "Volatility", "Sharpe"])
    if len(frontier) > 80:
        frontier = frontier.sort_values(["Volatility", "Return"])
        frontier = frontier.groupby(pd.cut(frontier["Volatility"], bins=55, duplicates="drop"), observed=True).apply(lambda x: x.nlargest(1, "Return")).reset_index(drop=True)
    selected_ret = float(weights @ mu.loc[weights.index])
    selected_vol = float(math.sqrt(max(weights.values @ cov.loc[weights.index, weights.index].values @ weights.values, 0)))
    selected_sharpe = (selected_ret - rf) / selected_vol if selected_vol else 0.0
    max_vol = float(max(selected_vol * 1.4, frontier["Volatility"].max() if len(frontier) else selected_vol, 0.01))
    cml_x = np.linspace(0, max_vol, 60)
    cml = pd.DataFrame({"Volatility": cml_x, "Return": rf + selected_sharpe * cml_x})
    return frontier, cml, status


def compute_monte_carlo_frontier_payload(returns: pd.DataFrame, rf: float, cov_method: str, trials: int = 2000) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """FinQuant-style random portfolio cloud and individual asset risk/return payload.

    Uses the same daily return matrix as all other analytics. This mirrors the
    reference chart structure: random portfolios, Sharpe color scale, individual
    assets, frontier overlay handled by frontend.
    """
    assets = list(returns.columns)
    mu = returns.mean() * TRADING_DAYS
    cov = covariance_matrix(returns, cov_method).loc[assets, assets]
    rng = np.random.default_rng(42)
    rows = []
    n = len(assets)
    for i in range(int(min(max(trials, 500), 5000))):
        raw = rng.random(n)
        raw = raw / raw.sum()
        w = normalize_weights(pd.Series(raw, index=assets), cap=MAX_SINGLE_WEIGHT)
        ret = float(w @ mu.loc[w.index])
        vol = float(math.sqrt(max(w.values @ cov.loc[w.index, w.index].values @ w.values, 0.0)))
        sharpe = float((ret - rf) / vol) if vol > 0 else np.nan
        rows.append({"Portfolio": f"Random {i+1}", "Return": ret, "Volatility": vol, "Sharpe": sharpe})
    cloud = pd.DataFrame(rows).replace([np.inf, -np.inf], np.nan).dropna()
    asset_rows = pd.DataFrame({"Asset": assets, "Return": mu.loc[assets].values, "Volatility": np.sqrt(np.diag(cov.loc[assets, assets].values))})
    return cloud, asset_rows


def compute_relative_frontier_payload(returns: pd.DataFrame, bench_ret: pd.Series, rf: float, cov_method: str, trials: int = 1200) -> pd.DataFrame:
    """Benchmark-relative feasible portfolio cloud using actual ^GSPC daily returns.

    No benchmark proxy return is fabricated. Each random portfolio is converted
    to a daily return series and compared with the real Yahoo ^GSPC return series.
    """
    common = returns.index.intersection(bench_ret.index)
    r = returns.loc[common]
    b = bench_ret.loc[common]
    assets = list(r.columns)
    rng = np.random.default_rng(42)
    rows = []
    n = len(assets)
    bench_ann = annualized_return(b)
    for i in range(int(min(max(trials, 300), 3000))):
        raw = rng.random(n)
        raw = raw / raw.sum()
        w = normalize_weights(pd.Series(raw, index=assets), cap=MAX_SINGLE_WEIGHT)
        pr = r.mul(w, axis=1).sum(axis=1)
        active = pr - b
        rows.append({
            "Portfolio": f"Relative {i+1}",
            "ActiveVolatility": float(active.std() * np.sqrt(TRADING_DAYS)),
            "ExcessReturn": float(annualized_return(pr) - bench_ann),
            "InformationRatio": float(((annualized_return(pr) - bench_ann) / (active.std() * np.sqrt(TRADING_DAYS))) if active.std() > 0 else 0.0),
        })
    return pd.DataFrame(rows).replace([np.inf, -np.inf], np.nan).dropna()



def _series_to_daily_points(s: pd.Series, value_name: str) -> List[Dict[str, Any]]:
    """Explicit daily point format for frontend charts: [{Date, value_name}]. No aggregation."""
    x = s.dropna().astype(float).copy()
    x.index = pd.to_datetime(x.index, errors="coerce")
    x = x.loc[~pd.isna(x.index)].sort_index()
    return [{"Date": pd.Timestamp(idx).strftime("%Y-%m-%d"), value_name: float(val)} for idx, val in x.items()]

def _daily_drawdown_points_from_returns(r: pd.Series, value_name: str) -> List[Dict[str, Any]]:
    """Compute drawdown directly from DAILY returns and return one point per trading day. No aggregation."""
    x = r.dropna().astype(float).copy()
    x.index = pd.to_datetime(x.index, errors="coerce")
    x = x.loc[~pd.isna(x.index)].sort_index()
    eq = (1.0 + x).cumprod()
    dd = (eq / eq.cummax() - 1.0).replace([np.inf, -np.inf], np.nan).dropna()
    return _series_to_daily_points(dd, value_name)

def _build_daily_returns_matrix(returns: pd.DataFrame, bench_ret: pd.Series, pr: pd.Series) -> pd.DataFrame:
    """One source of truth for all time-series charts and Quantstats inputs."""
    common = returns.index.intersection(bench_ret.index).intersection(pr.index)
    matrix = returns.loc[common].copy().astype(float)
    matrix.insert(0, "Portfolio Daily Return", pr.loc[common].astype(float))
    matrix.insert(1, "Benchmark Daily Return", bench_ret.loc[common].astype(float))
    matrix.insert(2, "Active Daily Return", (pr.loc[common] - bench_ret.loc[common]).astype(float))
    matrix.index = pd.to_datetime(matrix.index)
    matrix.index.name = "Date"
    return matrix.reset_index()

def compute_quantstats_payload(pr: pd.Series, bench_ret: pd.Series, rolling_window: int, rf: float, initial_capital: float = 1_000_000) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, pd.Series, str, str]:
    """Quantstats metrics plus a real quantstats HTML tearsheet saved for iframe display."""
    pr = pr.dropna().astype(float)
    bench_ret = bench_ret.reindex(pr.index).dropna().astype(float)
    pr = pr.reindex(bench_ret.index).dropna()
    bench_ret = bench_ret.reindex(pr.index).dropna()
    qs_daily_audit = assert_daily_return_inputs(pr.to_frame("portfolio"), bench_ret, "Quantstats")
    qs_status = f"quantstats unavailable; internal metrics used; daily input validated: {qs_daily_audit['return_observations']} observations, median gap {qs_daily_audit['median_gap_days']:.2f} days"
    qs_html_url = ""
    qs_rows: List[Dict[str, Any]] = []
    if qs is not None and len(pr) > 30:
        qs_status = f"quantstats available; DAILY returns validated ({qs_daily_audit['return_observations']} obs, median gap {qs_daily_audit['median_gap_days']:.2f}d); full HTML tearsheet attempted and Plotly mirrors shown below"
        try:
            qs_rows.extend([
                {"Metric": "QS Sharpe", "Value": float(qs.stats.sharpe(pr, rf=rf))},
                {"Metric": "QS Sortino", "Value": float(qs.stats.sortino(pr, rf=rf))},
                {"Metric": "QS Calmar", "Value": float(qs.stats.calmar(pr))},
                {"Metric": "QS Win Rate", "Value": float(qs.stats.win_rate(pr))},
                {"Metric": "QS Best Day", "Value": float(qs.stats.best(pr))},
                {"Metric": "QS Worst Day", "Value": float(qs.stats.worst(pr))},
                {"Metric": "QS Max Drawdown", "Value": float(qs.stats.max_drawdown(pr))},
                {"Metric": "QS CAGR", "Value": float(qs.stats.cagr(pr))},
            ])
        except Exception as exc:
            qs_status = f"quantstats imported; selected stats fallback used: {str(exc)[:160]}"
        try:
            QUANTSTATS_HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
            qs.reports.html(returns=pr, benchmark=bench_ret, rf=rf, title="QFA Quantstats Tearsheet — Portfolio vs S&P 500 Daily", output=str(QUANTSTATS_HTML_PATH), compounded=True, periods_per_year=TRADING_DAYS, download_filename=str(QUANTSTATS_HTML_PATH))
            qs_html_url = f"/api/quantstats-html?ts={int(time.time())}"
            qs_status = qs_status + "; full quantstats HTML generated"
        except Exception:
            try:
                qs.reports.html(pr, benchmark=bench_ret, output=str(QUANTSTATS_HTML_PATH), title="QFA Quantstats Tearsheet")
                qs_html_url = f"/api/quantstats-html?ts={int(time.time())}"
                qs_status = qs_status + "; full quantstats HTML generated via minimal signature"
            except Exception as exc2:
                qs_status = qs_status + f"; full HTML failed: {str(exc2)[:180]}"
    if not qs_rows:
        den = pr.std() * np.sqrt(TRADING_DAYS)
        qs_rows = [
            {"Metric": "Sharpe", "Value": float((annualized_return(pr) - rf) / den) if den else 0.0},
            {"Metric": "Sortino", "Value": float((annualized_return(pr) - rf) / downside_deviation(pr)) if downside_deviation(pr) else 0.0},
            {"Metric": "Win Rate", "Value": float((pr > 0).mean()) if len(pr) else 0.0},
            {"Metric": "Best Day", "Value": float(pr.max()) if len(pr) else 0.0},
            {"Metric": "Worst Day", "Value": float(pr.min()) if len(pr) else 0.0},
            {"Metric": "Max Drawdown", "Value": max_drawdown_from_returns(pr)},
            {"Metric": "CAGR", "Value": annualized_return(pr)},
        ]
    # HARD RULE: Quantstats and all mirror charts below are fed by DAILY returns.
    # No lower-frequency aggregation is permitted. We deliberately do not use aggregate("W").
    _idx = pr.dropna().index
    daily_table = pd.DataFrame({
        "Date": [pd.Timestamp(x).strftime("%Y-%m-%d") for x in _idx],
        "Portfolio Daily Return": pr.dropna().values,
        "Benchmark Daily Return": bench_ret.reindex(_idx).values,
        "Active Daily Return": (pr.dropna() - bench_ret.reindex(_idx)).values,
    })
    roll_vol = (pr.rolling(rolling_window, min_periods=max(20, rolling_window // 3)).std() * np.sqrt(TRADING_DAYS)).replace([np.inf, -np.inf], np.nan).dropna()
    active_curve = (1 + (pr - bench_ret).fillna(0)).cumprod() - 1
    return pd.DataFrame(qs_rows), daily_table, pr.dropna(), roll_vol, active_curve, qs_status, qs_html_url

def compute_institutional_report(price_df: pd.DataFrame, payload: Dict[str, Any]) -> Dict[str, Any]:
    if REQUIRE_YAHOO_DAILY_ONLY:
        if str(payload.get("data_source", "yahoo")).strip().lower() != "yahoo":
            raise ValueError("Yahoo Finance daily-only mode is locked. Uploaded/synthetic/fallback data is not allowed.")
        if str(payload.get("source_interval", "1d")).strip().lower() != "1d":
            raise ValueError("Only Yahoo Finance interval='1d' is allowed. Lower-frequency inputs are rejected.")
        if bool(payload.get("synthetic_data_allowed", False)):
            raise ValueError("Synthetic data is forbidden. The platform must fail instead of fabricating prices or returns.")
        if bool(payload.get("lower_frequency_aggregate_allowed", False)):
            raise ValueError("Lower-frequency aggregation is forbidden. Every chart must use daily returns.")
    df, daily_audit = clean_price_frame(price_df)
    benchmark_symbol = normalize_benchmark_symbol(payload.get("benchmark_symbol", "^GSPC"))
    df, compute_fx_audit = _ensure_bist_fx_benchmark_in_clean_prices(df, benchmark_symbol)
    daily_audit["compute_stage_fx_benchmark_guard"] = compute_fx_audit
    rf = float(payload.get("risk_free_rate", DEFAULT_RF))
    initial_capital = float(payload.get("initial_capital", 1_000_000))
    rolling_window = int(payload.get("rolling_window", 63))
    cov_method = str(payload.get("covariance_method", payload.get("cov_method", "ledoit_wolf")))
    best_rule = str(payload.get("best_strategy_rule", "highest_sharpe"))
    stress_family = str(payload.get("stress_family", "All"))
    min_severity = float(payload.get("min_severity", 0.0))

    returns_all = df.pct_change().dropna()
    has_bist_assets = any(_is_turkish_bist_ticker(c) for c in returns_all.columns)
    if has_bist_assets:
        if XU100_USD_BENCHMARK_SYMBOL not in returns_all.columns:
            raise ValueError("Turkish BIST assets require USD-converted XU100 benchmark (^XU100 / USDTRY=X). Benchmark proxy/fallback is disabled.")
        active_benchmark_symbol = XU100_USD_BENCHMARK_SYMBOL
        active_benchmark_label = XU100_USD_BENCHMARK_LABEL
        bench_ret = returns_all[XU100_USD_BENCHMARK_SYMBOL].rename("XU100 USD Daily Return")
        returns = returns_all.drop(columns=[c for c in [XU100_USD_BENCHMARK_SYMBOL, BENCHMARK_SYMBOL] if c in returns_all.columns])
    elif benchmark_symbol in returns_all.columns:
        active_benchmark_symbol = benchmark_symbol
        active_benchmark_label = BENCHMARK_LABEL if benchmark_symbol == BENCHMARK_SYMBOL else benchmark_symbol
        bench_ret = returns_all[benchmark_symbol].rename("Benchmark Daily Return")
        returns = returns_all.drop(columns=[benchmark_symbol])
    else:
        raise ValueError("Required Yahoo benchmark ^GSPC is missing after daily price cleaning. Benchmark proxy/fallback is disabled.")
    returns = returns.dropna(axis=1, how="any")
    common = returns.index.intersection(bench_ret.index)
    returns, bench_ret = returns.loc[common], bench_ret.loc[common]
    returns = returns.sort_index()
    bench_ret = bench_ret.sort_index()
    if returns.shape[1] < 3:
        raise ValueError("Need at least 3 valid non-benchmark price columns after cleaning.")
    daily_return_audit = assert_daily_return_inputs(returns, bench_ret, "Core analytics / PyPortfolioOpt / Quantstats")

    strategies = build_strategies(returns, bench_ret, rf, cov_method)
    metrics = [strategy_metrics(name, w, returns, bench_ret, initial_capital, rf) for name, w in strategies.items()]
    if not strategies or not metrics:
        raise ValueError("No portfolio strategy could be computed from the validated daily return matrix. Check asset coverage, optimizer constraints, and Yahoo availability.")
    best_name = choose_strategy(metrics, best_rule)
    if best_name not in strategies:
        raise ValueError(f"Selected strategy {best_name!r} is missing from computed strategy weights. Available strategies: {list(strategies.keys())}")
    best_metric = next((m for m in metrics if m.get("Strategy") == best_name), None)
    if best_metric is None:
        raise ValueError(f"Selected strategy {best_name!r} has no metrics row. Metrics strategies: {[m.get('Strategy') for m in metrics]}")
    weights = strategies[best_name]
    pr = returns.mul(weights, axis=1).sum(axis=1).rename("Portfolio Daily Return")
    # SINGLE SOURCE OF TRUTH: all charts, PyPortfolioOpt diagnostics and Quantstats mirrors use this daily return series.
    daily_returns_matrix = _build_daily_returns_matrix(returns, bench_ret, pr)
    active = pr - bench_ret
    eq = (1 + pr).cumprod() * initial_capital
    if len(eq):
        best_metric["Final Value"] = float(eq.iloc[-1])
        best_metric["Total Return %"] = float(eq.iloc[-1] / initial_capital - 1.0)
    bench_curve = (1 + bench_ret).cumprod() * initial_capital
    dd = eq / eq.cummax() - 1
    bdd = bench_curve / bench_curve.cummax() - 1

    cov = covariance_matrix(returns, cov_method)
    port_vol = math.sqrt(float(weights.values @ cov.loc[weights.index, weights.index].values @ weights.values)) if len(weights) else 0.0
    marginal = cov.loc[weights.index, weights.index].values @ weights.values
    contrib = weights.values * marginal / (port_vol ** 2) if port_vol > 0 else weights.values
    rc = pd.DataFrame({"Asset": weights.index, "Weight": weights.values, "Contribution %": contrib}).sort_values("Contribution %", ascending=False)

    roll_sharpe = ((pr.rolling(rolling_window).mean() * TRADING_DAYS - rf) / (pr.rolling(rolling_window).std() * np.sqrt(TRADING_DAYS))).replace([np.inf, -np.inf], np.nan).dropna()
    roll_beta = rolling_beta(pr, bench_ret, rolling_window)
    rolling_asset_betas_df = rolling_asset_betas(returns, bench_ret, rolling_window)
    beta_summary_df = beta_summary_table(returns, bench_ret, rolling_asset_betas_df)
    rolling_te = rolling_tracking_error(pr, bench_ret, rolling_window)
    rolling_var_nav_95 = rolling_var_nav_ratio(pr, initial_capital, confidence=0.95, window=63)
    rolling_var_nav_99 = rolling_var_nav_ratio(pr, initial_capital, confidence=0.99, window=63)
    pca_var, pca_load = compute_pca(returns)
    stress_df, stress_kpis = stress_scenarios(weights, stress_family, min_severity)
    frontier_df, cml_df, opt_status = compute_efficient_frontier_payload(returns, weights, rf, cov_method)
    monte_carlo_df, asset_risk_return_df = compute_monte_carlo_frontier_payload(returns, rf, cov_method)
    relative_frontier_df = compute_relative_frontier_payload(returns, bench_ret, rf, cov_method)
    qs_metrics, daily_return_table, daily_returns, roll_vol, active_curve, qs_status, qs_html_url = compute_quantstats_payload(pr, bench_ret, rolling_window, rf, initial_capital)
    advanced_var_df, relative_var_df = compute_advanced_var_tables(pr, bench_ret, initial_capital)
    strategy_explanations_df = portfolio_strategy_explanations_table()

    data_quality = pd.DataFrame({
        "Asset": df.columns,
        "Observations": [int(df[c].notna().sum()) for c in df.columns],
        "Missing %": [float(df[c].isna().mean()) for c in df.columns],
        "First Date": [str(df[c].dropna().index.min().date()) if df[c].notna().any() else "" for c in df.columns],
        "Last Date": [str(df[c].dropna().index.max().date()) if df[c].notna().any() else "" for c in df.columns],
    })

    # Build and validate every frontend time-series chart from the same daily return matrix.
    equity_daily_points = _series_to_daily_points(eq, "Portfolio Equity Value")
    benchmark_equity_daily_points = _series_to_daily_points(bench_curve, "Benchmark Equity Value")
    portfolio_daily_return_points = _series_to_daily_points(pr, "Portfolio Daily Return")
    benchmark_daily_return_points = _series_to_daily_points(bench_ret, "Benchmark Daily Return")
    active_daily_return_points = _series_to_daily_points(active, "Active Daily Return")
    active_return_daily_points = _series_to_daily_points(active_curve, "Cumulative Active Return")
    drawdown_daily_points = _daily_drawdown_points_from_returns(pr, "Portfolio Daily Drawdown")
    benchmark_drawdown_daily_points = _daily_drawdown_points_from_returns(bench_ret, "Benchmark Daily Drawdown")
    rolling_sharpe_daily_points = _series_to_daily_points(roll_sharpe, "Rolling Sharpe")
    rolling_beta_daily_points = _series_to_daily_points(roll_beta, "Rolling Beta")
    rolling_tracking_error_daily_points = _series_to_daily_points(rolling_te, "Rolling Tracking Error")
    rolling_var_nav_95_points = _series_to_daily_points(rolling_var_nav_95, "VaR NAV Ratio 95")
    rolling_var_nav_99_points = _series_to_daily_points(rolling_var_nav_99, "VaR NAV Ratio 99")
    rolling_asset_beta_points = json_safe(rolling_asset_betas_df.reset_index().rename(columns={rolling_asset_betas_df.index.name or 'index': 'Date'})) if not rolling_asset_betas_df.empty else []
    rolling_volatility_daily_points = _series_to_daily_points(roll_vol, "Rolling Annualized Volatility")
    time_series_chart_audit_df = pd.DataFrame([
        assert_daily_points(equity_daily_points, "Equity Curve"),
        assert_daily_points(drawdown_daily_points, "Drawdown"),
        assert_daily_points(portfolio_daily_return_points, "Daily Returns"),
        assert_daily_points(rolling_sharpe_daily_points, "Rolling Sharpe"),
        assert_daily_points(rolling_beta_daily_points, "Rolling Beta"),
        assert_daily_points(rolling_tracking_error_daily_points, "Rolling Tracking Error"),
        assert_daily_points(rolling_var_nav_95_points, "3M VaR NAV Ratio 95"),
        assert_daily_points(rolling_var_nav_99_points, "3M VaR NAV Ratio 99"),
        assert_daily_points(rolling_volatility_daily_points, "Rolling Volatility"),
        assert_daily_points(active_return_daily_points, "Cumulative Active Return"),
    ])

    key_metrics = pd.DataFrame([{"Metric": k, "Value": v} for k, v in best_metric.items() if k != "Strategy"])
    explanation_map = {
        "highest_sharpe": "The strategy with the strongest risk-adjusted return was selected. Ties are resolved with Information Ratio.",
        "lowest_tracking_error": "The strategy with the lowest benchmark-relative active risk was selected. This is suitable for benchmark-aware portfolio mandates.",
        "highest_information_ratio": "The strategy with the strongest excess return per unit of active risk was selected.",
        "minimum_volatility": "The strategy with the lowest annualized volatility was selected for defensive risk control.",
    }
    return {
        "meta": {
            "benchmark": active_benchmark_symbol,
            "benchmark_label": active_benchmark_label,
            "benchmark_frequency": "Daily",
            "data_frequency": "Yahoo Finance Daily 1D; BIST assets are USD-converted with USDTRY=X when selected",
            "fx_engine": {
                "enabled": bool(has_bist_assets),
                "fx_symbol": USDTRY_SYMBOL if has_bist_assets else None,
                "bist_price_conversion": "BIST TRY close / USDTRY=X close" if has_bist_assets else None,
                "benchmark_conversion": "^XU100 TRY close / USDTRY=X close" if has_bist_assets else None,
                "synthetic_fx_used": False,
                "benchmark_proxy_used": False,
            },
            "data_policy": "YAHOO_DAILY_ONLY_NO_SYNTHETIC_NO_RESAMPLE",
            "synthetic_data_used": False,
            "lower_frequency_aggregate_used": False,
            "initial_capital": initial_capital,
            "risk_free_rate": rf,
            "selected_count": int(returns.shape[1]),
            "best_strategy": best_name,
            "best_strategy_rule": best_rule,
            "best_strategy_explanation": explanation_map.get(best_rule, explanation_map["highest_sharpe"]),
            "covariance_method": cov_method,
            "optimizer_engine": opt_status.get("optimizer_engine"),
            "pypfopt_status": opt_status.get("pypfopt_status"),
            "quantstats_status": qs_status,
            "quantstats_html_url": qs_html_url,
            "data_alignment": "Yahoo Finance interval=1d; daily business-day common sample; capped forward-fill limit=10 before returns; daily-only calculation; PyPortfolioOpt and Quantstats use the same daily return index.",
            "daily_price_audit": daily_audit,
            "daily_return_audit": daily_return_audit,
            "drawdown_point_count": int(len(pr.dropna())),
            "chart_frequency_enforced": "ALL line/scatter/risk/optimization/quantstats/drawdown inputs use daily returns only; no lower-frequency data; no lower-frequency aggregate; all frontend time-series charts are fed by explicit daily point arrays rebuilt from the daily return matrix.",
            "final_value_method": "Final Value = initial_capital * cumulative product of selected portfolio daily returns. It is recalculated from the final daily NAV after best strategy selection.",
            "start": str(returns.index.min().date()),
            "end": str(returns.index.max().date()),
        },
        "weights": json_safe(pd.DataFrame({"asset": weights.index, "weight": weights.values}).sort_values("weight", ascending=False)),
        "strategy_metrics": json_safe(pd.DataFrame(metrics).sort_values("Sharpe Ratio", ascending=False)),
        "key_metrics": json_safe(key_metrics),
        "risk_contrib": json_safe(rc),
        "strategy_explanations": json_safe(strategy_explanations_df),
        "advanced_var_table": json_safe(advanced_var_df),
        "relative_var_table": json_safe(relative_var_df),
        "summary": {
            "annual_return": best_metric["Annual Return"], "volatility": best_metric["Volatility"], "sharpe_ratio": best_metric["Sharpe Ratio"],
            "sortino_ratio": best_metric["Sortino Ratio"], "max_drawdown": best_metric["Max Drawdown"], "tracking_error": best_metric["Tracking Error"],
            "information_ratio": best_metric["Information Ratio"], "beta": best_metric["Beta"], "alpha": best_metric["Alpha"],
            "var_95": best_metric["VaR 95"], "cvar_95": best_metric["CVaR 95"], "es_95": best_metric["ES 95"],
            "relative_var_95": best_metric["Relative VaR 95"], "relative_cvar_95": best_metric["Relative CVaR 95"], "final_value": best_metric["Final Value"],
        },
        "prices_preview": json_safe(df.reset_index().head(20)),
        "data_quality": json_safe(data_quality),
        "data_frequency_audit": json_safe(pd.DataFrame([{**daily_audit, **daily_return_audit}])),
        "equity_curve": json_safe(eq), "benchmark_curve": json_safe(bench_curve), "drawdown_series": json_safe(dd), "benchmark_drawdown": json_safe(bdd),
        "rolling_sharpe": json_safe(roll_sharpe), "rolling_beta": json_safe(roll_beta), "rolling_asset_betas": json_safe(rolling_asset_betas_df), "beta_summary": json_safe(beta_summary_df), "rolling_tracking_error": json_safe(rolling_te), "rolling_var_nav_95": json_safe(rolling_var_nav_95), "rolling_var_nav_99": json_safe(rolling_var_nav_99),
        "pca_variance": json_safe(pca_var), "pca_loadings": json_safe(pca_load),
        "efficient_frontier": json_safe(frontier_df), "capital_market_line": json_safe(cml_df),
        "monte_carlo_frontier": json_safe(monte_carlo_df), "asset_risk_return": json_safe(asset_risk_return_df), "relative_frontier": json_safe(relative_frontier_df),
        "daily_returns_matrix": json_safe(daily_returns_matrix),
        # Explicit daily point arrays for EVERY time-series chart. Frontend must use these fields, not pandas Series objects.
        "equity_daily_points": json_safe(equity_daily_points),
        "benchmark_equity_daily_points": json_safe(benchmark_equity_daily_points),
        "portfolio_daily_return_points": json_safe(portfolio_daily_return_points),
        "benchmark_daily_return_points": json_safe(benchmark_daily_return_points),
        "active_daily_return_points": json_safe(active_daily_return_points),
        "active_return_daily_points": json_safe(active_return_daily_points),
        "drawdown_daily_points": json_safe(drawdown_daily_points),
        "benchmark_drawdown_daily_points": json_safe(benchmark_drawdown_daily_points),
        "rolling_sharpe_daily_points": json_safe(rolling_sharpe_daily_points),
        "rolling_beta_daily_points": json_safe(rolling_beta_daily_points),
        "rolling_asset_beta_points": json_safe(rolling_asset_beta_points),
        "rolling_tracking_error_daily_points": json_safe(rolling_tracking_error_daily_points),
        "rolling_var_nav_95_points": json_safe(rolling_var_nav_95_points),
        "rolling_var_nav_99_points": json_safe(rolling_var_nav_99_points),
        "rolling_volatility_daily_points": json_safe(rolling_volatility_daily_points),
        "quantstats_metrics": json_safe(qs_metrics), "daily_return_table": json_safe(daily_return_table),
        "daily_returns": json_safe(daily_returns), "rolling_volatility": json_safe(roll_vol), "active_return_curve": json_safe(active_curve),
        "time_series_chart_audit": json_safe(time_series_chart_audit_df),
        "stress_table": json_safe(stress_df), "stress_kpis": json_safe(stress_kpis),
    }


@app.get("/")
def root() -> Response:
    return Response(
        content=HTML_DOC,
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-QFA-Version": "qfa_all_timeseries_daily_point_by_point_v1",
        },
    )


@app.get("/health")
def health() -> Response:
    payload = {
        "runtime": {"status": "ok", "output_dir": str(OUTPUT_DIR), "cache_dir": str(CACHE_DIR), "in_colab": IN_COLAB, "python": sys.version.split()[0]},
        "api": {"schema_version": "institutional-v1", "strict_json_serialization": True, "pydantic_validation": True, "endpoints": ["/api/universe", "/api/yahoo-prices", "/api/compute-report", "/api/quantstats-html"], "disabled_endpoints": ["/api/parse-upload"]},
        "categories": list(ETF_UNIVERSE.keys()),
    }
    return api_ok("/health", payload)


@app.get("/api/universe")
def universe() -> Response:
    return api_ok("/api/universe", {"universe": ETF_UNIVERSE})


@app.get("/api/debug-version")
def debug_version() -> Response:
    return api_ok("/api/debug-version", {
        "build_id": "qfa_all_timeseries_daily_point_by_point_v1",
        "benchmark_symbol": BENCHMARK_SYMBOL,
        "benchmark_label": BENCHMARK_LABEL,
        "broad_equity_first_items": ETF_UNIVERSE.get("US Broad Equity", [])[:8],
    })


@app.get("/api/json-self-test")
def json_self_test() -> Response:
    payload = {
        "timestamp": pd.Timestamp("2026-04-26"),
        "datetime_index": pd.date_range("2026-01-01", periods=2),
        "series": pd.Series([1.0, np.nan, np.inf], index=pd.date_range("2026-01-01", periods=3)),
        "dataframe": pd.DataFrame({"Date": pd.date_range("2026-01-01", periods=2), "Value": [1.0, np.inf]}),
    }
    return api_ok("/api/json-self-test", {"self_test": assert_json_serializable(payload, "json_self_test")})


@app.get("/api/quantstats-html")
def quantstats_html() -> Response:
    if QUANTSTATS_HTML_PATH.exists():
        return Response(content=QUANTSTATS_HTML_PATH.read_text(encoding="utf-8", errors="ignore"), media_type="text/html", headers={"Cache-Control": "no-store"})
    return Response(content="<html><body><h3>Quantstats report not generated yet.</h3><p>Run Recompute first.</p></body></html>", media_type="text/html", headers={"Cache-Control": "no-store"})



@app.post("/api/yahoo-prices")
def yahoo_prices(payload: YahooPricesRequest) -> Response:
    endpoint = "/api/yahoo-prices"
    try:
        prices = load_yahoo_prices(payload.tickers, payload.start_date, payload.benchmark_symbol, payload.use_cache)
        if prices.empty or prices.shape[1] < 4:
            raise ValueError("Yahoo returned an insufficient price matrix after cleanup.")
        out_path = OUTPUT_DIR / "yahoo_prices_preview.json"
        out_path.write_text(json.dumps(qfa_json_content(prices, "prices"), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
        note = f"Returned {prices.shape[0]} rows and {prices.shape[1]-1} instruments including benchmark where available."
        return api_ok(endpoint, {"rows": json_safe(prices)}, saved_to=str(out_path), note=note)
    except Exception as exc:
        return api_error(endpoint, exc, hint="Reduce the universe or retry later if Yahoo throttles. Upload/synthetic fallback is disabled by daily-only policy.")


@app.post("/api/parse-upload")
async def parse_upload(price_file: UploadFile = File(...), meta_file: Optional[UploadFile] = File(None)) -> Response:
    endpoint = "/api/parse-upload"
    try:
        if not ALLOW_UPLOAD_MODE:
            raise ValueError("Upload mode is disabled. This build is locked to Yahoo Finance daily prices only.")
        if not price_file.filename:
            raise ValueError("A price file is required.")
        p_bytes = await price_file.read()
        if not p_bytes:
            raise ValueError("Uploaded price file is empty.")
        p_df = ensure_wide_price_df(parse_uploaded_file_bytes(price_file.filename, p_bytes))
        if "Date" not in p_df.columns:
            raise ValueError("Parsed price file must contain a Date column.")
        asset_cols = [c for c in p_df.columns if c != "Date"]
        if len(asset_cols) < 3:
            raise ValueError("Uploaded price file must contain at least 3 asset price columns.")
        meta_rows = []
        if meta_file is not None and meta_file.filename:
            m_bytes = await meta_file.read()
            if m_bytes:
                meta_rows = json_safe(parse_uploaded_file_bytes(meta_file.filename, m_bytes))
        out_path = OUTPUT_DIR / "parsed_upload_preview.json"
        out_path.write_text(json.dumps(qfa_json_content({"prices": p_df.head(50), "metadata": meta_rows}, "parsed_upload_preview"), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
        note = f"Parsed {len(p_df)} rows and {len(asset_cols)} asset columns."
        return api_ok(endpoint, {"prices": json_safe(p_df), "metadata": meta_rows}, saved_to=str(out_path), note=note)
    except Exception as exc:
        return api_error(endpoint, exc, hint="Use a wide file with Date plus price columns, or a long file with Date/Ticker/Close columns.")


@app.post("/api/compute-report")
def compute_report(payload: ComputeReportRequest) -> Response:
    endpoint = "/api/compute-report"
    try:
        request_dict = json_safe(payload.dict())
        df = pd.DataFrame(payload.rows)
        if df.empty:
            raise ValueError("rows cannot be empty.")
        report = compute_institutional_report(df, request_dict)
        safe_report = assert_json_serializable(report, "institutional_report")
        out_path = OUTPUT_DIR / "computed_institutional_report.json"
        out_path.write_text(json.dumps(qfa_json_content(safe_report, "computed_report_file"), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
        return api_ok(endpoint, {"report": safe_report}, saved_to=str(out_path), note="Report JSON passed strict serialization before response.")
    except Exception as exc:
        return api_error(endpoint, exc, hint="Check Date parsing, at least 3 valid assets, benchmark availability, and sufficient observations.")


def _port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def _serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    config = uvicorn.Config(app, host=host, port=port, reload=False, log_level="info")
    server = uvicorn.Server(config)
    server.run()


def _find_free_port(start_port: int = 8000, max_tries: int = 50) -> int:
    """Find a free local port. This prevents Colab from silently serving an old uvicorn thread."""
    for p in range(int(start_port), int(start_port) + int(max_tries)):
        if _port_is_free(p):
            return p
    raise RuntimeError(f"No free port found from {start_port} to {start_port + max_tries - 1}.")


def launch_colab(port: int = 8000, public: bool = False, ngrok_token: Optional[str] = None, force_new_port: bool = True) -> Optional[threading.Thread]:
    """
    Colab/Jupyter-safe launcher.

    Critical behavior:
    - If port 8000 is already occupied, the app DOES NOT reuse it.
    - It automatically moves to the next free port, so you never see an old cached server.
    - ngrok is optional and unrelated to JSON serialization.
    """
    selected_port = int(port)
    if force_new_port or not _port_is_free(selected_port):
        selected_port = _find_free_port(selected_port, 80)

    thread = threading.Thread(target=_serve, kwargs={"host": "127.0.0.1", "port": selected_port}, daemon=True)
    thread.start()
    time.sleep(2)

    local_url = f"http://127.0.0.1:{selected_port}"
    print("QFA Prime Finance Platform — DAILY DRAWDOWN hard-fixed build")
    print(f"Local URL: {local_url}")

    if IN_COLAB:
        try:
            from google.colab.output import eval_js
            proxy_url = eval_js(f"google.colab.kernel.proxyPort({selected_port})")
            print("Colab proxy URL:", proxy_url)
            print("Use this proxy URL. Do not use an old 8000 tab if the new port differs.")
        except Exception as exc:
            print("Colab proxy could not be created:", exc)

    if public:
        if not ngrok_token:
            print("ngrok was requested but no token was provided. ngrok is optional and not needed for the Timestamp fix.")
        else:
            try:
                from pyngrok import ngrok
                ngrok.set_auth_token(ngrok_token)
                tunnel = ngrok.connect(selected_port)
                print("ngrok public URL:", tunnel.public_url)
            except Exception as exc:
                print("ngrok failed. Use Colab proxy instead. Detail:", exc)

    return thread

if __name__ == "__main__":
    main()
