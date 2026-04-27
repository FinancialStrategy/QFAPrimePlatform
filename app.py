# -*- coding: utf-8 -*-
"""
QFA Prime Finance Platform - Institutional Colab PRO DAILY LOCKED Final
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
    """Institutional benchmark is always S&P 500 Daily Index (^GSPC), never ETF proxy."""
    return BENCHMARK_SYMBOL


ETF_UNIVERSE = {
    "US Broad Equity": ["IVV", "VOO", "VTI", "SCHB", "DIA", "IWM", "MDY"],
    "US Growth & Value": ["QQQ", "VUG", "IWF", "VTV", "IWD", "SCHG", "SCHV"],
    "US Sectors": ["XLK", "XLF", "XLV", "XLE", "XLI", "XLP", "XLY", "XLU", "XLB", "XLC", "XLRE"],
    "International Developed": ["VEA", "IEFA", "EFA", "VGK", "EWJ", "EWG", "EWU", "EWC"],
    "Emerging Markets": ["VWO", "IEMG", "EEM", "EWZ", "INDA", "FXI", "MCHI", "EWT", "EIDO", "EPOL", "EZA", "EPI"],
    "Turkey & EMEA": ["TUR", "GULF", "QDV5.DE", "DBXK.DE", "DX2J.DE", "IS3N.DE"],
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
<div class="side-card"><h3>Core Controls</h3><div class="side-grid"><div><label>Benchmark</label><input type="text" id="benchmarkSymbol" value="S&P 500 Daily Index (^GSPC)" readonly></div><div><label>Start Date</label><input type="date" id="startDate" value="2019-01-01"></div><div><label>Initial Capital</label><input type="number" id="initialCapital" value="1000000" step="1000"></div><div><label>Risk-Free Rate</label><input type="number" id="riskFreeRate" value="0.045" step="0.0001"></div><div><label>Rolling Window</label><input type="number" id="rollingWindow" value="63" step="1"></div></div></div>
<div class="side-card"><h3>Portfolio Model Controls</h3><div class="side-grid"><div><label>Expected Return Method</label><select id="expReturnMethod"><option value="historical_mean">Historical Mean</option><option value="ema_historical">EMA Historical</option><option value="capm">CAPM-like Benchmark Beta</option></select></div><div><label>Covariance Method</label><select id="covMethod"><option value="ledoit_wolf">Ledoit-Wolf</option><option value="shrinkage">Shrinkage</option><option value="sample">Sample</option></select></div><div><label>Best Strategy Rule</label><select id="bestStrategyRule"><option value="highest_sharpe">Highest Sharpe</option><option value="lowest_tracking_error">Lowest Tracking Error</option><option value="highest_information_ratio">Highest Information Ratio</option><option value="minimum_volatility">Minimum Volatility</option></select></div></div></div>
<div class="side-card"><h3>Stress Filters</h3><div class="side-grid"><div><label>Stress Family</label><select id="stressFamily"><option value="All">All</option><option value="crisis">crisis</option><option value="inflation">inflation</option><option value="banking stress">banking stress</option><option value="sharp rally">sharp rally</option><option value="sharp selloff">sharp selloff</option></select></div><div><label>Minimum Severity</label><input type="number" id="minSeverity" value="0" step="0.1"></div></div></div>
<div class="side-card"><h3>Data Source Policy</h3><div class="side-grid"><div><label>Mode</label><input type="text" id="dataMode" value="Yahoo Finance Daily Only" readonly></div><div class="smallnote"><b>LOCKED:</b> Yahoo Finance adjusted daily prices only. Upload/synthetic/fallback price modes are disabled. Every chart is generated from portfolio DAILY RETURNS; no weekly/monthly resampling is allowed.</div></div></div>
<div class="side-card"><h3>ETF Universe Drill-Down</h3><div id="categoryDrilldown"></div><button class="side-btn primary" id="recomputeBtn">Run Institutional Analysis</button><div style="height:8px"></div><div class="status" id="statusBox">Ready.</div></div></aside>
<main class="main"><div class="header"><h2>QFA Prime Finance Platform</h2><p id="headerMeta">Benchmark: S&P 500 Daily (^GSPC) • Frequency: DAILY RETURNS LOCKED • Periods/Year: 252 • Generated by MK FinTECH LabGEN@2026</p></div><div class="kpi-grid" id="kpiGrid"></div>
<div class="tabs"><button class="tab-btn active" onclick="showTab('tab-key', this)">Key Metrics</button><button class="tab-btn" onclick="showTab('tab-guide', this)">Best Strategy</button><button class="tab-btn" onclick="showTab('tab-info', this)">Info Hub</button><button class="tab-btn" onclick="showTab('tab-exec', this)">Dashboard</button><button class="tab-btn" onclick="showTab('tab-opt', this)">Optimization</button><button class="tab-btn" onclick="showTab('tab-risk', this)">Risk</button><button class="tab-btn" onclick="showTab('tab-factor', this)">Factor PCA</button><button class="tab-btn" onclick="showTab('tab-stress', this)">Stress</button><button class="tab-btn" onclick="showTab('tab-qs', this)">Quantstats</button><button class="tab-btn" onclick="showTab('tab-data', this)">Data QA</button></div>
<section id="tab-key" class="tab active"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Institutional Key Metrics Summary</h3></div><div class="chart-body"><div class="callout" id="keyMetricsHeader"></div><div style="height:12px"></div><div id="keyMetricsTable"></div></div></div></div></section>
<section id="tab-guide" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Best Strategy Guide</h3></div><div class="chart-body"><div class="callout" id="bestGuideBox"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Strategy Ranking</h3></div><div class="chart-body"><div id="strategyTable"></div></div></div></div></section>
<section id="tab-info" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Investment Universe Identity Map</h3></div><div class="chart-body"><div id="infoHubPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Asset Metadata</h3></div><div class="chart-body"><div id="assetMetaTable"></div></div></div></div></section>
<section id="tab-exec" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Executive Strategy Dashboard</h3></div><div class="chart-body"><div id="dashboardPlot" class="plot-slot"></div></div></div><div class="grid2"><div class="chart-card"><div class="chart-header"><h3>Rolling Sharpe</h3></div><div class="chart-body"><div id="rollingSharpePlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Rolling Beta vs Benchmark</h3></div><div class="chart-body"><div id="rollingBetaPlot" class="plot-slot short"></div></div></div></div></div></section>
<section id="tab-opt" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Portfolio Allocation</h3></div><div class="chart-body"><div id="allocationPlot" class="plot-slot short"></div><div id="allocationExplain" class="callout"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Efficient Frontier and Capital Market Line</h3></div><div class="chart-body"><div id="efficientFrontierPlot" class="plot-slot"></div><div class="callout" id="optimizerStatusBox"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Strategy Risk / Return Map</h3></div><div class="chart-body"><div id="strategyScatterPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Cumulative Return vs Benchmark</h3></div><div class="chart-body"><div id="equityPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Daily Drawdown — high-resolution trading-day series</h3></div><div class="chart-body"><div id="drawdownPlot" class="plot-slot"></div></div></div></div></section>
<section id="tab-risk" class="tab"><div class="stack"><div class="grid2"><div class="chart-card"><div class="chart-header"><h3>Risk Contribution</h3></div><div class="chart-body"><div id="riskContribPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>VaR / CVaR / Relative Risk</h3></div><div class="chart-body"><div id="riskBarPlot" class="plot-slot short"></div></div></div></div><div class="chart-card"><div class="chart-header"><h3>Risk Contribution Table</h3></div><div class="chart-body"><div id="riskContribTable"></div></div></div></div></section>
<section id="tab-factor" class="tab"><div class="stack"><div class="grid2"><div class="chart-card"><div class="chart-header"><h3>PCA Explained Variance</h3></div><div class="chart-body"><div id="pcaVariancePlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>PC1 Loadings</h3></div><div class="chart-body"><div id="pcaLoadingsPlot" class="plot-slot short"></div></div></div></div><div class="chart-card"><div class="chart-header"><h3>PCA Loadings Table</h3></div><div class="chart-body"><div id="pcaTable"></div></div></div></div></section>
<section id="tab-stress" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Stress Dashboard KPIs</h3></div><div class="chart-body"><div id="stressKpiGrid" class="kpi-grid"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Scenario Impact Ranking</h3></div><div class="chart-body"><div id="stressPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Stress Scenario Table</h3></div><div class="chart-body"><div id="stressTable"></div></div></div></div></section>
<section id="tab-qs" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Quantstats-Style Performance Tearsheet</h3></div><div class="chart-body"><div class="callout" id="qsStatusBox"></div><div style="height:12px"></div><div id="qsHtmlFrameBox"></div><div style="height:12px"></div><div id="qsMetricsTable"></div></div></div><div class="grid2"><div class="chart-card"><div class="chart-header"><h3>Daily Returns Distribution</h3></div><div class="chart-body"><div id="dailyReturnHistPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Daily Returns Time Series</h3></div><div class="chart-body"><div id="dailyReturnTsPlot" class="plot-slot short"></div></div></div></div><div class="grid2"><div class="chart-card"><div class="chart-header"><h3>Rolling Volatility</h3></div><div class="chart-body"><div id="rollingVolPlot" class="plot-slot short"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Active Return vs Benchmark</h3></div><div class="chart-body"><div id="activeReturnPlot" class="plot-slot short"></div></div></div></div></div></section>
<section id="tab-data" class="tab"><div class="stack"><div class="chart-card"><div class="chart-header"><h3>Data Quality Diagnostics</h3></div><div class="chart-body"><div id="dataQualityTable"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Time-Series Chart Audit</h3></div><div class="chart-body"><div class="callout">Every line chart is rendered from explicit daily point arrays produced by the backend. No lower-frequency/downsampled frontend series is used.</div><div style="height:12px"></div><div id="timeSeriesChartAuditTable"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Selected Prices Preview</h3></div><div class="chart-body"><div id="dataTable"></div></div></div><div class="chart-card"><div class="chart-header"><h3>Daily Returns Matrix Preview</h3></div><div class="chart-body"><div id="dailyReturnsMatrixTable"></div></div></div></div></section>
<div class="footer">Institutional Quantitative Platform — MK Istanbul Fintech LabGEN @2026</div></main></div>
<script>
try{const b=document.getElementById('bootBanner'); if(b) b.style.display='none';}catch(e){}
const QFA_BENCHMARK_SYMBOL = '^GSPC';
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
    type:'scatter',
    mode:'lines+markers',
    name:name,
    x:pts.map(p=>p.Date),
    y:pts.map(p=>Number(p[valueKey])),
    connectgaps:false,
    line:{width:1.6,shape:'linear',simplify:false},
    marker:{size:2.2,opacity:0.62},
    hovertemplate:'%{x}<br>'+name+': %{y}<extra></extra>'
  }, extra)
}
function dailyPctTrace(points, valueKey, name, extra={}){const tr=dailyTrace(points,valueKey,name,extra);tr.hovertemplate='%{x}<br>'+name+': %{y:.3%}<extra></extra>';return tr}
function dailyMoneyTrace(points, valueKey, name, extra={}){const tr=dailyTrace(points,valueKey,name,extra);tr.hovertemplate='%{x}<br>'+name+': $%{y:,.0f}<extra></extra>';return tr}
function dailyLayout(title, ytitle, yfmt=null){const lay={title:title,xaxis:{title:'Daily trading date',type:'date',rangeslider:{visible:true},tickformat:'%Y-%m-%d',hoverformat:'%Y-%m-%d'},yaxis:{title:ytitle},hovermode:'x unified'}; if(yfmt) lay.yaxis.tickformat=yfmt; return lay}

function plot(id,data,layout){if(typeof Plotly==='undefined'){document.getElementById(id).innerHTML='<div class="callout">Plotly CDN could not load. Backend and UI are running, but charts need internet/CDN access.</div>'; return;} Plotly.newPlot(id,data,Object.assign({paper_bgcolor:'white',plot_bgcolor:'white',font:{family:'Segoe UI, Arial',color:'#213043',size:12},margin:{l:62,r:34,t:50,b:72},legend:{orientation:'h',y:-.18}},layout||{}),{responsive:true,displayModeBar:false})}
async function fetchUniverse(){const res=await fetch('/api/universe');const js=await res.json();ETF_UNIVERSE=js.universe||{};buildDrilldown()} function buildDrilldown(){const host=document.getElementById('categoryDrilldown');host.innerHTML='';Object.entries(ETF_UNIVERSE).forEach(([cat,tickers],idx)=>{const box=document.createElement('div');box.className='category-box';box.innerHTML=`<div class="category-title"><span>${cat}</span><label><input type="checkbox" data-cat="${idx}" class="cat-toggle"> all</label></div>`;const list=document.createElement('div');list.className='ticker-list';tickers.forEach(t=>{const row=document.createElement('label');row.className='tick-item';row.innerHTML=`<input type="checkbox" class="ticker-check" data-category="${cat}" value="${t}"><span>${t}</span>`;list.appendChild(row)});box.appendChild(list);host.appendChild(box)});document.querySelectorAll('.cat-toggle').forEach(toggle=>{toggle.addEventListener('change',e=>{const cat=Object.keys(ETF_UNIVERSE)[Number(e.target.dataset.cat)];document.querySelectorAll(`.ticker-check[data-category="${cat}"]`).forEach(cb=>cb.checked=e.target.checked)})});['US Broad Equity','US Growth & Value','Emerging Markets','Fixed Income','Real Assets'].forEach(cat=>{document.querySelectorAll(`.ticker-check[data-category="${cat}"]`).forEach((cb,i)=>{if(i<Math.min(3,(ETF_UNIVERSE[cat]||[]).length))cb.checked=true})})}
function selectedTickers(){return[...document.querySelectorAll('.ticker-check:checked')].map(x=>x.value)} function selectedCategories(){return[...new Set([...document.querySelectorAll('.ticker-check:checked')].map(x=>x.dataset.category))]}
async function uploadAndParseFiles(){throw new Error('Upload mode is disabled: Yahoo Finance daily-only policy is locked.')}
async function getYahooRows(tickers){const payload={tickers,start_date:document.getElementById('startDate').value,benchmark_symbol:QFA_BENCHMARK_SYMBOL,use_cache:false};const res=await fetch('/api/yahoo-prices',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});if(!res.ok)throw new Error(await res.text());return await res.json()}
async function recompute(){try{status('Working...');const tickers=selectedTickers();if(tickers.length<3){alert('Please select at least 3 ETFs.');status('Ready.');return}let metadata=[];const yh=await getYahooRows(tickers);const rows=yh.rows;const payload={rows,data_source:'yahoo',source_interval:'1d',synthetic_data_allowed:false,lower_frequency_aggregate_allowed:false,benchmark_symbol:QFA_BENCHMARK_SYMBOL,initial_capital:Number(document.getElementById('initialCapital').value||1000000),risk_free_rate:Number(document.getElementById('riskFreeRate').value||0.045),rolling_window:Number(document.getElementById('rollingWindow').value||63),expected_return_method:document.getElementById('expReturnMethod').value,covariance_method:document.getElementById('covMethod').value,best_strategy_rule:document.getElementById('bestStrategyRule').value,stress_family:document.getElementById('stressFamily').value,min_severity:Number(document.getElementById('minSeverity').value||0)};const res=await fetch('/api/compute-report',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});if(!res.ok)throw new Error(await res.text());const js=await res.json();CURRENT={report:js.report,metadata};renderAll();status('Done.')}catch(err){console.error(err);status('Failed.');alert(String(err))}}
function renderAll(){const r=CURRENT.report,s=r.summary;document.getElementById('headerMeta').textContent=`Benchmark: ${r.meta.benchmark_label || r.meta.benchmark} • Frequency: DAILY RETURNS LOCKED • Periods/Year: 252 • RF: ${fmtPct(r.meta.risk_free_rate)} | Generated by MK FinTECH LabGEN@2026`;document.getElementById('kpiGrid').innerHTML=`<div class="kpi-card"><div class="kpi-label">Assets</div><div class="kpi-value">${r.meta.selected_count}</div><div class="kpi-sub">${selectedCategories().join(' • ')}</div></div><div class="kpi-card"><div class="kpi-label">Best Strategy</div><div class="kpi-value">${r.meta.best_strategy}</div><div class="kpi-sub">Rule: ${r.meta.best_strategy_rule}</div></div><div class="kpi-card"><div class="kpi-label">Annual Return</div><div class="kpi-value">${fmtPct(s.annual_return)}</div><div class="kpi-sub">Vol: ${fmtPct(s.volatility)}</div></div><div class="kpi-card"><div class="kpi-label">Sharpe</div><div class="kpi-value">${fmtNum(s.sharpe_ratio)}</div><div class="kpi-sub">IR: ${fmtNum(s.information_ratio)}</div></div><div class="kpi-card"><div class="kpi-label">Max Drawdown</div><div class="kpi-value">${fmtPct(s.max_drawdown)}</div><div class="kpi-sub">CVaR 95: ${fmtPct(s.cvar_95)}</div></div><div class="kpi-card"><div class="kpi-label">Final Value</div><div class="kpi-value">${fmtMoney(s.final_value)}</div><div class="kpi-sub">Initial: ${fmtMoney(r.meta.initial_capital)}</div></div>`;
document.getElementById('keyMetricsHeader').innerHTML=`<b>Benchmark:</b> ${r.meta.benchmark_label || r.meta.benchmark} &nbsp; • &nbsp; <b>Frequency:</b> Yahoo Finance 1D -> Portfolio Daily Return Series -> all time-series charts &nbsp; • &nbsp; <b>Periods/Year:</b> 252 &nbsp; • &nbsp; <b>RF:</b> ${fmtPct(r.meta.risk_free_rate)} &nbsp; | &nbsp; Generated by MK FinTECH LabGEN@2026`;renderTable('keyMetricsTable',r.key_metrics);renderTable('strategyTable',r.strategy_metrics);document.getElementById('bestGuideBox').innerHTML=`<b>Selected strategy:</b> ${r.meta.best_strategy}<br><br>${r.meta.best_strategy_explanation}<br><br><b>Institutional guardrails:</b> long-only weights, single-name cap, cash-like ETF cap, Ledoit-Wolf covariance fallback, benchmark-relative TE/IR/alpha/beta diagnostics, VaR/CVaR/ES, PCA and stress scenario ranking.`;
const metaRows=(CURRENT.metadata&&CURRENT.metadata.length)?CURRENT.metadata:r.weights.map(w=>({category:[...Object.entries(ETF_UNIVERSE)].find(([k,v])=>v.includes(w.asset))?.[0]||'',ticker:w.asset,ISINCODE:'',name:w.asset,exchange:'',currency:'',type:'ETF'}));renderTable('assetMetaTable',metaRows);renderTable('riskContribTable',r.risk_contrib);renderTable('dataTable',r.prices_preview);renderTable('dataQualityTable',r.data_quality);renderTable('timeSeriesChartAuditTable',r.time_series_chart_audit||[]);renderTable('dailyReturnsMatrixTable',(r.daily_returns_matrix||[]).slice(0,30));renderTable('pcaTable',r.pca_loadings);renderTable('stressTable',r.stress_table);
document.getElementById('allocationExplain').innerHTML=`<b>Allocation Methodology</b><br>Available strategies are computed and ranked: equal weight, inverse volatility, minimum variance, max Sharpe approximation, and tracking-error aware blend. The chosen strategy is selected by the rule in the sidebar.`;
const catCounts={};metaRows.forEach(x=>{catCounts[x.category]=(catCounts[x.category]||0)+1});plot('infoHubPlot',[{type:'bar',x:Object.keys(catCounts),y:Object.values(catCounts),text:Object.values(catCounts),textposition:'outside'}],{title:'Investment Universe Identity Map'});plot('dashboardPlot',[{type:'bar',x:['Annual Return','Volatility','Sharpe','Max DD','Tracking Error','Information Ratio'],y:[s.annual_return,s.volatility,s.sharpe_ratio,s.max_drawdown,s.tracking_error,s.information_ratio]}],{title:'Executive Strategy Dashboard'});plot('allocationPlot',[{type:'bar',x:r.weights.map(x=>x.asset),y:r.weights.map(x=>x.weight),text:r.weights.map(x=>fmtPct(x.weight)),textposition:'outside'}],{title:'Portfolio Allocation',yaxis:{tickformat:'.0%'}});const eqPts=r.equity_daily_points||[];const beqPts=r.benchmark_equity_daily_points||[];plot('equityPlot',[dailyMoneyTrace(eqPts,'Portfolio Equity Value','Portfolio Daily Equity',{line:{width:3}}),dailyMoneyTrace(beqPts,'S&P 500 Equity Value','S&P 500 Daily Equity',{line:{width:2,dash:'dot'},opacity:0.85})],dailyLayout(`Cumulative Equity Curve — DAILY points only (${eqPts.length} observations)`, 'Portfolio value (USD)', null));const ddPts=r.drawdown_daily_points||[];const bddPts=r.benchmark_drawdown_daily_points||[];plot('drawdownPlot',[dailyPctTrace(ddPts,'Portfolio Daily Drawdown','Portfolio DAILY Drawdown',{fill:'tozeroy',line:{width:1.4,shape:'linear',simplify:false},marker:{size:2.2,opacity:0.70}}),dailyPctTrace(bddPts,'S&P 500 Daily Drawdown','S&P 500 DAILY Drawdown',{line:{width:1.2,dash:'dot',shape:'linear',simplify:false},marker:{size:2,opacity:0.45}})],dailyLayout(`Daily Drawdown — EVERY trading-day point (${ddPts.length} daily observations)`, 'Drawdown', '.0%'));plot('riskContribPlot',[{type:'bar',x:r.risk_contrib.map(x=>x.Asset),y:r.risk_contrib.map(x=>x['Contribution %']),text:r.risk_contrib.map(x=>fmtPct(x['Contribution %'])),textposition:'outside'}],{title:'Marginal Risk Contribution',yaxis:{tickformat:'.0%'}});plot('riskBarPlot',[{type:'bar',x:['VaR 95','CVaR 95','ES 95','Relative VaR 95','Relative CVaR 95'],y:[s.var_95,s.cvar_95,s.es_95,s.relative_var_95,s.relative_cvar_95]}],{title:'Absolute and Benchmark-Relative Tail Risk',yaxis:{tickformat:'.0%'}});const rsPts=r.rolling_sharpe_daily_points||[];const rbPts=r.rolling_beta_daily_points||[];plot('rollingSharpePlot',[dailyTrace(rsPts,'Rolling Sharpe','Rolling Sharpe',{line:{width:2}})],dailyLayout(`Rolling Sharpe — daily returns, rolling window (${rsPts.length} observations)`, 'Sharpe Ratio', null));plot('rollingBetaPlot',[dailyTrace(rbPts,'Rolling Beta','Rolling Beta vs S&P 500',{line:{width:2}})],Object.assign(dailyLayout(`Rolling Beta — daily returns vs S&P 500 (${rbPts.length} observations)`, 'Beta', null),{shapes:[{type:'line',xref:'paper',x0:0,x1:1,y0:1,y1:1,line:{dash:'dash',color:'gray'}}]}));plot('pcaVariancePlot',[{type:'bar',x:r.pca_variance.map(x=>x.Component),y:r.pca_variance.map(x=>x['Explained Variance'])}],{title:'PCA Explained Variance',yaxis:{tickformat:'.0%'}});plot('pcaLoadingsPlot',[{type:'bar',x:r.pca_loadings.map(x=>x.Asset),y:r.pca_loadings.map(x=>x.PC1)}],{title:'PC1 Loadings'});
const frontier=r.efficient_frontier||[];const cml=r.capital_market_line||[];const frontierTrace={type:'scatter',mode:'markers',name:'Efficient Frontier',x:frontier.map(x=>x.Volatility),y:frontier.map(x=>x.Return),marker:{size:7,color:frontier.map(x=>x.Sharpe),colorscale:'Viridis',showscale:true,colorbar:{title:'Sharpe'}}};const cmlTrace={type:'scattergl',mode:'lines',name:'Capital Market Line',x:cml.map(x=>x.Volatility),y:cml.map(x=>x.Return),line:{dash:'dash',width:3}};const stratTrace={type:'scatter',mode:'markers+text',name:'Strategies',x:r.strategy_metrics.map(x=>x.Volatility),y:r.strategy_metrics.map(x=>x['Annual Return']),text:r.strategy_metrics.map(x=>x.Strategy),textposition:'top center',marker:{size:12}};plot('efficientFrontierPlot',[frontierTrace,cmlTrace,stratTrace],{title:'Efficient Frontier / Capital Market Line',xaxis:{title:'Annualized Volatility',tickformat:'.0%'},yaxis:{title:'Annualized Return',tickformat:'.0%'}});plot('strategyScatterPlot',[{type:'scatter',mode:'markers+text',x:r.strategy_metrics.map(x=>x.Volatility),y:r.strategy_metrics.map(x=>x['Annual Return']),text:r.strategy_metrics.map(x=>x.Strategy),textposition:'top center',marker:{size:r.strategy_metrics.map(x=>Math.max(10,Math.abs(x['Sharpe Ratio']||0)*12))}}],{title:'Optimization Strategy Map',xaxis:{title:'Volatility',tickformat:'.0%'},yaxis:{title:'Annual Return',tickformat:'.0%'}});document.getElementById('optimizerStatusBox').innerHTML=`<b>Optimizer engine:</b> ${r.meta.optimizer_engine||'Internal'}<br><b>PyPortfolioOpt status:</b> ${r.meta.pypfopt_status||'not reported'}<br><b>Input frequency:</b> daily returns only / daily-only calculation<br><b>Capital Market Line:</b> slope is based on selected portfolio Sharpe ratio and configured risk-free rate.`;renderTable('qsMetricsTable',r.quantstats_metrics);document.getElementById('qsStatusBox').innerHTML=`<b>Quantstats package status:</b> ${r.meta.quantstats_status||'not reported'}<br><b>Data alignment:</b> ${r.meta.data_alignment||'common sample'}<br><b>Daily audit:</b> ${(r.meta.daily_return_audit&&r.meta.daily_return_audit.return_observations)||'—'} observations; median gap ${(r.meta.daily_return_audit&&r.meta.daily_return_audit.median_gap_days)||'—'} days; lower-frequency aggregate used: false<br>Below: real quantstats HTML tearsheet when available, plus Plotly mirrors. All Quantstats and PyPortfolioOpt inputs are audited daily-return series; daily-only calculation is used.`;document.getElementById('qsHtmlFrameBox').innerHTML=(r.meta.quantstats_html_url?`<iframe src="${r.meta.quantstats_html_url}" style="width:100%;height:900px;border:1px solid #d9e4ef;border-radius:14px;background:white;"></iframe>`:`<div class="note">Full quantstats HTML was not generated in this runtime; using Plotly mirror charts below.</div>`);plot('dailyReturnHistPlot',[{type:'histogram',x:(r.portfolio_daily_return_points||[]).map(x=>x['Portfolio Daily Return']),nbinsx:80}],{title:'Daily Portfolio Return Distribution',xaxis:{title:'Daily return',tickformat:'.1%'}});const prPts=r.portfolio_daily_return_points||[];const brPts=r.benchmark_daily_return_points||[];plot('dailyReturnTsPlot',[dailyPctTrace(prPts,'Portfolio Daily Return','Portfolio Daily Return',{line:{width:1.4}}),dailyPctTrace(brPts,'S&P 500 Daily Return','S&P 500 Daily Return',{line:{width:1.1},opacity:0.65})],dailyLayout(`Portfolio and S&P 500 DAILY Returns — tick-by-tick trading days (${prPts.length} observations)`, 'Daily return', '.1%'));const rvPts=r.rolling_volatility_daily_points||[];const arPts=r.active_return_daily_points||[];plot('rollingVolPlot',[dailyPctTrace(rvPts,'Rolling Annualized Volatility','Rolling Annualized Volatility',{line:{width:2}})],dailyLayout(`Rolling Annualized Volatility — daily returns (${rvPts.length} observations)`, 'Volatility', '.0%'));plot('activeReturnPlot',[dailyPctTrace(arPts,'Cumulative Active Return','Cumulative Active Return',{line:{width:2}})],dailyLayout(`Cumulative Active Return vs S&P 500 — DAILY points only (${arPts.length} observations)`, 'Cumulative active return', '.0%'));document.getElementById('stressKpiGrid').innerHTML=`<div class="kpi-card"><div class="kpi-label">Worst Scenario</div><div class="kpi-value">${r.stress_kpis.worst_scenario||'—'}</div><div class="kpi-sub">Impact: ${fmtPct(r.stress_kpis.worst_relative_return)}</div></div><div class="kpi-card"><div class="kpi-label">Avg Severity</div><div class="kpi-value">${fmtNum(r.stress_kpis.average_severity)}</div><div class="kpi-sub">Filtered scenarios</div></div><div class="kpi-card"><div class="kpi-label">Worst Drawdown Proxy</div><div class="kpi-value">${fmtPct(r.stress_kpis.worst_drawdown)}</div><div class="kpi-sub">Scenario loss estimate</div></div><div class="kpi-card"><div class="kpi-label">Scenario Count</div><div class="kpi-value">${r.stress_kpis.count}</div><div class="kpi-sub">Passing filters</div></div>`;plot('stressPlot',[{type:'bar',orientation:'h',y:r.stress_table.map(x=>x.Scenario).reverse(),x:r.stress_table.map(x=>x['Portfolio Impact']).reverse()}],{title:'Stress Scenario Impact Ranking',xaxis:{tickformat:'.0%'}})}
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
    return CACHE_DIR / f"yahoo_1d_daily_returns_matrix_v5_{safe}_{start_date}.csv"


def load_yahoo_prices(tickers: List[str], start_date: str, benchmark_symbol: str = BENCHMARK_SYMBOL, use_cache: bool = False) -> pd.DataFrame:
    requested = list(dict.fromkeys([t.strip() for t in tickers if str(t).strip()]))
    bench = normalize_benchmark_symbol(benchmark_symbol)
    all_tickers = list(dict.fromkeys(requested + [bench]))
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
                # Never reuse a cache that looks lower-frequency.
                if _median_gap <= 3.5:
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
        if isinstance(data.columns, pd.MultiIndex):
            for t in batch:
                if t in data.columns.get_level_values(0):
                    sub = data[t]
                    col = "Close" if "Close" in sub.columns else ("Adj Close" if "Adj Close" in sub.columns else None)
                    if col:
                        s = pd.to_numeric(sub[col], errors="coerce").rename(t)
                        frames.append(s)
        else:
            col = "Close" if "Close" in data.columns else ("Adj Close" if "Adj Close" in data.columns else None)
            if col and len(batch) == 1:
                frames.append(pd.to_numeric(data[col], errors="coerce").rename(batch[0]))
    if not frames:
        raise ValueError("No usable Yahoo Finance daily price series returned. Synthetic/upload fallback is disabled; reduce the universe or retry Yahoo later.")
    prices = pd.concat(frames, axis=1).sort_index()
    prices = prices.loc[:, ~prices.columns.duplicated()]
    # IMPORTANT: do not aggregate or pre-drop the Yahoo 1D matrix here.
    # The institutional daily engine below builds the common business-day sample,
    # applies capped forward fill, and computes daily_returns = prices.pct_change().
    usable_assets = [c for c in prices.columns if c in requested and c in prices.columns]
    if len(usable_assets) < 3:
        raise ValueError(f"Too few usable ETFs after Yahoo daily cleanup: {usable_assets}. Synthetic/upload fallback is disabled; try a smaller universe or retry Yahoo later.")
    keep = usable_assets + ([bench] if bench in prices.columns and bench not in usable_assets else [])
    prices = prices[keep]
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
    x = x.dropna()
    if len(x) < 10:
        return 0.0, 0.0, 0.0
    q = float(np.quantile(x, 1 - level))
    var = min(q, 0.0)
    cvar = float(x[x <= q].mean()) if (x <= q).any() else var
    es = cvar
    return var, cvar, es


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
        impact = sum(float(weights.get(asset, 0)) * sc[classify(asset)] for asset in weights.index)
        rows.append({
            "Scenario": sc["Scenario"], "Family": sc["Family"], "Severity": sc["Severity"],
            "Portfolio Impact": impact, "Worst Drawdown Proxy": min(impact, 0.0),
            "Interpretation": "loss" if impact < 0 else "gain"
        })
    df = pd.DataFrame(rows).sort_values("Portfolio Impact") if rows else pd.DataFrame(columns=["Scenario", "Family", "Severity", "Portfolio Impact", "Worst Drawdown Proxy", "Interpretation"])
    kpis = {
        "worst_scenario": df.iloc[0]["Scenario"] if len(df) else None,
        "average_severity": float(df["Severity"].mean()) if len(df) else 0.0,
        "worst_relative_return": float(df["Portfolio Impact"].min()) if len(df) else 0.0,
        "worst_drawdown": float(df["Worst Drawdown Proxy"].min()) if len(df) else 0.0,
        "count": int(len(df)),
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
    matrix.insert(1, "S&P 500 Daily Return", bench_ret.loc[common].astype(float))
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
        "S&P 500 Daily Return": bench_ret.reindex(_idx).values,
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
    rf = float(payload.get("risk_free_rate", DEFAULT_RF))
    initial_capital = float(payload.get("initial_capital", 1_000_000))
    rolling_window = int(payload.get("rolling_window", 63))
    cov_method = str(payload.get("covariance_method", payload.get("cov_method", "ledoit_wolf")))
    best_rule = str(payload.get("best_strategy_rule", "highest_sharpe"))
    stress_family = str(payload.get("stress_family", "All"))
    min_severity = float(payload.get("min_severity", 0.0))

    returns_all = df.pct_change().dropna()
    if benchmark_symbol in returns_all.columns:
        bench_ret = returns_all[benchmark_symbol].rename("S&P 500 Daily Return")
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
    best_name = choose_strategy(metrics, best_rule)
    weights = strategies[best_name]
    pr = returns.mul(weights, axis=1).sum(axis=1).rename("Portfolio Daily Return")
    # SINGLE SOURCE OF TRUTH: all charts, PyPortfolioOpt diagnostics and Quantstats mirrors use this daily return series.
    daily_returns_matrix = _build_daily_returns_matrix(returns, bench_ret, pr)
    active = pr - bench_ret
    eq = (1 + pr).cumprod() * initial_capital
    bench_curve = (1 + bench_ret).cumprod() * initial_capital
    dd = eq / eq.cummax() - 1
    bdd = bench_curve / bench_curve.cummax() - 1
    best_metric = next(m for m in metrics if m["Strategy"] == best_name)

    cov = covariance_matrix(returns, cov_method)
    port_vol = math.sqrt(float(weights.values @ cov.loc[weights.index, weights.index].values @ weights.values)) if len(weights) else 0.0
    marginal = cov.loc[weights.index, weights.index].values @ weights.values
    contrib = weights.values * marginal / (port_vol ** 2) if port_vol > 0 else weights.values
    rc = pd.DataFrame({"Asset": weights.index, "Weight": weights.values, "Contribution %": contrib}).sort_values("Contribution %", ascending=False)

    roll_sharpe = ((pr.rolling(rolling_window).mean() * TRADING_DAYS - rf) / (pr.rolling(rolling_window).std() * np.sqrt(TRADING_DAYS))).replace([np.inf, -np.inf], np.nan).dropna()
    roll_beta = rolling_beta(pr, bench_ret, rolling_window)
    pca_var, pca_load = compute_pca(returns)
    stress_df, stress_kpis = stress_scenarios(weights, stress_family, min_severity)
    frontier_df, cml_df, opt_status = compute_efficient_frontier_payload(returns, weights, rf, cov_method)
    qs_metrics, daily_return_table, daily_returns, roll_vol, active_curve, qs_status, qs_html_url = compute_quantstats_payload(pr, bench_ret, rolling_window, rf, initial_capital)

    data_quality = pd.DataFrame({
        "Asset": df.columns,
        "Observations": [int(df[c].notna().sum()) for c in df.columns],
        "Missing %": [float(df[c].isna().mean()) for c in df.columns],
        "First Date": [str(df[c].dropna().index.min().date()) if df[c].notna().any() else "" for c in df.columns],
        "Last Date": [str(df[c].dropna().index.max().date()) if df[c].notna().any() else "" for c in df.columns],
    })

    # Build and validate every frontend time-series chart from the same daily return matrix.
    equity_daily_points = _series_to_daily_points(eq, "Portfolio Equity Value")
    benchmark_equity_daily_points = _series_to_daily_points(bench_curve, "S&P 500 Equity Value")
    portfolio_daily_return_points = _series_to_daily_points(pr, "Portfolio Daily Return")
    benchmark_daily_return_points = _series_to_daily_points(bench_ret, "S&P 500 Daily Return")
    active_daily_return_points = _series_to_daily_points(active, "Active Daily Return")
    active_return_daily_points = _series_to_daily_points(active_curve, "Cumulative Active Return")
    drawdown_daily_points = _daily_drawdown_points_from_returns(pr, "Portfolio Daily Drawdown")
    benchmark_drawdown_daily_points = _daily_drawdown_points_from_returns(bench_ret, "S&P 500 Daily Drawdown")
    rolling_sharpe_daily_points = _series_to_daily_points(roll_sharpe, "Rolling Sharpe")
    rolling_beta_daily_points = _series_to_daily_points(roll_beta, "Rolling Beta")
    rolling_volatility_daily_points = _series_to_daily_points(roll_vol, "Rolling Annualized Volatility")
    time_series_chart_audit_df = pd.DataFrame([
        assert_daily_points(equity_daily_points, "Equity Curve"),
        assert_daily_points(drawdown_daily_points, "Drawdown"),
        assert_daily_points(portfolio_daily_return_points, "Daily Returns"),
        assert_daily_points(rolling_sharpe_daily_points, "Rolling Sharpe"),
        assert_daily_points(rolling_beta_daily_points, "Rolling Beta"),
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
            "benchmark": benchmark_symbol,
            "benchmark_label": BENCHMARK_LABEL if benchmark_symbol == BENCHMARK_SYMBOL else benchmark_symbol,
            "benchmark_frequency": "Daily",
            "data_frequency": "Yahoo Finance Daily 1D",
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
            "start": str(returns.index.min().date()),
            "end": str(returns.index.max().date()),
        },
        "weights": json_safe(pd.DataFrame({"asset": weights.index, "weight": weights.values}).sort_values("weight", ascending=False)),
        "strategy_metrics": json_safe(pd.DataFrame(metrics).sort_values("Sharpe Ratio", ascending=False)),
        "key_metrics": json_safe(key_metrics),
        "risk_contrib": json_safe(rc),
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
        "rolling_sharpe": json_safe(roll_sharpe), "rolling_beta": json_safe(roll_beta),
        "pca_variance": json_safe(pca_var), "pca_loadings": json_safe(pca_load),
        "efficient_frontier": json_safe(frontier_df), "capital_market_line": json_safe(cml_df),
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
