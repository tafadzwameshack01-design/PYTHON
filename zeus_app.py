"""
╔══════════════════════════════════════════════════════════════════════════╗
║   ZEUS ⚡ NEURAL FOOTBALL INTELLIGENCE SYSTEM v4.0                       ║
║   OVER 0.5/1.5/2.5 · BTTS · Home Win · Away Win                         ║
║   Dixon-Coles · ELO Ratings · Brier-Score Learning · Calibration        ║
║   Exponential Decay · Parallel Fetch · 75+ World Leagues · Zero Budget  ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import streamlit as st
import requests
import sqlite3
import json
import math
import os
import time
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd

# scipy for accurate Poisson — graceful fallback if unavailable on Python 3.14
try:
    from scipy.stats import poisson as _scipy_poisson
    HAS_SCIPY = True
except Exception:
    HAS_SCIPY = False

st.set_page_config(
    page_title="ZEUS ⚡ Neural Football AI",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={"About": "ZEUS Neural Football AI v4.0 — Adaptive Learning · Dixon-Coles · ELO · Multi-Bet · 75+ Leagues"},
)

# ════════════════════════════════════════════════════════════════
#  CSS
# ════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Barlow+Condensed:wght@400;600;700&family=Barlow:wght@400;500&display=swap');

:root {
  --bg:#040b04; --surface:#0a160a; --card:#0d1b0d; --border:#183018;
  --green:#39ff14; --green2:#00c853; --gold:#ffb300; --gold2:#ff8f00;
  --cyan:#00e5ff; --red:#ff1744; --purple:#ea80fc; --orange:#ff6d00;
  --text:#d4f0d4; --muted:#4e724e;
}
html,body,.stApp{background:var(--bg)!important;font-family:'Barlow',sans-serif;}
.stApp::before{content:'';position:fixed;inset:0;
  background-image:linear-gradient(rgba(57,255,20,.02) 1px,transparent 1px),
  linear-gradient(90deg,rgba(57,255,20,.02) 1px,transparent 1px);
  background-size:60px 60px;animation:gridMove 30s linear infinite;pointer-events:none;z-index:0;}
@keyframes gridMove{100%{background-position:60px 60px,60px 60px;}}
#MainMenu,footer,header{visibility:hidden;}
.block-container{padding-top:.5rem!important;max-width:1320px;position:relative;z-index:1;}
.zeus-hero{text-align:center;padding:22px 0 8px;}
.zeus-logo{font-family:'Bebas Neue',cursive;font-size:5.5rem;letter-spacing:12px;line-height:1;
  background:linear-gradient(135deg,#39ff14 0%,#69ff47 40%,#ffb300 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;
  animation:logoGlow 4s ease-in-out infinite;}
@keyframes logoGlow{0%,100%{filter:drop-shadow(0 0 8px rgba(57,255,20,.4));}
  50%{filter:drop-shadow(0 0 28px rgba(57,255,20,.9));}}
.zeus-tagline{font-family:'Barlow Condensed',sans-serif;font-size:.78rem;letter-spacing:4px;
  text-transform:uppercase;color:var(--muted);margin-top:4px;}
.zeus-version{font-family:'Barlow Condensed',sans-serif;font-size:.66rem;letter-spacing:3px;
  text-transform:uppercase;color:var(--cyan);margin-top:2px;opacity:.75;}
.zeus-bar{width:80px;height:2px;background:linear-gradient(90deg,transparent,var(--green),transparent);
  margin:12px auto 0;animation:barPulse 2s ease-in-out infinite;}
@keyframes barPulse{0%,100%{width:80px;opacity:.6;}50%{width:200px;opacity:1;}}
.metrics-row{display:flex;gap:10px;margin:14px 0;flex-wrap:wrap;}
.metric-box{flex:1;min-width:90px;background:var(--surface);border:1px solid var(--border);
  border-radius:12px;padding:12px 14px;text-align:center;transition:border-color .3s;}
.metric-box:hover{border-color:var(--green);}
.metric-val{font-family:'Bebas Neue',cursive;font-size:2rem;color:var(--green);line-height:1;display:block;}
.metric-val.gold{color:var(--gold);} .metric-val.cyan{color:var(--cyan);}
.metric-val.purple{color:var(--purple);} .metric-val.red{color:var(--red);}
.metric-lbl{font-family:'Barlow Condensed',sans-serif;font-size:.67rem;color:var(--muted);
  text-transform:uppercase;letter-spacing:1.5px;}
.scan-line{font-family:'Barlow Condensed',sans-serif;font-size:.78rem;color:var(--green);
  letter-spacing:3px;text-transform:uppercase;text-align:center;padding:8px;
  animation:scanFade .9s ease-in-out infinite;}
@keyframes scanFade{0%,100%{opacity:1;}50%{opacity:.2;}}
.pick-card{background:var(--card);border:1px solid var(--border);border-radius:18px;
  padding:22px 26px;margin:14px 0;position:relative;overflow:hidden;opacity:0;
  animation:cardReveal .5s ease forwards;transition:transform .25s,box-shadow .25s;}
.pick-card:hover{transform:translateY(-4px);box-shadow:0 14px 44px rgba(57,255,20,.14);}
.pick-card:nth-child(1){animation-delay:.04s} .pick-card:nth-child(2){animation-delay:.12s}
.pick-card:nth-child(3){animation-delay:.20s} .pick-card:nth-child(4){animation-delay:.28s}
.pick-card:nth-child(5){animation-delay:.36s} .pick-card:nth-child(6){animation-delay:.44s}
.pick-card:nth-child(7){animation-delay:.52s}
@keyframes cardReveal{from{opacity:0;transform:translateY(18px);}to{opacity:1;transform:translateY(0);}}
.pick-card.elite{border-color:var(--gold);background:linear-gradient(135deg,#0d1b0d 0%,#1a1400 100%);
  animation:cardReveal .5s ease forwards,eliteGlow 3s ease-in-out infinite;}
.pick-card.strong{border-color:var(--green2);}
.pick-card.btts{border-color:var(--purple);}
.pick-card.result{border-color:var(--cyan);}
@keyframes eliteGlow{0%,100%{box-shadow:0 0 16px rgba(255,179,0,.1);}50%{box-shadow:0 0 44px rgba(255,179,0,.32);}}
.rank-badge{position:absolute;top:14px;right:20px;font-family:'Bebas Neue',cursive;font-size:4rem;
  line-height:1;color:rgba(57,255,20,.05);pointer-events:none;user-select:none;}
.card-league{font-family:'Barlow Condensed',sans-serif;font-size:.7rem;letter-spacing:3px;
  text-transform:uppercase;color:var(--muted);margin-bottom:6px;}
.card-teams{font-family:'Bebas Neue',cursive;font-size:2rem;letter-spacing:3px;color:var(--text);
  line-height:1.1;margin-bottom:10px;}
.card-vs{color:var(--muted);font-size:1rem;padding:0 8px;}
.card-bet{font-family:'Barlow Condensed',sans-serif;font-weight:700;font-size:1.5rem;
  letter-spacing:1px;margin-bottom:12px;}
.bet-over05{color:var(--cyan);} .bet-over15{color:var(--green);}
.bet-over25{color:var(--gold);} .bet-btts{color:var(--purple);}
.bet-home{color:var(--green2);} .bet-away{color:var(--orange);}
.conf-track{background:rgba(255,255,255,.06);border-radius:999px;height:6px;margin:8px 0 10px;overflow:hidden;}
.conf-fill{height:100%;border-radius:999px;animation:fillBar 1.2s cubic-bezier(.22,1,.36,1) forwards;transform-origin:left;}
.conf-fill.elite{background:linear-gradient(90deg,var(--gold2),var(--gold));}
.conf-fill.strong{background:linear-gradient(90deg,var(--green2),var(--green));}
.conf-fill.btts{background:linear-gradient(90deg,#7b1fa2,var(--purple));}
.conf-fill.result{background:linear-gradient(90deg,#006064,var(--cyan));}
@keyframes fillBar{from{width:0!important;}}
.conf-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;}
.conf-pct{font-family:'Bebas Neue',cursive;font-size:1.6rem;letter-spacing:2px;}
.conf-pct.elite{color:var(--gold);} .conf-pct.strong{color:var(--green);}
.conf-pct.btts{color:var(--purple);} .conf-pct.result{color:var(--cyan);}
.tier-chip{font-family:'Barlow Condensed',sans-serif;font-size:.7rem;font-weight:700;
  letter-spacing:2px;text-transform:uppercase;padding:3px 10px;border-radius:999px;}
.tier-chip.elite{background:rgba(255,179,0,.15);color:var(--gold);border:1px solid rgba(255,179,0,.4);}
.tier-chip.strong{background:rgba(57,255,20,.1);color:var(--green);border:1px solid rgba(57,255,20,.3);}
.tier-chip.btts{background:rgba(234,128,252,.1);color:var(--purple);border:1px solid rgba(234,128,252,.3);}
.tier-chip.result{background:rgba(0,229,255,.1);color:var(--cyan);border:1px solid rgba(0,229,255,.3);}
.pills-row{display:flex;gap:6px;flex-wrap:wrap;margin-top:10px;}
.pill{font-family:'Barlow Condensed',sans-serif;font-size:.73rem;letter-spacing:1px;
  padding:3px 9px;border-radius:6px;white-space:nowrap;}
.pill-time{background:rgba(57,255,20,.08);color:var(--green);border:1px solid rgba(57,255,20,.2);}
.pill-xg{background:rgba(255,179,0,.08);color:var(--gold);border:1px solid rgba(255,179,0,.2);}
.pill-elo{background:rgba(0,229,255,.08);color:var(--cyan);border:1px solid rgba(0,229,255,.2);}
.pill-btts{background:rgba(41,182,246,.08);color:#29b6f6;border:1px solid rgba(41,182,246,.2);}
.pill-form{background:rgba(234,128,252,.08);color:var(--purple);border:1px solid rgba(234,128,252,.2);}
.pill-h2h{background:rgba(255,64,64,.08);color:#ff6464;border:1px solid rgba(255,64,64,.2);}
.pill-learn{background:rgba(0,200,83,.08);color:#00c853;border:1px solid rgba(0,200,83,.2);}
.pill-dc{background:rgba(255,109,0,.08);color:var(--orange);border:1px solid rgba(255,109,0,.2);}
.ai-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-top:10px;}
.ai-factor{background:rgba(57,255,20,.04);border:1px solid rgba(57,255,20,.1);
  border-radius:8px;padding:6px 8px;text-align:center;}
.ai-factor-val{font-family:'Bebas Neue',cursive;font-size:1.1rem;color:var(--green);display:block;line-height:1;}
.ai-factor-val.gold{color:var(--gold);} .ai-factor-val.cyan{color:var(--cyan);}
.ai-factor-val.purple{color:var(--purple);}
.ai-factor-lbl{font-family:'Barlow Condensed',sans-serif;font-size:.62rem;color:var(--muted);
  text-transform:uppercase;letter-spacing:1px;}
.card-reason{font-family:'Barlow',sans-serif;font-size:.8rem;color:var(--muted);margin-top:10px;
  line-height:1.55;border-left:2px solid var(--border);padding-left:10px;}
.countdown{font-family:'Bebas Neue',cursive;font-size:.85rem;letter-spacing:2px;color:var(--green);}
.no-picks{text-align:center;padding:52px 24px;font-family:'Barlow Condensed',sans-serif;
  font-size:1.1rem;color:var(--muted);letter-spacing:2px;}
.no-picks-icon{font-size:3rem;display:block;margin-bottom:12px;}
hr{border-color:rgba(57,255,20,.08)!important;}
.stTabs [data-baseweb="tab-list"]{background:var(--surface);border-radius:12px;padding:4px;
  gap:2px;border:1px solid var(--border);}
.stTabs [data-baseweb="tab"]{border-radius:8px;font-family:'Barlow Condensed',sans-serif;
  letter-spacing:1px;color:var(--muted);font-size:.9rem;}
.stTabs [aria-selected="true"]{background:rgba(57,255,20,.12)!important;color:var(--green)!important;}
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
#  CONSTANTS
# ════════════════════════════════════════════════════════════════
ESPN_SOCCER   = "https://site.api.espn.com/apis/site/v2/sports/soccer"
ESPN_CORE     = "https://sports.core.api.espn.com/v2/sports/soccer"
TSDB_BASE     = "https://www.thesportsdb.com/api/v1/json/3"
CAT_OFFSET    = timedelta(hours=2)
WINDOW_HOURS  = 8
MIN_GAMES     = 5
HISTORY_GAMES = 38
TOP_N         = 7
LEARNING_RATE = 0.006
DECAY_LAMBDA  = 0.04   # exponential decay per game (recent games weighted more)
ELO_K         = 20.0  # ELO update K-factor
ELO_BASE      = 1500.0
WEIGHTS_FILE  = os.path.join(os.path.dirname(__file__), "data", "weights_checkpoint.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Accept": "application/json",
}

BET_TYPES: Dict[str, Dict] = {
    "OVER_05":  {"label": "OVER 0.5 Goals",   "line": 0.5,  "gate": 86.0, "css": "over05", "emoji": "⚡"},
    "OVER_15":  {"label": "OVER 1.5 Goals",   "line": 1.5,  "gate": 76.0, "css": "over15", "emoji": "⚽"},
    "OVER_25":  {"label": "OVER 2.5 Goals",   "line": 2.5,  "gate": 63.0, "css": "over25", "emoji": "🔥"},
    "BTTS_YES": {"label": "Both Teams Score", "line": None, "gate": 68.0, "css": "btts",   "emoji": "🎯"},
    "HOME_WIN": {"label": "Home Win",          "line": None, "gate": 72.0, "css": "home",   "emoji": "🏠"},
    "AWAY_WIN": {"label": "Away Win",          "line": None, "gate": 72.0, "css": "away",   "emoji": "✈️"},
}

DEFAULT_WEIGHTS: Dict[str, Dict[str, float]] = {
    "OVER_05":  {"poisson_p":0.50,"hist_rate":0.20,"xg_norm":0.05,"form":0.10,"btts":0.05,"streak":0.05,"h2h":0.05},
    "OVER_15":  {"poisson_p":0.42,"hist_rate":0.26,"xg_norm":0.14,"form":0.08,"btts":0.05,"streak":0.00,"h2h":0.05},
    "OVER_25":  {"poisson_p":0.35,"hist_rate":0.25,"xg_norm":0.20,"btts":0.10,"form":0.05,"streak":0.00,"h2h":0.05},
    "BTTS_YES": {"poisson_btts":0.40,"hist_btts":0.35,"xg_balance":0.10,"form":0.10,"h2h":0.05},
    "HOME_WIN": {"poisson_hw":0.45,"hist_hw":0.25,"form_diff":0.15,"xg_diff":0.10,"h2h":0.05},
    "AWAY_WIN": {"poisson_aw":0.45,"hist_aw":0.25,"form_diff":0.15,"xg_diff":0.10,"h2h":0.05},
}

# League home advantage multipliers (empirically observed)
LEAGUE_HOME_ADV: Dict[str, float] = {
    "eng.1":1.05,"eng.2":1.04,"esp.1":1.08,"ger.1":1.03,"ita.1":1.07,"fra.1":1.06,
    "ned.1":1.05,"por.1":1.08,"tur.1":1.10,"bra.1":1.09,"arg.1":1.08,"mex.1":1.07,
    "usa.1":1.03,"jpn.1":1.05,"kor.1":1.04,"chn.1":1.06,"sau.1":1.09,"nga.1":1.11,
    "rsa.1":1.08,"egy.1":1.10,"uae.1":1.08,"qat.1":1.07,
}

LEAGUES: List[Tuple[str, str, str]] = [
    ("eng.1","Premier League","🏴󠁧󠁢󠁥󠁮󠁧󠁿"),("eng.2","Championship","🏴󠁧󠁢󠁥󠁮󠁧󠁿"),("eng.3","League One","🏴󠁧󠁢󠁥󠁮󠁧󠁿"),("eng.4","League Two","🏴󠁧󠁢󠁥󠁮󠁧󠁿"),
    ("esp.1","La Liga","🇪🇸"),("esp.2","Segunda División","🇪🇸"),
    ("ger.1","Bundesliga","🇩🇪"),("ger.2","2. Bundesliga","🇩🇪"),("ger.3","3. Liga","🇩🇪"),
    ("ita.1","Serie A","🇮🇹"),("ita.2","Serie B","🇮🇹"),
    ("fra.1","Ligue 1","🇫🇷"),("fra.2","Ligue 2","🇫🇷"),
    ("ned.1","Eredivisie","🇳🇱"),("ned.2","Eerste Divisie","🇳🇱"),
    ("por.1","Primeira Liga","🇵🇹"),("por.2","Liga Portugal 2","🇵🇹"),
    ("sco.1","Scottish Premiership","🏴󠁧󠁢󠁳󠁣󠁴󠁿"),("sco.2","Scottish Championship","🏴󠁧󠁢󠁳󠁣󠁴󠁿"),
    ("tur.1","Süper Lig","🇹🇷"),("tur.2","TFF First League","🇹🇷"),
    ("bel.1","Belgian Pro League","🇧🇪"),("gre.1","Super League Greece","🇬🇷"),
    ("ukr.1","Ukrainian Premier","🇺🇦"),("den.1","Superligaen","🇩🇰"),
    ("swe.1","Allsvenskan","🇸🇪"),("nor.1","Eliteserien","🇳🇴"),
    ("aut.1","Austrian Bundesliga","🇦🇹"),("sui.1","Swiss Super League","🇨🇭"),
    ("cze.1","Czech First League","🇨🇿"),("pol.1","Ekstraklasa","🇵🇱"),
    ("rou.1","Liga 1 Romania","🇷🇴"),("srb.1","Serbian SuperLiga","🇷🇸"),
    ("hun.1","OTP Bank Liga","🇭🇺"),("bul.1","First Professional League","🇧🇬"),
    ("cro.1","HNL Croatia","🇭🇷"),("svk.1","Fortuna Liga Slovakia","🇸🇰"),
    ("fin.1","Veikkausliiga","🇫🇮"),("isr.1","Israeli Premier","🇮🇱"),
    ("rus.1","Russian Premier","🇷🇺"),
    ("usa.1","MLS","🇺🇸"),("usa.2","USL Championship","🇺🇸"),
    ("mex.1","Liga MX","🇲🇽"),("mex.2","Ascenso MX","🇲🇽"),
    ("bra.1","Brasileirão","🇧🇷"),("bra.2","Série B","🇧🇷"),
    ("arg.1","Primera División","🇦🇷"),("col.1","Liga Betplay","🇨🇴"),
    ("chi.1","Primera Chile","🇨🇱"),("ecu.1","Liga Pro Ecuador","🇪🇨"),
    ("per.1","Liga 1 Peru","🇵🇪"),("uru.1","Uruguay Primera","🇺🇾"),
    ("ven.1","Liga Futve","🇻🇪"),("par.1","División Profesional","🇵🇾"),
    ("jpn.1","J1 League","🇯🇵"),("jpn.2","J2 League","🇯🇵"),
    ("kor.1","K League 1","🇰🇷"),("chn.1","Chinese Super League","🇨🇳"),
    ("aus.1","A-League","🇦🇺"),("ind.1","Indian Super League","🇮🇳"),
    ("tha.1","Thai League 1","🇹🇭"),("mys.1","Super League Malaysia","🇲🇾"),
    ("sau.1","Saudi Pro League","🇸🇦"),("uae.1","UAE Pro League","🇦🇪"),
    ("egy.1","Egyptian Premier","🇪🇬"),("rsa.1","PSL South Africa","🇿🇦"),
    ("mar.1","Botola Pro Morocco","🇲🇦"),("nga.1","NPFL Nigeria","🇳🇬"),
    ("qat.1","Qatar Stars League","🇶🇦"),
    ("uefa.champions","Champions League","🏆"),("uefa.europa","Europa League","🟠"),
    ("uefa.europaconference","Conference League","🟢"),
    ("conmebol.libertadores","Copa Libertadores","🏆"),
    ("concacaf.champions","CONCACAF Champions","🌎"),
]

# ════════════════════════════════════════════════════════════════
#  SECTION 1: POISSON + DIXON-COLES MATHEMATICS ENGINE
# ════════════════════════════════════════════════════════════════

def _pmf(k: int, lam: float) -> float:
    """Poisson PMF — uses scipy if available for accuracy."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    if HAS_SCIPY:
        return float(_scipy_poisson.pmf(k, lam))
    try:
        return (lam ** k) * math.exp(-lam) / math.factorial(k)
    except Exception:
        return 0.0


def _dc_tau(home: int, away: int, lam_h: float, lam_a: float, rho: float) -> float:
    """
    Dixon-Coles correction for low-scoring outcomes.
    Corrects Poisson over-estimates 0-0 and under-estimates 1-0/0-1.
    """
    if home == 0 and away == 0:
        return max(0.001, 1.0 - lam_h * lam_a * rho)
    if home == 1 and away == 0:
        return max(0.001, 1.0 + lam_a * rho)
    if home == 0 and away == 1:
        return max(0.001, 1.0 + lam_h * rho)
    if home == 1 and away == 1:
        return max(0.001, 1.0 - rho)
    return 1.0


def _joint_prob(h: int, a: int, lam_h: float, lam_a: float, rho: float = -0.13) -> float:
    """Joint probability of exact scoreline with Dixon-Coles correction."""
    return _pmf(h, lam_h) * _pmf(a, lam_a) * _dc_tau(h, a, lam_h, lam_a, rho)


def poisson_over_line(lam_home: float, lam_away: float, line: float, rho: float = -0.13) -> float:
    lam_home, lam_away = max(0.05, lam_home), max(0.05, lam_away)
    p_under = 0.0
    cap = 16
    for h in range(cap):
        for a in range(cap):
            if (h + a) <= line:
                p_under += _joint_prob(h, a, lam_home, lam_away, rho)
    return max(0.0, min(1.0, 1.0 - p_under))


def poisson_btts(lam_home: float, lam_away: float) -> float:
    lam_home, lam_away = max(0.05, lam_home), max(0.05, lam_away)
    return max(0.0, min(1.0, (1 - _pmf(0, lam_home)) * (1 - _pmf(0, lam_away))))


def poisson_home_win(lam_home: float, lam_away: float, rho: float = -0.13) -> float:
    lam_home, lam_away = max(0.05, lam_home), max(0.05, lam_away)
    p = 0.0
    cap = 16
    for h in range(cap):
        for a in range(cap):
            if h > a:
                p += _joint_prob(h, a, lam_home, lam_away, rho)
    return max(0.0, min(1.0, p))


def poisson_away_win(lam_home: float, lam_away: float, rho: float = -0.13) -> float:
    lam_home, lam_away = max(0.05, lam_home), max(0.05, lam_away)
    p = 0.0
    cap = 16
    for h in range(cap):
        for a in range(cap):
            if a > h:
                p += _joint_prob(h, a, lam_home, lam_away, rho)
    return max(0.0, min(1.0, p))


def safe_mean(lst: list, weights: Optional[list] = None) -> float:
    if not lst:
        return 0.0
    if weights:
        wsum = sum(weights)
        if wsum > 0:
            return float(sum(v * w for v, w in zip(lst, weights)) / wsum)
    return float(np.mean(lst))


def exp_weights(n: int) -> List[float]:
    """Exponential decay weights — most recent game gets highest weight."""
    return [math.exp(DECAY_LAMBDA * i) for i in range(n)]


# ════════════════════════════════════════════════════════════════
#  SECTION 2: ELO RATING ENGINE
# ════════════════════════════════════════════════════════════════

def elo_expected(elo_home: float, elo_away: float, home_adv: float = 50.0) -> float:
    return 1.0 / (1.0 + 10.0 ** ((elo_away - elo_home - home_adv) / 400.0))


def elo_update(elo: float, expected: float, actual: float) -> float:
    return elo + ELO_K * (actual - expected)


def score_to_outcome(home_score: int, away_score: int) -> Tuple[float, float]:
    if home_score > away_score:
        return 1.0, 0.0
    if away_score > home_score:
        return 0.0, 1.0
    return 0.5, 0.5


# ════════════════════════════════════════════════════════════════
#  SECTION 3: DATABASE + LEARNING ENGINE
# ════════════════════════════════════════════════════════════════

_db_lock = threading.Lock()


@st.cache_resource
def get_db() -> sqlite3.Connection:
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "zeus_v4.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS api_cache (
            cache_key TEXT PRIMARY KEY, data TEXT, ts REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS picks_log (
            id           TEXT PRIMARY KEY,
            match        TEXT,
            league       TEXT,
            league_id    TEXT,
            bet          TEXT,
            bet_type     TEXT DEFAULT 'OVER_25',
            xg_total     REAL,
            confidence   REAL,
            raw_prob     REAL DEFAULT 0.0,
            kickoff      TEXT,
            result       TEXT DEFAULT 'pending',
            home_score   INTEGER DEFAULT -1,
            away_score   INTEGER DEFAULT -1,
            factors_json TEXT DEFAULT '{}',
            logged_at    TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS model_weights (
            bet_type TEXT,
            factor   TEXT,
            weight   REAL,
            wins     INTEGER DEFAULT 0,
            losses   INTEGER DEFAULT 0,
            updates  INTEGER DEFAULT 0,
            brier_contrib REAL DEFAULT 0.0,
            PRIMARY KEY (bet_type, factor)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS elo_ratings (
            team_id  TEXT PRIMARY KEY,
            team_name TEXT,
            elo      REAL DEFAULT 1500.0,
            games    INTEGER DEFAULT 0,
            updated  TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calibration (
            bet_type TEXT,
            bucket   TEXT,
            pred_sum REAL DEFAULT 0.0,
            actual   INTEGER DEFAULT 0,
            count    INTEGER DEFAULT 0,
            PRIMARY KEY (bet_type, bucket)
        )
    """)

    # Auto-migrate picks_log
    for col, defval in [
        ("bet_type","'OVER_25'"),("home_score","-1"),("away_score","-1"),
        ("factors_json","'{}'"),("raw_prob","0.0"),
    ]:
        try:
            conn.execute(f"ALTER TABLE picks_log ADD COLUMN {col} TEXT DEFAULT {defval}")
        except Exception:
            pass

    conn.commit()
    _init_weights(conn)
    return conn


def _init_weights(conn: sqlite3.Connection):
    """Seed from checkpoint file first, then default dict."""
    source = DEFAULT_WEIGHTS.copy()
    try:
        if os.path.exists(WEIGHTS_FILE):
            with open(WEIGHTS_FILE, "r") as f:
                ckpt = json.load(f)
            for bt in DEFAULT_WEIGHTS:
                if bt in ckpt:
                    source[bt] = ckpt[bt]
    except Exception:
        pass

    for bet_type, factors in source.items():
        for factor, w in factors.items():
            conn.execute(
                "INSERT OR IGNORE INTO model_weights (bet_type,factor,weight) VALUES (?,?,?)",
                (bet_type, factor, float(w))
            )
    conn.commit()


def get_weights(bet_type: str) -> Dict[str, float]:
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT factor, weight FROM model_weights WHERE bet_type=?", (bet_type,)
        ).fetchall()
        if not rows:
            return DEFAULT_WEIGHTS.get(bet_type, {})
        w = {r[0]: max(0.01, float(r[1])) for r in rows}
        total = sum(w.values())
        return {k: v / total for k, v in w.items()} if total > 0 else w
    except Exception:
        return DEFAULT_WEIGHTS.get(bet_type, {})


def update_weights(bet_type: str, factors: Dict[str, float], won: bool, raw_prob: float = 0.5):
    """
    Brier-score gradient update.
    Brier = (prediction - outcome)^2
    Gradient w.r.t. factor weight ≈ 2*(pred - outcome) * factor_contribution
    """
    try:
        conn = get_db()
        current = get_weights(bet_type)
        outcome = 1.0 if won else 0.0
        pred = raw_prob  # use calibrated prediction

        brier_gradient = 2.0 * (pred - outcome)  # positive if over-predicted

        new_w = {}
        for factor, val in factors.items():
            if factor not in current:
                continue
            # Contribution of this factor to the prediction
            contribution = val - 0.5  # centered: >0 means factor was bullish
            # Gradient descent: reduce weight if contributed to wrong prediction
            delta = -LEARNING_RATE * brier_gradient * contribution
            new_w[factor] = max(0.01, min(0.75, current[factor] + delta))

        # Normalize
        total = sum(new_w.values())
        if total > 0:
            new_w = {k: v / total for k, v in new_w.items()}

        brier_val = (pred - outcome) ** 2
        result_col = "wins" if won else "losses"
        for factor, weight in new_w.items():
            with _db_lock:
                conn.execute(
                    f"""UPDATE model_weights
                        SET weight=?, {result_col}={result_col}+1,
                            updates=updates+1, brier_contrib=brier_contrib+?
                        WHERE bet_type=? AND factor=?""",
                    (weight, brier_val, bet_type, factor)
                )
        conn.commit()

        # Save checkpoint
        _save_weights_checkpoint(conn)

        # Update calibration
        bucket = f"{int(raw_prob * 10) * 10}-{int(raw_prob * 10) * 10 + 10}"
        with _db_lock:
            conn.execute("""
                INSERT INTO calibration (bet_type,bucket,pred_sum,actual,count)
                VALUES (?,?,?,?,1)
                ON CONFLICT(bet_type,bucket) DO UPDATE SET
                    pred_sum=pred_sum+excluded.pred_sum,
                    actual=actual+excluded.actual,
                    count=count+1
            """, (bet_type, bucket, raw_prob, 1 if won else 0))
        conn.commit()

    except Exception:
        pass


def _save_weights_checkpoint(conn: sqlite3.Connection):
    """Save current weights to JSON for persistence across restarts."""
    try:
        rows = conn.execute("SELECT bet_type, factor, weight FROM model_weights").fetchall()
        ckpt: Dict[str, Any] = {
            "_meta": {
                "version": "4.0",
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "description": "Commit this file to persist learned weights"
            }
        }
        for bet_type, factor, weight in rows:
            if bet_type not in ckpt:
                ckpt[bet_type] = {}
            ckpt[bet_type][factor] = round(float(weight), 6)
        os.makedirs(os.path.dirname(WEIGHTS_FILE), exist_ok=True)
        with open(WEIGHTS_FILE, "w") as f:
            json.dump(ckpt, f, indent=2)
    except Exception:
        pass


def get_elo(team_id: str, team_name: str) -> float:
    try:
        conn = get_db()
        row = conn.execute("SELECT elo FROM elo_ratings WHERE team_id=?", (team_id,)).fetchone()
        return float(row[0]) if row else ELO_BASE
    except Exception:
        return ELO_BASE


def update_elo(home_id: str, home_name: str, away_id: str, away_name: str,
               home_score: int, away_score: int, league_id: str):
    try:
        conn = get_db()
        elo_h = get_elo(home_id, home_name)
        elo_a = get_elo(away_id, away_name)
        home_adv = 40.0  # home advantage in ELO points
        exp_h = elo_expected(elo_h, elo_a, home_adv)
        act_h, act_a = score_to_outcome(home_score, away_score)
        new_h = elo_update(elo_h, exp_h, act_h)
        new_a = elo_update(elo_a, 1.0 - exp_h, act_a)
        now = datetime.now(timezone.utc).isoformat()
        with _db_lock:
            conn.execute("""
                INSERT INTO elo_ratings (team_id,team_name,elo,games,updated)
                VALUES (?,?,?,1,?)
                ON CONFLICT(team_id) DO UPDATE SET
                    elo=excluded.elo, team_name=excluded.team_name,
                    games=games+1, updated=excluded.updated
            """, (home_id, home_name, new_h, now))
            conn.execute("""
                INSERT INTO elo_ratings (team_id,team_name,elo,games,updated)
                VALUES (?,?,?,1,?)
                ON CONFLICT(team_id) DO UPDATE SET
                    elo=excluded.elo, team_name=excluded.team_name,
                    games=games+1, updated=excluded.updated
            """, (away_id, away_name, new_a, now))
        conn.commit()
    except Exception:
        pass


# ── Cache helpers ────────────────────────────────────────────────

def cache_get(key: str, ttl: int) -> Optional[Any]:
    try:
        conn = get_db()
        row = conn.execute("SELECT data, ts FROM api_cache WHERE cache_key=?", (key,)).fetchone()
        if row and (time.time() - row[1]) < ttl:
            return json.loads(row[0])
    except Exception:
        pass
    return None


def cache_set(key: str, data: Any):
    try:
        conn = get_db()
        with _db_lock:
            conn.execute("INSERT OR REPLACE INTO api_cache VALUES (?,?,?)",
                         (key, json.dumps(data, default=str), time.time()))
        conn.commit()
    except Exception:
        pass


def save_pick(pick: Dict):
    try:
        pid = hashlib.md5(f"{pick['match']}{pick['kickoff_utc']}{pick['bet_type']}".encode()).hexdigest()[:12]
        conn = get_db()
        with _db_lock:
            conn.execute("""
                INSERT OR IGNORE INTO picks_log
                (id,match,league,league_id,bet,bet_type,xg_total,confidence,raw_prob,
                 kickoff,factors_json,logged_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                pid, pick["match"], pick["league"], pick.get("league_id", ""),
                pick["bet"], pick["bet_type"], pick["xg_total"],
                pick["confidence"], pick.get("raw_prob", pick["confidence"] / 100.0),
                pick["kickoff_utc"], json.dumps(pick.get("factors", {})),
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            ))
        conn.commit()
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════
#  SECTION 4: TIME HELPERS
# ════════════════════════════════════════════════════════════════

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_cat(utc_str: str) -> str:
    try:
        dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        return (dt + CAT_OFFSET).strftime("%d %b · %H:%M CAT")
    except Exception:
        return "—"


def parse_utc(utc_str: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
    except Exception:
        return None


def in_window(utc_str: str) -> bool:
    dt = parse_utc(utc_str)
    if not dt:
        return False
    n = now_utc()
    return n <= dt <= n + timedelta(hours=WINDOW_HOURS)


def minutes_to_kickoff(utc_str: str) -> int:
    dt = parse_utc(utc_str)
    if not dt:
        return 9999
    return max(0, int((dt - now_utc()).total_seconds() / 60))


# ════════════════════════════════════════════════════════════════
#  SECTION 5: MULTI-SOURCE DATA FETCHERS (parallel)
# ════════════════════════════════════════════════════════════════

def safe_get(url: str, params: Dict = None, timeout: int = 10) -> Optional[Dict]:
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def _parse_score(raw) -> int:
    if raw is None:
        return 0
    if isinstance(raw, dict):
        raw = raw.get("value", raw.get("displayValue", 0))
    try:
        return int(float(str(raw)))
    except (ValueError, TypeError):
        return 0


def fetch_scoreboard(league_id: str) -> List[Dict]:
    result = []
    for delta in [0, 1]:
        date_str = (now_utc() + timedelta(days=delta)).strftime("%Y%m%d")
        key = f"sb4_{league_id}_{date_str}"
        cached = cache_get(key, ttl=240)
        if cached is not None:
            result.extend(cached)
            continue
        data = safe_get(f"{ESPN_SOCCER}/{league_id}/scoreboard", params={"dates": date_str})
        if not data:
            continue
        events = []
        for ev in data.get("events", []):
            comps = ev.get("competitions", [])
            if not comps:
                continue
            comp = comps[0]
            competitors = comp.get("competitors", [])
            if len(competitors) < 2:
                continue
            home_c = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
            away_c = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])
            status_type = comp.get("status", {}).get("type", {})
            events.append({
                "event_id":  ev.get("id", ""),
                "date":      ev.get("date", ""),
                "home_id":   str(home_c.get("team", {}).get("id", "")),
                "home_name": home_c.get("team", {}).get("displayName", ""),
                "away_id":   str(away_c.get("team", {}).get("id", "")),
                "away_name": away_c.get("team", {}).get("displayName", ""),
                "status":    status_type.get("name", ""),
                "completed": status_type.get("completed", False),
                "league_id": league_id,
            })
        cache_set(key, events)
        result.extend(events)
    return result


def fetch_team_schedule_espn(league_id: str, team_id: str) -> List[Dict]:
    date_tag = now_utc().strftime("%Y%m%d")
    key = f"sched4_{league_id}_{team_id}_{date_tag}"
    cached = cache_get(key, ttl=3600)
    if cached is not None:
        return cached

    data = safe_get(f"{ESPN_SOCCER}/{league_id}/teams/{team_id}/schedule")
    if not data:
        return []

    games = []
    for ev in data.get("events", []):
        comps = ev.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        if not comp.get("status", {}).get("type", {}).get("completed", False):
            continue
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue
        home_c = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
        away_c = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])
        hs  = _parse_score(home_c.get("score"))
        as_ = _parse_score(away_c.get("score"))
        home_id = str(home_c.get("team", {}).get("id", ""))
        away_id = str(away_c.get("team", {}).get("id", ""))
        games.append({
            "date":       ev.get("date", ""),
            "home_name":  home_c.get("team", {}).get("displayName", ""),
            "away_name":  away_c.get("team", {}).get("displayName", ""),
            "home_id":    home_id,
            "away_id":    away_id,
            "home_score": hs,
            "away_score": as_,
            "total":      hs + as_,
        })

    games.sort(key=lambda g: g.get("date", ""))
    games = games[-HISTORY_GAMES:]
    cache_set(key, games)
    return games


def fetch_tsdb_team_last15(team_name: str) -> List[Dict]:
    key = f"tsdb4_{hashlib.md5(team_name.encode()).hexdigest()[:10]}"
    cached = cache_get(key, ttl=7200)
    if cached is not None:
        return cached

    sr = safe_get(f"{TSDB_BASE}/searchteams.php", params={"t": team_name}, timeout=6)
    if not sr or not sr.get("teams"):
        return []
    team_id = sr["teams"][0].get("idTeam", "")
    if not team_id:
        return []

    er = safe_get(f"{TSDB_BASE}/eventslast15.php", params={"id": team_id}, timeout=6)
    if not er or not er.get("results"):
        return []

    games = []
    for ev in er["results"]:
        try:
            hs  = int(ev.get("intHomeScore", 0) or 0)
            as_ = int(ev.get("intAwayScore", 0) or 0)
            home = ev.get("strHomeTeam", "")
            away = ev.get("strAwayTeam", "")
            if not home or not away:
                continue
            games.append({
                "date":       ev.get("dateEvent", ""),
                "home_name":  home,
                "away_name":  away,
                "home_id":    "",
                "away_id":    "",
                "home_score": hs,
                "away_score": as_,
                "total":      hs + as_,
            })
        except Exception:
            pass

    cache_set(key, games)
    return games


def fetch_league_standings(league_id: str) -> Dict[str, int]:
    """Returns team_name → league position mapping."""
    key = f"stand4_{league_id}_{now_utc().strftime('%Y%W')}"
    cached = cache_get(key, ttl=86400)
    if cached is not None:
        return cached

    data = safe_get(f"{ESPN_SOCCER}/{league_id}/standings")
    if not data:
        return {}

    positions = {}
    try:
        for group in data.get("standings", {}).get("groups", []):
            for i, entry in enumerate(group.get("standings", {}).get("entries", []), 1):
                name = entry.get("team", {}).get("displayName", "")
                if name:
                    positions[name] = i
    except Exception:
        pass

    cache_set(key, positions)
    return positions


def fetch_team_schedule(league_id: str, team_id: str, team_name: str) -> List[Dict]:
    espn_games = fetch_team_schedule_espn(league_id, team_id)
    if len(espn_games) >= MIN_GAMES:
        return espn_games

    tsdb_games = fetch_tsdb_team_last15(team_name)
    if not tsdb_games:
        return espn_games

    seen = set()
    combined = []
    for g in espn_games + tsdb_games:
        k = f"{g.get('date','')[:10]}_{g.get('home_name','')}_{g.get('away_name','')}"
        if k not in seen:
            seen.add(k)
            combined.append(g)

    combined.sort(key=lambda g: g.get("date", ""))
    return combined[-HISTORY_GAMES:]


def parallel_fetch_scoreboards(leagues: List[Tuple]) -> Dict[str, List[Dict]]:
    """Fetch all league scoreboards in parallel."""
    results: Dict[str, List[Dict]] = {}

    def _fetch(lid):
        return lid, fetch_scoreboard(lid)

    with ThreadPoolExecutor(max_workers=15) as ex:
        futures = {ex.submit(_fetch, lid): lid for lid, _, _ in leagues}
        for fut in as_completed(futures):
            try:
                lid, data = fut.result(timeout=15)
                results[lid] = data
            except Exception:
                results[futures[fut]] = []

    return results


def parallel_fetch_schedules(pairs: List[Tuple[str, str, str]]) -> Dict[str, List[Dict]]:
    """Fetch team schedules in parallel. pairs = (league_id, team_id, team_name)"""
    results: Dict[str, List[Dict]] = {}

    def _fetch(lid, tid, tname):
        return f"{lid}_{tid}", fetch_team_schedule(lid, tid, tname)

    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(_fetch, lid, tid, tname): f"{lid}_{tid}"
                   for lid, tid, tname in pairs}
        for fut in as_completed(futures):
            try:
                key, data = fut.result(timeout=20)
                results[key] = data
            except Exception:
                results[futures[fut]] = []

    return results


# ════════════════════════════════════════════════════════════════
#  SECTION 6: STATISTICS ENGINE (30+ features, exponential decay)
# ════════════════════════════════════════════════════════════════

def team_stats(games: List[Dict], team_name: str, league_id: str = "") -> Optional[Dict]:
    completed = [
        g for g in games
        if g.get("home_score", -1) >= 0 and g.get("away_score", -1) >= 0
    ]
    if len(completed) < MIN_GAMES:
        return None

    # Separate home and away games
    home_games = [g for g in completed if g.get("home_name", "") == team_name]
    away_games = [g for g in completed if g.get("away_name", "") == team_name]

    def _split(gl, sc_key, co_key):
        if not gl:
            return None, None, None, None, None
        sc = [g[sc_key] for g in gl]
        co = [g[co_key] for g in gl]
        tot = [s + c for s, c in zip(sc, co)]
        n = len(gl)
        ew = exp_weights(n)
        return (
            safe_mean(sc, ew),
            safe_mean(co, ew),
            sum(1 for t in tot if t > 0.5) / n,
            sum(1 for t in tot if t > 1.5) / n,
            sum(1 for s, c in zip(sc, co) if s > 0 and c > 0) / n,
        )

    h_sc, h_co, h_o05, h_o15, h_btts = _split(home_games, "home_score", "away_score")
    a_sc, a_co, a_o05, a_o15, a_btts = _split(away_games, "away_score", "home_score")

    # Overall with exponential decay
    all_sc, all_co = [], []
    for g in completed:
        is_home = g.get("home_name", "") == team_name
        sc  = g["home_score"] if is_home else g["away_score"]
        co  = g["away_score"] if is_home else g["home_score"]
        all_sc.append(sc)
        all_co.append(co)

    n = len(completed)
    ew = exp_weights(n)
    all_tot = [s + c for s, c in zip(all_sc, all_co)]

    avg_sc   = safe_mean(all_sc, ew)
    avg_co   = safe_mean(all_co, ew)
    avg_tot  = safe_mean(all_tot, ew)

    # Weighted rates using exponential decay
    def weighted_rate(cond_list):
        return safe_mean(cond_list, ew)

    over05_r = weighted_rate([1.0 if t > 0.5 else 0.0 for t in all_tot])
    over15_r = weighted_rate([1.0 if t > 1.5 else 0.0 for t in all_tot])
    over25_r = weighted_rate([1.0 if t > 2.5 else 0.0 for t in all_tot])
    btts_r   = weighted_rate([1.0 if s > 0 and c > 0 else 0.0 for s, c in zip(all_sc, all_co)])
    cs_r     = weighted_rate([1.0 if c == 0 else 0.0 for c in all_co])
    wins_r   = weighted_rate([1.0 if s > c else 0.0 for s, c in zip(all_sc, all_co)])
    draw_r   = weighted_rate([1.0 if s == c else 0.0 for s, c in zip(all_sc, all_co)])

    # Scoring consistency (coefficient of variation — lower = more consistent)
    std_sc  = float(np.std(all_sc)) if len(all_sc) > 1 else 0.5
    cv_sc   = std_sc / (avg_sc + 0.1)

    # Form momentum: compare last 5 vs previous 5
    recent5 = all_tot[-5:] if n >= 5 else all_tot
    older   = all_tot[max(0, n-10):n-5] if n >= 10 else (all_tot[:-5] if n > 5 else all_tot)
    form_score = max(0.0, min(1.0, 0.5 + (safe_mean(recent5) - safe_mean(older)) / 4.0))

    # ELO-weighted form (recent 3 game win %)
    recent3_wins = sum(1 for s, c in zip(all_sc[-3:], all_co[-3:]) if s > c) / max(1, min(3, n))
    last3_avg    = safe_mean(all_tot[-3:]) if n >= 3 else avg_tot

    # Streak counters (clean, sequential)
    def count_streak(values, threshold):
        streak = 0
        for v in reversed(values):
            if v > threshold:
                streak += 1
            else:
                break
        return streak

    streak_over05 = count_streak(all_tot, 0.5)
    streak_over15 = count_streak(all_tot, 1.5)
    streak_over25 = count_streak(all_tot, 2.5)
    streak_btts   = count_streak(
        [1 if s > 0 and c > 0 else 0 for s, c in zip(all_sc, all_co)], 0.5
    )
    streak_wins   = count_streak(
        [1 if s > c else 0 for s, c in zip(all_sc, all_co)], 0.5
    )

    # League home advantage multiplier
    home_adv = LEAGUE_HOME_ADV.get(league_id, 1.05)

    return {
        "n": n, "n_home": len(home_games), "n_away": len(away_games),
        "avg_scored":  avg_sc, "avg_conceded": avg_co, "avg_total": avg_tot,
        "over05_rate": over05_r, "over15_rate": over15_r, "over25_rate": over25_r,
        "btts_rate": btts_r, "cs_rate": cs_r, "wins_rate": wins_r, "draw_rate": draw_r,
        # Venue splits
        "home_avg_scored":   h_sc   if h_sc   is not None else avg_sc,
        "home_avg_conceded": h_co   if h_co   is not None else avg_co,
        "home_over05_rate":  h_o05  if h_o05  is not None else over05_r,
        "home_over15_rate":  h_o15  if h_o15  is not None else over15_r,
        "home_btts_rate":    h_btts if h_btts is not None else btts_r,
        "away_avg_scored":   a_sc   if a_sc   is not None else avg_sc,
        "away_avg_conceded": a_co   if a_co   is not None else avg_co,
        "away_over05_rate":  a_o05  if a_o05  is not None else over05_r,
        "away_over15_rate":  a_o15  if a_o15  is not None else over15_r,
        "away_btts_rate":    a_btts if a_btts is not None else btts_r,
        # Form & Consistency
        "form_score": form_score, "last3_avg": last3_avg,
        "recent3_wins": recent3_wins, "cv_scored": cv_sc,
        # Streaks
        "streak_over05": streak_over05, "streak_over15": streak_over15,
        "streak_over25": streak_over25, "streak_btts": streak_btts,
        "streak_wins": streak_wins,
        # League
        "home_adv_mult": home_adv,
    }


def get_h2h_stats(home_sched, away_sched, home_name, away_name) -> Optional[Dict]:
    seen, totals, home_wins, away_wins, bttss = set(), [], [], [], []
    for g in home_sched + away_sched:
        gk = f"{g.get('date','')[:10]}_{g.get('home_name','')}_{g.get('away_name','')}"
        if gk in seen:
            continue
        seen.add(gk)
        if {home_name, away_name} != {g.get("home_name",""), g.get("away_name","")}:
            continue
        hs, as_ = g.get("home_score", 0), g.get("away_score", 0)
        t = hs + as_
        totals.append(t)
        home_wins.append(1 if hs > as_ else 0)
        away_wins.append(1 if as_ > hs else 0)
        bttss.append(1 if hs > 0 and as_ > 0 else 0)

    if len(totals) < 3:
        return None
    n = len(totals)
    return {
        "over05": sum(1 for t in totals if t > 0.5) / n,
        "over15": sum(1 for t in totals if t > 1.5) / n,
        "over25": sum(1 for t in totals if t > 2.5) / n,
        "btts":   sum(bttss) / n,
        "home_w": sum(home_wins) / n,
        "away_w": sum(away_wins) / n,
        "avg_tot": safe_mean(totals),
        "count":  n,
    }


# ════════════════════════════════════════════════════════════════
#  SECTION 7: PREDICTION ENGINE (xG + confidence per bet type)
# ════════════════════════════════════════════════════════════════

def _xg(home_st: Dict, away_st: Dict, league_id: str = "") -> Tuple[float, float]:
    """
    Venue-split xG with Dixon-Coles calibration and league home advantage.
    Dixon-Coles original formula uses attack strength × defense weakness × league average.
    We approximate with venue-split scored/conceded averages.
    """
    home_adv = home_st.get("home_adv_mult", 1.05)

    # Weighted venue-split xG
    xg_h = (
        0.55 * home_st["home_avg_scored"] * home_adv +
        0.45 * away_st["away_avg_conceded"]
    )
    xg_a = (
        0.55 * away_st["away_avg_scored"] +
        0.45 * home_st["home_avg_conceded"] / home_adv
    )
    return max(0.05, xg_h), max(0.05, xg_a)


def compute_over_confidence(
    home_st: Dict, away_st: Dict, line: float,
    h2h: Optional[Dict], bet_type: str, league_id: str = ""
) -> Tuple[float, float, Dict[str, float], str]:
    """Returns (confidence%, raw_prob, factor_values, reasoning)."""
    xg_h, xg_a = _xg(home_st, away_st, league_id)
    total_xg = xg_h + xg_a

    # Poisson with Dixon-Coles
    pois_p = poisson_over_line(xg_h, xg_a, line)

    # Historical rate — venue-weighted
    rate_map = {
        0.5: ("home_over05_rate", "away_over05_rate"),
        1.5: ("home_over15_rate", "away_over15_rate"),
        2.5: ("over25_rate", "over25_rate"),
    }
    hk, ak = rate_map.get(line, ("over25_rate", "over25_rate"))
    hist_combined = home_st.get(hk, home_st["over05_rate"]) * 0.5 + \
                    away_st.get(ak, away_st["over05_rate"]) * 0.5

    # xG normalized
    xg_ranges = {0.5: (0.3, 2.0), 1.5: (0.8, 3.5), 2.5: (1.5, 5.0)}
    xg_min, xg_max = xg_ranges.get(line, (1.5, 5.0))
    xg_norm = max(0.0, min(1.0, (total_xg - xg_min) / (xg_max - xg_min)))

    # BTTS
    btts_combined = (home_st["home_btts_rate"] + away_st["away_btts_rate"]) / 2

    # Form
    form_combined = (home_st["form_score"] + away_st["form_score"]) / 2

    # Streak
    streak_key_map = {0.5: "streak_over05", 1.5: "streak_over15", 2.5: "streak_over25"}
    sk = streak_key_map.get(line, "streak_over25")
    streak_val = min(1.0, (home_st.get(sk, 0) + away_st.get(sk, 0)) / 8.0)

    # H2H
    h2h_key_map = {0.5: "over05", 1.5: "over15", 2.5: "over25"}
    hk2 = h2h_key_map.get(line, "over25")
    h2h_val = h2h[hk2] if h2h else hist_combined

    factors = {
        "poisson_p":  pois_p,
        "hist_rate":  hist_combined,
        "xg_norm":    xg_norm,
        "form":       form_combined,
        "btts":       btts_combined,
        "streak":     streak_val,
        "h2h":        h2h_val,
    }

    weights = get_weights(bet_type)
    raw_prob = sum(factors.get(k, 0.5) * w for k, w in weights.items())
    raw_prob = max(0.01, min(0.999, raw_prob))
    confidence = raw_prob * 100

    reasoning = (
        f"Poisson+DC P(OVER {line}): {pois_p*100:.1f}% · "
        f"xG {xg_h:.2f}+{xg_a:.2f}={total_xg:.2f} · "
        f"Hist OVER {line}: {hist_combined*100:.0f}% · BTTS: {btts_combined*100:.0f}%"
    )
    if home_st.get(sk, 0) >= 3:
        reasoning += f" · Home {home_st[sk]}g OVER streak 🔥"
    if away_st.get(sk, 0) >= 3:
        reasoning += f" · Away {away_st[sk]}g OVER streak 🔥"
    if h2h:
        reasoning += f" · H2H({h2h['count']}g) OVER {line}: {h2h[hk2]*100:.0f}%"

    return round(confidence, 1), round(raw_prob, 4), factors, reasoning


def compute_btts_confidence(
    home_st: Dict, away_st: Dict, h2h: Optional[Dict], league_id: str = ""
) -> Tuple[float, float, Dict[str, float], str]:
    xg_h, xg_a = _xg(home_st, away_st, league_id)

    pois_btts  = poisson_btts(xg_h, xg_a)
    hist_btts  = (home_st["home_btts_rate"] + away_st["away_btts_rate"]) / 2
    xg_balance = min(xg_h, xg_a) / max(xg_h, xg_a) if max(xg_h, xg_a) > 0 else 0.5
    form_comb  = (home_st["form_score"] + away_st["form_score"]) / 2
    h2h_btts   = h2h["btts"] if h2h else hist_btts

    factors = {
        "poisson_btts": pois_btts,
        "hist_btts":    hist_btts,
        "xg_balance":   xg_balance,
        "form":         form_comb,
        "h2h":          h2h_btts,
    }

    weights  = get_weights("BTTS_YES")
    raw_prob = max(0.01, min(0.999, sum(factors.get(k, 0.5) * w for k, w in weights.items())))
    confidence = raw_prob * 100

    reasoning = (
        f"Poisson+DC BTTS: {pois_btts*100:.1f}% · Hist BTTS: {hist_btts*100:.0f}% · "
        f"xG balance: {xg_balance:.2f} · xG {xg_h:.2f} vs {xg_a:.2f}"
    )
    if h2h:
        reasoning += f" · H2H BTTS: {h2h['btts']*100:.0f}%"

    return round(confidence, 1), round(raw_prob, 4), factors, reasoning


def compute_result_confidence(
    home_st: Dict, away_st: Dict, h2h: Optional[Dict], side: str, league_id: str = ""
) -> Tuple[float, float, Dict[str, float], str]:
    xg_h, xg_a = _xg(home_st, away_st, league_id)

    if side == "HOME":
        pois_p    = poisson_home_win(xg_h, xg_a)
        hist_rate = home_st["wins_rate"] * 0.6 + (1 - away_st["wins_rate"]) * 0.4
        form_diff = max(0.0, min(1.0, 0.5 + (home_st["form_score"] - away_st["form_score"]) / 2))
        xg_diff   = max(0.0, min(1.0, (xg_h - xg_a + 3) / 6))
        h2h_val   = h2h["home_w"] if h2h else hist_rate
        bt        = "HOME_WIN"
        factors   = {"poisson_hw": pois_p, "hist_hw": hist_rate,
                     "form_diff": form_diff, "xg_diff": xg_diff, "h2h": h2h_val}
    else:
        pois_p    = poisson_away_win(xg_h, xg_a)
        hist_rate = away_st["wins_rate"] * 0.6 + (1 - home_st["wins_rate"]) * 0.4
        form_diff = max(0.0, min(1.0, 0.5 + (away_st["form_score"] - home_st["form_score"]) / 2))
        xg_diff   = max(0.0, min(1.0, (xg_a - xg_h + 3) / 6))
        h2h_val   = h2h["away_w"] if h2h else hist_rate
        bt        = "AWAY_WIN"
        factors   = {"poisson_aw": pois_p, "hist_aw": hist_rate,
                     "form_diff": form_diff, "xg_diff": xg_diff, "h2h": h2h_val}

    weights  = get_weights(bt)
    raw_prob = max(0.01, min(0.999, sum(factors.get(k, 0.5) * w for k, w in weights.items())))
    confidence = raw_prob * 100

    team_label = "Home" if side == "HOME" else "Away"
    dom_form = home_st["form_score"] if side == "HOME" else away_st["form_score"]
    reasoning = (
        f"Poisson+DC {team_label} Win: {pois_p*100:.1f}% · "
        f"Hist Win Rate: {hist_rate*100:.0f}% · "
        f"xG {xg_h:.2f} vs {xg_a:.2f} · Form {team_label}: {dom_form:.2f}"
    )
    if h2h:
        hw = "home_w" if side == "HOME" else "away_w"
        reasoning += f" · H2H Win: {h2h[hw]*100:.0f}%"

    return round(confidence, 1), round(raw_prob, 4), factors, reasoning


# ════════════════════════════════════════════════════════════════
#  SECTION 8: ZEUS MULTI-BET SCANNER (parallel)
# ════════════════════════════════════════════════════════════════

def get_card_tier(conf: float, bet_type: str) -> Tuple[str, str]:
    """BUG FIX: Was using literal f-string text instead of actual f-string."""
    emoji = "🏠" if bet_type == "HOME_WIN" else "✈️"
    if bet_type == "BTTS_YES":
        if conf >= 78: return "elite", "🎯 ELITE BTTS"
        if conf >= 68: return "btts",  "🎯 BTTS LOCK"
        return "btts", "🎯 BTTS"
    if bet_type in ("HOME_WIN", "AWAY_WIN"):
        if conf >= 80: return "elite", f"{emoji} ELITE WIN"
        if conf >= 72: return "result", f"{emoji} STRONG WIN"
        return "result", f"{emoji} RESULT"
    if conf >= 80: return "elite",  "🔥 ELITE"
    if conf >= 70: return "strong", "⚡ STRONG"
    return "strong", "✅ CONFIDENT"


@st.cache_data(ttl=300, show_spinner=False)
def scan_all_leagues() -> Tuple[List[Dict], int, int, int]:
    candidates: List[Dict] = []
    leagues_hit = games_eval = data_pts = 0

    # Step 1: Parallel scoreboard fetch across all leagues
    all_scoreboards = parallel_fetch_scoreboards(LEAGUES)

    # Step 2: Collect all upcoming games that need schedule data
    window_pairs: List[Tuple[str, str, str, str, str, str, str]] = []
    # (league_id, league_name, flag, home_id, home_name, away_id, away_name)
    league_meta: Dict[str, Tuple[str, str]] = {
        lid: (lname, flag) for lid, lname, flag in LEAGUES
    }

    seen_games = set()
    for league_id, events in all_scoreboards.items():
        lname, flag = league_meta.get(league_id, (league_id, "🌍"))
        for ev in events:
            if ev.get("completed", False):
                continue
            if not in_window(ev.get("date", "")):
                continue
            gk = f"{ev['home_id']}_{ev['away_id']}_{ev.get('date','')[:13]}"
            if gk in seen_games:
                continue
            seen_games.add(gk)
            window_pairs.append((
                league_id, lname, flag,
                ev["home_id"], ev["home_name"],
                ev["away_id"], ev["away_name"],
                ev["date"], ev.get("event_id", "")
            ))

    if not window_pairs:
        return [], 0, 0, 0

    leagues_hit = len({p[0] for p in window_pairs})

    # Step 3: Parallel schedule fetch for all teams
    schedule_requests = []
    for row in window_pairs:
        lid, lname, flag, hid, hname, aid, aname, date, eid = row
        schedule_requests.append((lid, hid, hname))
        schedule_requests.append((lid, aid, aname))

    # Deduplicate
    schedule_requests = list({(lid, tid, tname): (lid, tid, tname)
                               for lid, tid, tname in schedule_requests}.values())

    all_schedules = parallel_fetch_schedules(schedule_requests)

    # Step 4: Evaluate each game
    for row in window_pairs:
        lid, lname, flag, hid, hname, aid, aname, date, eid = row

        home_sched = all_schedules.get(f"{lid}_{hid}", [])
        away_sched = all_schedules.get(f"{lid}_{aid}", [])

        if not home_sched:
            home_sched = fetch_team_schedule(lid, hid, hname)
        if not away_sched:
            away_sched = fetch_team_schedule(lid, aid, aname)

        data_pts += len(home_sched) + len(away_sched)

        home_st = team_stats(home_sched, hname, lid)
        away_st = team_stats(away_sched, aname, lid)
        if home_st is None or away_st is None:
            continue

        games_eval += 1
        h2h    = get_h2h_stats(home_sched, away_sched, hname, aname)
        xg_h, xg_a = _xg(home_st, away_st, lid)
        total_xg    = round(xg_h + xg_a, 2)

        # ELO
        elo_h = get_elo(hid, hname)
        elo_a = get_elo(aid, aname)
        elo_diff = round(elo_h - elo_a, 0)

        base = {
            "match":       f"{hname} vs {aname}",
            "home":        hname, "away": aname,
            "home_id":     hid, "away_id": aid,
            "league":      f"{flag} {lname}", "league_id": lid,
            "kickoff_utc": date,
            "kickoff_cat": to_cat(date),
            "mins_away":   minutes_to_kickoff(date),
            "xg_total":    total_xg, "xg_home": round(xg_h, 2), "xg_away": round(xg_a, 2),
            "home_n":      home_st["n"], "away_n": away_st["n"],
            "home_form":   home_st["form_score"], "away_form": away_st["form_score"],
            "home_btts":   round(home_st["home_btts_rate"] * 100),
            "away_btts":   round(away_st["away_btts_rate"] * 100),
            "h2h_count":   h2h["count"] if h2h else 0,
            "elo_home":    round(elo_h), "elo_away": round(elo_a), "elo_diff": elo_diff,
        }

        for bet_type, bt_meta in BET_TYPES.items():
            gate = bt_meta["gate"]

            if bet_type in ("OVER_05", "OVER_15", "OVER_25"):
                line = bt_meta["line"]
                conf, rprob, factors, reasoning = compute_over_confidence(
                    home_st, away_st, line, h2h, bet_type, lid
                )
                pois_p = poisson_over_line(xg_h, xg_a, line)
                sk_map = {"OVER_05":"streak_over05","OVER_15":"streak_over15","OVER_25":"streak_over25"}
                rk_map = {"OVER_05":"over05_rate","OVER_15":"over15_rate","OVER_25":"over25_rate"}
                extra  = {
                    "poisson_p":  round(pois_p, 4),
                    "over_rate":  round(home_st.get(rk_map[bet_type], 0) * 100),
                    "streak_val": home_st.get(sk_map[bet_type], 0),
                }

            elif bet_type == "BTTS_YES":
                conf, rprob, factors, reasoning = compute_btts_confidence(home_st, away_st, h2h, lid)
                pois_p = poisson_btts(xg_h, xg_a)
                extra  = {
                    "poisson_p": round(pois_p, 4),
                    "btts_hist": round((home_st["home_btts_rate"] + away_st["away_btts_rate"]) / 2 * 100),
                }

            elif bet_type in ("HOME_WIN", "AWAY_WIN"):
                side = "HOME" if bet_type == "HOME_WIN" else "AWAY"
                conf, rprob, factors, reasoning = compute_result_confidence(
                    home_st, away_st, h2h, side, lid
                )
                pois_p = (poisson_home_win(xg_h, xg_a) if side == "HOME"
                          else poisson_away_win(xg_h, xg_a))
                extra  = {
                    "poisson_p": round(pois_p, 4),
                    "win_hist":  round((home_st["wins_rate"] if side == "HOME"
                                       else away_st["wins_rate"]) * 100),
                }
            else:
                continue

            if conf < gate:
                continue

            tier, tier_label = get_card_tier(conf, bet_type)
            candidates.append({
                **base,
                "bet_type":   bet_type,
                "bet":        f"{bt_meta['emoji']} {bt_meta['label']}",
                "confidence": conf,
                "raw_prob":   rprob,
                "tier":       tier,
                "tier_label": tier_label,
                "reasoning":  reasoning,
                "factors":    factors,
                **extra,
            })

    candidates.sort(key=lambda x: x["confidence"], reverse=True)

    # Keep best bet per match (top 7 unique matches)
    seen_matches: set = set()
    top_picks: List[Dict] = []
    for c in candidates:
        mkey = c["match"]
        if mkey not in seen_matches:
            seen_matches.add(mkey)
            top_picks.append(c)
        if len(top_picks) >= TOP_N:
            break

    for i, p in enumerate(top_picks, 1):
        p["rank"] = i
        save_pick(p)

    return top_picks, leagues_hit, games_eval, data_pts


# ════════════════════════════════════════════════════════════════
#  SECTION 9: AUTO-GRADER + CALIBRATION LEARNER
# ════════════════════════════════════════════════════════════════

def grade_and_learn() -> int:
    conn = get_db()
    try:
        pending = conn.execute("""
            SELECT id, match, league_id, kickoff, bet_type, factors_json, raw_prob,
                   home_score, away_score
            FROM picks_log WHERE result='pending'
        """).fetchall()
    except Exception:
        return 0

    updated = 0
    for (row_id, match, league_id, kickoff, bet_type,
         factors_json_str, raw_prob, stored_hs, stored_as) in pending:

        ko = parse_utc(kickoff)
        if not ko or (now_utc() - ko).total_seconds() < 5400:  # 90 min
            continue
        if not league_id:
            continue

        parts = match.split(" vs ", 1)
        if len(parts) != 2:
            continue
        home_name, away_name = parts[0].strip(), parts[1].strip()

        date_str = ko.strftime("%Y%m%d")
        data = safe_get(f"{ESPN_SOCCER}/{league_id}/scoreboard", params={"dates": date_str})
        if not data:
            continue

        for ev in data.get("events", []):
            comps = ev.get("competitions", [])
            if not comps:
                continue
            comp = comps[0]
            if not comp.get("status", {}).get("type", {}).get("completed", False):
                continue
            competitors = comp.get("competitors", [])
            if len(competitors) < 2:
                continue
            names = {c.get("team", {}).get("displayName", "") for c in competitors}
            if home_name not in names and away_name not in names:
                continue

            home_c = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
            away_c = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])
            hs  = _parse_score(home_c.get("score"))
            as_ = _parse_score(away_c.get("score"))
            tot = hs + as_

            # Grade result
            bt = bet_type or "OVER_25"
            result_map = {
                "OVER_05":  "WON" if tot > 0.5 else "LOST",
                "OVER_15":  "WON" if tot > 1.5 else "LOST",
                "OVER_25":  "WON" if tot > 2.5 else "LOST",
                "BTTS_YES": "WON" if hs > 0 and as_ > 0 else "LOST",
                "HOME_WIN": "WON" if hs > as_ else "LOST",
                "AWAY_WIN": "WON" if as_ > hs else "LOST",
            }
            result = result_map.get(bt, "LOST")

            with _db_lock:
                conn.execute(
                    "UPDATE picks_log SET result=?,home_score=?,away_score=? WHERE id=?",
                    (result, hs, as_, row_id)
                )
            updated += 1

            # Update ELO ratings
            home_id_row = next(
                (c.get("team", {}).get("id", "") for c in competitors if c.get("homeAway") == "home"), ""
            )
            away_id_row = next(
                (c.get("team", {}).get("id", "") for c in competitors if c.get("homeAway") == "away"), ""
            )
            if home_id_row and away_id_row:
                update_elo(str(home_id_row), home_name, str(away_id_row), away_name,
                           hs, as_, league_id)

            # Adaptive weight learning (Brier gradient)
            try:
                factors = json.loads(factors_json_str or "{}")
                rp = float(raw_prob) if raw_prob else 0.5
                if factors and bt:
                    update_weights(bt, factors, won=(result == "WON"), raw_prob=rp)
            except Exception:
                pass
            break

    if updated:
        conn.commit()
    return updated


# ════════════════════════════════════════════════════════════════
#  SECTION 10: UI RENDERING
# ════════════════════════════════════════════════════════════════

def countdown_html(kickoff_utc: str, pick_id: str) -> str:
    return f"""
<div id="cd_{pick_id}" style="font-family:'Barlow Condensed',sans-serif;font-size:.85rem;letter-spacing:2px;color:#39ff14;">
  ⏱ Calculating...
</div>
<script>
(function(){{
  var target=new Date("{kickoff_utc}"),el=document.getElementById("cd_{pick_id}");
  function tick(){{
    var now=new Date(),diff=target-now;
    if(diff<=0){{el.innerHTML="🔴 LIVE NOW";el.style.color="#ff1744";return;}}
    var h=Math.floor(diff/3600000),m=Math.floor((diff%3600000)/60000),s=Math.floor((diff%60000)/1000);
    var p=[];if(h>0)p.push(h+"h");p.push(("0"+m).slice(-2)+"m");p.push(("0"+s).slice(-2)+"s");
    el.innerHTML="⏱ KICKOFF IN "+p.join(" ");
  }}
  tick();setInterval(tick,1000);
}})();
</script>
"""


def _form_label(score: float) -> str:
    if score >= 0.65: return "🔥 HOT"
    if score <= 0.35: return "❄️ COLD"
    return "➡️ STABLE"


def render_pick_card(pick: Dict):
    tier     = pick["tier"]
    conf     = pick["confidence"]
    bet_type = pick["bet_type"]
    bt_meta  = BET_TYPES.get(bet_type, BET_TYPES["OVER_25"])
    pick_id  = hashlib.md5(f"{pick['match']}{bet_type}".encode()).hexdigest()[:6]
    bar_w    = min(99, int(conf))
    pois_pct = pick.get("poisson_p", 0) * 100
    elo_diff = pick.get("elo_diff", 0)
    elo_str  = f"+{elo_diff}" if elo_diff > 0 else str(elo_diff)

    conn = get_db()
    try:
        upd_row = conn.execute(
            "SELECT SUM(updates) FROM model_weights WHERE bet_type=?", (bet_type,)
        ).fetchone()
        total_updates = int(upd_row[0] or 0) if upd_row else 0
    except Exception:
        total_updates = 0

    learn_html = (
        f'<span class="pill pill-learn">🧠 {total_updates} AI PICKS LEARNT</span>'
        if total_updates > 0 else ""
    )
    h2h_html = (
        f'<span class="pill pill-h2h">H2H({pick["h2h_count"]}g)</span>'
        if pick.get("h2h_count", 0) > 0 else ""
    )
    dc_html = '<span class="pill pill-dc">Dixon-Coles ✓</span>'

    card_html = f"""
<div class="pick-card {tier}">
  <div class="rank-badge">#{pick['rank']}</div>
  <div class="card-league">{pick['league']}</div>
  <div class="card-teams">{pick['home']} <span class="card-vs">vs</span> {pick['away']}</div>
  <div class="card-bet bet-{bt_meta['css']}">{pick['bet']}</div>

  <div class="conf-row">
    <span class="conf-pct {tier}">{conf:.1f}%</span>
    <span class="tier-chip {tier}">{pick['tier_label']}</span>
  </div>
  <div class="conf-track">
    <div class="conf-fill {tier}" style="width:{bar_w}%;"></div>
  </div>

  <div class="ai-grid">
    <div class="ai-factor">
      <span class="ai-factor-val cyan">{pois_pct:.1f}%</span>
      <div class="ai-factor-lbl">Poisson+DC</div>
    </div>
    <div class="ai-factor">
      <span class="ai-factor-val gold">{pick['xg_total']:.2f}</span>
      <div class="ai-factor-lbl">xG Total</div>
    </div>
    <div class="ai-factor">
      <span class="ai-factor-val">{pick['home_btts']:.0f}/{pick['away_btts']:.0f}%</span>
      <div class="ai-factor-lbl">BTTS%</div>
    </div>
    <div class="ai-factor">
      <span class="ai-factor-val">{_form_label(pick['home_form'])}</span>
      <div class="ai-factor-lbl">Home Form</div>
    </div>
    <div class="ai-factor">
      <span class="ai-factor-val">{_form_label(pick['away_form'])}</span>
      <div class="ai-factor-lbl">Away Form</div>
    </div>
    <div class="ai-factor">
      <span class="ai-factor-val purple">{pick['home_n']}+{pick['away_n']}g</span>
      <div class="ai-factor-lbl">Data</div>
    </div>
  </div>

  <div class="pills-row">
    <span class="pill pill-time">{pick['kickoff_cat']}</span>
    <span class="pill pill-xg">xG {pick['xg_home']:.2f}+{pick['xg_away']:.2f}</span>
    <span class="pill pill-elo">ELO diff {elo_str}</span>
    {h2h_html}
    {dc_html}
    {learn_html}
  </div>
  <div class="card-reason">{pick['reasoning']}</div>
</div>
"""
    st.markdown(card_html, unsafe_allow_html=True)

    # Countdown timer — try st.html first (Streamlit 1.35+), then components fallback
    try:
        st.html(countdown_html(pick["kickoff_utc"], pick_id))
    except AttributeError:
        try:
            import streamlit.components.v1 as components
            components.html(countdown_html(pick["kickoff_utc"], pick_id), height=30)
        except Exception:
            pass


# ════════════════════════════════════════════════════════════════
#  SECTION 11: MAIN APP
# ════════════════════════════════════════════════════════════════

def main():
    # Auto-refresh every 60 seconds
    count = 0
    try:
        from streamlit_autorefresh import st_autorefresh
        count = st_autorefresh(interval=60_000, key="zeus_v4_refresh")
    except Exception:
        pass

    # Hero
    st.markdown("""
<div class="zeus-hero">
  <span class="zeus-logo">⚡ ZEUS</span>
  <div class="zeus-tagline">Neural Football Intelligence · Multi-Bet · Adaptive AI · Dixon-Coles · ELO</div>
  <div class="zeus-version">
    v4.0 · OVER 0.5/1.5/2.5 · BTTS · Home/Away Win · Brier-Score Learning · Calibration · 75+ Leagues
  </div>
  <div class="zeus-bar"></div>
</div>
""", unsafe_allow_html=True)

    # Auto-grade + learn
    newly_graded = grade_and_learn()

    tab_picks, tab_results, tab_brain, tab_calib, tab_about = st.tabs([
        "🎯 Top Picks", "🏆 Results", "🧠 AI Brain", "📊 Calibration", "🌍 System",
    ])

    # ── TAB 1: TOP PICKS ────────────────────────────────────────────
    with tab_picks:
        now_cat = (now_utc() + CAT_OFFSET).strftime("%d %b %Y · %H:%M CAT")
        st.caption(
            f"🕐 {now_cat} &nbsp;·&nbsp; Scanning games in next {WINDOW_HOURS}h "
            f"&nbsp;·&nbsp; Auto-refresh 60s &nbsp;·&nbsp; Dixon-Coles+ELO active "
            f"&nbsp;·&nbsp; Scan #{count or '—'}"
        )
        if newly_graded:
            st.toast(f"🧠 Zeus learned from {newly_graded} graded pick(s)!", icon="⚡")

        with st.spinner(""):
            st.markdown(
                '<div class="scan-line">⚡ ZEUS v4.0 SCANNING 75+ LEAGUES — DIXON-COLES · ELO · BRIER-LEARNING ⚡</div>',
                unsafe_allow_html=True
            )
            picks, leagues_hit, games_eval, data_pts = scan_all_leagues()

        elite_cnt  = sum(1 for p in picks if p["tier"] == "elite")
        goals_cnt  = sum(1 for p in picks if "OVER" in p.get("bet_type", ""))
        btts_cnt   = sum(1 for p in picks if p.get("bet_type") == "BTTS_YES")
        result_cnt = sum(1 for p in picks if p.get("bet_type") in ("HOME_WIN", "AWAY_WIN"))

        st.markdown(f"""
<div class="metrics-row">
  <div class="metric-box"><span class="metric-val">{len(picks)}</span><div class="metric-lbl">Picks Today</div></div>
  <div class="metric-box"><span class="metric-val gold">{elite_cnt}</span><div class="metric-lbl">🔥 Elite</div></div>
  <div class="metric-box"><span class="metric-val">{goals_cnt}</span><div class="metric-lbl">⚽ Goals</div></div>
  <div class="metric-box"><span class="metric-val purple">{btts_cnt}</span><div class="metric-lbl">🎯 BTTS</div></div>
  <div class="metric-box"><span class="metric-val cyan">{result_cnt}</span><div class="metric-lbl">🏠✈️ Result</div></div>
  <div class="metric-box"><span class="metric-val">{leagues_hit}</span><div class="metric-lbl">Leagues Hit</div></div>
  <div class="metric-box"><span class="metric-val">{games_eval}</span><div class="metric-lbl">Games Eval</div></div>
  <div class="metric-box"><span class="metric-val cyan">{data_pts:,}</span><div class="metric-lbl">Data Points</div></div>
</div>
""", unsafe_allow_html=True)

        st.markdown("---")

        if not picks:
            st.markdown(f"""
<div class="no-picks">
  <span class="no-picks-icon">⏳</span>
  No games meet Zeus's confidence thresholds in the next {WINDOW_HOURS} hours.<br>
  Gates: <strong>OVER 0.5 ≥86%</strong> · <strong>OVER 1.5 ≥76%</strong> · 
  <strong>OVER 2.5 ≥63%</strong> · <strong>BTTS ≥68%</strong> · <strong>Result ≥72%</strong><br>
  Zeus uses Dixon-Coles correction + ELO ratings + exponential decay weighting.<br>
  Continuously scanning — check back as fixtures enter the {WINDOW_HOURS}h window.
</div>
""", unsafe_allow_html=True)
        else:
            cols = st.columns(3)
            cols[0].markdown(
                '<span style="font-family:Barlow Condensed;color:#ffb300;font-size:.85rem;">'
                '🔥 ELITE — Maximum multi-model edge</span>', unsafe_allow_html=True)
            cols[1].markdown(
                '<span style="font-family:Barlow Condensed;color:#39ff14;font-size:.85rem;">'
                '⚡ STRONG — Clear statistical signal</span>', unsafe_allow_html=True)
            cols[2].markdown(
                '<span style="font-family:Barlow Condensed;color:#ea80fc;font-size:.85rem;">'
                '🎯 BTTS / ✈️ RESULT — High-confidence specialty</span>', unsafe_allow_html=True)
            st.markdown("---")
            for pick in picks:
                render_pick_card(pick)

    # ── TAB 2: RESULTS ──────────────────────────────────────────────
    with tab_results:
        st.subheader("🏆 Zeus Pick Results — Auto-Graded")
        if newly_graded:
            st.success(f"✅ {newly_graded} new pick(s) graded and learned from this refresh.")

        try:
            conn = get_db()
            rows = conn.execute("""
                SELECT match, league, bet, bet_type, xg_total, confidence,
                       kickoff, result, home_score, away_score, logged_at
                FROM picks_log ORDER BY logged_at DESC LIMIT 500
            """).fetchall()

            if not rows:
                st.info("No picks logged yet — visit 🎯 Top Picks to generate predictions.")
            else:
                df = pd.DataFrame(rows, columns=[
                    "Match","League","Bet","Bet Type","xG","Conf%",
                    "Kickoff UTC","Result","Home Score","Away Score","Logged"
                ])
                df["Conf%"] = df["Conf%"].apply(lambda x: f"{float(x):.1f}%")
                df["xG"]    = df["xG"].apply(lambda x: f"{float(x):.2f}")

                won  = df[df["Result"] == "WON"]
                lost = df[df["Result"] == "LOST"]
                pend = df[df["Result"] == "pending"]
                tot  = len(won) + len(lost)
                wr   = f"{len(won)/tot*100:.1f}%" if tot > 0 else "—"

                c1,c2,c3,c4,c5 = st.columns(5)
                c1.metric("✅ Won",       len(won))
                c2.metric("❌ Lost",      len(lost))
                c3.metric("⏳ Pending",   len(pend))
                c4.metric("Total Graded", tot)
                c5.metric("Win Rate",     wr)

                if tot > 0:
                    st.markdown("**Win Rate by Bet Type**")
                    graded = df[df["Result"].isin(["WON","LOST"])]
                    for bt in BET_TYPES:
                        bdf = graded[graded["Bet Type"] == bt]
                        if bdf.empty:
                            continue
                        bw  = len(bdf[bdf["Result"] == "WON"])
                        bwr = f"{bw/len(bdf)*100:.0f}%"
                        st.markdown(
                            f"**{BET_TYPES[bt]['emoji']} {BET_TYPES[bt]['label']}**: "
                            f"{bw}/{len(bdf)} won ({bwr})"
                        )

                st.divider()
                st.markdown("### ✅ Correct Picks")
                if won.empty:
                    st.info("No graded wins yet — picks are auto-graded ~90 min after kickoff.")
                else:
                    for _, r in won.iterrows():
                        sc = (f" | {int(r['Home Score'])}-{int(r['Away Score'])}"
                              if r['Home Score'] >= 0 else "")
                        st.markdown(
                            f"⚽ **{r['Match']}** · {r['League']} · **{r['Bet']}** · "
                            f"xG: {r['xG']} · Conf: **{r['Conf%']}**{sc} · "
                            f"<span style='color:#39ff14;font-weight:700;'>WON ✅</span>",
                            unsafe_allow_html=True
                        )
                        st.divider()

                st.markdown("### ❌ Missed Picks — AI Learns From These")
                if lost.empty:
                    st.info("No missed picks yet.")
                else:
                    for _, r in lost.iterrows():
                        sc = (f" | {int(r['Home Score'])}-{int(r['Away Score'])}"
                              if r['Home Score'] >= 0 else "")
                        st.markdown(
                            f"⚽ **{r['Match']}** · {r['League']} · **{r['Bet']}** · "
                            f"xG: {r['xG']} · Conf: **{r['Conf%']}**{sc} · "
                            f"<span style='color:#ff1744;font-weight:700;'>MISSED ❌ → AI UPDATED WEIGHTS</span>",
                            unsafe_allow_html=True
                        )
                        st.divider()

                if not pend.empty:
                    with st.expander(f"⏳ Pending — {len(pend)} picks awaiting results"):
                        st.dataframe(
                            pend[["Match","League","Bet","Conf%","Kickoff UTC"]],
                            hide_index=True, use_container_width=True
                        )
        except Exception as e:
            st.info(f"Results log unavailable: {e}")

    # ── TAB 3: AI BRAIN ──────────────────────────────────────────────
    with tab_brain:
        st.subheader("🧠 Zeus Adaptive Intelligence — Brier-Score Learning")
        st.markdown(
            "Zeus uses **Brier-score gradient descent** to update weights after every graded pick. "
            "Winning picks reinforce strong-contributing factors; losing picks penalize them. "
            "**Dixon-Coles** corrects Poisson for low-scoring bias. **ELO ratings** track team "
            "strength across time. **Exponential decay** gives recent games 3× more weight."
        )

        try:
            conn = get_db()
            rows = conn.execute(
                "SELECT bet_type,factor,weight,wins,losses,updates,brier_contrib "
                "FROM model_weights ORDER BY bet_type,weight DESC"
            ).fetchall()
            if rows:
                df_w = pd.DataFrame(rows, columns=["Bet Type","Factor","Weight","Wins","Losses","Updates","Brier Sum"])
                df_w["Weight"]    = df_w["Weight"].apply(lambda x: f"{float(x)*100:.1f}%")
                df_w["Brier Sum"] = df_w["Brier Sum"].apply(lambda x: f"{float(x):.3f}")

                for bt in BET_TYPES:
                    bdf = df_w[df_w["Bet Type"] == bt]
                    if bdf.empty:
                        continue
                    total_upd = bdf["Updates"].astype(int).sum()
                    total_w   = bdf["Wins"].astype(int).sum()
                    total_l   = bdf["Losses"].astype(int).sum()
                    with st.expander(
                        f"{BET_TYPES[bt]['emoji']} {BET_TYPES[bt]['label']} — "
                        f"{total_upd} updates · {total_w}W / {total_l}L"
                    ):
                        st.dataframe(
                            bdf[["Factor","Weight","Wins","Losses","Updates","Brier Sum"]],
                            hide_index=True, use_container_width=True
                        )
        except Exception as e:
            st.info(f"Weight data unavailable: {e}")

        st.divider()
        # ELO Top 20
        try:
            conn = get_db()
            elo_rows = conn.execute(
                "SELECT team_name,elo,games FROM elo_ratings ORDER BY elo DESC LIMIT 20"
            ).fetchall()
            if elo_rows:
                st.markdown("**🏆 Top 20 Teams by ELO Rating**")
                elo_df = pd.DataFrame(elo_rows, columns=["Team","ELO","Games"])
                elo_df["ELO"] = elo_df["ELO"].apply(lambda x: f"{float(x):.0f}")
                st.dataframe(elo_df, hide_index=True, use_container_width=True)
        except Exception:
            pass

        st.divider()
        st.markdown("**Learning Parameters**")
        st.markdown(f"- Algorithm: Brier-score gradient descent (MSE minimization)")
        st.markdown(f"- Learning rate: `{LEARNING_RATE}`")
        st.markdown(f"- Dixon-Coles ρ: `-0.13` (corrects 0-0 / 1-0 / 0-1 bias)")
        st.markdown(f"- Exponential decay λ: `{DECAY_LAMBDA}` per game (~3× weight for recent games)")
        st.markdown(f"- ELO K-factor: `{ELO_K}`")
        st.markdown(f"- Weights checkpointed to `data/weights_checkpoint.json` (commit to persist)")

    # ── TAB 4: CALIBRATION ──────────────────────────────────────────
    with tab_calib:
        st.subheader("📊 Prediction Calibration — Reliability Analysis")
        st.markdown(
            "Calibration shows whether Zeus's confidence scores match actual win rates. "
            "A perfectly calibrated model has 70% confidence picks winning 70% of the time. "
            "Zeus auto-adjusts weights to improve calibration after every graded pick."
        )

        try:
            conn = get_db()
            cal_rows = conn.execute("""
                SELECT bet_type, bucket, pred_sum, actual, count
                FROM calibration ORDER BY bet_type, bucket
            """).fetchall()

            if not cal_rows:
                st.info("No calibration data yet — calibration builds after picks are graded.")
            else:
                cal_df = pd.DataFrame(cal_rows, columns=["Bet Type","Bucket","Pred Sum","Actual","Count"])
                cal_df["Avg Pred"] = (cal_df["Pred Sum"] / cal_df["Count"].clip(lower=1) * 100).round(1)
                cal_df["Win Rate"] = (cal_df["Actual"] / cal_df["Count"].clip(lower=1) * 100).round(1)
                cal_df["Error"]    = (cal_df["Avg Pred"] - cal_df["Win Rate"]).abs().round(1)
                cal_df["n picks"]  = cal_df["Count"]

                for bt in BET_TYPES:
                    bdf = cal_df[cal_df["Bet Type"] == bt]
                    if bdf.empty:
                        continue
                    total_n = bdf["n picks"].sum()
                    brier   = (bdf["Error"]**2).mean() if len(bdf) > 0 else None
                    with st.expander(
                        f"{BET_TYPES[bt]['emoji']} {BET_TYPES[bt]['label']} — "
                        f"{total_n} graded · Brier Error {brier:.1f}%" if brier else
                        f"{BET_TYPES[bt]['emoji']} {BET_TYPES[bt]['label']} — {total_n} graded"
                    ):
                        st.dataframe(
                            bdf[["Bucket","Avg Pred","Win Rate","Error","n picks"]],
                            hide_index=True, use_container_width=True
                        )
        except Exception as e:
            st.info(f"Calibration data unavailable: {e}")

    # ── TAB 5: SYSTEM INFO ──────────────────────────────────────────
    with tab_about:
        st.subheader("🌍 Zeus Neural System v4.0 — Full Architecture")
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("""
**Prediction Models (ensemble):**
- **Poisson + Dixon-Coles** — corrects low-score bias (ρ=-0.13)
- **Venue-split xG** — home/away scored + conceded averages
- **Historical Rate** — weighted by exponential decay (recent=3×)
- **Head-to-Head** — last N meetings extracted
- **Form Momentum** — last 5 vs last 10 comparison
- **Streak Detection** — consecutive OVER/BTTS/WIN runs
- **ELO Ratings** — team strength (K=20, home adv=40pts)

**Learning Engine:**
- **Brier-score gradient descent** (MSE loss function)
- **Online learning** — updates after every graded pick
- **Calibration tracking** — reliability per confidence bucket
- **Weight normalization** — weights always sum to 1.0
- **JSON checkpoint** — weights persist to `data/weights_checkpoint.json`

**Data Sources:**
- ESPN Soccer API (primary, free, no key required)
- TheSportsDB (supplement for thin leagues, free)
- **Parallel fetching** (ThreadPoolExecutor, 15 workers)
- **Smart caching** — scoreboard 4min, schedule 1hr, standings 1wk
""")

        with col2:
            st.markdown("**Confidence Gates (minimum to show)**")
            for bt, meta in BET_TYPES.items():
                st.markdown(f"**{meta['emoji']} {meta['label']}**: ≥ {meta['gate']}%")
            st.divider()
            st.markdown("**Base Rates vs Zeus Gates**")
            st.markdown("OVER 0.5: ~75% base → Zeus ≥86% only")
            st.markdown("OVER 1.5: ~60% base → Zeus ≥76% only")
            st.markdown("OVER 2.5: ~50% base → Zeus ≥63% only")
            st.markdown("BTTS: ~50% base → Zeus ≥68% only")
            st.markdown("1X2: variable → Zeus ≥72% only")
            st.divider()
            st.markdown(f"**Leagues Monitored:** {len(LEAGUES)}")
            st.markdown(f"**Window:** Next {WINDOW_HOURS} hours")
            st.markdown(f"**Auto-refresh:** Every 60 seconds")
            st.markdown(f"**AI Engine:** Python {__import__('sys').version.split()[0]}")
            st.markdown(f"**Scipy Poisson:** {'✅ Active' if HAS_SCIPY else '⚠️ Fallback'}")

        st.divider()
        st.subheader(f"All {len(LEAGUES)} Leagues Monitored")
        league_data = [
            {"Flag": flag, "League": lname, "Region": lid.split(".")[0].upper(), "ID": lid}
            for lid, lname, flag in LEAGUES
        ]
        st.dataframe(pd.DataFrame(league_data), hide_index=True, use_container_width=True)


if __name__ == "__main__":
    main()
