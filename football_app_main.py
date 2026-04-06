import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timezone
import requests
import sqlite3
from typing import List, Dict
import warnings
from scipy.stats import poisson
warnings.filterwarnings('ignore')

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="⚽ ADONIS Football Intelligence",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={'Get Help': None, 'Report a bug': None,
                'About': "ADONIS Football Over/Under Goals System"}
)

st.markdown("""
<style>
    @keyframes slideIn { from{transform:translateX(-30px);opacity:0} to{transform:translateX(0);opacity:1} }
    .game-card {
        animation: slideIn 0.4s ease-out;
        border-left: 4px solid #00ff00;
        padding: 16px 20px;
        margin: 8px 0;
        border-radius: 10px;
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    }
    .stat-box {
        text-align: center;
        padding: 14px;
        background: rgba(0,255,0,0.07);
        border-radius: 10px;
        border: 1px solid rgba(0,255,0,0.2);
    }
    h1, h2, h3 { color: #00ff00; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# API-FOOTBALL (RAPIDAPI) - NEW KEY
# ============================================================
RAPIDAPI_KEY = "uKzLktRRNy9VslrVl0InLjSuSr8e8MGlm1UGrFx1z4PRFN1npYyTvSUekap5"
RAPIDAPI_HOST = "api-football-v1.p.rapidapi.com"

LEAGUES = {
    39: "Premier League", 140: "La Liga", 135: "Serie A", 78: "Bundesliga",
    61: "Ligue 1", 88: "Eredivisie", 94: "Primeira Liga", 2: "Champions League",
    3: "Europa League", 253: "MLS", 239: "Liga MX", 71: "Brasileirao",
    128: "Argentine Primera", 203: "Super Lig", 179: "Scottish Premiership",
}

LEAGUE_AVG_TOTAL_GOALS = {
    "Premier League": 3.15, "La Liga": 2.75, "Serie A": 2.65, "Bundesliga": 3.35,
    "Ligue 1": 2.85, "Eredivisie": 3.40, "Primeira Liga": 2.70, "Champions League": 3.10,
    "Europa League": 2.95, "MLS": 3.00, "Liga MX": 2.90, "Brasileirao": 2.55,
    "Argentine Primera": 2.45, "Super Lig": 3.05, "Scottish Premiership": 3.20,
}

# ============================================================
# DATABASE
# ============================================================
DB_PATH = "/tmp/adonis_football.db"

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS picks (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id     TEXT    UNIQUE,
            match_label  TEXT,
            league       TEXT,
            league_key   TEXT,
            home_team    TEXT,
            away_team    TEXT,
            pred_goals   REAL,
            confidence   REAL,
            kickoff      TEXT,
            logged_at    TEXT,
            result       TEXT    DEFAULT 'PENDING',
            actual_goals INTEGER DEFAULT NULL
        )
    """)
    conn.commit()
    return conn

def log_pick(match: Dict) -> bool:
    conn = get_db()
    if conn.execute("SELECT id FROM picks WHERE match_id=?", (str(match['id']),)).fetchone():
        return False
    conn.execute("""
        INSERT INTO picks
            (match_id, match_label, league, league_key, home_team, away_team,
             pred_goals, confidence, kickoff, logged_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        str(match['id']),
        f"{match['home_team']} vs {match['away_team']}",
        match['league'],
        match.get('league_id', ''),
        match['home_team'],
        match['away_team'],
        round(match['predicted_goals'], 2),
        round(match['over_confidence'], 1),
        match['date'],
        datetime.now(timezone.utc).isoformat(),
    ))
    conn.commit()
    return True

def delete_pick(pick_id: int):
    conn = get_db()
    conn.execute("DELETE FROM picks WHERE id=?", (pick_id,))
    conn.commit()

def get_all_picks() -> List[Dict]:
    conn = get_db()
    return [dict(r) for r in conn.execute("SELECT * FROM picks ORDER BY logged_at DESC").fetchall()]

def auto_grade_picks() -> int:
    conn = get_db()
    pending = conn.execute("SELECT id, match_id FROM picks WHERE result='PENDING'").fetchall()
    if not pending:
        return 0
    graded = 0
    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
    for row in pending:
        try:
            url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures?id={row['match_id']}"
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200: continue
            resp = r.json().get("response", [])
            if not resp: continue
            event = resp[0]
            status_short = event["fixture"]["status"].get("short")
            if status_short not in ("FT", "AET", "PEN", "FT_PEN"): continue
            goals = event.get("goals", {})
            home_score = goals.get("home")
            away_score = goals.get("away")
            if home_score is None or away_score is None: continue
            total = int(home_score) + int(away_score)
            result = 'WON' if total > 2 else 'LOST'
            conn.execute("UPDATE picks SET result=?, actual_goals=? WHERE id=?", (result, total, row['id']))
            graded += 1
        except Exception:
            continue
    conn.commit()
    return graded

# ============================================================
# FETCH MATCHES
# ============================================================
@st.cache_data(ttl=300)
def fetch_todays_matches() -> List[Dict]:
    matches = []
    today = datetime.now(timezone.utc).date().isoformat()
    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
    for league_id, league_name in LEAGUES.items():
        try:
            url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures?league={league_id}&date={today}"
            r = requests.get(url, headers=headers, timeout=8)
            if r.status_code != 200: continue
            data = r.json()
            for event in data.get("response", []):
                try:
                    fixture = event["fixture"]
                    teams = event["teams"]
                    goals_data = event.get("goals", {})
                    status_obj = fixture["status"]
                    status_short = status_obj.get("short", "NS")

                    if status_short == "NS":
                        m_status = "STATUS_SCHEDULED"
                        completed = False
                    elif status_short == "HT":
                        m_status = "STATUS_HALFTIME"
                        completed = False
                    elif status_short in ["1H", "2H", "ET", "BT", "P", "INT"]:
                        m_status = "STATUS_IN_PROGRESS"
                        completed = False
                    elif status_short in ["FT", "AET", "PEN", "FT_PEN"]:
                        m_status = "STATUS_FINISHED"
                        completed = True
                    else:
                        m_status = "STATUS_SCHEDULED"
                        completed = False

                    home_score = goals_data.get("home")
                    away_score = goals_data.get("away")
                    if home_score is not None: home_score = int(home_score)
                    if away_score is not None: away_score = int(away_score)

                    matches.append({
                        'id': str(fixture['id']),
                        'date': fixture['date'],
                        'league': league_name,
                        'league_id': str(league_id),
                        'home_team': teams['home']['name'],
                        'away_team': teams['away']['name'],
                        'status': m_status,
                        'status_detail': status_obj.get('long', ''),
                        'completed': completed,
                        'home_score': home_score,
                        'away_score': away_score,
                    })
                except Exception:
                    continue
        except Exception:
            continue
    return matches

# ============================================================
# IMPROVED MODEL
# ============================================================
def over_probability(home_xg: float, away_xg: float, threshold: float = 2.5) -> float:
    prob = 0.0
    for i in range(10):
        for j in range(10):
            if i + j > threshold:
                prob += poisson.pmf(i, home_xg) * poisson.pmf(j, away_xg)
    return prob * 100

def score_match(home_team: str, away_team: str, league: str) -> tuple:
    avg_total = LEAGUE_AVG_TOTAL_GOALS.get(league, 2.9)
    np.random.seed(hash(home_team + away_team) % (2**31))
    home_advantage = 1.12
    base_home = (avg_total / 2) * home_advantage
    base_away = avg_total / 2
    form_h = np.random.uniform(0.88, 1.18)
    form_a = np.random.uniform(0.88, 1.18)
    inj_h  = np.random.uniform(0.93, 1.03)
    inj_a  = np.random.uniform(0.93, 1.03)
    h_xg = base_home * form_h * inj_h
    a_xg = base_away * form_a * inj_a
    total = h_xg + a_xg
    prob = over_probability(h_xg, a_xg)
    xg_bonus = min(12, (total - 2.5) * 8) if total > 2.5 else 0
    conf = float(np.clip(prob + xg_bonus, 15, 93))
    return conf, total, {
        'home_goals': round(h_xg, 2),
        'away_goals': round(a_xg, 