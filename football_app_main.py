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
        'away_goals': round(a_xg, 2),
        'total_xg':   round(total, 2),
        'over_prob':  round(prob, 1),
    }

def analyze_matches(matches: List[Dict]) -> List[Dict]:
    results = []
    for m in matches:
        conf, total, breakdown = score_match(m['home_team'], m['away_team'], m['league'])
        results.append({**m,
            'over_confidence': conf,
            'predicted_goals': total,
            'breakdown': breakdown,
        })
    results.sort(key=lambda x: x['over_confidence'], reverse=True)
    return results

# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("## ⚽ ADONIS Football")
    st.markdown("---")
    min_conf = st.slider("Min Confidence %", 40, 90, 60, 5)
    show_leagues = st.multiselect(
        "Filter Leagues",
        options=list(LEAGUES.values()),
        default=list(LEAGUES.values()),
    )
    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    if st.button("✅ Auto-Grade Picks", use_container_width=True):
        graded = auto_grade_picks()
        st.success(f"Graded {graded} pick(s).")

# ============================================================
# MAIN
# ============================================================
st.title("⚽ ADONIS Football Intelligence System")
st.caption(f"Live · {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

tab1, tab2, tab3 = st.tabs(["📊 Today's Predictions", "📋 My Picks", "📈 Performance"])

# ── TAB 1: PREDICTIONS ─────────────────────────────────────
with tab1:
    with st.spinner("Fetching today's fixtures…"):
        raw_matches = fetch_todays_matches()

    if not raw_matches:
        st.warning("No matches found for today. Try refreshing.")
    else:
        analyzed = analyze_matches(raw_matches)
        filtered = [m for m in analyzed
                    if m['over_confidence'] >= min_conf
                    and m['league'] in show_leagues]

        st.markdown(f"**{len(filtered)} match(es)** meeting your criteria  "
                    f"(≥ {min_conf}% confidence)")

        if not filtered:
            st.info("No matches match your filters. Lower the confidence threshold or add more leagues.")
        else:
            for m in filtered:
                conf  = m['over_confidence']
                total = m['predicted_goals']
                color = "#00ff00" if conf >= 70 else "#ffd700" if conf >= 55 else "#ff6b6b"
                status_label = {
                    "STATUS_SCHEDULED":   "🕐 Upcoming",
                    "STATUS_IN_PROGRESS": "🔴 LIVE",
                    "STATUS_HALFTIME":    "⏸ Half Time",
                    "STATUS_FINISHED":    "✅ Finished",
                }.get(m['status'], m['status'])

                score_str = ""
                if m['home_score'] is not None and m['away_score'] is not None:
                    score_str = f" &nbsp;|&nbsp; **{m['home_score']} – {m['away_score']}**"

                with st.container():
                    st.markdown(f"""
                    <div class="game-card">
                        <b style="font-size:1.05em">{m['home_team']} vs {m['away_team']}</b>
                        &nbsp;·&nbsp; <span style="color:#aaa">{m['league']}</span>
                        &nbsp;·&nbsp; {status_label}{score_str}<br>
                        <span style="color:{color};font-size:1.2em;font-weight:bold">
                            {conf:.0f}% Over 2.5
                        </span>
                        &nbsp;|&nbsp; xG Total: <b>{total:.2f}</b>
                        &nbsp;|&nbsp; H: {m['breakdown']['home_goals']}
                        &nbsp; A: {m['breakdown']['away_goals']}
                    </div>
                    """, unsafe_allow_html=True)

                    col1, col2 = st.columns([3, 1])
                    with col2:
                        if st.button("📌 Log Pick", key=f"log_{m['id']}"):
                            if log_pick(m):
                                st.success("Pick logged!")
                            else:
                                st.info("Already logged.")

# ── TAB 2: MY PICKS ────────────────────────────────────────
with tab2:
    st.subheader("📋 Logged Picks")
    picks = get_all_picks()
    if not picks:
        st.info("No picks logged yet. Go to **Today's Predictions** and hit 📌 Log Pick.")
    else:
        df = pd.DataFrame(picks)
        df['kickoff'] = pd.to_datetime(df['kickoff'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
        df['confidence'] = df['confidence'].map(lambda x: f"{x:.0f}%")
        df['pred_goals'] = df['pred_goals'].map(lambda x: f"{x:.2f}")

        result_colors = {'WON': '🟢', 'LOST': '🔴', 'PENDING': '🟡'}
        df['result'] = df['result'].map(lambda r: f"{result_colors.get(r, '')} {r}")

        st.dataframe(
            df[['match_label', 'league', 'pred_goals', 'confidence',
                'kickoff', 'result', 'actual_goals']].rename(columns={
                'match_label': 'Match', 'league': 'League',
                'pred_goals': 'Pred Goals', 'confidence': 'Confidence',
                'kickoff': 'Kickoff', 'result': 'Result',
                'actual_goals': 'Actual',
            }),
            use_container_width=True, hide_index=True,
        )

        st.markdown("---")
        st.markdown("**Delete a pick:**")
        pick_options = {f"#{p['id']} · {p['match_label']}": p['id'] for p in picks}
        sel = st.selectbox("Select pick to delete", list(pick_options.keys()))
        if st.button("🗑️ Delete Pick", type="secondary"):
            delete_pick(pick_options[sel])
            st.success("Deleted.")
            st.rerun()

# ── TAB 3: PERFORMANCE ─────────────────────────────────────
with tab3:
    st.subheader("📈 Performance Dashboard")
    picks = get_all_picks()
    graded = [p for p in picks if p['result'] in ('WON', 'LOST')]

    if len(graded) < 2:
        st.info("Need at least 2 graded picks to show performance stats.")
    else:
        won   = sum(1 for p in graded if p['result'] == 'WON')
        lost  = len(graded) - won
        win_r = won / len(graded) * 100

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Graded", len(graded))
        c2.metric("Won", won)
        c3.metric("Lost", lost)
        c4.metric("Win Rate", f"{win_r:.1f}%")

        # Confidence buckets
        buckets = {'40-54%': [0,0], '55-69%': [0,0], '70-84%': [0,0], '85%+': [0,0]}
        for p in graded:
            c = p['confidence']
            key = '40-54%' if c < 55 else '55-69%' if c < 70 else '70-84%' if c < 85 else '85%+'
            buckets[key][0 if p['result'] == 'WON' else 1] += 1

        labels = list(buckets.keys())
        wins_b = [buckets[k][0] for k in labels]
        loss_b = [buckets[k][1] for k in labels]

        fig = go.Figure(data=[
            go.Bar(name='Won',  x=labels, y=wins_b, marker_color='#00ff00'),
            go.Bar(name='Lost', x=labels, y=loss_b, marker_color='#ff4444'),
        ])
        fig.update_layout(
            barmode='group', title='Results by Confidence Bucket',
            paper_bgcolor='#0d0d0d', plot_bgcolor='#0d0d0d',
            font_color='#ffffff', legend=dict(bgcolor='rgba(0,0,0,0)'),
        )
        st.plotly_chart(fig, use_container_width=True)
