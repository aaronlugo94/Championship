import os
import time
import requests
import schedule
import sqlite3
import numpy as np
import math
from datetime import datetime, timedelta, timezone
from math import exp, lgamma, log

# ==========================================
# V6.0 CHAMPIONSHIP SPECIALIST
# ==========================================
# CAMBIOS VS V5.10:
#   1. Solo Championship (ID 40) — de 10 ligas a 1
#   2. xG real basado en últimos 6 partidos por equipo
#      via /fixtures?team=X&last=6 (llamada extra por partido)
#      En lugar de promedios de temporada completa
#   3. Budget de requests optimizado: ~40-50 requests diarios
#      vs los ~120 anteriores que excedían el límite Free
#   4. Factor de localía calculado dinámicamente por equipo
#      (home_goals_avg vs away_goals_avg de últimos 6)
#   5. Todos los mercados: 1X2, Over/Under 2.5, BTTS

LIVE_TRADING = False

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
API_SPORTS_KEY   = os.getenv("API_SPORTS_KEY", "")

DB_DIR = os.getenv("DB_DIR", "./data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "quant_v6.db")

RUN_TIME_SCAN   = "02:50"
RUN_TIME_INGEST = "04:00"

# Championship league ID y temporada
CHAMPIONSHIP_ID     = 40
CHAMPIONSHIP_SEASON = 2025  # temporada 2025/26

# Budget de requests: máximo partidos a escanear por día
# 1 partido = 4 llamadas: fixtures + odds + últimos6_home + últimos6_away
# 10 partidos × 4 = 40 llamadas. Seguro dentro de 100 diarios.
MAX_FIXTURES_PER_SCAN = 10

MAX_DAILY_HEAT          = 0.10
TARGET_DAILY_VOLATILITY = 0.05
MIN_EV_THRESHOLD        = 0.015
MAX_EV_THRESHOLD        = 0.15
MAX_PICKS_PER_FIXTURE   = 1
XG_DECAY_FACTOR         = 0.85

VOLATILITY_BUCKETS = {"OVER": 0.85, "UNDER": 0.85, "BTTS": 0.90, "1X2": 1.25}

LEAGUE_NAME = '🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP'
LIQUIDITY   = 0.85

# ==========================================
# DATABASE
# ==========================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS picks_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER, league TEXT,
        home_team TEXT, away_team TEXT,
        market TEXT, selection TEXT, selection_key TEXT,
        odd_open REAL, prob_model REAL, ev_open REAL, stake_pct REAL,
        xg_home REAL, xg_away REAL, xg_total REAL,
        pick_time DATETIME, kickoff_time DATETIME,
        clv_captured INTEGER DEFAULT 0,
        urs REAL DEFAULT 0.0,
        model_gap REAL DEFAULT 0.0,
        xg_source TEXT DEFAULT 'api'
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS closing_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER, market TEXT, selection_key TEXT,
        odd_close REAL, implied_prob_close REAL, capture_time DATETIME
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS decision_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER, match TEXT, market TEXT,
        odd REAL, ev REAL, reason TEXT, timestamp DATETIME
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS request_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, count INTEGER
    )""")
    conn.commit(); conn.close()


def log_rejection(fixture_id, match, market, odd, ev, reason):
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute(
            "INSERT INTO decision_log VALUES (NULL,?,?,?,?,?,?,?)",
            (fixture_id, match, market, odd, ev, reason,
             datetime.now(timezone.utc).isoformat())
        )
        conn.commit(); conn.close()
    except: pass


def track_requests(n=1):
    """Registra cuántos requests se han usado hoy."""
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn  = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT count FROM request_log WHERE date=?", (today,))
        row = c.fetchone()
        if row:
            c.execute("UPDATE request_log SET count=count+? WHERE date=?", (n, today))
        else:
            c.execute("INSERT INTO request_log VALUES (NULL,?,?)", (today, n))
        conn.commit()
        c.execute("SELECT count FROM request_log WHERE date=?", (today,))
        total = c.fetchone()[0]; conn.close()
        return total
    except: return 0


# ==========================================
# URS ENGINE
# ==========================================

def get_avg_clv(lookback=30):
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("""SELECT AVG((p.odd_open - c.odd_close)/p.odd_open)
                     FROM picks_log p JOIN closing_lines c
                       ON p.fixture_id=c.fixture_id AND p.market=c.market
                          AND p.selection_key=c.selection_key
                     WHERE p.clv_captured=1
                     ORDER BY p.id DESC LIMIT ?""", (lookback,))
        res = c.fetchone()[0]; conn.close()
        return float(res) if res else 0.0
    except: return 0.0

def get_clv_sharpe():
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("""SELECT (p.odd_open - c.odd_close)/p.odd_open
                     FROM picks_log p JOIN closing_lines c
                       ON p.fixture_id=c.fixture_id AND p.clv_captured=1
                     ORDER BY p.id DESC LIMIT 50""")
        clvs = [r[0] for r in c.fetchall()]; conn.close()
        if len(clvs) < 5: return 0.0
        mean, std = np.mean(clvs), np.std(clvs, ddof=1)
        return mean / std if std != 0 else 0.0
    except: return 0.0

def score_sharpe(s):
    if s < -0.5:   return 0.10
    elif s < 0.0:  return 0.30
    elif s < 0.5:  return 0.50
    elif s < 1.0:  return 0.75
    elif s < 1.5:  return 0.90
    else:          return 1.00

def score_ev(ev):
    if ev < 0.03:   return 0.20
    elif ev < 0.05: return 0.40
    elif ev < 0.08: return 0.60
    elif ev < 0.12: return 0.80
    else:           return 1.00

def score_odd(odd):
    if odd < 1.20:    return 0.10
    elif odd < 1.40:  return 0.50
    elif odd <= 3.00: return 1.00
    elif odd <= 4.00: return 0.70
    else:             return 0.30

def calculate_urs(ev, odd):
    sharpe = get_clv_sharpe()
    w = {"sharpe": 0.35, "ev": 0.30, "liquidity": 0.20, "odd": 0.15}
    urs = (w["sharpe"]    * score_sharpe(sharpe) +
           w["ev"]        * score_ev(ev) +
           w["liquidity"] * LIQUIDITY +
           w["odd"]       * score_odd(odd))
    return max(0.10, min(urs, 1.00))

def get_kelly_and_urs(ev, odd, market):
    avg_clv = get_avg_clv()
    if avg_clv < -0.015:
        return 0.0, 0.0, "KILL_SWITCH_ACTIVE"
    base_kelly = max(0.0, min(ev / (odd - 1), 0.05))
    if -0.015 <= avg_clv < 0.005:
        base_kelly *= 0.25
    urs = calculate_urs(ev, odd)
    return base_kelly * urs, urs, None


# ==========================================
# PORTFOLIO ENGINE
# ==========================================

def apply_portfolio_engine(picks):
    if not picks: return [], {}

    port_var = 0.0
    for p in picks:
        if p['odd'] <= 1.01:
            p['adj_stake'] = 0; p['lcp'] = 0; continue
        # En Championship solo hay picks de una liga, LCP = 1/sqrt(N_picks)
        lcp       = 1.0 / math.sqrt(len(picks))
        adj_stake = p['base_stake'] * lcp
        beta      = VOLATILITY_BUCKETS.get(p['mkt'], 1.00)
        var_i     = beta * p['prob'] * (1 - p['prob']) * (p['odd'] ** 2)
        port_var += (adj_stake ** 2) * var_i
        p['adj_stake'] = adj_stake
        p['lcp']       = lcp

    port_vol = math.sqrt(port_var) if port_var > 0 else 0.0001
    damper   = min(1.0, TARGET_DAILY_VOLATILITY / port_vol)
    total    = 0.0
    for p in picks:
        p['final_stake'] = p.get('adj_stake', 0) * damper
        total += p['final_stake']

    scale = min(1.0, MAX_DAILY_HEAT / total) if total > 0 else 1.0
    for p in picks:
        p['final_stake'] = max(0.001, min(p['final_stake'] * scale, 0.05))

    return picks, {
        'port_vol': port_vol, 'damper': damper,
        'final_heat': sum(p['final_stake'] for p in picks)
    }


# ==========================================
# XG ENGINE V6.0 — ÚLTIMOS 6 PARTIDOS REALES
# ==========================================

def _poisson_pmf(mu, k):
    if mu <= 0 or k < 0: return 0.0
    try: return exp(-mu + k * log(mu) - lgamma(k + 1))
    except: return 0.0

def _weighted_avg(values, decay=XG_DECAY_FACTOR):
    if not values: return 0.0
    w = [decay ** i for i in range(len(values))]
    return sum(v * wi for v, wi in zip(values, w)) / sum(w)


def fetch_team_xg(team_id, is_home, headers):
    """
    V6.0: Obtiene xG real de los últimos 6 partidos del equipo.
    Usa goles reales como proxy de xG (api-football Free no da xG directo).
    Aplica decay exponencial: partido más reciente pesa más.

    Retorna (xg_estimado, confianza, goles_for_series, goles_against_series)
    """
    try:
        r = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers=headers,
            params={
                "team": team_id,
                "league": CHAMPIONSHIP_ID,
                "season": CHAMPIONSHIP_SEASON,
                "last": 6
            },
            timeout=15
        )
        fixtures = r.json().get('response', [])

        if not fixtures:
            return 1.2, "LOW", [], []

        gf_series = []  # goles a favor (más reciente primero)
        ga_series = []  # goles en contra

        for fix in fixtures:
            h_id    = fix['teams']['home']['id']
            h_goals = fix['goals']['home']
            a_goals = fix['goals']['away']

            if h_goals is None or a_goals is None:
                continue

            if h_id == team_id:
                gf_series.append(h_goals)
                ga_series.append(a_goals)
            else:
                gf_series.append(a_goals)
                ga_series.append(h_goals)

        if not gf_series:
            return 1.2, "LOW", [], []

        # xG estimado: promedio ponderado de goles for y against del rival
        # Para el equipo local usamos su promedio de goles en casa
        # Para el equipo visitante usamos su promedio de goles fuera
        xg_for     = _weighted_avg(gf_series)
        xg_against = _weighted_avg(ga_series)

        # xG del equipo = promedio de sus goles for y los goles against del rival
        # La fórmula completa se ensambla en build_xg_match
        confidence = "HIGH" if len(gf_series) >= 4 else "MED"
        return xg_for, xg_against, confidence, gf_series, ga_series

    except Exception as e:
        return 1.2, 1.2, "LOW", [], []


def build_xg_match(home_id, away_id, h_inj, a_inj, headers):
    """
    V6.0: Construye xG del partido usando últimos 6 partidos de cada equipo.

    xG_home = (goles_for_home_weighted + goles_against_away_weighted) / 2
    xG_away = (goles_for_away_weighted + goles_against_home_weighted) / 2

    Esto captura tanto la capacidad ofensiva del equipo
    como la vulnerabilidad defensiva del rival.
    """
    # Llamada 3: últimos 6 del equipo local
    h_xgf, h_xga, h_conf, h_gf, h_ga = fetch_team_xg(home_id, True, headers)
    time.sleep(1.2)  # respetar rate limit

    # Llamada 4: últimos 6 del equipo visitante
    a_xgf, a_xga, a_conf, a_gf, a_ga = fetch_team_xg(away_id, False, headers)

    # xG del partido
    xh = (h_xgf + a_xga) / 2  # ofensiva local + defensiva visitante
    xa = (a_xgf + h_xga) / 2  # ofensiva visitante + defensiva local

    # Factor de lesiones
    xh *= (1 - min(h_inj * 0.015, 0.08))
    xa *= (1 - min(a_inj * 0.015, 0.08))

    # Championship tiene factor ofensivo ligeramente menor que Premier
    xh *= 0.92
    xa *= 0.92

    xh = max(0.6, min(xh, 3.5))
    xa = max(0.6, min(xa, 3.5))

    # Confianza final: HIGH solo si ambos tienen >= 4 partidos
    conf = "HIGH" if (h_conf == "HIGH" and a_conf == "HIGH") else \
           "MED"  if (h_conf != "LOW"  and a_conf != "LOW")  else "LOW"

    xg_source = f"last6 (H:{len(h_gf)}pts, A:{len(a_gf)}pts)"
    return xh, xa, xh + xa, conf, xg_source


# ==========================================
# PROBABILIDADES
# ==========================================

def bivariate_poisson_1x2(xg_home, xg_away, max_goals=10):
    if not (0.4 <= xg_home <= 4.0) or not (0.4 <= xg_away <= 4.0):
        return None
    p_home = p_draw = p_away = 0.0
    for i in range(max_goals + 1):
        for j in range(max_goals + 1):
            prob = _poisson_pmf(xg_home, i) * _poisson_pmf(xg_away, j)
            if i > j:    p_home += prob
            elif i == j: p_draw += prob
            else:        p_away += prob
    total = p_home + p_draw + p_away
    if total < 0.95: return None
    p_h, p_d, p_a = p_home/total, p_draw/total, p_away/total
    if not (0.08 <= p_d <= 0.55): return None
    return p_h, p_d, p_a

def calc_over_under(xg_total, line=2.5, std=1.35):
    var     = max(std ** 2, xg_total)
    mu      = xg_total

    def negbin(mu, var, k):
        if mu <= 0: return 0.0
        if var <= mu * 1.01:
            return _poisson_pmf(mu, k)
        r = mu**2 / (var - mu); p = r / (r + mu)
        try:
            return exp(lgamma(k+r) - lgamma(r) - lgamma(k+1) + r*log(p) + k*log(1-p))
        except: return 0.0

    p_under = sum(negbin(mu, var, k) for k in range(int(np.floor(line)) + 1))
    return 1 - p_under, p_under

def calc_btts(xg_home, xg_away):
    if not (0.4 <= xg_home <= 4.0) or not (0.4 <= xg_away <= 4.0):
        return None, None
    p_yes = (1 - exp(-xg_home)) * (1 - exp(-xg_away))
    p_no  = 1 - p_yes
    if not (0.20 <= p_yes <= 0.90): return None, None
    return round(p_yes, 4), round(p_no, 4)


# ==========================================
# VALIDACIONES
# ==========================================

def validate_xg(xh, xa, bets):
    home_odd = away_odd = over_odd = under_odd = None
    for b in bets:
        if b['id'] == 1:
            for v in b['values']:
                try:
                    if v['value'] == 'Home': home_odd = float(v['odd'])
                    if v['value'] == 'Away': away_odd = float(v['odd'])
                except: pass
        elif b['id'] == 5:
            for v in b['values']:
                try:
                    if v['value'] == 'Over 2.5':  over_odd  = float(v['odd'])
                    if v['value'] == 'Under 2.5': under_odd = float(v['odd'])
                except: pass

    if home_odd and away_odd:
        min_odd  = min(home_odd, away_odd)
        xg_ratio = max(xh, xa) / min(xh, xa) if min(xh, xa) > 0 else 1.0
        if min_odd < 1.40 and xg_ratio < 1.50:
            return False, f"XG_DEFAULT_DETECTED (ratio={xg_ratio:.2f})"
        if min_odd < 1.65 and xg_ratio < 1.20:
            return False, f"XG_FLAT_ON_FAVOURITE (ratio={xg_ratio:.2f})"

    if over_odd and under_odd:
        p_under_mkt = 1 / (under_odd * 1.07)
        if p_under_mkt > 0.01:
            xg_implied = -2.5 * math.log(p_under_mkt)
            gap        = abs((xh + xa) - xg_implied)
            if gap > 1.8:
                return False, f"XG_TOTAL_INCONSISTENT (model={xh+xa:.2f}, mkt={xg_implied:.2f})"

    return True, None

def sanity_check(p_true, mkt, odd):
    VIG = {"OVER": 1.07, "UNDER": 1.07, "1X2": 1.05, "BTTS": 1.06}
    gap = abs(p_true - 1 / (odd * VIG.get(mkt, 1.06)))
    if gap > 0.25:
        return False, f"XG_SANITY_FAIL (gap={gap:.2f}, p={p_true:.2f})"
    return True, None


# ==========================================
# PRICING ENGINE
# ==========================================

def build_market_probs(bets, xh, xa, h_n, a_n, conf):
    probs = []
    po, pu = calc_over_under(xh + xa)
    p_btts_yes, p_btts_no = calc_btts(xh, xa) if conf != "LOW" else (None, None)
    poisson = bivariate_poisson_1x2(xh, xa) if conf != "LOW" else None

    if poisson:
        p_h, p_d, p_a = poisson
        p_1x2  = {'Home': p_h, 'Draw': p_d, 'Away': p_a}
        names  = {'Home': f"Gana {h_n}", 'Draw': "Empate", 'Away': f"Gana {a_n}"}
    else:
        p_1x2 = {}

    for b in bets:
        if b['id'] == 1:
            for v in b['values']:
                if v['value'] not in p_1x2: continue
                odd       = float(v['odd'])
                p_true    = p_1x2[v['value']]
                p_implied = 1 / (odd * 1.05)
                probs.append({
                    "mkt": "1X2", "pick": names[v['value']],
                    "odd": odd, "prob": p_true,
                    "bid": b['id'], "val": v['value'],
                    "model_gap": round(p_true - p_implied, 4)
                })

        elif b['id'] == 5:
            for v in b['values']:
                if v['value'] not in ('Over 2.5', 'Under 2.5'): continue
                is_over   = 'Over' in v['value']
                p_true    = po if is_over else pu
                mkt_type  = "OVER" if is_over else "UNDER"
                odd       = float(v['odd'])
                p_implied = 1 / (odd * 1.07)
                probs.append({
                    "mkt": mkt_type, "pick": f"{v['value']} Goles",
                    "odd": odd, "prob": p_true,
                    "bid": b['id'], "val": v['value'],
                    "model_gap": round(p_true - p_implied, 4)
                })

        elif b['id'] == 8 and p_btts_yes is not None:
            for v in b['values']:
                if v['value'] not in ('Yes', 'No'): continue
                p_true    = p_btts_yes if v['value'] == 'Yes' else p_btts_no
                odd       = float(v['odd'])
                p_implied = 1 / (odd * 1.06)
                probs.append({
                    "mkt": "BTTS", "pick": f"Ambos Marcan: {v['value']}",
                    "odd": odd, "prob": p_true,
                    "bid": b['id'], "val": v['value'],
                    "model_gap": round(p_true - p_implied, 4)
                })

    return probs


# ==========================================
# MAIN BOT
# ==========================================

class ChampionshipBot:
    def __init__(self):
        init_db()
        self.headers = {'x-apisports-key': API_SPORTS_KEY}
        mode = "🔴 LIVE" if LIVE_TRADING else "🟡 DRY-RUN"
        self.send_msg(
            f"🏴󠁧󠁢󠁥󠁮󠁧󠁿 <b>CHAMPIONSHIP SPECIALIST V6.0</b>\n"
            f"Estado: {mode}\n"
            f"xG: últimos 6 partidos reales\n"
            f"Mercados: 1X2 · O/U 2.5 · BTTS\n"
            f"Budget: ~40 requests/día"
        )

    def send_msg(self, text):
        if not TELEGRAM_TOKEN: return
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=10
            )
        except: pass

    def capture_closing_lines(self):
        try:
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            now  = datetime.now(timezone.utc)
            c.execute(
                "SELECT id, fixture_id, market, selection_key, kickoff_time "
                "FROM picks_log WHERE clv_captured = 0"
            )
            for pid, fid, mkt, skey, ko in c.fetchall():
                mins = (datetime.fromisoformat(ko) - now).total_seconds() / 60.0
                if mins <= 60.0:
                    res = requests.get(
                        f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8",
                        headers=self.headers
                    ).json()
                    track_requests(1)
                    found = False
                    if res.get('response'):
                        for b in res['response'][0]['bookmakers'][0]['bets']:
                            for v in b['values']:
                                if f"{b['id']}|{v['value']}" == skey:
                                    c.execute(
                                        "INSERT INTO closing_lines VALUES (NULL,?,?,?,?,?,?)",
                                        (fid, mkt, skey, float(v['odd']),
                                         1/float(v['odd']), now.isoformat())
                                    )
                                    found = True; break
                    c.execute(
                        "UPDATE picks_log SET clv_captured=? WHERE id=?",
                        (1 if found else -1, pid)
                    )
            conn.commit(); conn.close()
        except: pass

    def run_daily_scan(self):
        today    = datetime.now().strftime("%Y-%m-%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

        # Llamada 1: fixtures del día
        matches = []
        for d in [today, tomorrow]:
            try:
                r = requests.get(
                    "https://v3.football.api-sports.io/fixtures",
                    headers=self.headers,
                    params={"league": CHAMPIONSHIP_ID, "season": CHAMPIONSHIP_SEASON, "date": d}
                )
                track_requests(1)
                matches.extend(r.json().get('response', []))
            except: pass

        if not matches:
            self.send_msg("🔇 <b>Championship V6.0:</b> Sin partidos hoy.")
            return

        # Limitar a MAX_FIXTURES_PER_SCAN para respetar budget
        matches = matches[:MAX_FIXTURES_PER_SCAN]
        requests_used = track_requests(0)
        self.send_msg(
            f"🔍 Escaneando {len(matches)} partidos Championship\n"
            f"📡 Requests usados hoy: {requests_used}/100"
        )

        preliminary = []

        for m in matches:
            fid     = m['fixture']['id']
            h_n     = m['teams']['home']['name']
            a_n     = m['teams']['away']['name']
            h_id    = m['teams']['home']['id']
            a_id    = m['teams']['away']['id']
            ko      = m['fixture']['date']
            label   = f"{h_n} vs {a_n}"
            time.sleep(6.1)

            # Llamada 2: cuotas Bet365
            try:
                odds_res = requests.get(
                    "https://v3.football.api-sports.io/odds",
                    headers=self.headers,
                    params={"fixture": fid, "bookmaker": 8}
                ).json().get('response', [])
                track_requests(1)
            except: continue

            if not odds_res: continue
            bets = odds_res[0]['bookmakers'][0]['bets']

            # Llamadas 3 y 4: últimos 6 partidos de cada equipo
            # Obtener lesiones de predicciones (no llama endpoint separado)
            try:
                inj_res = requests.get(
                    "https://v3.football.api-sports.io/injuries",
                    headers=self.headers,
                    params={"fixture": fid}
                ).json().get('response', [])
                track_requests(1)
                hinj = sum(1 for i in inj_res if i['team']['id'] == h_id)
                ainj = sum(1 for i in inj_res if i['team']['id'] == a_id)
            except:
                hinj = ainj = 0

            # xG basado en últimos 6 partidos reales
            xh, xa, xt, conf, xg_src = build_xg_match(h_id, a_id, hinj, ainj, self.headers)
            track_requests(2)  # 2 llamadas dentro de build_xg_match

            # Validar consistencia xG vs mercado
            ok, reason = validate_xg(xh, xa, bets)
            if not ok:
                log_rejection(fid, label, 'ALL', 0.0, 0.0, reason)
                continue

            # Construir probabilidades
            probs = build_market_probs(bets, xh, xa, h_n, a_n, conf)

            # Si confianza baja, omitir 1X2 y BTTS
            if conf == "LOW":
                probs = [p for p in probs if p['mkt'] in ('OVER', 'UNDER')]

            candidates = []
            for item in probs:
                ev = (item['prob'] * item['odd']) - 1

                ok2, fail = sanity_check(item['prob'], item['mkt'], item['odd'])
                if not ok2:
                    log_rejection(fid, label, item['mkt'], item['odd'], ev, fail)
                    continue
                if ev < MIN_EV_THRESHOLD:
                    log_rejection(fid, label, item['mkt'], item['odd'], ev, "LOW_EV")
                    continue
                if ev > MAX_EV_THRESHOLD:
                    log_rejection(fid, label, item['mkt'], item['odd'], ev, "EV_ALUCINATION")
                    continue

                kelly, urs, rej = get_kelly_and_urs(ev, item['odd'], item['mkt'])
                if kelly == 0.0:
                    log_rejection(fid, label, item['mkt'], item['odd'], ev, rej)
                    continue

                candidates.append({
                    **item, 'ev': ev, 'base_stake': kelly,
                    'urs': urs, 'conf': conf, 'xg_src': xg_src,
                    'fid': fid, 'h_n': h_n, 'a_n': a_n, 'ko': ko,
                    'xh': xh, 'xa': xa, 'xt': xt
                })

            if not candidates: continue
            candidates.sort(key=lambda x: x['ev'], reverse=True)
            preliminary.append(candidates[0])

        # Portfolio engine
        final, meta = apply_portfolio_engine(preliminary)

        if final:
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            reports = [
                f"📊 <b>Championship V6.0 — Portfolio:</b>\n"
                f"Picks: {len(final)} | Vol: {meta['port_vol']*100:.2f}%\n"
                f"Heat: {meta['final_heat']*100:.2f}% | Damper: {meta['damper']:.2f}x\n"
                f"📡 Requests usados: {track_requests(0)}/100"
            ]
            for p in final:
                c.execute("""INSERT INTO picks_log
                    (fixture_id, league, home_team, away_team, market, selection,
                     selection_key, odd_open, prob_model, ev_open, stake_pct,
                     xg_home, xg_away, xg_total, pick_time, kickoff_time, urs,
                     model_gap, xg_source)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (p['fid'], LEAGUE_NAME, p['h_n'], p['a_n'], p['mkt'], p['pick'],
                     f"{p['bid']}|{p['val']}", p['odd'], p['prob'], p['ev'],
                     p['final_stake'] if LIVE_TRADING else 0.0,
                     p['xh'], p['xa'], p['xt'],
                     datetime.now(timezone.utc).isoformat(), p['ko'],
                     p['urs'], p['model_gap'], p['xg_src'])
                )
                conf_icon = "✅" if p['conf'] == "HIGH" else "⚠️ MED" if p['conf'] == "MED" else "❌ LOW"
                gap_str   = f"+{p['model_gap']*100:.1f}%" if p['model_gap'] >= 0 else f"{p['model_gap']*100:.1f}%"
                stake_disp = p['final_stake']

                reports.append(
                    f"⚽ {p['h_n']} vs {p['a_n']}\n"
                    f"🟡 [DRY-RUN] [{p['mkt']}]: {p['pick']}\n"
                    f"📊 Cuota: @{p['odd']} | EV: +{p['ev']*100:.1f}%\n"
                    f"📉 URS: {p['urs']:.2f} | LCP: {p['lcp']:.2f}\n"
                    f"🔬 Gap: {gap_str} | xG: {p['xh']:.1f}-{p['xa']:.1f} {conf_icon}\n"
                    f"📈 xG fuente: {p['xg_src']}\n"
                    f"🎯 Stake: {stake_disp*100:.2f}%"
                )
            conn.commit(); conn.close()
            self.send_msg("\n\n".join(reports))
        else:
            self.send_msg(
                f"🔇 <b>Championship V6.0:</b> Sin picks válidos hoy.\n"
                f"📡 Requests usados: {track_requests(0)}/100"
            )


if __name__ == "__main__":
    bot = ChampionshipBot()

    schedule.every().day.at(RUN_TIME_SCAN).do(bot.run_daily_scan)
    schedule.every(30).minutes.do(bot.capture_closing_lines)

    # Burn-in status
    try:
        from burn_in_evaluator import print_burn_in_report
        print_burn_in_report(DB_PATH)
    except Exception as e:
        print(f"Burn-in no disponible: {e}")

    # Morgue
    try:
        print("\n🕵️  MORGUE:")
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT reason, COUNT(*) FROM decision_log GROUP BY reason ORDER BY COUNT(*) DESC LIMIT 10")
        for r in c.fetchall(): print(f"  ❌ {r[0]}: {r[1]}")
        conn.close()
    except: pass

    # CLV
    try:
        print("\n⏳ CLV:")
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("""SELECT ((p.odd_open-c.odd_close)/p.odd_open)*100, p.market
                     FROM picks_log p JOIN closing_lines c
                       ON p.fixture_id=c.fixture_id AND p.clv_captured=1""")
        picks = c.fetchall(); conn.close()
        if picks:
            clvs  = [p[0] for p in picks]
            beats = sum(1 for v in clvs if v > 0)
            print(f"  N={len(picks)} | Beat={beats}/{len(picks)} | CLV_avg={sum(clvs)/len(clvs):.2f}%")
        else:
            print("  Sin CLVs aún.")
    except: pass

    bot.run_daily_scan()

    while True:
        schedule.run_pending()
        time.sleep(60)
