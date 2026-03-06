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
# V6.2 CHAMPIONSHIP SPECIALIST
# ==========================================
# CAMBIOS VS V6.1 (correcciones de lógica):
#   1. LÓGICA: candidato elegido por ev*urs (valor ajustado al riesgo)
#              en lugar de solo EV — el URS ya no se ignora en la selección
#   2. LÓGICA: std negbinom subido a 1.55 para activar sobredispersión
#              en el rango real de xG de Championship (era 1.35, inefectivo)
#   3. LÓGICA: sanity_check gap reducido 0.25→0.18 para filtrar
#              picks donde el modelo está mal calibrado vs mercado
#   4. LÓGICA: closing_lines protegido contra duplicados con COUNT previo
#   5. LÓGICA: stake mínima forzada eliminada — max(0.0) en lugar de
#              max(0.001) para respetar la lógica Kelly cuando hay poca convicción
#   6. LÓGICA: picks con final_stake < 0.005 filtrados antes de reportar

LIVE_TRADING = False

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
API_SPORTS_KEY   = os.getenv("API_SPORTS_KEY", "")

DB_DIR = os.getenv("DB_DIR", "./data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "quant_v6.db")

RUN_TIME_SCAN       = "09:00"   # D-1: cuotas líquidas, mercado abierto
RUN_TIME_MIDDAY_CLV = "12:30"   # D-0: captura intermedia pre-partido
RUN_TIME_INGEST     = "04:00"   # mantenido por compatibilidad

CHAMPIONSHIP_ID      = 40
CHAMPIONSHIP_SEASONS = [2025, 2024]  # FIX v6.1: fallback automático si 2025 sin datos

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

# Horarios de tareas semanales (días sin jornada)
RUN_TIME_XG_CACHE     = "08:00"   # Lunes: pre-caché xG 24 equipos
RUN_TIME_LINE_MONITOR = "10:00"   # Martes: monitoreo cuotas próxima jornada
RUN_TIME_INJURY_WATCH = "08:00"   # Miércoles: lesiones activas
RUN_TIME_LEAGUE_STATS = "08:00"   # Jueves: standings + stats de temporada

# Cache TTL: si el dato tiene menos de N horas, no volver a llamar la API
XG_CACHE_TTL_HOURS    = 20        # re-cachear si el dato tiene >20h de antigüedad
LINE_ALERT_MOVE_PCT   = 0.08      # alertar si la cuota se mueve >8% desde apertura


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
    # Cache semanal de xG por equipo
    c.execute("""CREATE TABLE IF NOT EXISTS team_xg_cache (
        team_id INTEGER PRIMARY KEY,
        team_name TEXT,
        gf_series TEXT,      -- JSON: lista de goles a favor (más reciente primero)
        ga_series TEXT,      -- JSON: lista de goles en contra
        xg_for REAL,
        xg_against REAL,
        confidence TEXT,
        updated_at DATETIME
    )""")
    # Snapshots de cuotas para monitoreo de movimiento de línea
    c.execute("""CREATE TABLE IF NOT EXISTS line_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER,
        home_team TEXT, away_team TEXT,
        kickoff_time TEXT,
        market TEXT, selection TEXT,
        odd_snapshot REAL,
        odd_open REAL,       -- primera cuota registrada para este fixture+market
        captured_at DATETIME
    )""")
    # Resumen semanal de estadísticas de liga
    c.execute("""CREATE TABLE IF NOT EXISTS league_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        season INTEGER,
        team_id INTEGER, team_name TEXT,
        played INTEGER, wins INTEGER, draws INTEGER, losses INTEGER,
        goals_for INTEGER, goals_against INTEGER,
        avg_goals_for REAL, avg_goals_against REAL,
        captured_at DATETIME
    )""")
    # Registro de lesiones activas por equipo
    c.execute("""CREATE TABLE IF NOT EXISTS injury_watch (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER, team_name TEXT,
        player_name TEXT, injury_type TEXT,
        status TEXT, expected_return TEXT,
        captured_at DATETIME
    )""")
    conn.commit()
    conn.close()


def log_rejection(fixture_id, match, market, odd, ev, reason):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "INSERT INTO decision_log VALUES (NULL,?,?,?,?,?,?,?)",
            (fixture_id, match, market, odd, ev, reason,
             datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
        conn.close()
    except:
        pass


def track_requests(n=1):
    """Registra cuántos requests se han usado hoy. n=0 solo consulta."""
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn  = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT count FROM request_log WHERE date=?", (today,))
        row = c.fetchone()
        if row:
            if n > 0:
                c.execute("UPDATE request_log SET count=count+? WHERE date=?", (n, today))
        else:
            c.execute("INSERT INTO request_log VALUES (NULL,?,?)", (today, max(n, 0)))
        conn.commit()
        c.execute("SELECT count FROM request_log WHERE date=?", (today,))
        total = c.fetchone()[0]
        conn.close()
        return total
    except:
        return 0


# ==========================================
# URS ENGINE
# ==========================================

def get_avg_clv(lookback=30):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""SELECT AVG((p.odd_open - c.odd_close)/p.odd_open)
                     FROM picks_log p JOIN closing_lines c
                       ON p.fixture_id=c.fixture_id AND p.market=c.market
                          AND p.selection_key=c.selection_key
                     WHERE p.clv_captured=1
                     ORDER BY p.id DESC LIMIT ?""", (lookback,))
        res = c.fetchone()[0]
        conn.close()
        return float(res) if res else 0.0
    except:
        return 0.0


def get_clv_sharpe():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""SELECT (p.odd_open - c.odd_close)/p.odd_open
                     FROM picks_log p JOIN closing_lines c
                       ON p.fixture_id=c.fixture_id AND p.clv_captured=1
                     ORDER BY p.id DESC LIMIT 50""")
        clvs = [r[0] for r in c.fetchall()]
        conn.close()
        if len(clvs) < 5:
            return 0.0
        mean, std = np.mean(clvs), np.std(clvs, ddof=1)
        return mean / std if std != 0 else 0.0
    except:
        return 0.0


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
    if not picks:
        return [], {}

    port_var = 0.0
    for p in picks:
        if p['odd'] <= 1.01:
            p['adj_stake'] = 0
            p['lcp'] = 0
            continue
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
        # FIX v6.2: max(0.0) en lugar de max(0.001) — respetar Kelly cuando hay poca convicción
        p['final_stake'] = max(0.0, min(p['final_stake'] * scale, 0.05))

    # FIX v6.2: filtrar picks con stake tan baja que no tiene sentido reportar
    picks = [p for p in picks if p['final_stake'] >= 0.005]

    return picks, {
        'port_vol': port_vol, 'damper': damper,
        'final_heat': sum(p['final_stake'] for p in picks)
    }


# ==========================================
# XG ENGINE V6.1
# ==========================================

def _poisson_pmf(mu, k):
    if mu <= 0 or k < 0:
        return 0.0
    try:
        return exp(-mu + k * log(mu) - lgamma(k + 1))
    except:
        return 0.0


def _weighted_avg(values, decay=XG_DECAY_FACTOR):
    if not values:
        return 0.0
    w = [decay ** i for i in range(len(values))]
    return sum(v * wi for v, wi in zip(values, w)) / sum(w)


def _form_factor(gf_series):
    """
    V6.1: Factor de forma reciente.
    Compara los últimos 3 partidos vs los 3 anteriores.
    Devuelve un multiplicador entre 0.85 y 1.15.
    Si hay menos de 6 partidos, devuelve 1.0 (neutro).
    """
    if len(gf_series) < 6:
        return 1.0
    recent   = _weighted_avg(gf_series[:3])
    previous = _weighted_avg(gf_series[3:6])
    if previous < 0.1:
        return 1.0
    ratio = recent / previous
    return max(0.85, min(ratio, 1.15))


def fetch_team_xg(team_id, season, headers, use_cache=True):
    """
    V6.2: Obtiene xG estimado de los últimos 6 partidos del equipo.
    Si use_cache=True y hay datos frescos en team_xg_cache (<TTL horas),
    los usa directamente sin llamar a la API — ahorra requests en días de jornada.
    """
    import json
    if use_cache:
        try:
            conn_c = sqlite3.connect(DB_PATH)
            cc = conn_c.cursor()
            cc.execute(
                "SELECT gf_series, ga_series, xg_for, xg_against, confidence, updated_at "
                "FROM team_xg_cache WHERE team_id=?", (team_id,)
            )
            row = cc.fetchone()
            conn_c.close()
            if row:
                updated = datetime.fromisoformat(row[5])
                age_hours = (datetime.now(timezone.utc) - updated).total_seconds() / 3600
                if age_hours < XG_CACHE_TTL_HOURS:
                    gf = json.loads(row[0])
                    ga = json.loads(row[1])
                    return float(row[2]), float(row[3]), row[4], gf, ga
        except:
            pass  # cache miss → llamar API normalmente

    try:
        r = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers=headers,
            params={
                "team":   team_id,
                "league": CHAMPIONSHIP_ID,
                "season": season,
                "last":   6
            },
            timeout=15
        )
        fixtures = r.json().get('response', [])

        if not fixtures:
            return 1.2, 1.2, "LOW", [], []  # FIX: 5 valores consistentes

        gf_series = []
        ga_series = []

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
            return 1.2, 1.2, "LOW", [], []  # FIX: 5 valores consistentes

        xg_for     = _weighted_avg(gf_series)
        xg_against = _weighted_avg(ga_series)
        confidence = "HIGH" if len(gf_series) >= 4 else "MED"

        # Guardar en cache para reutilizar en el scan del día de jornada
        try:
            import json
            conn_c = sqlite3.connect(DB_PATH)
            cc = conn_c.cursor()
            cc.execute("""INSERT OR REPLACE INTO team_xg_cache
                (team_id, gf_series, ga_series, xg_for, xg_against, confidence, updated_at)
                VALUES (?,?,?,?,?,?,?)""",
                (team_id, json.dumps(gf_series), json.dumps(ga_series),
                 xg_for, xg_against, confidence,
                 datetime.now(timezone.utc).isoformat())
            )
            conn_c.commit()
            conn_c.close()
        except:
            pass

        return xg_for, xg_against, confidence, gf_series, ga_series

    except Exception as e:
        return 1.2, 1.2, "LOW", [], []  # FIX: 5 valores consistentes


def resolve_season(headers):
    """
    V6.1: Detecta automáticamente qué temporada tiene fixtures activos.
    Intenta 2025 primero; si no hay datos, cae a 2024.
    """
    for season in CHAMPIONSHIP_SEASONS:
        try:
            r = requests.get(
                "https://v3.football.api-sports.io/fixtures",
                headers=headers,
                params={
                    "league": CHAMPIONSHIP_ID,
                    "season": season,
                    "next":   1
                },
                timeout=15
            )
            track_requests(1)
            if r.json().get('response'):
                return season
        except:
            pass
    return CHAMPIONSHIP_SEASONS[-1]  # fallback último de la lista


def build_xg_match(home_id, away_id, h_inj, a_inj, season, headers):
    """
    V6.1: Construye xG del partido usando últimos 6 partidos de cada equipo.

    xG_home = (goles_for_home_weighted + goles_against_away_weighted) / 2
    xG_away = (goles_for_away_weighted + goles_against_home_weighted) / 2

    FIX v6.1:
    - fetch_team_xg siempre devuelve 5 valores (bug path de error corregido)
    - sleep subido a 2.0s para respetar rate limit de 10 req/min
    - factor de forma aplicado sobre serie reciente
    - retorna requests_made para tracking preciso
    """
    requests_made = 0

    # Llamada home
    h_xgf, h_xga, h_conf, h_gf, h_ga = fetch_team_xg(home_id, season, headers)
    requests_made += 1
    time.sleep(2.0)  # FIX: era 1.2s, insuficiente para rate limit 10 req/min

    # Llamada away
    a_xgf, a_xga, a_conf, a_gf, a_ga = fetch_team_xg(away_id, season, headers)
    requests_made += 1

    # xG base del partido
    xh = (h_xgf + a_xga) / 2
    xa = (a_xgf + h_xga) / 2

    # V6.1: Factor de forma reciente (últimos 3 vs anteriores 3)
    xh *= _form_factor(h_gf)
    xa *= _form_factor(a_gf)

    # Factor de lesiones
    xh *= (1 - min(h_inj * 0.015, 0.08))
    xa *= (1 - min(a_inj * 0.015, 0.08))

    # Championship tiene factor ofensivo ligeramente menor que Premier
    xh *= 0.92
    xa *= 0.92

    xh = max(0.6, min(xh, 3.5))
    xa = max(0.6, min(xa, 3.5))

    conf = "HIGH" if (h_conf == "HIGH" and a_conf == "HIGH") else \
           "MED"  if (h_conf != "LOW"  and a_conf != "LOW")  else "LOW"

    xg_source = f"last6 (H:{len(h_gf)}pts, A:{len(a_gf)}pts)"

    return xh, xa, xh + xa, conf, xg_source, requests_made


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
    if total < 0.95:
        return None
    p_h, p_d, p_a = p_home/total, p_draw/total, p_away/total
    if not (0.08 <= p_d <= 0.55):
        return None
    return p_h, p_d, p_a


def calc_over_under(xg_total, line=2.5, std=1.55):  # FIX v6.2: 1.35→1.55 activa negbinom en rango real Championship
    var = max(std ** 2, xg_total)
    mu  = xg_total

    def negbin(mu, var, k):
        if mu <= 0:
            return 0.0
        if var <= mu * 1.01:
            return _poisson_pmf(mu, k)
        r = mu**2 / (var - mu)
        p = r / (r + mu)
        try:
            return exp(lgamma(k+r) - lgamma(r) - lgamma(k+1) + r*log(p) + k*log(1-p))
        except:
            return 0.0

    p_under = sum(negbin(mu, var, k) for k in range(int(np.floor(line)) + 1))
    return 1 - p_under, p_under


def calc_btts(xg_home, xg_away):
    if not (0.4 <= xg_home <= 4.0) or not (0.4 <= xg_away <= 4.0):
        return None, None
    p_yes = (1 - exp(-xg_home)) * (1 - exp(-xg_away))
    p_no  = 1 - p_yes
    if not (0.20 <= p_yes <= 0.90):
        return None, None
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
                except:
                    pass
        elif b['id'] == 5:
            for v in b['values']:
                try:
                    if v['value'] == 'Over 2.5':  over_odd  = float(v['odd'])
                    if v['value'] == 'Under 2.5': under_odd = float(v['odd'])
                except:
                    pass

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
    if gap > 0.18:  # FIX v6.2: era 0.25, demasiado permisivo — gap de 0.25 indica modelo mal calibrado
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
                if v['value'] not in p_1x2:
                    continue
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
                if v['value'] not in ('Over 2.5', 'Under 2.5'):
                    continue
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
                if v['value'] not in ('Yes', 'No'):
                    continue
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
        # V6.1: detectar temporada activa una sola vez al arrancar
        self.season = resolve_season(self.headers)
        mode = "🔴 LIVE" if LIVE_TRADING else "🟡 DRY-RUN"
        self.send_msg(
            f"🏴󠁧󠁢󠁥󠁮󠁧󠁿 <b>CHAMPIONSHIP SPECIALIST V6.2</b>\n"
            f"Estado: {mode}\n"
            f"Temporada activa: {self.season}/{self.season+1}\n"
            f"xG: últimos 6 partidos + factor de forma\n"
            f"Mercados: 1X2 · O/U 2.5 · BTTS\n"
            f"Budget: ~40 requests/día"
        )

    def send_msg(self, text):
        if not TELEGRAM_TOKEN:
            return
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=10
            )
        except:
            pass

    def _fetch_and_store_odds(self, c, fid, mkt, skey, pid, now, mark_captured=True):
        """
        Fetches current odds for a pick and stores them.
        mark_captured=True  → cierre final (clv_captured=1), usado 60min antes del KO
        mark_captured=False → snapshot intermedio, no marca como capturado (seguirá
                              siendo sobreescrito por el cierre real)
        """
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
                            "SELECT COUNT(*) FROM closing_lines WHERE fixture_id=? AND market=? AND selection_key=?",
                            (fid, mkt, skey)
                        )
                        exists = c.fetchone()[0] > 0
                        if mark_captured:
                            # Cierre final: INSERT o UPDATE si ya había snapshot intermedio
                            if exists:
                                c.execute(
                                    "UPDATE closing_lines SET odd_close=?, implied_prob_close=?, capture_time=? "
                                    "WHERE fixture_id=? AND market=? AND selection_key=?",
                                    (float(v['odd']), 1/float(v['odd']), now.isoformat(),
                                     fid, mkt, skey)
                                )
                            else:
                                c.execute(
                                    "INSERT INTO closing_lines VALUES (NULL,?,?,?,?,?,?)",
                                    (fid, mkt, skey, float(v['odd']),
                                     1/float(v['odd']), now.isoformat())
                                )
                        else:
                            # Snapshot intermedio: solo INSERT si aún no existe
                            if not exists:
                                c.execute(
                                    "INSERT INTO closing_lines VALUES (NULL,?,?,?,?,?,?)",
                                    (fid, mkt, skey, float(v['odd']),
                                     1/float(v['odd']), now.isoformat())
                                )
                        found = True
                        break
        return found

    def capture_midday_lines(self):
        """
        V6.3: Captura intermedia a las 12:30 UTC del día del partido.
        Guarda un snapshot de la cuota actual sin marcar el pick como capturado —
        el cierre real se sobreescribirá 60 min antes del KO.
        Útil para observar la magnitud del movimiento de línea D-1 → mediodía → cierre.
        Solo actúa sobre picks con kick-off en las próximas 6 horas.
        """
        try:
            conn = sqlite3.connect(DB_PATH)
            c    = conn.cursor()
            now  = datetime.now(timezone.utc)
            c.execute(
                "SELECT id, fixture_id, market, selection_key, kickoff_time "
                "FROM picks_log WHERE clv_captured = 0"
            )
            for pid, fid, mkt, skey, ko in c.fetchall():
                mins = (datetime.fromisoformat(ko) - now).total_seconds() / 60.0
                # Ventana: entre 6h y 2h antes del KO → snapshot intermedio
                if 120.0 <= mins <= 360.0:
                    self._fetch_and_store_odds(c, fid, mkt, skey, pid, now, mark_captured=False)
                    time.sleep(2.0)
            conn.commit()
            conn.close()
        except:
            pass

    def capture_closing_lines(self):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            now  = datetime.now(timezone.utc)
            c.execute(
                "SELECT id, fixture_id, market, selection_key, kickoff_time "
                "FROM picks_log WHERE clv_captured = 0"
            )
            for pid, fid, mkt, skey, ko in c.fetchall():
                mins = (datetime.fromisoformat(ko) - now).total_seconds() / 60.0
                if mins <= 60.0:
                    found = self._fetch_and_store_odds(
                        c, fid, mkt, skey, pid, now, mark_captured=True
                    )
                    time.sleep(2.0)
                    c.execute(
                        "UPDATE picks_log SET clv_captured=? WHERE id=?",
                        (1 if found else -1, pid)
                    )
            conn.commit()
            conn.close()
        except:
            pass

    def weekly_xg_cache(self):
        """
        LUNES 08:00 UTC — Pre-caché xG de los 24 equipos de Championship.
        Llama a last6 de cada equipo y guarda en team_xg_cache.
        En días de jornada, fetch_team_xg leerá de aquí y ahorrará ~20 requests.
        Coste: 24 requests. Si un equipo ya tiene cache fresca (<20h), lo salta.
        """
        import json
        try:
            # Obtener equipos activos de la temporada
            r = requests.get(
                "https://v3.football.api-sports.io/standings",
                headers=self.headers,
                params={"league": CHAMPIONSHIP_ID, "season": self.season}
            )
            track_requests(1)
            standings = r.json().get('response', [])
            if not standings:
                return

            teams = []
            for group in standings:
                for entry in group.get('league', {}).get('standings', [[]])[0]:
                    teams.append((entry['team']['id'], entry['team']['name']))

            cached = skipped = 0
            for team_id, team_name in teams:
                # Comprobar si ya hay cache fresca
                conn = sqlite3.connect(DB_PATH)
                cc = conn.cursor()
                cc.execute("SELECT updated_at FROM team_xg_cache WHERE team_id=?", (team_id,))
                row = cc.fetchone()
                conn.close()
                if row:
                    age = (datetime.now(timezone.utc) -
                           datetime.fromisoformat(row[0])).total_seconds() / 3600
                    if age < XG_CACHE_TTL_HOURS:
                        skipped += 1
                        continue

                # Llamar API y cachear (fetch_team_xg guarda automáticamente)
                fetch_team_xg(team_id, self.season, self.headers, use_cache=False)
                track_requests(1)
                cached += 1
                time.sleep(2.0)

            # Actualizar team_name en cache (no lo guarda fetch_team_xg)
            conn = sqlite3.connect(DB_PATH)
            cc = conn.cursor()
            for team_id, team_name in teams:
                cc.execute("UPDATE team_xg_cache SET team_name=? WHERE team_id=?",
                           (team_name, team_id))
            conn.commit()
            conn.close()

            self.send_msg(
                f"🔄 <b>xG Cache actualizada</b>\nEquipos cacheados: {cached} | Saltados (frescos): {skipped}\n📡 Requests usados hoy: {track_requests(0)}/100"
            )
        except Exception as e:
            self.send_msg(f"⚠️ weekly_xg_cache error: {e}")

    def line_monitor(self):
        """
        MARTES 10:00 UTC — Monitoreo de movimiento de cuotas para la próxima jornada.
        Obtiene fixtures de los próximos 7 días y compara cuotas actuales
        contra la primera cuota registrada (odd_open en line_snapshots).
        Alerta en Telegram si alguna línea se mueve >LINE_ALERT_MOVE_PCT (8%).
        Coste: ~10 requests (MAX_FIXTURES_PER_SCAN).
        """
        try:
            from datetime import timedelta
            target = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
            today  = datetime.now().strftime("%Y-%m-%d")

            matches = []
            for d_offset in range(7):
                d = (datetime.now() + timedelta(days=d_offset)).strftime("%Y-%m-%d")
                r = requests.get(
                    "https://v3.football.api-sports.io/fixtures",
                    headers=self.headers,
                    params={"league": CHAMPIONSHIP_ID, "season": self.season, "date": d}
                )
                track_requests(1)
                matches.extend(r.json().get('response', []))
                if len(matches) >= MAX_FIXTURES_PER_SCAN:
                    break
                time.sleep(2.0)

            matches = matches[:MAX_FIXTURES_PER_SCAN]
            alerts = []
            now    = datetime.now(timezone.utc)

            for m in matches:
                fid = m['fixture']['id']
                h_n = m['teams']['home']['name']
                a_n = m['teams']['away']['name']
                ko  = m['fixture']['date']

                r = requests.get(
                    "https://v3.football.api-sports.io/odds",
                    headers=self.headers,
                    params={"fixture": fid, "bookmaker": 8}
                ).json().get('response', [])
                track_requests(1)
                if not r:
                    time.sleep(2.0)
                    continue

                for b in r[0]['bookmakers'][0]['bets']:
                    for v in b['values']:
                        mkt_key = f"{b['id']}|{v['value']}"
                        odd_now = float(v['odd'])

                        conn = sqlite3.connect(DB_PATH)
                        cc   = conn.cursor()
                        cc.execute(
                            "SELECT odd_open FROM line_snapshots "
                            "WHERE fixture_id=? AND market=? ORDER BY id ASC LIMIT 1",
                            (fid, mkt_key)
                        )
                        first = cc.fetchone()

                        if first:
                            odd_open = first[0]
                            move = abs(odd_now - odd_open) / odd_open
                            if move >= LINE_ALERT_MOVE_PCT:
                                direction = "📉" if odd_now < odd_open else "📈"
                                alerts.append(
                                    f"{direction} <b>{h_n} vs {a_n}</b>\n"
                                    f"   {v['value']}: {odd_open:.2f} → {odd_now:.2f} "
                                    f"({move*100:+.1f}%)"
                                )
                        else:
                            # Primera vez que vemos este fixture — guardar como baseline
                            cc.execute(
                                """INSERT INTO line_snapshots
                                   (fixture_id, home_team, away_team, kickoff_time,
                                    market, selection, odd_snapshot, odd_open, captured_at)
                                   VALUES (?,?,?,?,?,?,?,?,?)""",
                                (fid, h_n, a_n, ko, mkt_key, v['value'],
                                 odd_now, odd_now, now.isoformat())
                            )
                        cc.execute(
                            """INSERT INTO line_snapshots
                               (fixture_id, home_team, away_team, kickoff_time,
                                market, selection, odd_snapshot, odd_open, captured_at)
                               VALUES (?,?,?,?,?,?,?,?,?)""",
                            (fid, h_n, a_n, ko, mkt_key, v['value'],
                             odd_now,
                             first[0] if first else odd_now,
                             now.isoformat())
                        )
                        conn.commit()
                        conn.close()
                time.sleep(2.0)

            if alerts:
                self.send_msg(
                    "🚨 <b>Line Monitor — Movimientos >8%</b>\n\n" +
                    "\n\n".join(alerts) +
                    f"\n\n📡 Requests: {track_requests(0)}/100"
                )
            else:
                self.send_msg(
                    f"✅ <b>Line Monitor:</b> Sin movimientos significativos.\n"
                    f"📡 Requests: {track_requests(0)}/100"
                )
        except Exception as e:
            self.send_msg(f"⚠️ line_monitor error: {e}")

    def injury_watch(self):
        """
        MIÉRCOLES 08:00 UTC — Lesiones activas de los 24 equipos.
        Guarda en injury_watch y alerta si hay bajas en equipos
        con partido en los próximos 5 días.
        Coste: 1 request (endpoint /injuries por temporada, no por fixture).
        """
        try:
            r = requests.get(
                "https://v3.football.api-sports.io/injuries",
                headers=self.headers,
                params={"league": CHAMPIONSHIP_ID, "season": self.season}
            )
            track_requests(1)
            injuries = r.json().get('response', [])
            if not injuries:
                self.send_msg("✅ <b>Injury Watch:</b> Sin lesiones registradas.")
                return

            conn = sqlite3.connect(DB_PATH)
            cc   = conn.cursor()
            now  = datetime.now(timezone.utc)

            # Limpiar registros anteriores y reescribir
            cc.execute("DELETE FROM injury_watch")
            team_counts = {}
            for inj in injuries:
                tid   = inj['team']['id']
                tname = inj['team']['name']
                pname = inj['player']['name']
                itype = inj.get('player', {}).get('type', 'Unknown')
                status = inj.get('player', {}).get('reason', 'Unknown')

                cc.execute(
                    """INSERT INTO injury_watch
                       (team_id, team_name, player_name, injury_type, status,
                        expected_return, captured_at)
                       VALUES (?,?,?,?,?,?,?)""",
                    (tid, tname, pname, itype, status, 'N/A', now.isoformat())
                )
                team_counts[tname] = team_counts.get(tname, 0) + 1

            conn.commit()
            conn.close()

            # Alertar equipos con 3+ bajas (impacto potencial en xG)
            heavy = [(t, n) for t, n in team_counts.items() if n >= 3]
            if heavy:
                lines = "\n".join(f"  🚑 {t}: {n} bajas" for t, n in
                                   sorted(heavy, key=lambda x: -x[1]))
                self.send_msg(
                    f"🚑 <b>Injury Watch — Equipos con 3+ bajas:</b>\n{lines}\n"
                    f"Total lesionados: {len(injuries)}\n"
                    f"📡 Requests: {track_requests(0)}/100"
                )
            else:
                self.send_msg(
                    f"✅ <b>Injury Watch:</b> {len(injuries)} lesionados, "
                    f"ningún equipo con 3+ bajas.\n"
                    f"📡 Requests: {track_requests(0)}/100"
                )
        except Exception as e:
            self.send_msg(f"⚠️ injury_watch error: {e}")

    def league_stats_ingest(self):
        """
        JUEVES 08:00 UTC — Standings + estadísticas de goles de la temporada.
        Guarda promedios de goles for/against por equipo en league_stats.
        Útil para calibrar si el modelo xG (basado en last6) diverge
        de la media de temporada completa — señal de equipo en racha atípica.
        Coste: 1 request.
        """
        try:
            r = requests.get(
                "https://v3.football.api-sports.io/standings",
                headers=self.headers,
                params={"league": CHAMPIONSHIP_ID, "season": self.season}
            )
            track_requests(1)
            standings = r.json().get('response', [])
            if not standings:
                return

            conn = sqlite3.connect(DB_PATH)
            cc   = conn.cursor()
            now  = datetime.now(timezone.utc)
            cc.execute("DELETE FROM league_stats WHERE season=?", (self.season,))

            teams_saved = 0
            for group in standings:
                for entry in group.get('league', {}).get('standings', [[]])[0]:
                    t   = entry['team']
                    all = entry['all']
                    played = all['played']
                    gf     = all['goals']['for']
                    ga     = all['goals']['against']
                    cc.execute(
                        """INSERT INTO league_stats
                           (season, team_id, team_name, played, wins, draws, losses,
                            goals_for, goals_against, avg_goals_for, avg_goals_against,
                            captured_at)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (self.season, t['id'], t['name'],
                         played, all['win'], all['draw'], all['lose'],
                         gf, ga,
                         round(gf / played, 3) if played else 0,
                         round(ga / played, 3) if played else 0,
                         now.isoformat())
                    )
                    teams_saved += 1

            conn.commit()
            conn.close()
            self.send_msg(
                f"📊 <b>League Stats actualizadas</b>\n"
                f"Equipos: {teams_saved} | Temporada: {self.season}/{self.season+1}\n"
                f"📡 Requests: {track_requests(0)}/100"
            )
        except Exception as e:
            self.send_msg(f"⚠️ league_stats_ingest error: {e}")

    def run_daily_scan(self):
        # V6.3: scan corre a las 09:00 UTC del día anterior (D-1)
        # Busca partidos de mañana y pasado mañana para capturar cuotas líquidas
        # con suficiente antelación. La Championship juega mayoritariamente
        # a las 15:00 y 19:45 UK, así que D-1 a las 09:00 UTC = ~30h de antelación.
        tomorrow       = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        day_after      = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

        # Llamada 1+2: fixtures de mañana y pasado mañana
        matches = []
        for d in [tomorrow, day_after]:
            try:
                r = requests.get(
                    "https://v3.football.api-sports.io/fixtures",
                    headers=self.headers,
                    params={
                        "league":  CHAMPIONSHIP_ID,
                        "season":  self.season,
                        "date":    d
                    }
                )
                track_requests(1)
                matches.extend(r.json().get('response', []))
            except:
                pass

        if not matches:
            self.send_msg(
                f"🔇 <b>Championship V6.2:</b> Sin partidos en los próximos 2 días.\n"
                f"📡 Temporada activa: {self.season}"
            )
            return

        # V6.2: si hay más partidos que el budget permite, priorizar
        # los que tienen kick-off más próximo (mercado más maduro y líquido).
        # Descartar partidos a más de 48h — cuotas aún poco representativas.
        now_utc = datetime.now(timezone.utc)
        def fixture_hours_away(m):
            try:
                ko = datetime.fromisoformat(m['fixture']['date'].replace('Z', '+00:00'))
                return (ko - now_utc).total_seconds() / 3600
            except:
                return 999

        matches = [m for m in matches if fixture_hours_away(m) <= 48]
        matches.sort(key=fixture_hours_away)          # más próximos primero
        matches = matches[:MAX_FIXTURES_PER_SCAN]
        requests_used = track_requests(0)
        self.send_msg(
            f"🔍 Escaneando {len(matches)} partidos Championship (D-1)\n"
            f"📅 Temporada: {self.season}/{self.season+1}\n"
            f"📡 Requests usados hoy: {requests_used}/100"
        )

        preliminary = []

        for m in matches:
            fid  = m['fixture']['id']
            h_n  = m['teams']['home']['name']
            a_n  = m['teams']['away']['name']
            h_id = m['teams']['home']['id']
            a_id = m['teams']['away']['id']
            ko   = m['fixture']['date']
            label = f"{h_n} vs {a_n}"
            time.sleep(6.1)

            # Llamada: cuotas Bet365
            try:
                odds_res = requests.get(
                    "https://v3.football.api-sports.io/odds",
                    headers=self.headers,
                    params={"fixture": fid, "bookmaker": 8}
                ).json().get('response', [])
                track_requests(1)
            except:
                continue

            if not odds_res:
                continue
            bets = odds_res[0]['bookmakers'][0]['bets']

            # Llamada: lesiones
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

            # V6.1: xG basado en últimos 6 + factor de forma
            # retorna requests_made para tracking preciso
            xh, xa, xt, conf, xg_src, req_made = build_xg_match(
                h_id, a_id, hinj, ainj, self.season, self.headers
            )
            track_requests(req_made)  # FIX: tracking exacto de requests reales

            # Validar consistencia xG vs mercado
            ok, reason = validate_xg(xh, xa, bets)
            if not ok:
                log_rejection(fid, label, 'ALL', 0.0, 0.0, reason)
                continue

            probs = build_market_probs(bets, xh, xa, h_n, a_n, conf)

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

            if not candidates:
                continue
            # FIX v6.2: ordenar por ev*urs (valor ajustado al riesgo), no solo EV
            # El URS existe para ser el criterio de selección — ignorarlo aquí lo anulaba
            candidates.sort(key=lambda x: x['ev'] * x['urs'], reverse=True)
            preliminary.append(candidates[0])

        # Portfolio engine
        final, meta = apply_portfolio_engine(preliminary)

        if final:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            reports = [
                f"📊 <b>Championship V6.2 — Portfolio:</b>\n"
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
            conn.commit()
            conn.close()
            self.send_msg("\n\n".join(reports))
        else:
            self.send_msg(
                f"🔇 <b>Championship V6.2:</b> Sin picks válidos hoy.\n"
                f"📡 Requests usados: {track_requests(0)}/100"
            )


if __name__ == "__main__":
    bot = ChampionshipBot()

    # ── TAREAS DIARIAS ──────────────────────────────────────────────
    # Scan D-1 a las 09:00 UTC: captura cuotas de apertura líquidas
    schedule.every().day.at(RUN_TIME_SCAN).do(bot.run_daily_scan)
    # Captura intermedia a las 12:30 UTC del día del partido (2-6h antes del KO)
    schedule.every().day.at(RUN_TIME_MIDDAY_CLV).do(bot.capture_midday_lines)
    # Cierre final: polling cada 30min, actúa solo si quedan ≤60min para el KO
    schedule.every(30).minutes.do(bot.capture_closing_lines)

    # ── TAREAS SEMANALES (días sin jornada) ──────────────────────
    # Lunes 08:00 UTC — pre-caché xG 24 equipos (~24 req, ahorra ~20 en el scan)
    schedule.every().monday.at(RUN_TIME_XG_CACHE).do(bot.weekly_xg_cache)
    # Martes 10:00 UTC — monitoreo movimiento de cuotas próxima jornada (~10 req)
    schedule.every().tuesday.at(RUN_TIME_LINE_MONITOR).do(bot.line_monitor)
    # Miércoles 08:00 UTC — lesiones activas todos los equipos (~1 req)
    schedule.every().wednesday.at(RUN_TIME_INJURY_WATCH).do(bot.injury_watch)
    # Jueves 08:00 UTC — standings + estadísticas de temporada (~1 req)
    schedule.every().thursday.at(RUN_TIME_LEAGUE_STATS).do(bot.league_stats_ingest)

    # Burn-in status
    try:
        from burn_in_evaluator import print_burn_in_report
        print_burn_in_report(DB_PATH)
    except Exception as e:
        print(f"Burn-in no disponible: {e}")

    # Morgue
    try:
        print("\n🕵️  MORGUE:")
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT reason, COUNT(*) FROM decision_log GROUP BY reason ORDER BY COUNT(*) DESC LIMIT 10")
        for r in c.fetchall():
            print(f"  ❌ {r[0]}: {r[1]}")
        conn.close()
    except:
        pass

    # CLV
    try:
        print("\n⏳ CLV:")
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""SELECT ((p.odd_open-c.odd_close)/p.odd_open)*100, p.market
                     FROM picks_log p JOIN closing_lines c
                       ON p.fixture_id=c.fixture_id AND p.clv_captured=1""")
        picks = c.fetchall()
        conn.close()
        if picks:
            clvs  = [p[0] for p in picks]
            beats = sum(1 for v in clvs if v > 0)
            print(f"  N={len(picks)} | Beat={beats}/{len(picks)} | CLV_avg={sum(clvs)/len(clvs):.2f}%")
        else:
            print("  Sin CLVs aún.")
    except:
        pass

    bot.run_daily_scan()

    while True:
        schedule.run_pending()
        time.sleep(60)
