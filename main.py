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
# V6.4 TRIPLE LEAGUE SPECIALIST
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
DB_PATH = os.path.join(DB_DIR, "quant_dual.db")

# Diagnóstico de DB al arrancar — imprime en logs Railway
print(f"  📂 DB_DIR={DB_DIR} | DB_PATH={DB_PATH}")
print(f"  📂 DB existe: {os.path.exists(DB_PATH)} | Tamaño: {os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0} bytes")
try:
    _chk = sqlite3.connect(DB_PATH)
    _xg  = _chk.execute("SELECT COUNT(*) FROM team_xg_cache").fetchone()[0] if _chk.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='team_xg_cache'").fetchone() else "tabla no existe"
    _pk  = _chk.execute("SELECT COUNT(*) FROM picks_log").fetchone()[0] if _chk.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='picks_log'").fetchone() else "tabla no existe"
    _chk.close()
    print(f"  📂 team_xg_cache={_xg} registros | picks_log={_pk} registros")
except Exception as _e:
    print(f"  📂 DB check error: {_e}")

RUN_TIME_SCAN       = "11:00"   # D-1: 11:00 UTC — cuotas abiertas Championship (UK) y Brasileirao (BRT)
RUN_TIME_MIDDAY_CLV = "16:00"   # D-0: 16:00 UTC — antes del KO Championship (15:00 UK) y Brasileirao (20:00 UTC)
RUN_TIME_INGEST     = "04:00"   # mantenido por compatibilidad

# Ligas objetivo — método por fecha evita bloqueo Free tier
TARGET_LEAGUES = {
    40: {
        "name":      "🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP",
        "liquidity": 0.85,
        "xg_std":    1.55,
        "seasons":   [2025, 2024],
    },
    71: {
        "name":      "🇧🇷 BRASILEIRAO",
        "liquidity": 0.80,
        "xg_std":    1.60,
        "seasons":   [2025],
    },
    262: {
        "name":      "🇲🇽 LIGA MX",
        "liquidity": 0.75,   # Bet365 cubre Liga MX pero con menor volumen que europeas
        "xg_std":    1.50,   # varianza similar a Championship
        "seasons":   [2025],
    },
}
MAX_FIXTURES_PER_LEAGUE = 6   # máx por liga — techo global MAX_FIXTURES_TOTAL protege el budget

MAX_FIXTURES_PER_SCAN  = 10  # máx por liga (Championship o Brasileirao tienen ~10 c/u)
MAX_FIXTURES_TOTAL     = 22  # techo global: 22 × 4 + 2 = 90 req — margen de 10 con 3 ligas

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
    # Migración: añadir depth si no existe en instalaciones anteriores
    try:
        c.execute("ALTER TABLE team_xg_cache ADD COLUMN depth INTEGER DEFAULT 6")
    except:
        pass

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
        depth INTEGER DEFAULT 6,  -- cuántos partidos se usaron (6=scan, 10=weekly)
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
    # V5.10 port: auto-heal de CLVs corruptos al arrancar
    # selection_keys válidas tienen formato "bid|value" (ej: "1|Home", "5|Over 2.5")
    # Si el último segmento es un número puro, la key está corrupta
    c.execute("SELECT id, selection_key FROM picks_log WHERE clv_captured = 1")
    for pid, skey in c.fetchall():
        if skey and skey.split('|')[-1].replace('.', '', 1).isdigit():
            c.execute("DELETE FROM closing_lines WHERE selection_key = ?", (skey,))
            c.execute("UPDATE picks_log SET clv_captured = -1 WHERE id = ?", (pid,))

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


def sync_request_counter(headers):
    """
    Sincroniza el contador interno con la API real de api-football.
    Llama al arrancar para corregir desfases entre deploys.
    La API devuelve el conteo real del día — más confiable que nuestro contador.
    """
    try:
        r = requests.get(
            "https://v3.football.api-sports.io/status",
            headers=headers, timeout=10
        )
        raw  = r.json()
        # api-football devuelve dict normalmente, pero lista cuando throttlea
        resp = raw if isinstance(raw, dict) else {}
        data    = resp.get('response', {})
        # response puede ser lista (throttled) o dict (normal)
        if isinstance(data, list):
            data = data[0] if data else {}
        current = data.get('requests', {}).get('current', None)
        if current is None:
            return
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn  = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # Sobrescribir con el valor real de la API
        c.execute("DELETE FROM request_log WHERE date=?", (today,))
        c.execute("INSERT INTO request_log VALUES (NULL,?,?)", (today, int(current)))
        conn.commit()
        conn.close()
        print(f"  📡 Contador sincronizado con API: {current}/100 requests hoy")
    except Exception as e:
        print(f"  ⚠️  sync_request_counter error: {e}")


# ==========================================
# URS ENGINE
# ==========================================

def get_avg_clv(lookback=30, market=None):
    """
    V5.10 port: kill-switch por mercado cuando se especifica market.
    Sin market → CLV global (para Sharpe y métricas generales).
    Con market → CLV específico del mercado (para kill-switch granular).
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        if market:
            c.execute("""SELECT AVG((p.odd_open - c.odd_close)/p.odd_open)
                         FROM picks_log p JOIN closing_lines c
                           ON p.fixture_id=c.fixture_id AND p.market=c.market
                              AND p.selection_key=c.selection_key
                         WHERE p.clv_captured=1 AND p.market=?
                         ORDER BY p.id DESC LIMIT ?""", (market, lookback))
        else:
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
    # V5.10 port: kill-switch granular por mercado
    avg_clv_market = get_avg_clv(market=market)
    avg_clv_global = get_avg_clv()
    # Si el mercado específico tiene CLV muy negativo, pausarlo solo a él
    if avg_clv_market < -0.015:
        return 0.0, 0.0, f"KILL_SWITCH_{market}"
    # Si el CLV global es muy negativo, pausar todo
    if avg_clv_global < -0.025:
        return 0.0, 0.0, "KILL_SWITCH_GLOBAL"
    base_kelly = max(0.0, min(ev / (odd - 1), 0.05))
    # Reducción de stake si el mercado está en zona gris
    if -0.015 <= avg_clv_market < 0.005:
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


def fetch_team_xg(team_id, season, headers, league_id=40, use_cache=True, depth=6):
    """
    V6.4: Obtiene xG estimado buscando partidos del equipo por fecha.
    El Free tier bloquea team+last y team+league+season.
    Único método que funciona: /fixtures?date=YYYY-MM-DD (ya probado).
    Busca hacia atrás día a día hasta encontrar N partidos del equipo.
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
                updated  = datetime.fromisoformat(row[5])
                age_h    = (datetime.now(timezone.utc) - updated).total_seconds() / 3600
                if age_h < XG_CACHE_TTL_HOURS:
                    gf = json.loads(row[0])
                    ga = json.loads(row[1])
                    print(f"    xG [{team_id}] CACHE HIT age={age_h:.1f}h conf={row[4]}")
                    return float(row[2]), float(row[3]), row[4], gf, ga, True
        except:
            pass

    # Buscar partidos del equipo fecha a fecha hacia atrás
    # 1 req por fecha, la misma fecha sirve para ambos equipos del partido
    gf_series = []
    ga_series = []
    days_searched = 0
    MAX_DAYS_BACK = 120  # 4 meses — cubre gap entre temporadas (Brasileirao dic→mar)

    def _extract_goals(fixtures, tid, strict_league=True):
        """Extrae goles del equipo de una lista de fixtures."""
        gf, ga = [], []
        for fix in fixtures:
            if strict_league and fix['league']['id'] != league_id:
                continue
            if fix['fixture']['status']['short'] != 'FT':
                continue
            h_id    = fix['teams']['home']['id']
            a_id    = fix['teams']['away']['id']
            h_goals = fix['goals']['home']
            a_goals = fix['goals']['away']
            if h_goals is None or a_goals is None:
                continue
            if h_id == tid:
                gf.append(h_goals); ga.append(a_goals)
            elif a_id == tid:
                gf.append(a_goals); ga.append(h_goals)
        return gf, ga

    for days_back in range(1, MAX_DAYS_BACK + 1):
        if len(gf_series) >= depth:
            break
        d = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        already_cached = d in _DATE_FIXTURES_CACHE
        all_day = _get_fixtures_for_date(d, headers)
        if not already_cached:
            days_searched += 1
        gf_day, ga_day = _extract_goals(all_day, team_id, strict_league=True)
        gf_series.extend(gf_day)
        ga_series.extend(ga_day)

    # Fallback: si no encontró suficientes partidos en la liga principal,
    # aceptar cualquier liga (Copa, torneo local) para tener forma del equipo
    if len(gf_series) < 2:
        gf_any, ga_any = [], []
        for days_back in range(1, MAX_DAYS_BACK + 1):
            if len(gf_any) >= depth:
                break
            d = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
            all_day = _get_fixtures_for_date(d, headers)
            gf_day, ga_day = _extract_goals(all_day, team_id, strict_league=False)
            gf_any.extend(gf_day)
            ga_any.extend(ga_day)
        if len(gf_any) > len(gf_series):
            print(f"    xG [{team_id}] fallback sin filtro liga: {len(gf_any)} partidos")
            gf_series, ga_series = gf_any, ga_any

    if not gf_series:
        print(f"    xG [{team_id}] sin partidos en {MAX_DAYS_BACK} días — DEFAULT 1.2/1.2 LOW")
        return 1.2, 1.2, "LOW", [], [], False

    xg_for     = _weighted_avg(gf_series)
    xg_against = _weighted_avg(ga_series)
    confidence = "HIGH" if len(gf_series) >= max(4, depth // 2) else "MED"
    print(f"    xG [{team_id}] {len(gf_series)} partidos en {days_searched} días — xG={xg_for:.2f}/{xg_against:.2f} {confidence}")

    try:
        conn_c = sqlite3.connect(DB_PATH)
        cc = conn_c.cursor()
        cc.execute("""INSERT OR REPLACE INTO team_xg_cache
            (team_id, gf_series, ga_series, xg_for, xg_against, confidence, updated_at, depth)
            VALUES (?,?,?,?,?,?,?,?)""",
            (team_id, json.dumps(gf_series), json.dumps(ga_series),
             xg_for, xg_against, confidence,
             datetime.now(timezone.utc).isoformat(), depth))
        conn_c.commit()
        conn_c.close()
    except:
        pass

    return xg_for, xg_against, confidence, gf_series, ga_series, False

def resolve_seasons(headers):
    """
    V6.4: Detecta temporada activa para CADA liga en TARGET_LEAGUES.
    Método por fecha — evita bloqueo Free tier de api-football.
    Retorna dict: {league_id: season}
    """
    seasons = {}
    # Buscar fixtures de los próximos 10 días para capturar ambas ligas
    # (Brasileirao puede no tener jornada esta semana si está en inicio de temporada)
    dates = [(datetime.now() + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(10)]
    found_leagues = set()

    for d in dates:
        if len(found_leagues) == len(TARGET_LEAGUES):
            break  # ya encontramos todas las ligas
        try:
            r = requests.get(
                "https://v3.football.api-sports.io/fixtures",
                headers=headers,
                params={"date": d},
                timeout=15
            )
            track_requests(1)
            for fix in r.json().get('response', []):
                lid = fix['league']['id']
                if lid in TARGET_LEAGUES and lid not in found_leagues:
                    seasons[lid] = fix['league']['season']
                    found_leagues.add(lid)
                    print(f"  ✅ {TARGET_LEAGUES[lid]['name']}: season={seasons[lid]} (fixtures el {d})")
        except:
            pass

    # Fallback para ligas sin fixtures próximos (parón o inicio de temporada)
    for lid, cfg in TARGET_LEAGUES.items():
        if lid not in seasons:
            seasons[lid] = cfg['seasons'][-1]
            print(f"  ⚠️  {cfg['name']}: sin fixtures próximos — usando season={seasons[lid]}")

    return seasons


def build_xg_match(home_id, away_id, h_inj, a_inj, season, headers, league_id=40, depth=4):
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

    # Llamada home — si viene de cache, cache_hit=True y no se gastó request
    h_xgf, h_xga, h_conf, h_gf, h_ga, h_cached = fetch_team_xg(home_id, season, headers)
    if not h_cached:
        requests_made += 1
        time.sleep(2.0)  # respetar rate limit 10 req/min solo si llamamos API

    # Llamada away
    a_xgf, a_xga, a_conf, a_gf, a_ga, a_cached = fetch_team_xg(away_id, season, headers)
    if not a_cached:
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
        # V5.10 port: detección directa del xG default 1.4/1.4 con favorito claro
        if 1.30 <= xh <= 1.50 and 1.30 <= xa <= 1.50 and min_odd < 1.60:
            return False, f"XG_LIKELY_DEFAULT (xh={xh:.2f}, xa={xa:.2f}, fav={min_odd:.2f})"

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


# Cache de respuestas /fixtures?date= compartida durante el scan
# Evita pedir la misma fecha múltiples veces para distintos equipos
_DATE_FIXTURES_CACHE = {}  # {date_str: [fixture, ...]}

def _get_fixtures_for_date(d, headers):
    """Obtiene fixtures de una fecha, usando cache en memoria del scan actual."""
    if d not in _DATE_FIXTURES_CACHE:
        try:
            r = requests.get(
                "https://v3.football.api-sports.io/fixtures",
                headers=headers,
                params={"date": d},
                timeout=10
            )
            _DATE_FIXTURES_CACHE[d] = r.json().get('response', [])
            time.sleep(0.3)
        except:
            _DATE_FIXTURES_CACHE[d] = []
    return _DATE_FIXTURES_CACHE[d]


def clear_date_cache():
    """Limpiar cache de fechas al inicio de cada scan."""
    _DATE_FIXTURES_CACHE.clear()


# ==========================================
# MAIN BOT
# ==========================================

class TripleLeagueBot:
    def __init__(self):
        init_db()
        self.headers = {'x-apisports-key': API_SPORTS_KEY}

        # Sincronizar contador de requests con la API real antes de cualquier lógica
        # Evita que deploys repetidos acumulen conteos falsos en request_log
        sync_request_counter(self.headers)

        # ── DIAGNÓSTICO DE ARRANQUE ──────────────────────────────────────────
        # Verifica API key, plan, acceso a Championship y temporada activa.
        # Todo se imprime en logs de Railway Y se envía a Telegram.
        api_ok, plan_info, req_info, access_ok, access_detail = self._startup_diagnostics()

        # Detectar temporada solo si la API responde
        self.seasons = resolve_seasons(self.headers)  # dict {league_id: season} if api_ok else (CHAMPIONSHIP_SEASON_OVERRIDE or 2025)

        mode = "🔴 LIVE" if LIVE_TRADING else "🟡 DRY-RUN"

        status_lines = [
            f"🌎 <b>TRIPLE LEAGUE BOT V6.4</b>",
            f"Estado: {mode}",
            f"",
            f"{'✅' if api_ok else '❌'} API: {plan_info}",
            f"📡 Requests hoy: {req_info}",
            f"{'✅' if access_ok else '❌'} Ligas: {access_detail}",
        ]
        if not api_ok:
            status_lines.append("⛔ Sin API — el bot no puede escanear partidos")

        self.send_msg("\n".join(status_lines))

    def _startup_diagnostics(self):
        """
        Verifica al arrancar:
        1. API key válida y plan activo
        2. Requests disponibles hoy
        3. Si Championship (ID 40) está accesible en el plan Free
           (algunas ligas están bloqueadas en Free tier)
        4. Si hay fixtures disponibles para la temporada
        Retorna: (api_ok, plan_info, req_info, championship_ok, detail)
        """
        # ── 1. Estado de la cuenta ───────────────────────────────────────────
        try:
            r = requests.get(
                "https://v3.football.api-sports.io/status",
                headers=self.headers,
                timeout=10
            )
            track_requests(1)
            raw_resp = r.json()
            data     = raw_resp if isinstance(raw_resp, dict) else {}
            resp_val = data.get('response', {})
            if isinstance(resp_val, list):
                resp_val = resp_val[0] if resp_val else {}
            sub  = resp_val.get('subscription', {})
            reqs = resp_val.get('requests', {})
            plan    = sub.get('plan', 'Unknown')
            active  = sub.get('active', False)
            current = reqs.get('current', '?')
            limit   = reqs.get('limit_day', '?')

            plan_info = f"{plan} ({'activo' if active else '⚠️ INACTIVO'})"
            req_info  = f"{current}/{limit}"

            print(f"  API plan: {plan_info} | Requests: {req_info}")

            if not active:
                return False, plan_info, req_info, False, "suscripción inactiva"

        except Exception as e:
            print(f"  ❌ API status error: {e}")
            return False, "error de conexión", "?/?", False, str(e)

        # ── 2. Acceso a Championship específicamente ─────────────────────────
        # En el plan Free, algunas ligas están bloqueadas.
        # Si Championship está bloqueada, la API devuelve error 499 o lista vacía
        # incluso con fixtures reales disponibles.
        try:
            # Verificar AMBAS ligas por fecha — evita bloqueo Free tier
            league_status = {}
            for days_ahead in range(5):
                d = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
                r = requests.get(
                    "https://v3.football.api-sports.io/fixtures",
                    headers=self.headers,
                    params={"date": d},
                    timeout=10
                )
                track_requests(1)
                for fix in r.json().get('response', []):
                    lid = fix['league']['id']
                    if lid in TARGET_LEAGUES and lid not in league_status:
                        league_status[lid] = fix['league']['season']
                if len(league_status) == len(TARGET_LEAGUES):
                    break
                time.sleep(0.5)

            lines = []
            for lid, cfg in TARGET_LEAGUES.items():
                if lid in league_status:
                    lines.append(f"  ✅ {cfg['name']}: season={league_status[lid]}")
                else:
                    lines.append(f"  ⚠️  {cfg['name']}: sin fixtures próximos (parón o bloqueada)")
            detail = " | ".join(
                f"{cfg['name'].split()[1]}={'✅' if lid in league_status else '⚠️'}"
                for lid, cfg in TARGET_LEAGUES.items()
            )
            all_ok = len(league_status) > 0  # basta con que al menos una liga tenga fixtures
            print("\n".join(lines))
            return True, plan_info, req_info, all_ok, detail

        except Exception as e:
            print(f"  ❌ Championship check error: {e}")
            return True, plan_info, req_info, False, str(e)

    def send_msg(self, text):
        if not TELEGRAM_TOKEN:
            print("⚠️  TELEGRAM_TOKEN vacío — mensaje no enviado")
            return
        if not TELEGRAM_CHAT_ID:
            print("⚠️  TELEGRAM_CHAT_ID vacío — mensaje no enviado")
            return
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=10
            )
            if not r.ok:
                print(f"⚠️  Telegram error {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"⚠️  Telegram excepción: {e}")

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
        LUNES 08:00 UTC — Pre-caché xG de todos los equipos activos.

        ESTRATEGIA DE BUDGET (máximo 50 req):
          Fase 1 — Discovery por fecha (compartido entre ligas):
            Buscar día a día hacia atrás. Una sola llamada por fecha
            sirve para TODAS las ligas a la vez (no llamar 3 veces la misma fecha).
            Máximo 14 llamadas de discovery = 14 req.

          Fase 2 — Cache last10 por equipo:
            Máximo 30 equipos × 1 req = 30 req.
            Si se acerca al límite, parar y dejar el resto para la siguiente semana.

          Total máximo: 14 + 30 = 44 req ← nunca superar esto
        """
        import json

        clear_date_cache()       # fecha cache fresco para el warmup
        BUDGET_MAX = 44          # nunca gastar más de esto en el warmup
        req_inicio = track_requests(0)

        def reqs_gastados():
            return track_requests(0) - req_inicio

        try:
            # ── FASE 1: Discovery — una sola pasada por fecha para TODAS las ligas ──
            teams_by_league = {lid: {} for lid in TARGET_LEAGUES}  # {lid: {team_id: name}}
            ligas_completas  = set()  # ligas con >= 18 equipos encontrados

            for days_back in range(1, 16):   # 15 fechas hacia atrás (sin hoy — partidos no terminados)
                if reqs_gastados() >= BUDGET_MAX:
                    break
                if len(ligas_completas) == len(TARGET_LEAGUES):
                    break

                d = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
                try:
                    r = requests.get(
                        "https://v3.football.api-sports.io/fixtures",
                        headers=self.headers,
                        params={"date": d},
                        timeout=10
                    )
                    track_requests(1)   # 1 sola llamada por fecha sirve para todas las ligas
                    for fix in r.json().get('response', []):
                        lid = fix['league']['id']
                        if lid not in TARGET_LEAGUES:
                            continue
                        if fix['fixture']['status']['short'] != 'FT':
                            continue  # solo contar equipos con partidos terminados
                        teams_by_league[lid][fix['teams']['home']['id']] = fix['teams']['home']['name']
                        teams_by_league[lid][fix['teams']['away']['id']] = fix['teams']['away']['name']
                        if len(teams_by_league[lid]) >= 18:
                            ligas_completas.add(lid)
                    time.sleep(0.5)
                except:
                    pass

            for lid, cfg in TARGET_LEAGUES.items():
                n = len(teams_by_league[lid])
                if n > 0:
                    names = list(teams_by_league[lid].values())
                    print(f"  {cfg['name']}: {n} equipos → {', '.join(names[:6])}{'...' if n>6 else ''}")
                else:
                    print(f"  ⚠️  {cfg['name']}: sin equipos en los últimos 14 días")

            # ── FASE 2: Cachear last10 por equipo — con techo de budget ──────────
            total_cached = total_skipped = 0

            for lid, cfg in TARGET_LEAGUES.items():
                season = self.seasons.get(lid, cfg['seasons'][-1])
                for team_id, team_name in teams_by_league[lid].items():
                    if reqs_gastados() >= BUDGET_MAX:
                        print(f"  ⚠️  Budget máximo alcanzado — parando cache")
                        break

                    # Saltar si ya tiene cache fresca con depth=10
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        cc = conn.cursor()
                        cc.execute(
                            "SELECT updated_at, depth FROM team_xg_cache WHERE team_id=?",
                            (team_id,)
                        )
                        row = cc.fetchone()
                        conn.close()
                        if row:
                            age = (datetime.now(timezone.utc) -
                                   datetime.fromisoformat(row[0])).total_seconds() / 3600
                            if age < XG_CACHE_TTL_HOURS and (row[1] or 0) >= 10:
                                total_skipped += 1
                                continue
                    except:
                        pass

                    # fetch_team_xg hace la llamada API internamente — NO hacer track_requests aquí
                    fetch_team_xg(
                        team_id, season, self.headers,
                        league_id=lid, use_cache=False, depth=10
                    )
                    # track_requests ya se llama dentro de fetch_team_xg — no duplicar
                    total_cached += 1
                    time.sleep(1.5)

                    # Actualizar nombre en cache
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        cc = conn.cursor()
                        cc.execute(
                            "UPDATE team_xg_cache SET team_name=?, depth=10 WHERE team_id=?",
                            (team_name, team_id)
                        )
                        conn.commit()
                        conn.close()
                    except:
                        pass

            req_total = reqs_gastados()
            self.send_msg(
                f"🔄 <b>xG Cache V6.4 actualizada</b>\n"
                f"Equipos cacheados (last10): {total_cached} | Saltados: {total_skipped}\n"
                f"📡 Requests del warmup: {req_total}/{BUDGET_MAX} máx"
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
                    params={"date": d}
                )
                track_requests(1)
                all_fix = r.json().get('response', [])
                matches.extend([f for f in all_fix if f['league']['id'] in TARGET_LEAGUES])
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

                # FIX v6.2: conexión abierta UNA vez por fixture, no por cada cuota
                conn = sqlite3.connect(DB_PATH)
                cc   = conn.cursor()
                for b in r[0]['bookmakers'][0]['bets']:
                    for v in b['values']:
                        mkt_key = f"{b['id']}|{v['value']}"
                        odd_now = float(v['odd'])

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
                            # Primera vez — este snapshot ES el baseline
                            cc.execute(
                                """INSERT INTO line_snapshots
                                   (fixture_id, home_team, away_team, kickoff_time,
                                    market, selection, odd_snapshot, odd_open, captured_at)
                                   VALUES (?,?,?,?,?,?,?,?,?)""",
                                (fid, h_n, a_n, ko, mkt_key, v['value'],
                                 odd_now, odd_now, now.isoformat())
                            )
                        # Siempre guardar snapshot actual para historial de movimiento
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

    def store_fixture_injuries(self, fid, h_id, h_n, a_id, a_n, inj_res):
        """
        FIX v6.2: El endpoint /injuries por temporada devuelve vacío en el plan Free.
        En su lugar, guardamos las lesiones que ya obtenemos en run_daily_scan
        (endpoint /injuries?fixture=X, que SÍ funciona en Free).
        Este método se llama desde run_daily_scan — coste 0 requests extra.
        """
        try:
            conn = sqlite3.connect(DB_PATH)
            cc   = conn.cursor()
            now  = datetime.now(timezone.utc)
            for inj in inj_res:
                tid   = inj['team']['id']
                tname = inj['team']['name']
                pname = inj.get('player', {}).get('name', 'Unknown')
                itype = inj.get('player', {}).get('type', 'Unknown')
                status = inj.get('player', {}).get('reason', 'Unknown')
                # INSERT OR IGNORE para no duplicar si el scan corre dos veces
                cc.execute(
                    """INSERT OR IGNORE INTO injury_watch
                       (team_id, team_name, player_name, injury_type, status,
                        expected_return, captured_at)
                       VALUES (?,?,?,?,?,?,?)""",
                    (tid, tname, pname, itype, status, 'N/A', now.isoformat())
                )
            conn.commit()
            conn.close()
        except:
            pass

    def injury_watch(self):
        """
        MIÉRCOLES 08:00 UTC — Resumen de lesiones acumuladas desde los scans.
        FIX v6.2: No hace llamada API propia (endpoint por temporada vacío en Free).
        Lee de injury_watch que se alimenta automáticamente desde run_daily_scan.
        Coste: 0 requests.
        """
        try:
            conn = sqlite3.connect(DB_PATH)
            cc   = conn.cursor()
            cc.execute(
                "SELECT team_name, COUNT(*) as n FROM injury_watch "
                "GROUP BY team_name ORDER BY n DESC"
            )
            rows = cc.fetchall()
            conn.close()

            if not rows:
                self.send_msg(
                    "✅ <b>Injury Watch:</b> Sin lesiones registradas esta semana.\n"
                    "(Se alimenta automáticamente del scan diario)"
                )
                return

            total = sum(r[1] for r in rows)
            heavy = [(t, n) for t, n in rows if n >= 3]

            if heavy:
                injury_lines = "\n".join(f"  🚑 {t}: {n} bajas" for t, n in heavy)
                self.send_msg(
                    f"🚑 <b>Injury Watch — Equipos con 3+ bajas:</b>\n"
                    f"{injury_lines}\n"
                    f"Total lesionados registrados: {total}\n"
                    f"(Datos de últimos scans · 0 requests)"
                )
            else:
                self.send_msg(
                    f"✅ <b>Injury Watch:</b> {total} lesionados registrados, "
                    f"ningún equipo con 3+ bajas.\n"
                    f"(Datos de últimos scans · 0 requests)"
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
            for league_id, cfg in TARGET_LEAGUES.items():
              season = self.seasons.get(league_id, cfg['seasons'][-1])
              r = requests.get(
                "https://v3.football.api-sports.io/standings",
                headers=self.headers,
                params={"league": league_id, "season": season}
              )
              track_requests(1)
              raw_resp = r.json()
              errors   = raw_resp.get('errors', {})
              standings = raw_resp.get('response', [])
              if errors:
                  print(f"  ⚠️  {cfg['name']} standings bloqueado: {str(errors)[:60]}")
              if not standings:
                continue

            conn = sqlite3.connect(DB_PATH)
            cc   = conn.cursor()
            now  = datetime.now(timezone.utc)
            cc.execute("DELETE FROM league_stats WHERE season=?", (season,))

            teams_saved = 0
            for group in standings:
                for entry in group.get('league', {}).get('standings', [[]])[0]:
                    t     = entry['team']
                    stats = entry['all']   # FIX: renombrado de 'all' para no colisionar con builtin
                    played = stats['played']
                    gf     = stats['goals']['for']
                    ga     = stats['goals']['against']
                    cc.execute(
                        """INSERT INTO league_stats
                           (season, team_id, team_name, played, wins, draws, losses,
                            goals_for, goals_against, avg_goals_for, avg_goals_against,
                            captured_at)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (season, t['id'], t['name'],
                         played, stats['win'], stats['draw'], stats['lose'],
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
                f"Equipos: {teams_saved} | Temporada: {season}\n"
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
        # Limpiar cache de fechas del scan anterior
        clear_date_cache()
        # V6.4: búsqueda por fecha — captura AMBAS ligas sin bloqueo Free tier
        matches_by_league = {lid: [] for lid in TARGET_LEAGUES}
        for d in [tomorrow, day_after]:
            try:
                r = requests.get(
                    "https://v3.football.api-sports.io/fixtures",
                    headers=self.headers,
                    params={"date": d}
                )
                track_requests(1)
                for fix in r.json().get('response', []):
                    lid = fix['league']['id']
                    if lid in TARGET_LEAGUES:
                        matches_by_league[lid].append(fix)
            except:
                pass

        # Limitar por liga, combinar, y aplicar techo global
        # Championship y Brasileirao tienen ~10 partidos c/u por jornada
        # Techo: 22 partidos × 4 req = 88 req + 2 fijos = 90 req (margen de 10)
        matches = []
        for lid, league_matches in matches_by_league.items():
            # Ordenar por proximidad al KO — mercados más maduros primero
            def _hours(m):
                try:
                    ko = datetime.fromisoformat(m['fixture']['date'].replace('Z', '+00:00'))
                    return (ko - datetime.now(timezone.utc)).total_seconds() / 3600
                except:
                    return 999
            league_matches.sort(key=_hours)
            matches.extend(league_matches[:MAX_FIXTURES_PER_SCAN])

        # Techo global: protección extra contra jornadas dobles simultáneas
        matches = matches[:MAX_FIXTURES_TOTAL]

        if not matches:
            self.send_msg(
                f"🔇 <b>Triple League V6.4:</b> Sin partidos en los próximos 2 días.\n"
                f"📡 Ligas: Championship · Brasileirao"
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
        champ_count = sum(1 for m in matches if m['league']['id'] == 40)
        bra_count   = sum(1 for m in matches if m['league']['id'] == 71)
        mx_count    = sum(1 for m in matches if m['league']['id'] == 262)
        req_est     = 2 + len(matches) * 4
        self.send_msg(
            f"🔍 <b>Triple League V6.4 — Scan D-1</b>\n"
            f"🏴󠁧󠁢󠁥󠁮󠁧󠁿 Championship: {champ_count} partidos\n"
            f"🇧🇷 Brasileirao:  {bra_count} partidos\n"
            f"🇲🇽 Liga MX:      {mx_count} partidos\n"
            f"📡 Requests estimados: ~{req_est}/100"
        )

        preliminary = []

        for m in matches:
            fid      = m['fixture']['id']
            h_n      = m['teams']['home']['name']
            a_n      = m['teams']['away']['name']
            h_id     = m['teams']['home']['id']
            a_id     = m['teams']['away']['id']
            ko       = m['fixture']['date']
            lid      = m['league']['id']
            cfg      = TARGET_LEAGUES[lid]
            l_name   = cfg['name']
            season   = self.seasons.get(lid, cfg['seasons'][-1])
            label    = f"{h_n} vs {a_n} ({l_name})"
            print(f"\n  ── {label} (fid={fid}) ──")
            print(f"     KO={ko} | liga={lid} | season={season}")
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

            # Llamada: lesiones por fixture (funciona en Free; guarda en injury_watch)
            try:
                inj_res = requests.get(
                    "https://v3.football.api-sports.io/injuries",
                    headers=self.headers,
                    params={"fixture": fid}
                ).json().get('response', [])
                track_requests(1)
                hinj = sum(1 for i in inj_res if i['team']['id'] == h_id)
                ainj = sum(1 for i in inj_res if i['team']['id'] == a_id)
                print(f"     Lesionados: {h_n}={hinj} {a_n}={ainj}")
                # FIX v6.2: persistir lesiones para injury_watch semanal (0 requests extra)
                self.store_fixture_injuries(fid, h_id, h_n, a_id, a_n, inj_res)
            except:
                hinj = ainj = 0
                print(f"     Lesionados: error al obtener — usando 0/0")

            # V6.1: xG basado en últimos 6 + factor de forma
            # retorna requests_made para tracking preciso
            xh, xa, xt, conf, xg_src, req_made = build_xg_match(
                h_id, a_id, hinj, ainj, season, self.headers, league_id=lid
            )
            print(f"     xG modelo: {h_n}={xh:.2f} {a_n}={xa:.2f} total={xt:.2f} conf={conf} src={xg_src}")
            print(f"     Requests usados hasta aquí: {track_requests(0)}/100")
            track_requests(req_made)  # FIX: tracking exacto de requests reales

            # Validar consistencia xG vs mercado
            ok, reason = validate_xg(xh, xa, bets)
            if not ok:
                log_rejection(fid, label, 'ALL', 0.0, 0.0, reason)
                continue

            probs = build_market_probs(bets, xh, xa, h_n, a_n, conf)

            if conf == "LOW":
                # xG default 1.2/1.2 — modelo sin datos reales, no generar picks
                print(f"     ⛔ xG LOW — skip completo del partido (sin datos reales)")
                log_rejection(fid, label, 'ALL', 0.0, 0.0, 'XG_LOW_SKIP')
                continue

            candidates = []
            for item in probs:
                ev = (item['prob'] * item['odd']) - 1

                ok2, fail = sanity_check(item['prob'], item['mkt'], item['odd'])
                if not ok2:
                    print(f"     ❌ {fail}: {item['mkt']} @{item['odd']:.2f} prob={item['prob']:.3f}")
                    log_rejection(fid, label, item['mkt'], item['odd'], ev, fail)
                    continue
                if ev < MIN_EV_THRESHOLD:
                    print(f"     ❌ LOW_EV: {item['mkt']} @{item['odd']:.2f} EV={ev*100:.1f}%")
                    log_rejection(fid, label, item['mkt'], item['odd'], ev, "LOW_EV")
                    continue
                if ev > MAX_EV_THRESHOLD:
                    print(f"     ❌ EV_ALUCINACION: {item['mkt']} @{item['odd']:.2f} EV={ev*100:.1f}%")
                    log_rejection(fid, label, item['mkt'], item['odd'], ev, "EV_ALUCINATION")
                    continue

                kelly, urs, rej = get_kelly_and_urs(ev, item['odd'], item['mkt'])
                if kelly == 0.0:
                    log_rejection(fid, label, item['mkt'], item['odd'], ev, rej)
                    continue

                    print(f"     ✅ CANDIDATO: {item['mkt']} @{item['odd']:.2f} EV={ev*100:.1f}% URS={urs:.2f}")
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
                f"📊 <b>Triple League V6.4 — Portfolio:</b>\n"
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
                f"🔇 <b>Triple League V6.4:</b> Sin picks válidos hoy.\n"
                f"📡 Requests usados: {track_requests(0)}/100"
            )


if __name__ == "__main__":
    bot = TripleLeagueBot()

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
                       ON p.fixture_id=c.fixture_id
                          AND p.market=c.market
                          AND p.selection_key=c.selection_key
                     WHERE p.clv_captured=1""")
        picks = c.fetchall()
        conn.close()
        if picks:
            clvs  = [p[0] for p in picks]
            beats = sum(1 for v in clvs if v > 0)
            print(f"  N={len(picks)} | Beat={beats}/{len(picks)} ({beats/len(picks)*100:.0f}%) | CLV_avg={sum(clvs)/len(clvs):.2f}%")
            # V5.10 port: desglose por mercado — crítico durante el burn-in
            mkts = {}
            for clv, mkt in picks:
                mkts.setdefault(mkt, []).append(clv)
            for mkt, vals in sorted(mkts.items()):
                beat_m = sum(1 for v in vals if v > 0)
                print(f"    {mkt:<8} N={len(vals)} CLV={sum(vals)/len(vals):.2f}% Beat={beat_m}/{len(vals)}")
        else:
            print("  Sin CLVs aún.")
    except:
        pass

    # ── AUTO-WARMUP DE CACHE ────────────────────────────────────────────────
    # Si la cache de xG está vacía (primer deploy o DB nueva), ejecutar
    # weekly_xg_cache antes del scan para que los picks tengan xG real.
    # Sin cache, fetch_team_xg usa last6 por league+season que puede estar
    # bloqueado en Free tier — resultando en xG LOW y picks no confiables.
    # ── AUTO-WARMUP: solo si cache vacía Y hay suficiente budget ───────────────
    cache_count = 0
    try:
        conn_check = sqlite3.connect(DB_PATH)
        cc = conn_check.cursor()
        cc.execute("SELECT COUNT(*) FROM team_xg_cache")
        cache_count = cc.fetchone()[0]
        conn_check.close()
        print(f"  📦 Cache xG al arrancar: {cache_count} equipos")
    except Exception as e:
        print(f"  Cache check error: {e}")

    reqs_al_arrancar = track_requests(0)
    reqs_disponibles = 100 - reqs_al_arrancar
    print(f"  📡 Requests disponibles: {reqs_disponibles}/100")

    if cache_count == 0 and reqs_disponibles >= 50:
        print("  ⚠️  Cache vacía + budget OK — ejecutando warmup...")
        bot.send_msg(
            "⏳ <b>Primera vez detectada</b>\n"
            "Calentando cache xG (last10 todos los equipos)...\n"
            "Esto toma ~3 minutos. El scan arranca después."
        )
        bot.weekly_xg_cache()
        reqs_post = 100 - track_requests(0)
        if reqs_post >= 30:
            print(f"  ✅ Warmup OK — {reqs_post} req restantes — arrancando scan")
            bot.run_daily_scan()
        else:
            bot.send_msg(
                f"✅ <b>Warmup completado</b>\n"
                f"⏰ Scan diferido — quedan solo {reqs_post} req.\n"
                f"El scan arranca mañana a las 11:00 UTC."
            )
            print(f"  ⚠️  Solo {reqs_post} req tras warmup — scan diferido")

    elif cache_count == 0 and reqs_disponibles < 50:
        bot.send_msg(
            f"⚠️ <b>Cache vacía, sin budget hoy</b>\n"
            f"Solo {reqs_disponibles} req disponibles.\n"
            f"Warmup automático el lunes 08:00 UTC.\n"
            f"Scan de hoy con xG fallback (picks marcados LOW)."
        )
        print(f"  ⚠️  Cache vacía pero solo {reqs_disponibles} req — scan con fallback")
        bot.run_daily_scan()

    else:
        print(f"  ✅ Cache OK ({cache_count} equipos) — scan directo")
        bot.run_daily_scan()

    while True:
        schedule.run_pending()
        time.sleep(60)
