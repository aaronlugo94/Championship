import os, time, math, json, csv, io, difflib, requests, sqlite3, numpy as np
from datetime import datetime, timedelta, timezone
from math import exp, lgamma, log

# ============================================================
# TRIPLE LEAGUE V7.2 — HYBRID ENGINE (BUGS CORREGIDOS)
# ============================================================
#
# FIXES vs V7.1:
#   1. Liga MX GENERA PICKS — restaurado /odds?fixture=X&bookmaker=8
#      del V6.5 que sí funciona en Free tier. V7.1 nunca appendeaba
#      a preliminary, Liga MX era letra muerta.
#
#   2. BSA odds — ahora usa Trend Resource como fuente primaria
#      de cuotas (home_odd, draw_odd, away_odd, ou25_odd).
#      V7.1 buscaba el H2H histórico entre esos dos equipos
#      que ya terminó — cuotas de un partido pasado, inútiles.
#
#   3. track_req corregido — era getter/setter mal diseñado.
#      Ahora: track_req(n) suma n, get_req() retorna total.
#      Llamar track_req(0) sumaba 0 pero retornaba el total,
#      ambiguo y propenso a errores.
#
#   4. MEX odds — restaurado flujo completo: fixtures api-football
#      → /odds?fixture=X&bookmaker=8 → pick engine completo.
#      V7.1 construía el dict de odds pero nunca lo llenaba
#      con nada real y nunca generaba picks.
#
# FUENTES DE DATOS (las 3 funcionando realmente):
#
#   [1] football-data.co.uk CSV (descarga estática, 2×/semana)
#       → xG con shots reales HST/AST para 8 ligas europeas
#       → xG goles proxy para BSA y MEX
#       → fixtures.csv con cuotas de apertura para europeas
#       → CLV capture: fixtures.csv actualizado D-0
#
#   [2] football-data.org fd.org (sin límite diario, 10 req/min)
#       → Trend Resource: pct_o25, pct_bts, cuotas BSA/europeas
#       → /competitions/BSA/matches: fixtures Brasileirao con IDs
#       → /competitions/ELC/standings: factor home/away real
#
#   [3] api-football Free (100 req/día)
#       → /fixtures?date=D (Liga MX fixtures)
#       → /odds?fixture=X&bookmaker=8 (cuotas Bet365 Liga MX)
#       → /injuries?fixture=X (lesiones Liga MX)
#
# MOTOR MATEMÁTICO (calibrado con datos reales):
#   Dixon-Coles rho=-0.13 | NegBinom std por liga | shrinkage 0.75
#   blend Trend 30% | Kelly fraccionado 0.20 | URS selector
#
# PARÁMETROS CALIBRADOS (tus archivos reales):
#   BSA: mu=2.39 std=1.54 Over25=43.4% BTTS=47.9%
#   MEX: mu=2.84 std=1.68 Over25=55.1% BTTS=56.6%
#   I1:  mu=2.44 std=1.51 conv_home=0.290 conv_away=0.331
# ============================================================

LIVE_TRADING = False

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
API_SPORTS_KEY   = os.getenv("API_SPORTS_KEY", "")
FD_ORG_TOKEN     = os.getenv("FD_ORG_TOKEN", "")

DB_DIR   = os.getenv("DB_DIR", "./data")
DATA_DIR = os.path.join(DB_DIR, "csv")   # siempre /app/data/csv — Railway crea el subdir
os.makedirs(DB_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH   = os.path.join(DB_DIR, "quant_v72.db")
AUDIT_CSV = os.path.join(DB_DIR, "picks_audit_v72.csv")

print(f"  📂 DB={DB_PATH}", flush=True)
print(f"  📂 DATA_DIR={DATA_DIR}", flush=True)

RUN_TIME_CSV_UPDATE = "06:00"
RUN_TIME_AUDIT      = "07:00"
RUN_TIME_SCAN       = "06:30"
RUN_TIME_CLV        = "16:00"
RUN_TIME_STANDINGS  = "09:00"   # martes

# ============================================================
# CONFIGURACIÓN DE LIGAS
# ============================================================

TARGET_LEAGUES = {
    # ── Europeas — CSV co.uk con shots HST/AST ────────────────────────────
    "E0":  {"name": "🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier",       "liq": 0.90, "xg_std": 1.60, "avg_goals": 2.82,
             "conv_home": 0.31, "conv_away": 0.32, "has_shots": True,
             "fbd_code": None,  "source": "csv_euro", "league_factor": 1.00},
    "E1":  {"name": "🏴󠁧󠁢󠁥󠁮󠁧󠁿 Championship",  "liq": 0.85, "xg_std": 1.55, "avg_goals": 2.55,
             "conv_home": 0.30, "conv_away": 0.31, "has_shots": True,
             "fbd_code": "ELC", "source": "csv_euro", "league_factor": 0.92},
    "SP1": {"name": "🇪🇸 La Liga",        "liq": 0.88, "xg_std": 1.55, "avg_goals": 2.65,
             "conv_home": 0.30, "conv_away": 0.31, "has_shots": True,
             "fbd_code": "PD",  "source": "csv_euro", "league_factor": 0.95},
    "D1":  {"name": "🇩🇪 Bundesliga",     "liq": 0.88, "xg_std": 1.70, "avg_goals": 3.10,
             "conv_home": 0.32, "conv_away": 0.33, "has_shots": True,
             "fbd_code": "BL1", "source": "csv_euro", "league_factor": 1.05},
    "I1":  {"name": "🇮🇹 Serie A",        "liq": 0.87, "xg_std": 1.51, "avg_goals": 2.44,
             "conv_home": 0.29, "conv_away": 0.33, "has_shots": True,
             "fbd_code": "SA",  "source": "csv_euro", "league_factor": 0.93},
    "F1":  {"name": "🇫🇷 Ligue 1",        "liq": 0.85, "xg_std": 1.52, "avg_goals": 2.45,
             "conv_home": 0.30, "conv_away": 0.31, "has_shots": True,
             "fbd_code": "FL1", "source": "csv_euro", "league_factor": 0.93},
    "N1":  {"name": "🇳🇱 Eredivisie",     "liq": 0.82, "xg_std": 1.75, "avg_goals": 3.20,
             "conv_home": 0.33, "conv_away": 0.34, "has_shots": True,
             "fbd_code": "DED", "source": "csv_euro", "league_factor": 1.05},
    "P1":  {"name": "🇵🇹 Primeira",       "liq": 0.82, "xg_std": 1.50, "avg_goals": 2.50,
             "conv_home": 0.30, "conv_away": 0.31, "has_shots": True,
             "fbd_code": "PPL", "source": "csv_euro", "league_factor": 0.93},
    "B1":  {"name": "🇧🇪 Jupiler",        "liq": 0.82, "xg_std": 1.58, "avg_goals": 2.72,
             "conv_home": 0.31, "conv_away": 0.32, "has_shots": True,
             "fbd_code": None,  "source": "csv_euro", "league_factor": 0.95},
    "T1":  {"name": "🇹🇷 Süper Lig",      "liq": 0.80, "xg_std": 1.62, "avg_goals": 2.68,
             "conv_home": 0.31, "conv_away": 0.32, "has_shots": True,
             "fbd_code": None,  "source": "csv_euro", "league_factor": 0.92},
    "G1":  {"name": "🇬🇷 Super League",   "liq": 0.78, "xg_std": 1.55, "avg_goals": 2.48,
             "conv_home": 0.30, "conv_away": 0.31, "has_shots": True,
             "fbd_code": None,  "source": "csv_euro", "league_factor": 0.90},
    # ── Segundas divisiones — mayor ineficiencia de mercado ──────────────
    "D2":  {"name": "🇩🇪 Bundesliga 2",   "liq": 0.82, "xg_std": 1.62, "avg_goals": 2.95,
             "conv_home": 0.31, "conv_away": 0.32, "has_shots": True,
             "fbd_code": None,  "source": "csv_euro", "league_factor": 0.95},
    "I2":  {"name": "🇮🇹 Serie B",        "liq": 0.78, "xg_std": 1.55, "avg_goals": 2.42,
             "conv_home": 0.29, "conv_away": 0.30, "has_shots": True,
             "fbd_code": None,  "source": "csv_euro", "league_factor": 0.90},
    "SP2": {"name": "🇪🇸 Segunda Div",    "liq": 0.80, "xg_std": 1.52, "avg_goals": 2.48,
             "conv_home": 0.30, "conv_away": 0.31, "has_shots": True,
             "fbd_code": None,  "source": "csv_euro", "league_factor": 0.92},
    "F2":  {"name": "🇫🇷 Ligue 2",        "liq": 0.78, "xg_std": 1.50, "avg_goals": 2.35,
             "conv_home": 0.29, "conv_away": 0.30, "has_shots": True,
             "fbd_code": None,  "source": "csv_euro", "league_factor": 0.90},
    "SC0": {"name": "🏴󠁧󠁢󠁳󠁣󠁴 Premiership",   "liq": 0.80, "xg_std": 1.58, "avg_goals": 2.62,
             "conv_home": 0.30, "conv_away": 0.31, "has_shots": True,
             "fbd_code": None,  "source": "csv_euro", "league_factor": 0.92},
    "E2":  {"name": "🏴󠁧󠁢󠁥󠁮󠁧󠁿 League One",   "liq": 0.78, "xg_std": 1.52, "avg_goals": 2.60,
             "conv_home": 0.30, "conv_away": 0.31, "has_shots": True,
             "fbd_code": None,  "source": "csv_euro", "league_factor": 0.90},
    # ── BSA — goles proxy + Trend fd.org + fixtures fd.org ───────────────
    "BSA": {"name": "🇧🇷 Brasileirao",    "liq": 0.80, "xg_std": 1.54, "avg_goals": 2.39,
             "conv_home": None, "conv_away": None, "has_shots": False,
             "fbd_code": "BSA", "source": "csv_extra", "league_factor": 0.95},
    # ── MEX — goles proxy + api-football odds ─────────────────────────────
    "MEX": {"name": "🇲🇽 Liga MX",        "liq": 0.75, "xg_std": 1.68, "avg_goals": 2.84,
             "conv_home": None, "conv_away": None, "has_shots": False,
             "fbd_code": None,  "source": "csv_extra", "league_factor": 1.00},
}

CSV_URLS = {
    "E0":  "https://www.football-data.co.uk/mmz4281/2526/E0.csv",
    "E1":  "https://www.football-data.co.uk/mmz4281/2526/E1.csv",
    "SP1": "https://www.football-data.co.uk/mmz4281/2526/SP1.csv",
    "D1":  "https://www.football-data.co.uk/mmz4281/2526/D1.csv",
    "I1":  "https://www.football-data.co.uk/mmz4281/2526/I1.csv",
    "F1":  "https://www.football-data.co.uk/mmz4281/2526/F1.csv",
    "N1":  "https://www.football-data.co.uk/mmz4281/2526/N1.csv",
    "P1":  "https://www.football-data.co.uk/mmz4281/2526/P1.csv",
    "BSA": "https://www.football-data.co.uk/new/BSA.csv",
    "MEX": "https://www.football-data.co.uk/new/MEX.csv",
    "B1":  "https://www.football-data.co.uk/mmz4281/2526/B1.csv",
    "T1":  "https://www.football-data.co.uk/mmz4281/2526/T1.csv",
    "G1":  "https://www.football-data.co.uk/mmz4281/2526/G1.csv",
    "D2":  "https://www.football-data.co.uk/mmz4281/2526/D2.csv",
    "I2":  "https://www.football-data.co.uk/mmz4281/2526/I2.csv",
    "SP2": "https://www.football-data.co.uk/mmz4281/2526/SP2.csv",
    "F2":  "https://www.football-data.co.uk/mmz4281/2526/F2.csv",
    "SC0": "https://www.football-data.co.uk/mmz4281/2526/SC0.csv",
    "E2":  "https://www.football-data.co.uk/mmz4281/2526/E2.csv",
}

FIXTURES_URL = "https://www.football-data.co.uk/fixtures.csv"

# En Railway todos los CSVs se descargan via CSV_URLS al DATA_DIR del volumen.
# USER_UPLOADS solo se usa en desarrollo local si tienes los archivos ya descargados.
# Para activar: pon los xlsx/csv en DATA_DIR y el download_csv los encontrará por antigüedad.
USER_UPLOADS = {}   # vacío → Railway siempre descarga desde football-data.co.uk

MIN_EV  = 0.015   # global fallback
# MIN_EV por mercado — calibrado post análisis 29 picks
MIN_EV_MKT = {
    "UNDER": 0.050,   # Under sobreestimado — requiere margen alto (BR 42%)
    "OVER":  0.020,   # Over sin sesgo claro — margen moderado
    "DC":    0.010,   # DC: 100% beat rate — margen mínimo
    "1X2":   0.025,   # 1X2: mercado eficiente — margen alto
    "BTTS":  0.020,   # BTTS Sí: moderado
    "BTTS_NO": 0.020, # BTTS No: moderado
    "DNB":     0.025, # DNB: mercado semi-eficiente
}
MAX_EV  = 0.15
KELLY   = 0.20
MAX_STK = 0.04
MAX_HEAT= 0.10
KILL_DRAWDOWN = 0.15  # pausa si bankroll cae 15% desde el máximo histórico
TGT_VOL = 0.05
VOL_BKT = {"OVER": 0.85, "UNDER": 0.65, "BTTS": 0.90, "BTTS_NO": 0.90, "1X2": 1.25, "DC": 1.20, "DNB": 1.15}
# Under: 0.65 (reducido por BR 42%) | DC: 1.20 (aumentado por BR 100%)
DC_RHO  = -0.13
XG_TTL  = 72      # horas cache xG
XG_DEC  = 0.85    # decay temporal

# ============================================================
# DATABASE
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS picks_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id TEXT, league TEXT, div TEXT,
        home_team TEXT, away_team TEXT,
        market TEXT, selection TEXT,
        odd_open REAL, prob_model REAL, ev_open REAL, stake_pct REAL,
        xg_home REAL, xg_away REAL, xg_total REAL, xg_source TEXT,
        trend_pct_o25 REAL, trend_pct_bts REAL,
        pick_time DATETIME, kickoff_time TEXT,
        clv_captured INTEGER DEFAULT 0,
        urs REAL DEFAULT 0.0, model_gap REAL DEFAULT 0.0,
        result TEXT DEFAULT 'PENDING', profit REAL DEFAULT 0.0,
        UNIQUE(fixture_id, market, selection)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS team_xg_cache (
        team_key TEXT PRIMARY KEY, div TEXT, team_name TEXT,
        gf_series TEXT, ga_series TEXT, shots_for TEXT, shots_against TEXT,
        xg_for REAL, xg_against REAL,
        confidence TEXT, updated_at TEXT, depth INTEGER
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS rejections_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        label TEXT, market TEXT, odd REAL, ev REAL, reason TEXT, logged_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS closing_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id TEXT, market TEXT, selection TEXT,
        odd_open REAL, odd_close REAL, clv_pct REAL, captured_at TEXT
    )""")
    conn.commit(); conn.close()

# ============================================================
# REQUEST TRACKING — FIX #3
# track_req(n) suma n al contador y retorna el total
# get_req()    retorna el total sin modificarlo
# ============================================================

_REQ = 0

def track_req(n=1):
    global _REQ; _REQ += n; return _REQ

def get_req():
    return _REQ

def reset_req():
    global _REQ; _REQ = 0

# ============================================================
# TELEGRAM
# ============================================================

def send_msg(text, use_html=True):
    import re
    if not TELEGRAM_TOKEN:
        print(f"[MSG] {text[:200]}", flush=True); return
    text = text.replace("**", "")
    if len(text) > 4000:
        for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
            send_msg(chunk, use_html); return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML" if use_html else None},
            timeout=20
        )
        if r.status_code == 400 and use_html:
            send_msg(re.sub(r'<[^>]+>', '', text), use_html=False)
    except Exception as e:
        print(f"Telegram err: {e}", flush=True)

def log_rej(label, market, odd, ev, reason):
    print(f"     ❌ {reason}: {market} @{odd:.2f} EV={ev*100:.1f}%", flush=True)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO rejections_log VALUES (NULL,?,?,?,?,?,?)",
                     (label, market, odd, ev, reason,
                      datetime.now(timezone.utc).isoformat()))
        conn.commit(); conn.close()
    except: pass

# ============================================================
# CSV ENGINE — FUENTE [1]
# ============================================================

_CSV_MEM = {}   # cache en memoria por sesión

def download_csv(div, force=False):
    """Descarga CSV y lo guarda en DATA_DIR. Solo si >12h de antigüedad."""
    # Usar archivo subido por el usuario si existe
    upload_path = USER_UPLOADS.get(div)
    if upload_path and os.path.exists(upload_path) and not force:
        return upload_path

    ext  = ".csv"   # football-data.co.uk siempre sirve CSV, incluyendo /new/BSA y /new/MEX
    path = os.path.join(DATA_DIR, f"{div}{ext}")

    if not force and os.path.exists(path):
        if (time.time() - os.path.getmtime(path)) / 3600 < 12:
            return path

    url = CSV_URLS.get(div)
    if not url:
        return path if os.path.exists(path) else None
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        if r.status_code == 200 and len(r.content) > 500:
            # Validar que es CSV real (primera línea debe tener comas)
            first_line = r.content[:200].decode("utf-8", errors="ignore").split("\n")[0]
            if "," in first_line:
                with open(path, "wb") as f: f.write(r.content)
                print(f"  📥 {div} descargado ({len(r.content)//1024}KB)", flush=True)
            else:
                print(f"  ⚠️ {div}: respuesta no es CSV válido — conservando archivo anterior", flush=True)
        elif r.status_code != 200:
            print(f"  ⚠️ {div}: HTTP {r.status_code} — conservando archivo anterior", flush=True)
        return path
    except Exception as e:
        print(f"  ⚠️ CSV {div}: {e}", flush=True)
        return path if os.path.exists(path) else None

def load_csv(div):
    """Carga CSV/XLSX en DataFrame, cache en memoria."""
    import pandas as pd
    if div in _CSV_MEM: return _CSV_MEM[div]
    path = download_csv(div)
    if not path: return None
    try:
        try:    df = pd.read_csv(path, encoding="utf-8-sig")
        except: df = pd.read_csv(path, encoding="latin-1")
        # BSA y MEX en /new/ usan nombres de columna distintos — normalizar
        df = df.rename(columns={"Home": "HomeTeam", "Away": "AwayTeam",
                                 "HG": "FTHG", "AG": "FTAG"})
        # Closing odds como apertura si no hay apertura
        for src_c, dst_c in [("AvgCH","AvgH"),("AvgCD","AvgD"),("AvgCA","AvgA"),
                              ("B365CH","B365H"),("B365CD","B365D"),("B365CA","B365A"),
                              ("PSCH","PSH"),("PSCD","PSD"),("PSCA","PSA")]:
            if src_c in df.columns and dst_c not in df.columns:
                df[dst_c] = df[src_c]
        df = df.dropna(subset=["HomeTeam", "AwayTeam"])
        _CSV_MEM[div] = df
        played = df.dropna(subset=["FTHG","FTAG"])
        print(f"  📊 {div}: {len(played)} jugados | último: {played['Date'].iloc[-1] if len(played)>0 else 'N/A'}",
              flush=True)
        return df
    except Exception as e:
        print(f"  ⚠️ load_csv {div}: {e}", flush=True); return None

def get_fixtures_csv():
    """
    fixtures.csv de co.uk — partidos próximos con cuotas apertura.
    Actualizado viernes (fin de semana) y martes (entre semana).
    """
    import pandas as pd
    try:
        r = requests.get(FIXTURES_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        if r.status_code != 200: return None
        try:    df = pd.read_csv(io.StringIO(r.content.decode("utf-8-sig")))
        except: df = pd.read_csv(io.StringIO(r.content.decode("latin-1")),
                                  on_bad_lines="skip")
        df.columns = df.columns.str.strip()
        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
        print(f"  📅 fixtures.csv: {len(df)} partidos", flush=True)
        return df
    except Exception as e:
        print(f"  ⚠️ fixtures.csv: {e}", flush=True); return None

def get_odds_from_row(row, cfg):
    """
    Extrae cuotas de una fila del CSV.
    Prioridad: PS (Pinnacle) → B365 → Avg → Max
    """
    def best(*cols):
        for c in cols:
            try:
                v = row.get(c) if hasattr(row, 'get') else getattr(row, c, None)
                if v is not None and not (isinstance(v, float) and math.isnan(v)):
                    f = float(v)
                    if f > 1.01: return f
            except: pass
        return None
    return {
        "H":      best("PSH",    "B365H",    "AvgH",    "MaxH"),
        "D":      best("PSD",    "B365D",    "AvgD",    "MaxD"),
        "A":      best("PSA",    "B365A",    "AvgA",    "MaxA"),
        "O25":    best("P>2.5",  "B365>2.5", "Avg>2.5", "Max>2.5"),
        "U25":    best("P<2.5",  "B365<2.5", "Avg<2.5", "Max<2.5"),
        "BTTS_Y": best("BbAvBBTS","B365BTTSY"),
    }

# ============================================================
# XG ENGINE — SHOTS + GOLES DECAY
# ============================================================

def _wavg(vals, decay=XG_DEC):
    if not vals: return 0.0
    w = [decay**i for i in range(len(vals))]
    return sum(v*wi for v,wi in zip(vals,w)) / sum(w)

def _form(series):
    """Últimos 3 vs anteriores 3 → multiplicador 0.85–1.15."""
    if len(series) < 6: return 1.0
    r = _wavg(series[:3]); p = _wavg(series[3:6])
    if p < 0.1: return 1.0
    return max(0.85, min(r/p, 1.15))

def _form_pts(gf_series, ga_series, n=5):
    """
    Forma basada en puntos reales (V/E/D) de los últimos N partidos.
    Retorna multiplicador 0.80–1.20 basado en rendimiento reciente.
    Mejor señal que _form() porque captura rachas reales de resultados.
    """
    if len(gf_series) < 3 or len(ga_series) < 3:
        return 1.0
    pts = []
    for gf, ga in zip(gf_series[:n], ga_series[:n]):
        if gf > ga:   pts.append(3)   # victoria
        elif gf == ga: pts.append(1)  # empate
        else:          pts.append(0)  # derrota
    if not pts: return 1.0
    # Promedio ponderado con decay — partidos recientes pesan más
    weights = [0.85**i for i in range(len(pts))]
    avg_pts = sum(p*w for p,w in zip(pts,weights)) / sum(weights)
    # Normalizar: 3pts = forma perfecta (1.20), 0pts = forma terrible (0.80)
    # Media esperada ~1.3 pts/partido en liga típica → factor neutral
    factor = 0.80 + (avg_pts / 3.0) * 0.40
    return round(max(0.80, min(factor, 1.20)), 3)

def get_team_stats(df, team_name, cfg, depth=8):
    """
    Extrae xG del equipo desde el DataFrame.
    Con shots (europeas): xG = wavg(HST) × conv_rate × form
    Sin shots (BSA/MEX):  xG = wavg(FTHG) × form
    """
    import pandas as pd
    teams = pd.concat([df["HomeTeam"], df["AwayTeam"]]).dropna().unique().tolist()
    match = difflib.get_close_matches(team_name, teams, n=1, cutoff=0.55)
    if not match: return None, None, [], [], "LOW"

    name   = match[0]
    played = df.dropna(subset=["FTHG","FTAG"])
    rows   = played[(played["HomeTeam"]==name)|(played["AwayTeam"]==name)].tail(depth)
    if len(rows) < 2: return None, None, [], [], "LOW"

    gf_l, ga_l, sf_l, sa_l = [], [], [], []
    for _, row in rows.iterrows():
        ih = (row["HomeTeam"] == name)
        # Clipear a 3 — un partido de 4-0 no debe sesgar el xG promedio
        gf_l.append(min(float(row["FTHG"] if ih else row["FTAG"]), 3.0))
        ga_l.append(min(float(row["FTAG"] if ih else row["FTHG"]), 3.0))
        if cfg["has_shots"]:
            try:
                hst = float(row.get("HST", float("nan")))
                ast = float(row.get("AST", float("nan")))
                if not (math.isnan(hst) or math.isnan(ast)):
                    sf_l.append(hst if ih else ast)
                    sa_l.append(ast if ih else hst)
            except: pass

    # Forma xG (goles marcados/concedidos) — detecta tendencia ofensiva/defensiva
    ff_f = _form(gf_l); ff_a = _form(ga_l)
    # Forma puntos (V/E/D) — detecta rachas de resultados reales
    # Blend: 60% forma xG + 40% forma puntos para capturar contexto real
    fp_f = _form_pts(gf_l, ga_l, n=5)
    fp_a = _form_pts(ga_l, gf_l, n=5)   # para déficit: invertir perspectiva
    ff_f_final = ff_f * 0.60 + fp_f * 0.40
    ff_a_final = ff_a * 0.60 + fp_a * 0.40

    if cfg["has_shots"] and sf_l:
        xgf = _wavg(sf_l) * cfg["conv_home"] * ff_f_final
        xga = _wavg(sa_l) * cfg["conv_away"] * ff_a_final
        src = "shots"
    else:
        xgf = _wavg(gf_l) * ff_f_final
        xga = _wavg(ga_l) * ff_a_final
        src = "goals_proxy"

    conf = "HIGH" if len(gf_l)>=6 else "MED" if len(gf_l)>=3 else "LOW"
    print(f"    [{name}] {len(gf_l)}pts via {src} → xGF={xgf:.2f} xGA={xga:.2f} {conf} [forma={ff_f_final:.2f}]", flush=True)
    return xgf, xga, gf_l, ga_l, conf

def build_xg(home_name, away_name, div, cfg, df, inj_h=0, inj_a=0):
    """
    xG_home = (att_home + def_weakness_away) / 2
    xG_away = (att_away + def_weakness_home) / 2
    Cache SQLite TTL=72h para no recalcular en cada scan.
    """
    def from_cache(key):
        try:
            conn = sqlite3.connect(DB_PATH); cc = conn.cursor()
            cc.execute("SELECT xg_for,xg_against,confidence,updated_at FROM team_xg_cache WHERE team_key=?", (key,))
            row = cc.fetchone(); conn.close()
            if row:
                age = (datetime.now(timezone.utc) -
                       datetime.fromisoformat(row[3].replace("+00:00","")).replace(tzinfo=timezone.utc)
                       ).total_seconds()/3600
                if age < XG_TTL:
                    return float(row[0]), float(row[1]), row[2]
        except: pass
        return None, None, None

    def to_cache(key, name, xgf, xga, gf, ga, conf):
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.execute("""INSERT OR REPLACE INTO team_xg_cache
                (team_key,div,team_name,gf_series,ga_series,shots_for,shots_against,
                 xg_for,xg_against,confidence,updated_at,depth) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (key,div,name,json.dumps(gf),json.dumps(ga),"[]","[]",
                 xgf,xga,conf,datetime.now(timezone.utc).isoformat(),len(gf)))
            conn.commit(); conn.close()
        except: pass

    kh = f"{div}:{home_name}"; ka = f"{div}:{away_name}"
    hf, ha, hc = from_cache(kh)
    if hf is None and df is not None:
        hf, ha, hg, hga, hc = get_team_stats(df, home_name, cfg)
        if hf: to_cache(kh, home_name, hf, ha, hg, hga, hc)

    af, aa, ac = from_cache(ka)
    if af is None and df is not None:
        af, aa, ag, aga, ac = get_team_stats(df, away_name, cfg)
        if af: to_cache(ka, away_name, af, aa, ag, aga, ac)

    if hf is None or af is None:
        print(f"    DEFAULT 1.20/1.20 — sin datos para {home_name} vs {away_name}", flush=True)
        return 1.20, 1.20, 2.40, "LOW", "default"

    xh = (hf + aa) / 2
    xa = (af + ha) / 2
    xh *= (1 - min(inj_h*0.015, 0.08))
    xa *= (1 - min(inj_a*0.015, 0.08))
    lf  = cfg.get("league_factor", 1.0)
    xh  = max(0.60, min(xh*lf, 3.80))
    xa  = max(0.60, min(xa*lf, 3.80))
    conf = "HIGH" if (hc=="HIGH" and ac=="HIGH") else \
           "MED"  if (hc!="LOW" and ac!="LOW") else "LOW"
    src  = "csv_shots" if cfg["has_shots"] else "csv_goals_proxy"
    print(f"    xG: H={xh:.2f} A={xa:.2f} T={xh+xa:.2f} {conf}", flush=True)
    return xh, xa, xh+xa, conf, src

# ============================================================
# FOOTBALL-DATA.ORG — FUENTE [2]
# ============================================================

_TREND_MEM = {}

def fetch_trends(date_str, codes):
    """
    1 request → todos los partidos del día para las ligas pedidas.
    Retorna {f'{h_id}_{a_id}': trend_entry}
    """
    if not FD_ORG_TOKEN: return {}
    key = f"{date_str}_{'_'.join(sorted(codes))}"
    if key in _TREND_MEM: return _TREND_MEM[key]
    try:
        r = requests.get(
            "https://api.football-data.org/v4/trends/",
            headers={"X-Auth-Token": FD_ORG_TOKEN},
            params={"date": date_str, "competitions": ",".join(codes),
                    "window": 8, "considerSide": "true"},
            timeout=15
        )
        track_req()
        if r.status_code != 200:
            print(f"  ⚠️ Trends {date_str} HTTP {r.status_code}", flush=True); return {}
        result = {}
        for m in r.json().get("matches", []):
            h_id = m.get("homeTeam",{}).get("id")
            a_id = m.get("awayTeam",{}).get("id")
            if h_id and a_id: result[f"{h_id}_{a_id}"] = m
        _TREND_MEM[key] = result
        print(f"  ✅ Trends {date_str}: {len(result)} partidos", flush=True)
        return result
    except Exception as e:
        print(f"  ⚠️ Trends: {e}", flush=True); return {}

def extract_trend(entry):
    """Extrae pct_o25, pct_bts y cuotas del Trend Resource."""
    if not entry: return {}
    ht = entry.get("homeTrend",{}); at = entry.get("awayTrend",{})
    odds = entry.get("odds",{})
    def avg_pct(k):
        h = ht.get(k); a = at.get(k)
        return (h+a)/200 if h is not None and a is not None else None
    ou   = odds.get("overUnder",{})
    bts  = odds.get("bothTeamsToScore",{})
    m1x2 = odds.get("match",{})
    return {
        "pct_o25":  avg_pct("pctOver25"),
        "pct_bts":  avg_pct("pctBtts"),
        "ou25_odd": ou.get("over25"),
        "bts_odd":  bts.get("yes"),
        "home_odd": m1x2.get("home"),
        "draw_odd": m1x2.get("draw"),
        "away_odd": m1x2.get("away"),
    }

def get_fd_org_matches(competition_code, date_str):
    """
    GET /v4/competitions/{code}/matches?dateFrom=D&dateTo=D
    Retorna lista de partidos con IDs de equipos (necesarios para Trend lookup).
    """
    if not FD_ORG_TOKEN: return []
    try:
        r = requests.get(
            f"https://api.football-data.org/v4/competitions/{competition_code}/matches",
            headers={"X-Auth-Token": FD_ORG_TOKEN},
            params={"dateFrom": date_str, "dateTo": date_str},
            timeout=15
        )
        track_req()
        if r.status_code != 200: return []
        return r.json().get("matches", [])
    except Exception as e:
        print(f"  ⚠️ fd.org matches {competition_code}: {e}", flush=True); return []

# ============================================================
# API-FOOTBALL — FUENTE [3] — LIGA MX
# ============================================================

def apif_get(path, params, headers):
    """Request genérico a api-football con tracking."""
    try:
        r = requests.get(
            f"https://v3.football.api-sports.io/{path}",
            headers=headers, params=params, timeout=12
        )
        track_req()
        return r.json().get("response", [])
    except Exception as e:
        print(f"  ⚠️ apif {path}: {e}", flush=True); return []

def get_mx_season(headers):
    """
    Detecta el season activo de Liga MX en api-football.
    El Clausura 2026 puede estar catalogado como season=2025 o season=2026
    dependiendo de cómo indexe api-football ese torneo.
    Llama /leagues?id=262, busca el season con 'current: true'.
    Fallback: 2026 si falla.
    """
    try:
        res = apif_get("leagues", {"id": 262}, headers)
        for entry in res:
            seasons = entry.get("seasons", [])
            for s in seasons:
                if s.get("current"):
                    yr = s.get("year", 2026)
                    print(f"  ✅ Liga MX season activo: {yr}", flush=True)
                    return yr
        # Si no hay current=true, usar el más reciente
        all_years = []
        for entry in res:
            for s in entry.get("seasons", []):
                all_years.append(s.get("year", 0))
        if all_years:
            yr = max(all_years)
            print(f"  ⚠️ Liga MX: sin season current, usando más reciente: {yr}", flush=True)
            return yr
    except Exception as e:
        print(f"  ⚠️ get_mx_season: {e}", flush=True)
    print("  ⚠️ Liga MX season: usando fallback 2026", flush=True)
    return 2026

def get_mx_fixtures(dates, headers):
    """Fixtures Liga MX para mañana y pasado mañana. ~3 req (1 season + 2 fixtures)."""
    season = get_mx_season(headers)
    # Si el season detectado es 2025 y estamos en 2026, probar 2026 también
    seasons_to_try = [season]
    import datetime as _dt
    if season == 2025 and _dt.datetime.now().year == 2026:
        seasons_to_try = [2026, 2025]
    elif season == 2026:
        seasons_to_try = [2026]
    matches = []
    for d in dates:
        found = []
        for s in seasons_to_try:
            res = apif_get("fixtures", {"date": d, "league": 262, "season": s}, headers)
            if res:
                found = res
                if s != season:
                    print(f"  ✅ Liga MX fixtures encontrados con season={s}", flush=True)
                break
            time.sleep(1.5)
        matches.extend(found)
        time.sleep(1.5)
    return matches

def get_mx_odds(fixture_id, headers):
    """
    Cuotas Bet365 para un fixture de Liga MX.
    FIX #1 y #4: este endpoint SÍ funciona en Free tier — restaurado del V6.5.
    Retorna dict con H, D, A, O25, U25, BTTS_Y o None si no hay odds.
    """
    time.sleep(6.1)   # respetar rate limit 10 req/min
    res = apif_get("odds", {"fixture": fixture_id, "bookmaker": 8}, headers)
    if not res: return None
    try:
        bets = res[0]["bookmakers"][0]["bets"]
        odds = {}
        for b in bets:
            if b["id"] == 1:   # Match Winner
                for v in b["values"]:
                    if v["value"] == "Home": odds["H"] = float(v["odd"])
                    if v["value"] == "Draw": odds["D"] = float(v["odd"])
                    if v["value"] == "Away": odds["A"] = float(v["odd"])
            elif b["id"] == 5:  # Goals Over/Under
                for v in b["values"]:
                    if v["value"] == "Over 2.5":  odds["O25"] = float(v["odd"])
                    if v["value"] == "Under 2.5": odds["U25"] = float(v["odd"])
            elif b["id"] == 8:  # BTTS
                for v in b["values"]:
                    if v["value"] == "Yes": odds["BTTS_Y"] = float(v["odd"])
        return odds if odds.get("H") else None
    except: return None

def get_mx_injuries(fixture_id, h_id, a_id, headers):
    """Lesiones por fixture. 1 req. Coste: 0 si no hay datos."""
    time.sleep(2.0)
    res = apif_get("injuries", {"fixture": fixture_id}, headers)
    hinj = sum(1 for i in res if i.get("team",{}).get("id") == h_id)
    ainj = sum(1 for i in res if i.get("team",{}).get("id") == a_id)
    return hinj, ainj

# ============================================================
# MOTOR MATEMÁTICO — DIXON-COLES + NEGBINOM
# ============================================================

def _pmf(mu, k):
    if mu <= 0 or k < 0: return 0.0
    try: return exp(-mu + k*log(mu) - lgamma(k+1))
    except: return 0.0

def dixon_coles(lh, la, rho=DC_RHO):
    """Dixon-Coles 1X2 con corrección scores bajos (0-0, 1-0, 0-1, 1-1)."""
    ph = pd_ = pa = 0.0
    for x in range(10):
        for y in range(10):
            p = _pmf(lh,x) * _pmf(la,y)
            c = 1.0
            if   x==0 and y==0: c = 1 - lh*la*rho
            elif x==0 and y==1: c = 1 + lh*rho
            elif x==1 and y==0: c = 1 + la*rho
            elif x==1 and y==1: c = 1 - rho
            p = max(0.0, p*c)
            if x>y: ph+=p
            elif x==y: pd_+=p
            else: pa+=p
    t = ph+pd_+pa
    if t<0.01: return 0.33,0.33,0.34
    return ph/t, pd_/t, pa/t

def negbinom_ou(xg_total, std, line=2.5):
    """NegBinom O/U con std calibrado empíricamente por liga."""
    mu=xg_total; var=max(std**2, mu)
    def nb(k):
        if mu<=0: return 0.0
        if var<=mu*1.01: return _pmf(mu,k)
        r=mu**2/(var-mu); p=r/(r+mu)
        try: return exp(lgamma(k+r)-lgamma(r)-lgamma(k+1)+r*log(p)+k*log(1-p))
        except: return 0.0
    pu = sum(nb(k) for k in range(int(np.floor(line))+1))
    return round(1-pu,4), round(pu,4)

def btts_prob(xh, xa):
    if not (0.4<=xh<=4.0 and 0.4<=xa<=4.0): return None, None
    y = (1-exp(-xh))*(1-exp(-xa))
    if not 0.20<=y<=0.90: return None, None
    return round(y,4), round(1-y,4)

def shrink(p, a=0.75):
    """Shrinkage hacia 0.5 — evita probabilidades extremas."""
    return 0.5+(p-0.5)*a

def blend(pm, pe, w=0.30):
    """70% modelo + 30% empírico del Trend Resource."""
    if pe is None or not 0.05<=pe<=0.95: return pm
    return pm*(1-w)+pe*w

# ============================================================
# PRICING ENGINE
# ============================================================

def build_probs(xh, xa, conf, h_n, a_n, cfg, odds, trend):
    """
    Construye candidatos con probabilidades calibradas.
    odds: dict con H/D/A/O25/U25/BTTS_Y (del CSV o api-football)
    trend: dict con pct_o25/pct_bts/cuotas (del Trend Resource)
    """
    out = []; liq = cfg["liq"]; std = cfg["xg_std"]; ts = trend or {}

    # ── 1X2 Dixon-Coles ──────────────────────────────────────────────────
    dc_h, dc_d, dc_a = dixon_coles(xh, xa)
    # Cuotas: CSV/api-football primero, Trend como fallback
    oh = odds.get("H") or ts.get("home_odd")
    od = odds.get("D") or ts.get("draw_odd")
    oa = odds.get("A") or ts.get("away_odd")

    if conf != "LOW" and oh and od and oa:
        # Normalizar implied probs con vig removal
        i1,i2,i3 = 1/(oh*1.05),1/(od*1.05),1/(oa*1.05); t=i1+i2+i3
        ih,id_,ia = i1/t, i2/t, i3/t
        # Blend modelo + mercado según liquidez de la liga
        def bm(m,i): return m*(1-liq)+i*liq
        t2 = bm(dc_h,ih)+bm(dc_d,id_)+bm(dc_a,ia)
        ph = bm(dc_h,ih)/t2; pd_=bm(dc_d,id_)/t2; pa=bm(dc_a,ia)/t2
    else:
        ph, pd_, pa = dc_h, dc_d, dc_a

    # 1X2: solo cuotas ≥ 2.50 en ligas menos líquidas (liq < 0.88)
    # Mercado muy eficiente en ligas top — solo apostamos donde hay ineficiencia
    MIN_ODD_1X2 = 2.50 if liq < 0.88 else 3.00
    for prob, odd_val, pick in [(ph,oh,f"Gana {h_n}"),
                                 (pd_,od,"Empate"),
                                 (pa,oa,f"Gana {a_n}")]:
        if odd_val and odd_val >= MIN_ODD_1X2:
            out.append({"mkt":"1X2","pick":pick,"odd":odd_val,"prob":prob,
                        "model_gap":round(prob-1/(odd_val*1.05),4)})

    # ── DOUBLE CHANCE ────────────────────────────────────────────────────
    # DC calculado de probabilidades Dixon-Coles ya calibradas
    # 1X2 odds → DC odds via fair value (sin vig 1.04)
    if conf != "LOW" and oh and od and oa:
        dc_1x  = ph + pd_   # Local o Empate
        dc_x2  = pd_ + pa   # Empate o Visitante
        dc_12  = ph + pa    # Local o Visitante (sin empate)
        # Cuota fair DC = 1 / prob; aplicar vig mínimo de DC (más bajo que 1X2)
        for dc_prob, dc_pick, vig in [
            (dc_1x, f"DC: {h_n} o Empate", 1.04),
            (dc_x2, f"DC: Empate o {a_n}", 1.04),
            (dc_12, f"DC: {h_n} o {a_n}",  1.04),
        ]:
            if dc_prob > 0.01:
                # Cuota fair implícita desde probs del mercado
                if dc_pick.endswith("o Empate"):
                    dc_odd = 1/(1/oh + 1/od) if oh and od else None
                elif dc_pick.endswith(f"o {a_n}"):
                    dc_odd = 1/(1/od + 1/oa) if od and oa else None
                else:
                    dc_odd = 1/(1/oh + 1/oa) if oh and oa else None
                # FILTRO CALIBRADO: DC solo funciona con cuota < 1.52
                # Análisis 40 picks: cuota <1.50 = 100% BR, cuota 1.50-1.60 = 33% BR
                if dc_odd and 1.01 < dc_odd < 1.52:
                    out.append({"mkt":"DC","pick":dc_pick,"odd":round(dc_odd,2),
                                "prob":dc_prob,
                                "model_gap":round(dc_prob-1/(dc_odd*vig),4)})

    # ── DRAW NO BET ──────────────────────────────────────────────────────
    # DNB: si empata devuelven. EV real = ph/(ph+pa) para local,  pa/(ph+pa) para visitante.
    # Cuota fair derivada de las probabilidades Dixon-Coles sin empate.
    # Solo en ligas con liq < 0.88 (mercado menos eficiente) y conf HIGH.
    if conf == "HIGH" and oh and oa and liq < 0.88:
        ph_dnb = ph / max(ph + pa, 0.01)   # prob local sin empate
        pa_dnb = pa / max(ph + pa, 0.01)   # prob visitante sin empate
        # Cuota fair DNB desde las cuotas 1X2 del mercado (sin vig)
        # DNB_H fair = 1 / (1/oh - 1/od) cuando od es la cuota empate
        try:
            dnb_h_odd = round(1 / (1/oh - 1/od), 2) if oh and od and (1/oh - 1/od) > 0.05 else None
            dnb_a_odd = round(1 / (1/oa - 1/od), 2) if oa and od and (1/oa - 1/od) > 0.05 else None
        except (ZeroDivisionError, ValueError):
            dnb_h_odd = dnb_a_odd = None
        # Solo cuotas razonables (1.10 - 4.00)
        if dnb_h_odd and 1.10 < dnb_h_odd < 4.00:
            out.append({"mkt":"DNB","pick":f"DNB: Gana {h_n}",
                        "odd":dnb_h_odd,"prob":ph_dnb,
                        "model_gap":round(ph_dnb - 1/(dnb_h_odd*1.05), 4)})
        if dnb_a_odd and 1.10 < dnb_a_odd < 4.00:
            out.append({"mkt":"DNB","pick":f"DNB: Gana {a_n}",
                        "odd":dnb_a_odd,"prob":pa_dnb,
                        "model_gap":round(pa_dnb - 1/(dnb_a_odd*1.05), 4)})

    # ── O/U 2.5 ──────────────────────────────────────────────────────────
    has_trend = ts.get("pct_o25") is not None
    po_raw, _ = negbinom_ou(xh+xa, std)
    po = shrink(blend(po_raw, ts.get("pct_o25")))
    pu = 1 - po
    # Shrinkage calibrado post análisis de 29 picks: Under 42% beat rate
    # → shrinkage más agresivo para reducir sobreestimación de prob Under
    if not has_trend:
        pu = shrink(pu, a=0.45)   # sin Trend: agresivo (Under 42% BR → recalibrando)
        po = 1 - pu
    else:
        pu = shrink(pu, a=0.60)   # con Trend: conservador
        po = 1 - pu
    # Cuota Over: CSV/api-football primero, Trend como fallback
    o25 = odds.get("O25") or ts.get("ou25_odd")
    u25 = odds.get("U25")
    if o25 and o25>1.01:
        xg_total = xh + xa
        # FILTRO CALIBRADO: Over en xG 2.5-3.2 tiene 33% BR (análisis 40 picks)
        # Solo apostar Over si el xG total es suficientemente alto (>3.2) o bajo (<2.5)
        if not (2.50 <= xg_total <= 3.20):
            out.append({"mkt":"OVER","pick":"Over 2.5 Goles","odd":o25,"prob":po,
                        "model_gap":round(po-1/(o25*1.07),4)})
        else:
            pass  # xG 2.5-3.2 → skip Over (rango sin edge)
    if u25 and u25>1.01:
        xg_total = xh + xa
        # FILTRO CALIBRADO: Under en xG 1.8-2.2 tiene 33% BR (análisis 40 picks)
        # Ese rango es el más peligroso — mercado ya lo sabe
        if not (1.80 <= xg_total <= 2.20):
            out.append({"mkt":"UNDER","pick":"Under 2.5 Goles","odd":u25,"prob":pu,
                        "model_gap":round(pu-1/(u25*1.07),4)})
        else:
            pass  # xG 1.8-2.2 → skip Under (rango descalibrado)

    # ── BTTS Sí / No ─────────────────────────────────────────────────────
    if conf != "LOW":
        py, pn = btts_prob(xh, xa)
        if py:
            py = blend(py, ts.get("pct_bts"), w=0.25); pn = 1-py
            ob  = odds.get("BTTS_Y") or ts.get("bts_odd")
            obn = odds.get("BTTS_N")  # cuota explícita del CSV si existe
            # Si no hay cuota explícita BTTS_N, derivar de BTTS_Y
            if not obn and ob and ob > 1.01:
                obn_derived = round(1 / max(0.01, 1 - 1/ob), 2)
                # Solo usar si la cuota derivada es razonable (1.20 - 4.00)
                obn = obn_derived if 1.20 < obn_derived < 4.00 else None
            if ob and ob > 1.01:
                out.append({"mkt":"BTTS","pick":"Ambos Marcan: Sí","odd":ob,"prob":py,
                            "model_gap":round(py-1/(ob*1.06),4)})
            if obn and obn > 1.05:
                out.append({"mkt":"BTTS_NO","pick":"Ambos Marcan: No",
                            "odd":round(float(obn),2),"prob":pn,
                            "model_gap":round(pn-1/(float(obn)*1.06),4)})
    return out

# ============================================================
# VALIDACIONES + KELLY + PORTFOLIO (del V6.5 + V7.1)
# ============================================================

def validate_xg(xh, xa, oh, oa, o25):
    if oh and oa:
        mo=min(oh,oa); rat=max(xh,xa)/min(xh,xa) if min(xh,xa)>0 else 1
        if mo<1.40 and rat<1.50: return False,"XG_DEFAULT_DETECTED"
        if mo<1.65 and rat<1.20: return False,"XG_FLAT_ON_FAV"
        if 1.30<=xh<=1.50 and 1.30<=xa<=1.50 and mo<1.60: return False,"XG_LIKELY_DEFAULT"
    if o25:
        pu=1/(o25*1.07)
        if 0.01 < pu < 1.0 and abs((xh+xa)+2.5*math.log(pu))>1.8:
            return False,"XG_INCONSISTENT"
    return True, None

def sanity(p, mkt, odd):
    VIG={"OVER":1.07,"UNDER":1.07,"1X2":1.05,"BTTS":1.06,"DC":1.04,"BTTS_NO":1.06,"DNB":1.05}
    gap=abs(p-1/(odd*VIG.get(mkt,1.06)))
    if gap>0.18: return False,f"SANITY_FAIL(gap={gap:.2f})"
    return True, None

def kelly_urs(ev, odd, mkt):
    if odd<=1.0 or ev<=0: return 0.0,0.0,"EV_NEG"
    prob=(ev+1)/odd; q=1-prob; b=odd-1
    fk=(b*prob-q)/b if b>0 else 0.0
    if fk<=0: return 0.0,0.0,"KELLY_NEG"
    k=min(fk*KELLY*VOL_BKT.get(mkt,1.0),MAX_STK)
    urs=(ev*(prob**0.5))/max(0.5,odd-1)
    return round(k,5), round(urs,4), "OK"

def portfolio(cands):
    if not cands: return [],{}
    for p in cands:
        p["adj"]=p["base_stake"]*VOL_BKT.get(p["mkt"],1.0)
    pv=sum((p["adj"]*(p["odd"]-1))**2 for p in cands)
    pvol=math.sqrt(pv) if pv>0 else 0.0001
    damp=min(1.0,TGT_VOL/pvol)
    tot=sum(p["adj"]*damp for p in cands)
    sc=min(1.0,MAX_HEAT/tot) if tot>0 else 1.0
    for p in cands:
        p["final_stake"]=max(0.0,min(p["adj"]*damp*sc,0.05))
    picks=[p for p in cands if p["final_stake"]>=0.005]
    return picks,{"pvol":pvol,"damp":damp,"heat":sum(p["final_stake"] for p in picks)}

# ============================================================
# AUDIT — RESOLUCIÓN AUTOMÁTICA CON CSV
# ============================================================

def init_audit():
    if not os.path.exists(AUDIT_CSV):
        with open(AUDIT_CSV,"w",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow([
                "Date","Div","Home","Away","Pick","Market",
                "Prob","Odd","EV","Status","Stake","Profit",
                "xGH","xGA","FTHG","FTAG","pct_o25","pct_bts","xg_src"
            ])

def check_result(pick, mkt, fthg, ftag, home_name="", away_name=""):
    """
    Resuelve el resultado de un pick.
    FIX #3: 1X2 ahora recibe home_name y away_name para saber exactamente
    qué equipo apostamos — ya no depende de si 'home'/'away' está en el string.
    pick = 'Gana Arsenal', home_name='Manchester City', away_name='Arsenal'
    → Arsenal es visitante, si ag>hg → WIN, si hg>ag → LOSS.
    """
    try:
        if math.isnan(float(fthg)) or math.isnan(float(ftag)): return "PENDING"
    except: return "PENDING"
    hg=int(float(fthg)); ag=int(float(ftag))
    if mkt=="OVER":  return "WIN" if hg+ag>2.5 else "LOSS"
    if mkt=="UNDER": return "WIN" if hg+ag<2.5 else "LOSS"
    if mkt in ("BTTS","BTTS_NO"):
        y=hg>0 and ag>0
        return "WIN" if ("Sí" in pick and y) or ("No" in pick and not y) else "LOSS"
    if mkt=="DNB":
        # DNB: empate = PUSH (devuelven) → tratamos como PENDING para no contar
        if hg==ag: return "PENDING"   # empate → push, no cuenta como WIN ni LOSS
        team_pick = pick.replace("DNB: Gana ","").strip()
        sim_h = difflib.SequenceMatcher(None, team_pick.lower(), home_name.lower()).ratio()
        sim_a = difflib.SequenceMatcher(None, team_pick.lower(), away_name.lower()).ratio()
        if sim_h >= 0.60 and sim_h >= sim_a:
            return "WIN" if hg>ag else "LOSS"
        if sim_a >= 0.60 and sim_a > sim_h:
            return "WIN" if ag>hg else "LOSS"
        return "PENDING"
    if mkt=="DC":
        if "o Empate" in pick and not pick.startswith("DC: Empate"):
            # 1X: local gana o empate
            return "WIN" if hg>=ag else "LOSS"
        elif pick.count(" o ") == 1 and "Empate o" in pick:
            # X2: empate o visitante gana
            return "WIN" if hg<=ag else "LOSS"
        else:
            # 12: cualquier equipo gana (no empate)
            return "WIN" if hg!=ag else "LOSS"
    if mkt=="1X2":
        if "Empate" in pick: return "WIN" if hg==ag else "LOSS"
        # Extraer el nombre del equipo del pick: "Gana Arsenal" → "Arsenal"
        team_pick = pick.replace("Gana ","").strip()
        # Fuzzy match contra home y away para saber cuál es
        sim_h = difflib.SequenceMatcher(None, team_pick.lower(), home_name.lower()).ratio()
        sim_a = difflib.SequenceMatcher(None, team_pick.lower(), away_name.lower()).ratio()
        if sim_h >= 0.60 and sim_h >= sim_a:
            return "WIN" if hg>ag else "LOSS"   # apostamos al local
        if sim_a >= 0.60 and sim_a > sim_h:
            return "WIN" if ag>hg else "LOSS"   # apostamos al visitante
        return "PENDING"   # no se pudo identificar el equipo
    return "PENDING"

def run_audit():
    """07:00 UTC — resuelve picks PENDING con resultados del CSV. 0 requests."""
    import pandas as pd
    if not os.path.exists(AUDIT_CSV): return
    rows=[]; resolved=wins=losses=0; db_updates=[]
    try:
        with open(AUDIT_CSV,"r",encoding="utf-8") as f:
            reader=csv.reader(f); header=next(reader); rows.append(header)
            for row in reader:
                if len(row)<11: rows.append(row); continue
                if row[9]=="PENDING":
                    div=row[1]; home=row[2]; away=row[3]
                    pick=row[4]; mkt=row[5]
                    try: odd=float(row[7]); stake=float(row[10])
                    except: rows.append(row); continue
                    df=load_csv(div)
                    if df is not None:
                        played=df.dropna(subset=["FTHG","FTAG"])
                        teams=pd.concat([played["HomeTeam"],played["AwayTeam"]]).unique()
                        rh=difflib.get_close_matches(home,teams,n=1,cutoff=0.55)
                        ra=difflib.get_close_matches(away,teams,n=1,cutoff=0.55)
                        if rh and ra:
                            m=played[(played["HomeTeam"]==rh[0])&(played["AwayTeam"]==ra[0])]
                            if not m.empty:
                                fthg=float(m.iloc[-1]["FTHG"])
                                ftag=float(m.iloc[-1]["FTAG"])
                                res=check_result(pick,mkt,fthg,ftag,
                                                 home_name=rh[0],away_name=ra[0])
                                if res in("WIN","LOSS"):
                                    profit = round(stake*odd-stake if res=="WIN" else -stake, 4)
                                    row[9]=res; row[14]=str(fthg); row[15]=str(ftag)
                                    row[11]=str(profit)
                                    wins+=res=="WIN"; losses+=res=="LOSS"; resolved+=1
                                    # Sincronizar resultado a picks_log DB
                                    db_updates.append((res, profit, fthg, ftag,
                                                        row[2], row[3], row[5]))
                rows.append(row)
        with open(AUDIT_CSV,"w",newline="",encoding="utf-8") as f:
            csv.writer(f).writerows(rows)
        # Actualizar picks_log en DB con resultados resueltos
        if db_updates:
            try:
                conn_a = sqlite3.connect(DB_PATH)
                for res, profit, fthg, ftag, home, away, mkt in db_updates:
                    conn_a.execute("""
                        UPDATE picks_log SET result=?, profit=?
                        WHERE home_team LIKE ? AND away_team LIKE ?
                          AND market=? AND result='PENDING'
                    """, (res, profit,
                          f"%{home[:8]}%", f"%{away[:8]}%", mkt))
                conn_a.commit(); conn_a.close()
            except Exception as db_e:
                print(f"  ⚠️ audit DB sync: {db_e}", flush=True)
        if resolved:
            send_msg(f"🔬 <b>Auditoría V7.2</b>\nResueltos: {resolved} | ✅{wins} WIN | ❌{losses} LOSS")
    except Exception as e:
        print(f"  ⚠️ audit: {e}", flush=True)

def calc_pnl():
    import pandas as pd
    if not os.path.exists(AUDIT_CSV): return
    try:
        df=pd.read_csv(AUDIT_CSV)
        df["Profit"]=pd.to_numeric(df["Profit"],errors="coerce").fillna(0)
        df["Stake"]=pd.to_numeric(df["Stake"],errors="coerce").fillna(0)
        total=df["Profit"].sum()
        nw=(df["Status"]=="WIN").sum(); nl=(df["Status"]=="LOSS").sum()
        np_=(df["Status"]=="PENDING").sum()
        br=nw/(nw+nl)*100 if nw+nl else 0
        avg_ev=df[df["Status"].isin(["WIN","LOSS"])]["EV"].astype(float).mean()*100 if nw+nl else 0
        send_msg(
            f"💰 <b>PnL V7.2</b>\n"
            f"Total: {total:+.4f} U | W/L/Pend: {nw}/{nl}/{np_}\n"
            f"Beat Rate: {br:.1f}% | Avg EV: +{avg_ev:.1f}%\n"
            f"Burn-in: {nw+nl}/30 picks"
        )
    except Exception as e:
        print(f"  ⚠️ pnl: {e}", flush=True)

# ============================================================
# KILL-SWITCH — protección de capital
# ============================================================

def kill_switch_check():
    """
    Verifica si el bankroll cayó KILL_DRAWDOWN desde el máximo histórico.
    Si LIVE_TRADING y drawdown > 15% → pausa automática + alerta Telegram.
    Retorna True si el sistema debe pausarse.
    """
    if not LIVE_TRADING:
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("""
            SELECT profit FROM picks_log
            WHERE result IN ('WIN','LOSS')
            ORDER BY id ASC
        """).fetchall()
        conn.close()
        if len(rows) < 5:
            return False
        # Calcular curva de bankroll acumulada
        cumulative = 0.0
        peak = 0.0
        for (p,) in rows:
            cumulative += float(p or 0)
            if cumulative > peak:
                peak = cumulative
        # Drawdown desde el pico
        drawdown = (peak - cumulative) / (1 + peak) if peak > 0 else 0
        if drawdown >= KILL_DRAWDOWN:
            send_msg(
                f"🚨 <b>KILL-SWITCH ACTIVADO</b>\n"
                f"Drawdown: -{drawdown*100:.1f}% desde el pico\n"
                f"PnL acumulado: {cumulative:+.4f}U | Pico: {peak:+.4f}U\n"
                f"Sistema en pausa. Revisar modelo antes de reactivar."
            )
            print(f"  🚨 KILL-SWITCH: drawdown {drawdown*100:.1f}% — pausando", flush=True)
            return True
        elif drawdown >= KILL_DRAWDOWN * 0.7:
            # Advertencia al 70% del límite
            send_msg(
                f"⚠️ <b>Drawdown warning</b>: -{drawdown*100:.1f}% "
                f"(límite: -{KILL_DRAWDOWN*100:.0f}%)"
            )
    except Exception as e:
        print(f"  ⚠️ kill_switch_check: {e}", flush=True)
    return False

# ============================================================
# BOT PRINCIPAL
# ============================================================

class TripleLeagueV72:
    def __init__(self):
        self.apif_h = {
            "x-apisports-key": API_SPORTS_KEY,
            "x-rapidapi-host": "v3.football.api-sports.io"
        }
        init_db(); init_audit(); reset_req()
        print("--- V7.2 HYBRID ENGINE STARTED ---", flush=True)
        try:
            conn=sqlite3.connect(DB_PATH)
            xg_n=conn.execute("SELECT COUNT(*) FROM team_xg_cache").fetchone()[0]
            pk_n=conn.execute("SELECT COUNT(*) FROM picks_log").fetchone()[0]
            conn.close()
            send_msg(
                f"🚀 <b>V7.2 HYBRID — ONLINE</b>\n"
                f"📂 xG cache: {xg_n} equipos | picks: {pk_n}\n"
                f"⚡ Dixon-Coles + NegBinom calibrado\n"
                f"📊 [1] CSV co.uk (shots) + [2] fd.org (Trend) + [3] api-football (MEX)\n"
                f"{'🟢 LIVE' if LIVE_TRADING else '🔴 DRY-RUN'}"
            )
        except Exception as e:
            send_msg(f"🚀 V7.2 iniciado ({e})")

        # ── BURN-IN EVALUATOR ─────────────────────────────────
        try:
            from burn_in_evaluator import print_burn_in_report
            r = print_burn_in_report(DB_PATH)
            if r['ready_for_live'] and not LIVE_TRADING:
                send_msg("🟢 <b>BURN-IN SUPERADO.</b> Activar LIVE_TRADING manualmente.")
        except ImportError:
            print("  ⚠️ burn_in_evaluator.py no encontrado — omitiendo evaluación.", flush=True)
        except Exception as e:
            print(f"  ⚠️ burn-in eval: {e}", flush=True)

    def _filter(self, probs, label, fid, h_n, a_n, ko, xh, xa, xt, conf, src, div, ts):
        """Aplica filtros y retorna candidatos válidos listos para el portfolio."""
        cands=[]
        for item in probs:
            ev=(item["prob"]*item["odd"])-1
            ok2,fail=sanity(item["prob"],item["mkt"],item["odd"])
            if not ok2:     log_rej(label,item["mkt"],item["odd"],ev,fail); continue
            min_ev_mkt = MIN_EV_MKT.get(item["mkt"], MIN_EV)
            if ev<min_ev_mkt: log_rej(label,item["mkt"],item["odd"],ev,"LOW_EV"); continue
            if ev>MAX_EV:   log_rej(label,item["mkt"],item["odd"],ev,"EV_ALUCINATION"); continue
            k,urs,rej=kelly_urs(ev,item["odd"],item["mkt"])
            if k==0.0:      log_rej(label,item["mkt"],item["odd"],ev,rej); continue
            print(f"     ✅ {item['mkt']} @{item['odd']:.2f} EV={ev*100:.1f}% URS={urs:.3f}",flush=True)
            cands.append({**item,"ev":ev,"base_stake":k,"urs":urs,
                          "conf":conf,"xg_src":src,
                          "fid":fid,"h_n":h_n,"a_n":a_n,"ko":ko,
                          "xh":xh,"xa":xa,"xt":xt,"div":div,
                          "trend_pct_o25":ts.get("pct_o25"),
                          "trend_pct_bts":ts.get("pct_bts")})
        return cands

    def _save_pick(self, p, today_str, conn_c):
        """Guarda pick en DB y CSV de auditoría. INSERT OR IGNORE previene duplicados."""
        cfg_p=TARGET_LEAGUES.get(p["div"],{})
        conn_c.execute("""INSERT OR IGNORE INTO picks_log
            (fixture_id,league,div,home_team,away_team,market,selection,
             odd_open,prob_model,ev_open,stake_pct,
             xg_home,xg_away,xg_total,xg_source,
             trend_pct_o25,trend_pct_bts,
             pick_time,kickoff_time,urs,model_gap)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (str(p["fid"]),cfg_p.get("name",""),p["div"],p["h_n"],p["a_n"],
             p["mkt"],p["pick"],
             p["odd"],p["prob"],p["ev"],
             p["final_stake"],
             p["xh"],p["xa"],p["xt"],p["xg_src"],
             p.get("trend_pct_o25"),p.get("trend_pct_bts"),
             datetime.now(timezone.utc).isoformat(),p["ko"],
             p["urs"],p["model_gap"])
        )
        # Solo escribir al CSV si realmente se insertó (no era duplicado)
        if conn_c.rowcount > 0:
            with open(AUDIT_CSV,"a",newline="",encoding="utf-8") as f:
                csv.writer(f).writerow([
                    today_str,p["div"],p["h_n"],p["a_n"],
                    p["pick"],p["mkt"],p["prob"],p["odd"],p["ev"],
                    "PENDING",p["final_stake"],0,
                    p["xh"],p["xa"],"","",
                    p.get("trend_pct_o25",""),p.get("trend_pct_bts",""),p["xg_src"]
                ])

    def _report_pick(self, p):
        """Envía mensaje Telegram para un pick."""
        cfg_p=TARGET_LEAGUES.get(p["div"],{})
        ci="✅" if p["conf"]=="HIGH" else "⚠️" if p["conf"]=="MED" else "❌"
        gs=f"+{p['model_gap']*100:.1f}%" if p["model_gap"]>=0 else f"{p['model_gap']*100:.1f}%"
        tl=""
        if p.get("trend_pct_o25") is not None:
            tl=f"\n📈 Trend: o25={p['trend_pct_o25']*100:.0f}% bts={p.get('trend_pct_bts') or 0:.0f}%"
        send_msg(
            f"⚽ <b>{p['h_n']} vs {p['a_n']}</b>\n"
            f"🏟 {cfg_p.get('name','')}\n"
            f"{'🟢 [LIVE]' if LIVE_TRADING else '🟡 [DRY-RUN]'} [{p['mkt']}]: <b>{p['pick']}</b>\n"
            f"📊 Cuota: @{p['odd']:.2f} | EV: +{p['ev']*100:.1f}%\n"
            f"🧠 Prob: {p['prob']*100:.1f}% | Gap: {gs}\n"
            f"🏦 Stake: {p['final_stake']*100:.2f}% | URS: {p['urs']:.3f}\n"
            f"🎯 xG: H={p['xh']:.2f} A={p['xa']:.2f} | {ci} {p['conf']}"
            f"{tl}"
        )

    # ── REFRESH CSVS ─────────────────────────────────────────────────────

    def refresh_csvs(self):
        """06:00 UTC — descarga CSVs frescos antes del scan."""
        _CSV_MEM.clear()
        updated=[]
        for div in TARGET_LEAGUES:
            path=download_csv(div, force=True)
            if path: updated.append(div)
        send_msg(f"📥 <b>CSVs actualizados:</b> {', '.join(updated)}")

    # ── SCAN PRINCIPAL D-1 ────────────────────────────────────────────────

    def run_daily_scan(self):
        """
        11:00 UTC — analiza partidos de mañana y pasado.

        FLUJO REAL (las 3 fuentes alimentan preliminary):

        EUROPEAS (E0,E1,SP1,D1,I1,F1,N1,P1):
          [1] CSV co.uk → xG con shots HST/AST + cuotas apertura
          [2] fd.org Trend → pct_o25, pct_bts (calibración) + cuotas fallback
          → preliminary.append ✅

        BRASILEIRAO (BSA):
          [1] CSV co.uk BRA.xlsx → xG goles proxy
          [2] fd.org /matches → fixtures con IDs de equipos
          [2] fd.org Trend → cuotas H/D/A/O25 + pct_o25/pct_bts
          → preliminary.append ✅

        LIGA MX (MEX):
          [1] CSV co.uk MEX.xlsx → xG goles proxy
          [3] api-football /fixtures → fixtures del día
          [3] api-football /odds?fixture=X&bookmaker=8 → cuotas Bet365
          [3] api-football /injuries → lesiones
          → preliminary.append ✅
        """
        import pandas as pd
        reset_req(); _CSV_MEM.clear(); _TREND_MEM.clear()

        tomorrow  = (datetime.now()+timedelta(days=1)).strftime("%Y-%m-%d")
        day_after = (datetime.now()+timedelta(days=2)).strftime("%Y-%m-%d")
        today_str = datetime.now().strftime("%d/%m/%Y")
        preliminary = []

        send_msg(
            f"🔍 <b>V7.2 Scan D-1</b>\n"
            f"🗓 {tomorrow} / {day_after}\n"
            f"[1] CSV co.uk + [2] fd.org + [3] api-football"
        )

        # ── [2] Trend Resource — 1 req por fecha ─────────────────────────
        fd_codes = [cfg["fbd_code"] for cfg in TARGET_LEAGUES.values()
                    if cfg.get("fbd_code")]
        all_trends = {}
        for d in [tomorrow, day_after]:
            all_trends.update(fetch_trends(d, fd_codes))
            time.sleep(7.0)

        # ── [1] fixtures.csv → europeas ───────────────────────────────────
        fix_df = get_fixtures_csv()

        euro_divs = [d for d,c in TARGET_LEAGUES.items() if c["source"]=="csv_euro"]

        if fix_df is not None:
            for d_off in [1,2]:
                target_date = (datetime.now()+timedelta(days=d_off)).date()
                daily = fix_df[
                    (fix_df["Date"].dt.date==target_date) &
                    (fix_df["Div"].isin(euro_divs))
                ]
                for _, row in daily.iterrows():
                    div=row.get("Div")
                    if div not in TARGET_LEAGUES: continue
                    cfg=TARGET_LEAGUES[div]
                    h_n=str(row.get("HomeTeam","")).strip()
                    a_n=str(row.get("AwayTeam","")).strip()
                    if not h_n or not a_n: continue
                    ko=str(row.get("Date",""))
                    label=f"{h_n} vs {a_n} ({cfg['name']})"
                    fid=f"{div}_{h_n}_{a_n}"
                    print(f"\n  ── {label} ──", flush=True)

                    # [1] xG desde CSV histórico con shots
                    df_hist=load_csv(div)
                    xh,xa,xt,conf,src=build_xg(h_n,a_n,div,cfg,df_hist)

                    # [1] Cuotas desde fixtures.csv
                    odds=get_odds_from_row(row,cfg)

                    # [2] Trend por nombre (fixtures.csv no tiene team_id)
                    ts={}
                    for te in all_trends.values():
                        hn_t=te.get("homeTeam",{}).get("name","")
                        an_t=te.get("awayTeam",{}).get("name","")
                        if (difflib.SequenceMatcher(None,h_n,hn_t).ratio()>0.60 and
                            difflib.SequenceMatcher(None,a_n,an_t).ratio()>0.60):
                            ts=extract_trend(te); break

                    ok,reason=validate_xg(xh,xa,odds.get("H"),odds.get("A"),odds.get("O25"))
                    if not ok: log_rej(label,"ALL",0,0,reason); continue
                    if conf=="LOW": log_rej(label,"ALL",0,0,"XG_LOW_SKIP"); continue

                    probs=build_probs(xh,xa,conf,h_n,a_n,cfg,odds,ts)
                    cands=self._filter(probs,label,fid,h_n,a_n,ko,xh,xa,xt,conf,src,div,ts)
                    if cands:
                        cands.sort(key=lambda x:x["ev"]*x["urs"],reverse=True)
                        preliminary.append(cands[0])   # ← EUROPEAS ALIMENTAN preliminary ✅

        # ── [1]+[2] BSA — CSV goles proxy + fd.org fixtures + Trend ──────
        df_bra=load_csv("BSA")
        cfg_bra=TARGET_LEAGUES["BSA"]

        for d in [tomorrow, day_after]:
            bsa_matches=get_fd_org_matches("BSA", d)
            time.sleep(7.0)
            for m in bsa_matches:
                ht=m.get("homeTeam",{}); at=m.get("awayTeam",{})
                h_n=ht.get("name",""); a_n=at.get("name","")
                h_id=ht.get("id",""); a_id=at.get("id","")
                ko=m.get("utcDate",""); fid=str(m.get("id",""))
                label=f"{h_n} vs {a_n} ({cfg_bra['name']})"
                print(f"\n  ── {label} ──", flush=True)

                # [1] xG desde CSV BRA.xlsx goles proxy
                xh,xa,xt,conf,src=build_xg(h_n,a_n,"BSA",cfg_bra,df_bra)

                # [2] Trend → cuotas H/D/A/O25 + pct_o25/pct_bts
                # FIX #2: cuotas del Trend Resource, NO del H2H histórico
                ts={}
                trend_key=f"{h_id}_{a_id}"
                if trend_key in all_trends:
                    ts=extract_trend(all_trends[trend_key])

                # Odds: Trend como fuente primaria para BSA
                odds={
                    "H":    ts.get("home_odd"),
                    "D":    ts.get("draw_odd"),
                    "A":    ts.get("away_odd"),
                    "O25":  ts.get("ou25_odd"),
                    "U25":  None,
                    "BTTS_Y": ts.get("bts_odd"),
                }

                if not odds.get("H"):
                    # Fallback: intentar api-football para cuotas BSA
                    # fd.org da el fixture_id del partido de BSA
                    apif_fid = m.get("id")
                    if apif_fid and get_req() < 80:
                        apif_odds = get_mx_odds(apif_fid, self.apif_h)
                        if apif_odds:
                            odds.update(apif_odds)
                            print(f"     📡 BSA odds via api-football", flush=True)
                    if not odds.get("H"):
                        print(f"     ⚠️ BSA sin cuotas — skip", flush=True)
                        log_rej(label,"ALL",0,0,"NO_ODDS_AVAILABLE"); continue

                ok,reason=validate_xg(xh,xa,odds["H"],odds["A"],odds["O25"])
                if not ok: log_rej(label,"ALL",0,0,reason); continue
                if conf=="LOW": log_rej(label,"ALL",0,0,"XG_LOW_SKIP"); continue

                probs=build_probs(xh,xa,conf,h_n,a_n,cfg_bra,odds,ts)
                cands=self._filter(probs,label,fid,h_n,a_n,ko,xh,xa,xt,conf,src,"BSA",ts)
                if cands:
                    cands.sort(key=lambda x:x["ev"]*x["urs"],reverse=True)
                    preliminary.append(cands[0])   # ← BSA ALIMENTA preliminary ✅

        # ── [1]+[3] MEX — CSV goles proxy + api-football odds ────────────
        # FIX #1 y #4: MEX ahora genera picks reales
        df_mex=load_csv("MEX")
        cfg_mex=TARGET_LEAGUES["MEX"]
        mx_fixtures=get_mx_fixtures([tomorrow,day_after], self.apif_h)

        for m in mx_fixtures[:8]:
            h_n=m["teams"]["home"]["name"]
            a_n=m["teams"]["away"]["name"]
            h_id=m["teams"]["home"]["id"]
            a_id=m["teams"]["away"]["id"]
            ko=m["fixture"]["date"]
            fid=str(m["fixture"]["id"])
            label=f"{h_n} vs {a_n} ({cfg_mex['name']})"
            print(f"\n  ── {label} (fid={fid}) ──", flush=True)

            # [1] xG desde CSV MEX.xlsx goles proxy
            xh,xa,xt,conf,src=build_xg(h_n,a_n,"MEX",cfg_mex,df_mex)

            # [3] Cuotas Bet365 via api-football — FIX #4 restaurado
            odds_mx=get_mx_odds(fid, self.apif_h)
            if not odds_mx:
                print(f"     ⚠️ MEX sin cuotas Bet365 — skip", flush=True)
                log_rej(label,"ALL",0,0,"NO_ODDS_APIF"); continue

            # [3] Lesiones
            inj_h,inj_a=get_mx_injuries(fid,h_id,a_id,self.apif_h)
            if inj_h or inj_a:
                print(f"     🚑 Lesiones: {h_n}={inj_h} {a_n}={inj_a}", flush=True)
                # Recalcular xG con factor lesiones
                xh,xa,xt,conf,src=build_xg(h_n,a_n,"MEX",cfg_mex,df_mex,inj_h,inj_a)

            ok,reason=validate_xg(xh,xa,odds_mx.get("H"),odds_mx.get("A"),odds_mx.get("O25"))
            if not ok: log_rej(label,"ALL",0,0,reason); continue
            if conf=="LOW": log_rej(label,"ALL",0,0,"XG_LOW_SKIP"); continue

            probs=build_probs(xh,xa,conf,h_n,a_n,cfg_mex,odds_mx,{})
            cands=self._filter(probs,label,fid,h_n,a_n,ko,xh,xa,xt,conf,src,"MEX",{})
            if cands:
                cands.sort(key=lambda x:x["ev"]*x["urs"],reverse=True)
                preliminary.append(cands[0])   # ← MEX ALIMENTA preliminary ✅

        # ── Portfolio + reporte ───────────────────────────────────────────
        final,meta=portfolio(preliminary)

        req_total=get_req()   # FIX #3: get_req() no modifica el contador
        if not final:
            send_msg(
                f"🧹 <b>V7.2 Scan completado</b>\n"
                f"Candidatos analizados: {len(preliminary)} | Picks: 0\n"
                f"📡 api-football: {req_total}/100"
            )
            return

        conn=sqlite3.connect(DB_PATH); c=conn.cursor()
        send_msg(
            f"📊 <b>V7.2 Portfolio — {len(final)} picks</b>\n"
            f"Vol: {meta['pvol']*100:.2f}% | Damper: {meta['damp']:.2f}x\n"
            f"Heat: {meta['heat']*100:.2f}%\n"
            f"📡 api-football: {req_total}/100"
        )
        for p in final:
            self._save_pick(p, today_str, c)
            self._report_pick(p)
        conn.commit(); conn.close()

    # ── CLV CAPTURE ──────────────────────────────────────────────────────

    def capture_clv(self):
        """
        16:00 UTC — captura cuotas de cierre.
        Europeas: fixtures.csv de co.uk (0 req api-football).
        MEX: /odds?fixture=X (1 req por pick).
        """
        import pandas as pd
        fix_df=get_fixtures_csv()
        try:
            conn=sqlite3.connect(DB_PATH); cc=conn.cursor()
            cc.execute(
                "SELECT id,fixture_id,div,home_team,away_team,market,selection,odd_open,kickoff_time "
                "FROM picks_log WHERE clv_captured=0 AND result IN ('PENDING','WIN','LOSS')"
            )
            pending=cc.fetchall(); conn.close()
            if not pending: return

            now_utc=datetime.now(timezone.utc); clv_lines=[]
            for row in pending:
                pid,fid,div,home,away,mkt,sel,odd_open,ko_str=row
                try:
                    ko=datetime.fromisoformat(ko_str.replace("Z","+00:00"))
                    hours_diff = (ko-now_utc).total_seconds()/3600
                    # Capturar picks que se juegan hoy (hasta 12h antes del KO)
                    # o que ya empezaron hace menos de 3h (para partidos tarde)
                    if not -3 <= hours_diff <= 12: continue
                except: continue

                odd_close=None
                cfg=TARGET_LEAGUES.get(div,{})

                def _clv_1x2(co, home_n, away_n, sel_str):
                    """Fuzzy match para saber si apostamos home, away o empate."""
                    if "Empate" in sel_str: return co.get("D")
                    team = sel_str.replace("Gana ","").strip()
                    sh = difflib.SequenceMatcher(None, team.lower(), home_n.lower()).ratio()
                    sa = difflib.SequenceMatcher(None, team.lower(), away_n.lower()).ratio()
                    if sh >= 0.60 and sh >= sa: return co.get("H")
                    if sa >= 0.60 and sa > sh:  return co.get("A")
                    return None   # no identificado

                if div=="MEX":
                    # [3] api-football para cerrar MEX
                    close=get_mx_odds(fid, self.apif_h)
                    if close:
                        if mkt=="OVER":   odd_close=close.get("O25")
                        elif mkt=="UNDER": odd_close=close.get("U25")
                        elif mkt=="1X2":   odd_close=_clv_1x2(close, home, away, sel)
                        elif mkt=="BTTS":  odd_close=close.get("BTTS_Y")
                elif div=="BSA":
                    # [2] fd.org Trend Resource para cerrar BSA — 0 req extra si ya se llamó hoy
                    today_d=datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    fd_codes={"BSA": cfg.get("fbd_code","BSA")}
                    ts_map=fetch_trends(today_d, fd_codes)
                    ts=extract_trend(ts_map, home, away, "BSA", today_d)
                    if ts:
                        co={"H":ts.get("home_odd"),"D":ts.get("draw_odd"),
                            "A":ts.get("away_odd"),"O25":ts.get("ou25_odd")}
                        if mkt=="OVER":   odd_close=co.get("O25")
                        elif mkt=="UNDER":
                            o=co.get("O25")
                            odd_close=round(1/(1-1/o),2) if o and o>1.01 else None
                        elif mkt=="1X2":   odd_close=_clv_1x2(co, home, away, sel)
                        elif mkt=="BTTS":  odd_close=ts.get("bts_odd")
                elif fix_df is not None:
                    # [1] fixtures.csv para europeas — usar Pinnacle como closing line
                    # Pinnacle es el bookmaker más eficiente — mejor referencia de mercado
                    rh=difflib.get_close_matches(home,fix_df["HomeTeam"].dropna().unique(),n=1,cutoff=0.55)
                    ra=difflib.get_close_matches(away,fix_df["AwayTeam"].dropna().unique(),n=1,cutoff=0.55)
                    if rh and ra:
                        m=fix_df[(fix_df["HomeTeam"]==rh[0])&(fix_df["AwayTeam"]==ra[0])]
                        if not m.empty:
                            row=m.iloc[0]
                            def best_close(*cols):
                                for c in cols:
                                    try:
                                        v=row.get(c) if hasattr(row,"get") else getattr(row,c,None)
                                        if v is not None:
                                            f=float(v)
                                            if f>1.01 and not (f!=f): return f
                                    except: pass
                                return None
                            # Prioridad: Pinnacle > BetVictor > B365 > Avg
                            co={
                                "H":   best_close("PSH","VCH","B365H","AvgH","BbAvH"),
                                "D":   best_close("PSD","VCD","B365D","AvgD","BbAvD"),
                                "A":   best_close("PSA","VCA","B365A","AvgA","BbAvA"),
                                "O25": best_close("P>2.5","B365>2.5","Avg>2.5","BbAv>2.5"),
                                "U25": best_close("P<2.5","B365<2.5","Avg<2.5","BbAv<2.5"),
                                "BTTS_Y": best_close("B365BTTSY","BbAvBBTS"),
                            }
                            if mkt=="OVER":    odd_close=co.get("O25")
                            elif mkt=="UNDER": odd_close=co.get("U25")
                            elif mkt=="1X2":   odd_close=_clv_1x2(co, home, away, sel)
                            elif mkt=="BTTS":  odd_close=co.get("BTTS_Y")
                            elif mkt=="DC":
                                # DC closing: derivar de Pinnacle H/D/A
                                oh,od,oa=co.get("H"),co.get("D"),co.get("A")
                                if oh and od and oa:
                                    if "o Empate" in sel and not sel.startswith("DC: Empate"):
                                        odd_close=round(1/(1/oh+1/od),2)
                                    elif "Empate o" in sel:
                                        odd_close=round(1/(1/od+1/oa),2)
                                    else:
                                        odd_close=round(1/(1/oh+1/oa),2)
                            elif mkt=="DNB":
                                oh,od,oa=co.get("H"),co.get("D"),co.get("A")
                                if oh and od:
                                    try: odd_close=round(1/(1/oh-1/od),2) if (1/oh-1/od)>0.05 else None
                                    except: odd_close=None

                if odd_close and odd_open:
                    clv_pct=(odd_close/odd_open-1)*100
                    conn=sqlite3.connect(DB_PATH)
                    conn.execute("INSERT INTO closing_lines VALUES (NULL,?,?,?,?,?,?,?)",
                                 (fid,mkt,sel,odd_open,odd_close,clv_pct,now_utc.isoformat()))
                    conn.execute("UPDATE picks_log SET clv_captured=1 WHERE id=?",(pid,))
                    conn.commit(); conn.close()
                    clv_lines.append(f"{sel} @{odd_open:.2f}→{odd_close:.2f} CLV={clv_pct:+.1f}%")

            if clv_lines:
                pos = sum(1 for l in clv_lines if "CLV=+" in l)
                neg = len(clv_lines) - pos
                beat_rate = pos/len(clv_lines)*100 if clv_lines else 0
                send_msg(
                    f"📉 <b>CLV V7.2</b> — {len(clv_lines)} picks\n"
                    f"Beat closing: {pos}/{len(clv_lines)} ({beat_rate:.0f}%)\n"
                    f"{'\n'.join(clv_lines[:10])}"
                    + (f"\n... +{len(clv_lines)-10} más" if len(clv_lines)>10 else "")
                )
        except Exception as e:
            print(f"  ⚠️ CLV: {e}", flush=True)

    # ── WEEKLY — STANDINGS fd.org ─────────────────────────────────────────

    def weekly_standings(self):
        """Martes 09:00 — Standings HOME/AWAY desde fd.org. ~8 req."""
        if not FD_ORG_TOKEN: return
        updated=[]
        for div,cfg in TARGET_LEAGUES.items():
            code=cfg.get("fbd_code")
            if not code: continue
            try:
                r=requests.get(
                    f"https://api.football-data.org/v4/competitions/{code}/standings",
                    headers={"X-Auth-Token": FD_ORG_TOKEN}, timeout=15
                )
                track_req()
                if r.status_code==200:
                    # Guardar factor home/away en cache para build_xg futuro
                    conn=sqlite3.connect(DB_PATH)
                    now=datetime.now(timezone.utc).isoformat()
                    for st in r.json().get("standings",[]):
                        stype=st.get("type","TOTAL")
                        for e in st.get("table",[]):
                            t=e.get("team",{}); pl=e.get("playedGames",0)
                            key=f"STAND_{code}_{t.get('id','')}_{stype}"
                            conn.execute(
                                """INSERT OR REPLACE INTO team_xg_cache
                                (team_key,div,team_name,gf_series,ga_series,
                                 shots_for,shots_against,xg_for,xg_against,
                                 confidence,updated_at,depth) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                                (key,code,t.get("name",""),"[]","[]","[]","[]",
                                 e.get("goalsFor",0)/pl if pl else 0,
                                 e.get("goalsAgainst",0)/pl if pl else 0,
                                 stype,now,pl)
                            )
                    conn.commit(); conn.close()
                    updated.append(code)
                time.sleep(7.0)
            except Exception as e:
                print(f"  ⚠️ standings {code}: {e}", flush=True)
        send_msg(f"📊 <b>Standings V7.2:</b> {', '.join(updated)}")

# ============================================================
# SCHEDULER — horas UTC fijas, independiente del boot
# ============================================================

def _hhmm(t_str):
    """'06:00' → (6, 0)"""
    h, m = t_str.split(":")
    return int(h), int(m)


# ============================================================
# DASHBOARD WEB — servidor HTTP en hilo separado
# ============================================================

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>V7.2 · Quant Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Barlow:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#07080d;
  --s1:#0c0e17;
  --s2:#11141f;
  --border:#1c2035;
  --border2:#252840;
  --accent:#4f6ef7;
  --accent2:#7c6fff;
  --green:#22c55e;
  --red:#ef4444;
  --amber:#f59e0b;
  --text:#e8eaf6;
  --muted:#5a5f80;
  --mono:'IBM Plex Mono',monospace;
  --sans:'Barlow',sans-serif;
}
body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;overflow-x:hidden}

/* ── HEADER ── */
header{
  display:flex;align-items:center;justify-content:space-between;
  padding:1.25rem 2rem;
  border-bottom:1px solid var(--border);
  background:var(--s1);
}
.logo{display:flex;align-items:center;gap:10px}
.logo-mark{width:28px;height:28px;background:var(--accent);border-radius:6px;display:flex;align-items:center;justify-content:center}
.logo-mark svg{width:16px;height:16px;fill:#fff}
.logo h1{font-size:1rem;font-weight:600;letter-spacing:.02em;color:var(--text)}
.logo span{font-family:var(--mono);font-size:.65rem;color:var(--muted);margin-top:1px}
.header-right{display:flex;align-items:center;gap:12px}
.live-pill{display:flex;align-items:center;gap:6px;font-family:var(--mono);font-size:.65rem;color:var(--green);background:rgba(34,197,94,.08);border:1px solid rgba(34,197,94,.2);padding:4px 10px;border-radius:99px}
.live-dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:blink 1.8s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
#last-update{font-family:var(--mono);font-size:.65rem;color:var(--muted)}

/* ── STAT CARDS ── */
.stats-row{
  display:grid;
  grid-template-columns:repeat(8,minmax(0,1fr));
  border-bottom:1px solid var(--border);
}
.stat{
  padding:1.25rem 1.5rem;
  border-right:1px solid var(--border);
  transition:background .15s;
}
.stat:last-child{border-right:none}
.stat:hover{background:var(--s2)}
.stat-label{font-family:var(--mono);font-size:.58rem;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:.5rem}
.stat-value{font-size:1.65rem;font-weight:600;letter-spacing:-.03em;line-height:1}
.stat-sub{font-family:var(--mono);font-size:.6rem;color:var(--muted);margin-top:.3rem}
.green{color:var(--green)} .red{color:var(--red)} .blue{color:var(--accent)} .amber{color:var(--amber)} .white{color:var(--text)}

/* ── TOOLBAR ── */
.toolbar{
  display:flex;align-items:center;gap:8px;padding:.85rem 2rem;
  background:var(--s1);border-bottom:1px solid var(--border);
  flex-wrap:wrap;
}
.seg{display:flex;gap:2px;background:var(--s2);border:1px solid var(--border);border-radius:8px;padding:2px}
.seg-btn{
  font-family:var(--mono);font-size:.65rem;letter-spacing:.04em;
  padding:5px 12px;border:none;background:transparent;color:var(--muted);
  border-radius:6px;cursor:pointer;transition:all .15s;
}
.seg-btn:hover{color:var(--text)}
.seg-btn.active{background:var(--accent);color:#fff}
.spacer{flex:1}
#search{
  font-family:var(--mono);font-size:.7rem;padding:7px 12px;
  background:var(--s2);border:1px solid var(--border);color:var(--text);
  border-radius:7px;outline:none;width:200px;transition:border-color .15s;
}
#search:focus{border-color:var(--accent)}
#search::placeholder{color:var(--muted)}
#resolve-btn{
  font-family:var(--mono);font-size:.65rem;letter-spacing:.04em;
  padding:7px 14px;background:rgba(79,110,247,.12);
  border:1px solid rgba(79,110,247,.3);color:var(--accent);
  border-radius:7px;cursor:pointer;transition:all .15s;white-space:nowrap;
}
#resolve-btn:hover{background:rgba(79,110,247,.2)}
#resolve-btn:disabled{opacity:.5;cursor:default}

/* ── TABLE ── */
.table-wrap{overflow-x:auto;min-height:400px}
table{width:100%;border-collapse:collapse;font-size:.78rem}
thead th{
  font-family:var(--mono);font-size:.58rem;text-transform:uppercase;
  letter-spacing:.08em;color:var(--muted);padding:10px 16px;
  text-align:left;border-bottom:1px solid var(--border);
  cursor:pointer;user-select:none;white-space:nowrap;
  background:var(--s1);position:sticky;top:0;
}
thead th:hover{color:var(--text)}
thead th.sorted{color:var(--accent)}
thead th.sorted::after{content:' ↓';font-size:.55rem}
thead th.sorted.asc::after{content:' ↑'}
tbody tr{border-bottom:1px solid var(--border);transition:background .1s}
tbody tr:hover{background:var(--s2)}
tbody td{padding:10px 16px;white-space:nowrap;font-family:var(--mono);font-size:.73rem;vertical-align:middle}
td.party{font-family:var(--sans);font-size:.82rem;font-weight:500;color:var(--text);white-space:normal;min-width:160px}
td.muted-td{color:var(--muted)}

/* ── BADGES ── */
.badge{display:inline-flex;align-items:center;padding:2px 7px;border-radius:4px;font-size:.6rem;font-weight:500;letter-spacing:.05em;gap:4px}
.b-win{background:rgba(34,197,94,.1);color:var(--green);border:1px solid rgba(34,197,94,.2)}
.b-loss{background:rgba(239,68,68,.1);color:var(--red);border:1px solid rgba(239,68,68,.2)}
.b-pend{background:rgba(79,110,247,.1);color:var(--accent);border:1px solid rgba(79,110,247,.2)}
.b-under{background:rgba(56,189,248,.08);color:#38bdf8;border:1px solid rgba(56,189,248,.15)}
.b-over{background:rgba(251,146,60,.08);color:#fb923c;border:1px solid rgba(251,146,60,.15)}
.b-dc{background:rgba(167,139,250,.08);color:#a78bfa;border:1px solid rgba(167,139,250,.15)}
.b-1x2{background:rgba(250,204,21,.08);color:#facc15;border:1px solid rgba(250,204,21,.15)}

.pos{color:var(--green)} .neg{color:var(--red)} .neu{color:var(--muted)}
.ev-high{color:var(--green)} .ev-mid{color:var(--amber)} .ev-low{color:var(--muted)}

/* ── EMPTY ── */
.empty{text-align:center;padding:5rem;color:var(--muted);font-family:var(--mono);font-size:.8rem}

/* ── MINI CHART BAR ── */
.xg-bar{display:flex;gap:3px;align-items:center}
.xg-seg{height:4px;border-radius:2px;min-width:3px}

/* ── RESPONSIVE ── */
@media(max-width:900px){
  .stats-row{grid-template-columns:repeat(4,1fr)}
  .stat{border-bottom:1px solid var(--border)}
  header{padding:1rem}
  .toolbar{padding:.75rem 1rem}
  tbody td{padding:8px 10px}
}
</style>
</head>
<body>

<header>
  <div class="logo">
    <div class="logo-mark">
      <svg viewBox="0 0 16 16"><path d="M2 12 L8 4 L14 12 Z"/></svg>
    </div>
    <div>
      <h1>V7.2 Quant</h1>
      <span>Triple League Specialist</span>
    </div>
  </div>
  <div class="header-right">
    <span id="last-update"></span>
    <div class="live-pill"><span class="live-dot"></span>LIVE</div>
  </div>
</header>

<div class="stats-row">
  <div class="stat">
    <div class="stat-label">Picks totales</div>
    <div class="stat-value white" id="s-total">—</div>
    <div class="stat-sub" id="s-sub-total"></div>
  </div>
  <div class="stat">
    <div class="stat-label">Win</div>
    <div class="stat-value green" id="s-win">—</div>
    <div class="stat-sub" id="s-sub-win"></div>
  </div>
  <div class="stat">
    <div class="stat-label">Loss</div>
    <div class="stat-value red" id="s-loss">—</div>
    <div class="stat-sub" id="s-sub-loss"></div>
  </div>
  <div class="stat">
    <div class="stat-label">Pending</div>
    <div class="stat-value blue" id="s-pend">—</div>
    <div class="stat-sub">esperando resultado</div>
  </div>
  <div class="stat">
    <div class="stat-label">Beat Rate</div>
    <div class="stat-value" id="s-br">—</div>
    <div class="stat-sub">mín. 52% para live</div>
  </div>
  <div class="stat">
    <div class="stat-label">Avg EV</div>
    <div class="stat-value blue" id="s-ev">—</div>
    <div class="stat-sub">apertura</div>
  </div>
  <div class="stat">
    <div class="stat-label">PnL (u)</div>
    <div class="stat-value" id="s-pnl">—</div>
    <div class="stat-sub">unidades de bankroll</div>
  </div>
  <div class="stat">
    <div class="stat-label">Burn-in</div>
    <div class="stat-value" id="s-burn">—</div>
    <div class="stat-sub" id="s-burn-sub">picks resueltos</div>
  </div>
</div>

<div class="toolbar">
  <div class="seg" id="filter-seg">
    <button class="seg-btn active" data-filter="all">Todos</button>
    <button class="seg-btn" data-filter="WIN">Win</button>
    <button class="seg-btn" data-filter="LOSS">Loss</button>
    <button class="seg-btn" data-filter="PENDING">Pending</button>
  </div>
  <div class="seg" id="mkt-seg">
    <button class="seg-btn active" data-mkt="all">Mercados</button>
    <button class="seg-btn" data-mkt="UNDER">Under</button>
    <button class="seg-btn" data-mkt="OVER">Over</button>
    <button class="seg-btn" data-mkt="DC">DC</button>
    <button class="seg-btn" data-mkt="1X2">1X2</button>
    <button class="seg-btn" data-mkt="BTTS">BTTS Sí</button>
    <button class="seg-btn" data-mkt="BTTS_NO">BTTS No</button>
    <button class="seg-btn" data-mkt="DNB">DNB</button>
  </div>
  <div class="spacer"></div>
  <input id="search" type="text" placeholder="buscar equipo, liga...">
  <button id="resolve-btn" onclick="resolveData()">resolver picks</button>
</div>

<div class="table-wrap">
<table id="picks-table">
  <thead>
    <tr>
      <th data-col="date">Fecha</th>
      <th data-col="div">Liga</th>
      <th data-col="match">Partido</th>
      <th data-col="market">Mkt</th>
      <th data-col="odd">Cuota</th>
      <th data-col="ev">EV</th>
      <th data-col="prob">Prob</th>
      <th data-col="stake">Stake</th>
      <th data-col="xg">xG</th>
      <th data-col="status">Resultado</th>
      <th data-col="profit">Profit</th>
    </tr>
  </thead>
  <tbody id="picks-body">
    <tr><td colspan="11" class="empty">cargando...</td></tr>
  </tbody>
</table>
</div>

<script>
let allPicks = [], sortCol = 'date', sortDir = -1;
let activeFlt = 'all', activeMkt = 'all';

const mktBadge = m => {
  const map = {UNDER:'b-under',OVER:'b-over',DC:'b-dc','1X2':'b-1x2',BTTS:'b-under',BTTS_NO:'b-over',DNB:'b-dc'};
  return `<span class="badge ${map[m]||'b-1x2'}">${m}</span>`;
};
const statusBadge = s => {
  const map = {WIN:'b-win',LOSS:'b-loss',PENDING:'b-pend'};
  const icon = {WIN:'▲',LOSS:'▼',PENDING:'◎'};
  return `<span class="badge ${map[s]||''}">${icon[s]||''}${s}</span>`;
};
const evClass = v => v >= 0.10 ? 'ev-high' : v >= 0.05 ? 'ev-mid' : 'ev-low';
const fmtDate = d => { if (!d) return '—'; const p = d.split('T')[0].split('-'); return `${p[2]}/${p[1]}/${p[0].slice(2)}`; };
const xgBar = (h, a) => {
  const total = (h||0) + (a||0);
  if (!total) return '—';
  const hw = Math.round((h/total)*60);
  return `<div class="xg-bar"><span style="font-size:.6rem;color:var(--muted)">${(h||0).toFixed(1)}</span><div class="xg-seg" style="width:${hw}px;background:var(--accent)"></div><div class="xg-seg" style="width:${60-hw}px;background:var(--red)"></div><span style="font-size:.6rem;color:var(--muted)">${(a||0).toFixed(1)}</span></div>`;
};

function updateStats(s) {
  const resolved = s.wins + s.losses;
  const br = resolved ? s.wins/resolved : 0;
  document.getElementById('s-total').textContent = s.total;
  document.getElementById('s-sub-total').textContent = `${resolved} resueltos`;
  document.getElementById('s-win').textContent = s.wins;
  document.getElementById('s-sub-win').textContent = resolved ? `${(s.wins/resolved*100).toFixed(1)}%` : '';
  document.getElementById('s-loss').textContent = s.losses;
  document.getElementById('s-sub-loss').textContent = resolved ? `${(s.losses/resolved*100).toFixed(1)}%` : '';
  document.getElementById('s-pend').textContent = s.pending;
  const brEl = document.getElementById('s-br');
  brEl.textContent = resolved ? (br*100).toFixed(1)+'%' : '—';
  brEl.className = 'stat-value ' + (br >= 0.55 ? 'green' : br >= 0.50 ? 'amber' : 'red');
  document.getElementById('s-ev').textContent = '+' + s.avg_ev.toFixed(1) + '%';
  const pnlEl = document.getElementById('s-pnl');
  pnlEl.textContent = (s.pnl >= 0 ? '+' : '') + s.pnl.toFixed(4);
  pnlEl.className = 'stat-value ' + (s.pnl > 0 ? 'green' : s.pnl < 0 ? 'red' : 'white');
  const burnEl = document.getElementById('s-burn');
  burnEl.textContent = s.resolved + '/30';
  burnEl.className = 'stat-value ' + (s.resolved >= 30 ? 'green' : 'blue');
  document.getElementById('s-burn-sub').textContent = s.resolved >= 30 ? '¡listo para live!' : `faltan ${30-s.resolved}`;
  document.getElementById('last-update').textContent = new Date().toLocaleTimeString('es-MX',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
}

function render() {
  const search = document.getElementById('search').value.toLowerCase();
  let data = allPicks.filter(p => {
    if (activeFlt !== 'all' && p.status !== activeFlt) return false;
    if (activeMkt !== 'all' && p.market !== activeMkt) return false;
    if (search) {
      const hay = ((p.home||'') + ' ' + (p.away||'') + ' ' + (p.div||'') + ' ' + (p.market||'')).toLowerCase();
      if (!hay.includes(search)) return false;
    }
    return true;
  });
  data.sort((a,b) => {
    let av = a[sortCol] ?? '', bv = b[sortCol] ?? '';
    if (typeof av === 'string') av = av.toLowerCase();
    if (typeof bv === 'string') bv = bv.toLowerCase();
    return av < bv ? sortDir : av > bv ? -sortDir : 0;
  });
  if (!data.length) {
    document.getElementById('picks-body').innerHTML = `<tr><td colspan="11" class="empty">sin picks con ese filtro</td></tr>`;
    return;
  }
  const rows = data.map(p => {
    const ev = parseFloat(p.ev||0);
    const prob = parseFloat(p.prob||0)*100;
    const stake = parseFloat(p.stake||0)*100;
    const profit = parseFloat(p.profit||0);
    const profitStr = p.status === 'PENDING'
      ? `<span class="neu">—</span>`
      : `<span class="${profit >= 0 ? 'pos' : 'neg'}">${profit >= 0 ? '+' : ''}${profit.toFixed(4)}</span>`;
    return `<tr>
      <td class="muted-td">${fmtDate(p.date)}</td>
      <td class="muted-td">${p.div||'—'}</td>
      <td class="party">${p.home||''} <span style="color:var(--muted);font-weight:300">vs</span> ${p.away||''}</td>
      <td>${mktBadge(p.market)}</td>
      <td>@${parseFloat(p.odd||0).toFixed(2)}</td>
      <td class="${evClass(ev)}">+${(ev*100).toFixed(1)}%</td>
      <td class="muted-td">${prob.toFixed(1)}%</td>
      <td class="muted-td">${stake.toFixed(2)}%</td>
      <td>${xgBar(p.xg_h, p.xg_a)}</td>
      <td>${statusBadge(p.status)}</td>
      <td>${profitStr}</td>
    </tr>`;
  }).join('');
  document.getElementById('picks-body').innerHTML = rows;
}

async function loadData() {
  try {
    const r = await fetch('/api/picks');
    const data = await r.json();
    allPicks = data.picks || [];
    updateStats(data.stats);
    render();
  } catch(e) {
    document.getElementById('picks-body').innerHTML = `<tr><td colspan="11" class="empty">error cargando datos</td></tr>`;
  }
}

async function resolveData() {
  const btn = document.getElementById('resolve-btn');
  btn.textContent = 'resolviendo...'; btn.disabled = true;
  try {
    const r = await fetch('/api/resolve');
    const d = await r.json();
    btn.textContent = d.error ? 'error' : `✓ ${d.resolved||0} resueltos (${d.wins||0}W/${d.losses||0}L)`;
    await loadData();
  } catch(e) { btn.textContent = 'error'; }
  setTimeout(() => { btn.textContent = 'resolver picks'; btn.disabled = false; }, 5000);
}

document.querySelectorAll('#filter-seg .seg-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('#filter-seg .seg-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active'); activeFlt = btn.dataset.filter; render();
  });
});
document.querySelectorAll('#mkt-seg .seg-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('#mkt-seg .seg-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active'); activeMkt = btn.dataset.mkt; render();
  });
});
document.querySelectorAll('thead th[data-col]').forEach(th => {
  th.addEventListener('click', () => {
    const col = th.dataset.col;
    if (sortCol === col) { sortDir *= -1; th.classList.toggle('asc'); }
    else { sortCol = col; sortDir = -1; document.querySelectorAll('thead th').forEach(t => { t.classList.remove('sorted','asc'); }); }
    th.classList.add('sorted'); render();
  });
});
document.getElementById('search').addEventListener('input', render);

loadData();
setInterval(loadData, 60000);
</script>
</body>
</html>
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # silenciar logs HTTP

    def do_GET(self):
        if self.path == "/" or self.path == "/dashboard":
            self._serve_html()
        elif self.path == "/api/picks":
            self._serve_api()
        elif self.path == "/api/sync":
            self._serve_sync()
        elif self.path == "/api/resolve":
            self._serve_resolve()
        else:
            self.send_response(404); self.end_headers()

    def _serve_sync(self):
        """Sincroniza CSV → picks_log DB para picks históricos sin resultado."""
        try:
            import csv as csvmod
            synced = 0
            if os.path.exists(AUDIT_CSV):
                with open(AUDIT_CSV, "r", encoding="utf-8") as f:
                    reader = csvmod.reader(f)
                    header = next(reader)
                    rows = list(reader)
                conn_s = sqlite3.connect(DB_PATH)
                # Cargar todos los picks PENDING de la DB indexados por equipo+mercado
                pending = conn_s.execute(
                    "SELECT id, home_team, away_team, market FROM picks_log WHERE result='PENDING'"
                ).fetchall()
                # Construir índice: (home_lower, away_lower, market) → id
                idx = {}
                for pid, ht, at, mk in pending:
                    key = (ht.lower()[:6], at.lower()[:6], mk)
                    idx[key] = pid
                for row in rows:
                    if len(row) < 12: continue
                    status = row[9]
                    if status not in ("WIN", "LOSS"): continue
                    home, away, mkt = row[2], row[3], row[5]
                    try: profit = float(row[11] or 0)
                    except: profit = 0.0
                    key = (home.lower()[:6], away.lower()[:6], mkt)
                    if key in idx:
                        conn_s.execute(
                            "UPDATE picks_log SET result=?, profit=? WHERE id=?",
                            (status, profit, idx[key])
                        )
                        synced += 1
                conn_s.commit(); conn_s.close()
            payload = json.dumps({"ok": True, "synced": synced}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(payload)
        except Exception as e:
            err = json.dumps({"error": str(e)}).encode()
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(err)

    def _serve_html(self):
        content = DASHBOARD_HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(content))
        self.end_headers()
        self.wfile.write(content)

    def _serve_api(self):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                SELECT pick_time, div, home_team, away_team,
                       market, selection, odd_open, prob_model, ev_open,
                       result, stake_pct, profit, xg_home, xg_away
                FROM picks_log ORDER BY id DESC LIMIT 500
            """)
            rows = c.fetchall()
            conn.close()

            picks = []
            wins = losses = pending = 0
            total_ev = total_pnl = 0.0
            resolved = 0

            for r in rows:
                status = r[9] or "PENDING"
                if status == "WIN": wins += 1; resolved += 1
                elif status == "LOSS": losses += 1; resolved += 1
                else: pending += 1
                ev = float(r[8] or 0)
                total_ev += ev
                total_pnl += float(r[11] or 0)
                picks.append({
                    "date": r[0], "div": r[1], "home": r[2], "away": r[3],
                    "market": r[4], "pick": r[5],
                    "odd": r[6], "prob": r[7], "ev": r[8],
                    "status": status, "stake": r[10], "profit": r[11],
                    "xg_h": r[12], "xg_a": r[13]
                })

            n = len(picks)
            avg_ev = (total_ev / n * 100) if n else 0
            payload = json.dumps({
                "picks": picks,
                "stats": {
                    "total": n, "wins": wins, "losses": losses,
                    "pending": pending, "avg_ev": avg_ev,
                    "pnl": total_pnl, "resolved": resolved
                }
            }).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(payload))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(payload)
        except Exception as e:
            err = json.dumps({"error": str(e)}).encode()
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(err)

    def _serve_resolve(self):
        """Resuelve picks PENDING contra los CSVs de co.uk directamente."""
        try:
            import pandas as pd
            resolved = wins = losses = 0
            conn_r = sqlite3.connect(DB_PATH)
            pending = conn_r.execute("""
                SELECT id, div, home_team, away_team, market, selection,
                       odd_open, stake_pct
                FROM picks_log WHERE result='PENDING'
            """).fetchall()

            for pid, div, home, away, mkt, pick, odd, stake in pending:
                path = os.path.join(DATA_DIR, f"{div}.csv")
                if not os.path.exists(path):
                    continue
                try:
                    try:    df = pd.read_csv(path, encoding="utf-8-sig")
                    except: df = pd.read_csv(path, encoding="latin-1")
                    df = df.rename(columns={"Home":"HomeTeam","Away":"AwayTeam",
                                            "HG":"FTHG","AG":"FTAG"})
                    played = df.dropna(subset=["FTHG","FTAG"])
                    teams = pd.concat([played["HomeTeam"],played["AwayTeam"]]).unique()
                    import difflib as dl
                    rh = dl.get_close_matches(home, teams, n=1, cutoff=0.55)
                    ra = dl.get_close_matches(away, teams, n=1, cutoff=0.55)
                    if not rh or not ra:
                        continue
                    m = played[(played["HomeTeam"]==rh[0])&(played["AwayTeam"]==ra[0])]
                    if m.empty:
                        continue
                    fthg = float(m.iloc[-1]["FTHG"])
                    ftag = float(m.iloc[-1]["FTAG"])
                    res = check_result(pick, mkt, fthg, ftag,
                                       home_name=rh[0], away_name=ra[0])
                    if res not in ("WIN","LOSS"):
                        continue
                    profit = round(float(stake or 0)*float(odd or 0) - float(stake or 0)
                                   if res=="WIN" else -float(stake or 0), 4)
                    conn_r.execute(
                        "UPDATE picks_log SET result=?, profit=? WHERE id=?",
                        (res, profit, pid)
                    )
                    resolved += 1
                    if res=="WIN": wins += 1
                    else: losses += 1
                except Exception:
                    continue

            conn_r.commit(); conn_r.close()
            payload = json.dumps({
                "ok": True, "resolved": resolved,
                "wins": wins, "losses": losses
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(payload)
        except Exception as e:
            err = json.dumps({"error": str(e)}).encode()
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(err)

def auto_resolve():
    """Resuelve picks PENDING contra CSVs al arrancar. Sin requests externos."""
    import difflib as dl
    import pandas as pd
    resolved = wins = losses = 0
    try:
        conn_r = sqlite3.connect(DB_PATH)
        pending = conn_r.execute("""
            SELECT id, div, home_team, away_team, market, selection,
                   odd_open, stake_pct
            FROM picks_log WHERE result='PENDING'
        """).fetchall()
        for pid, div, home, away, mkt, pick, odd, stake in pending:
            path = os.path.join(DATA_DIR, f"{div}.csv")
            if not os.path.exists(path):
                continue
            try:
                try:    df = pd.read_csv(path, encoding="utf-8-sig")
                except: df = pd.read_csv(path, encoding="latin-1")
                df = df.rename(columns={"Home":"HomeTeam","Away":"AwayTeam",
                                        "HG":"FTHG","AG":"FTAG"})
                played = df.dropna(subset=["FTHG","FTAG"])
                teams = pd.concat([played["HomeTeam"], played["AwayTeam"]]).unique()
                rh = dl.get_close_matches(home, teams, n=1, cutoff=0.55)
                ra = dl.get_close_matches(away, teams, n=1, cutoff=0.55)
                if not rh or not ra: continue
                m = played[(played["HomeTeam"]==rh[0])&(played["AwayTeam"]==ra[0])]
                if m.empty: continue
                fthg = float(m.iloc[-1]["FTHG"])
                ftag = float(m.iloc[-1]["FTAG"])
                res = check_result(pick, mkt, fthg, ftag,
                                   home_name=rh[0], away_name=ra[0])
                if res not in ("WIN","LOSS"): continue
                profit = round(
                    float(stake or 0)*float(odd or 0) - float(stake or 0)
                    if res=="WIN" else -float(stake or 0), 4)
                conn_r.execute(
                    "UPDATE picks_log SET result=?, profit=? WHERE id=?",
                    (res, profit, pid))
                resolved += 1
                if res=="WIN": wins += 1
                else: losses += 1
            except Exception:
                continue
        conn_r.commit(); conn_r.close()
        if resolved:
            print(f"  ✅ Auto-resolve: {resolved} picks ({wins}W/{losses}L)", flush=True)
    except Exception as e:
        print(f"  ⚠️ auto_resolve: {e}", flush=True)

def start_dashboard(port=8080):
    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f"  🌐 Dashboard en http://0.0.0.0:{port}", flush=True)
    return server

if __name__ == "__main__":
    # Arrancar dashboard web en puerto 8080
    start_dashboard(port=int(os.getenv("PORT", "8080")))
    auto_resolve()   # resolver picks PENDING contra CSVs al arrancar
    bot = TripleLeagueV72()

    # Registro de última ejecución por tarea (fecha UTC)
    _last_run = {}

    def _ran_today(key):
        return _last_run.get(key) == datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _mark_ran(key):
        _last_run[key] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _ran_this_week(key):
        return _last_run.get(key) == datetime.now(timezone.utc).strftime("%Y-W%W")

    def _mark_ran_week(key):
        _last_run[key] = datetime.now(timezone.utc).strftime("%Y-W%W")

    if os.getenv("SELF_TEST", "False") == "True":
        bot.refresh_csvs()
        bot.run_daily_scan()

    CSV_H,  CSV_M  = _hhmm(RUN_TIME_CSV_UPDATE)   # 06:00
    AUDIT_H,AUDIT_M= _hhmm(RUN_TIME_AUDIT)         # 07:00
    SCAN_H, SCAN_M = _hhmm(RUN_TIME_SCAN)           # 06:30
    CLV_H,  CLV_M  = _hhmm(RUN_TIME_CLV)            # 16:00
    STAND_H,STAND_M= _hhmm(RUN_TIME_STANDINGS)      # 09:00 martes

    print("⏰ Scheduler UTC activo", flush=True)
    while True:
        now = datetime.now(timezone.utc)
        hh, mm = now.hour, now.minute

        # 06:00 — refresh CSVs
        if (hh, mm) == (CSV_H, CSV_M) and not _ran_today("csv"):
            _mark_ran("csv")
            try: bot.refresh_csvs()
            except Exception as e: print(f"⚠️ refresh_csvs: {e}", flush=True)

        # 07:00 — audit + pnl + auto-resolve
        if (hh, mm) == (AUDIT_H, AUDIT_M) and not _ran_today("audit"):
            _mark_ran("audit")
            try: run_audit()
            except Exception as e: print(f"⚠️ run_audit: {e}", flush=True)
            try: auto_resolve()
            except Exception as e: print(f"⚠️ auto_resolve: {e}", flush=True)
            try: calc_pnl()
            except Exception as e: print(f"⚠️ calc_pnl: {e}", flush=True)

        # 06:30 — kill-switch check + scan principal
        if (hh, mm) == (SCAN_H, SCAN_M) and not _ran_today("scan"):
            _mark_ran("scan")
            if not kill_switch_check():
                try: bot.run_daily_scan()
                except Exception as e: print(f"⚠️ run_daily_scan: {e}", flush=True)
            else:
                print("  🚨 Scan omitido — kill-switch activo", flush=True)

        # 16:00 — capture CLV
        if (hh, mm) == (CLV_H, CLV_M) and not _ran_today("clv"):
            _mark_ran("clv")
            try: bot.capture_clv()
            except Exception as e: print(f"⚠️ capture_clv: {e}", flush=True)

        # Martes 09:00 — standings
        if now.weekday() == 1 and (hh, mm) == (STAND_H, STAND_M) and not _ran_this_week("standings"):
            _mark_ran_week("standings")
            try: bot.weekly_standings()
            except Exception as e: print(f"⚠️ weekly_standings: {e}", flush=True)

        time.sleep(30)
