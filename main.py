import os, time, math, json, csv, io, difflib, requests, sqlite3, numpy as np
from datetime import datetime, timedelta, timezone
from math import exp, lgamma, log

# ============================================================
# LOGGER ESTRUCTURADO — salida clara para Railway logs
# ============================================================

class Log:
    """Logger con timestamps UTC y niveles. Todo va a stdout para Railway."""

    _session_stats = {
        "picks_generados": 0,
        "picks_rechazados": 0,
        "csvs_descargados": 0,
        "errores": 0,
        "clv_capturados": 0,
        "picks_resueltos": 0,
    }

    @staticmethod
    def _ts():
        return datetime.now(timezone.utc).strftime("%H:%M:%S")

    @staticmethod
    def info(msg, section=None):
        prefix = f"[{Log._ts()}]"
        if section: prefix += f" [{section}]"
        print(f"{prefix} {msg}", flush=True)

    @staticmethod
    def ok(msg, section=None):
        prefix = f"[{Log._ts()}] ✅"
        if section: prefix += f" [{section}]"
        print(f"{prefix} {msg}", flush=True)

    @staticmethod
    def warn(msg, section=None):
        prefix = f"[{Log._ts()}] ⚠️"
        if section: prefix += f" [{section}]"
        print(f"{prefix} {msg}", flush=True)
        Log._session_stats["errores"] += 1

    @staticmethod
    def err(msg, section=None):
        prefix = f"[{Log._ts()}] ❌"
        if section: prefix += f" [{section}]"
        print(f"{prefix} {msg}", flush=True)
        Log._session_stats["errores"] += 1

    @staticmethod
    def pick(label, mkt, odd, ev, urs, xgh, xga, conf):
        Log._session_stats["picks_generados"] += 1
        print(
            f"[{Log._ts()}] 🎯 PICK | {label} | {mkt} @{odd:.2f} "
            f"EV={ev*100:.1f}% URS={urs:.3f} xG={xgh:.2f}/{xga:.2f} {conf}",
            flush=True
        )

    @staticmethod
    def rej(label, mkt, odd, ev, reason):
        Log._session_stats["picks_rechazados"] += 1
        print(
            f"[{Log._ts()}] ↩  REJ  | {label} | {mkt} @{odd:.2f} "
            f"EV={ev*100:.1f}% → {reason}",
            flush=True
        )

    @staticmethod
    def scan_start(dates):
        print(f"\n{'='*60}", flush=True)
        print(f"[{Log._ts()}] 🔍 SCAN — {' / '.join(dates)}", flush=True)
        print(f"{'='*60}", flush=True)
        Log._session_stats["picks_generados"] = 0
        Log._session_stats["picks_rechazados"] = 0

    @staticmethod
    def scan_end(n_picks, n_leagues, heat, vol):
        print(f"{'='*60}", flush=True)
        print(
            f"[{Log._ts()}] 📊 SCAN FIN | {n_picks} picks | {n_leagues} ligas "
            f"| Heat={heat*100:.1f}% Vol={vol*100:.1f}%",
            flush=True
        )
        r = Log._session_stats["picks_rechazados"]
        g = Log._session_stats["picks_generados"]
        pct = g/(g+r)*100 if (g+r) > 0 else 0
        print(f"[{Log._ts()}]    Candidatos: {g+r} analizados → {g} picks ({pct:.0f}% pass rate)", flush=True)
        print(f"{'='*60}\n", flush=True)

    @staticmethod
    def audit_end(resolved, wins, losses):
        Log._session_stats["picks_resueltos"] += resolved
        if resolved:
            br = wins/resolved*100 if resolved else 0
            print(
                f"[{Log._ts()}] 🔬 AUDIT | {resolved} resueltos "
                f"| {wins}W/{losses}L | BR={br:.1f}%",
                flush=True
            )
        else:
            print(f"[{Log._ts()}] 🔬 AUDIT | 0 picks nuevos resueltos", flush=True)

    @staticmethod
    def clv_end(n, beat):
        Log._session_stats["clv_capturados"] += n
        if n:
            print(
                f"[{Log._ts()}] 📉 CLV | {n} capturados "
                f"| Beat closing: {beat:.0f}%",
                flush=True
            )
        else:
            print(f"[{Log._ts()}] 📉 CLV | 0 picks en ventana de captura", flush=True)

    @staticmethod
    def csv_ok(div, kb):
        Log._session_stats["csvs_descargados"] += 1
        print(f"[{Log._ts()}] 📥 CSV  | {div} ({kb}KB)", flush=True)

    @staticmethod
    def daily_summary():
        s = Log._session_stats
        print(f"\n{'='*60}", flush=True)
        print(f"[{Log._ts()}] 📋 RESUMEN DEL DÍA", flush=True)
        print(f"  CSVs descargados: {s['csvs_descargados']}", flush=True)
        print(f"  Picks generados:  {s['picks_generados']}", flush=True)
        print(f"  Picks resueltos:  {s['picks_resueltos']}", flush=True)
        print(f"  CLV capturados:   {s['clv_capturados']}", flush=True)
        print(f"  Errores/warnings: {s['errores']}", flush=True)
        print(f"{'='*60}\n", flush=True)

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
    "1X2":   0.030,   # 1X2: EV mín 3% — umbral cuota bajado a 2.00
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


# ── Cache en memoria para el calendario de copas ─────────────────────────────
_CUP_CAL_CACHE = {}  # {date_str: [matches]}
_CUP_CAL_TS    = {}  # {date_str: datetime}

# ── FBref xG real — URLs shooting stats por liga ─────────────────────────────
FBREF_SHOOTING_URLS = {
    "E0":  "https://fbref.com/en/comps/9/shooting/Premier-League-Stats",
    "E1":  "https://fbref.com/en/comps/10/shooting/Championship-Stats",
    "E2":  "https://fbref.com/en/comps/15/shooting/League-One-Stats",
    "D1":  "https://fbref.com/en/comps/20/shooting/Bundesliga-Stats",
    "D2":  "https://fbref.com/en/comps/33/shooting/2-Bundesliga-Stats",
    "I1":  "https://fbref.com/en/comps/11/shooting/Serie-A-Stats",
    "I2":  "https://fbref.com/en/comps/22/shooting/Serie-B-Stats",
    "SP1": "https://fbref.com/en/comps/12/shooting/La-Liga-Stats",
    "SP2": "https://fbref.com/en/comps/17/shooting/Segunda-Division-Stats",
    "F1":  "https://fbref.com/en/comps/13/shooting/Ligue-1-Stats",
    "F2":  "https://fbref.com/en/comps/60/shooting/Ligue-2-Stats",
    "N1":  "https://fbref.com/en/comps/23/shooting/Eredivisie-Stats",
    "P1":  "https://fbref.com/en/comps/32/shooting/Primeira-Liga-Stats",
    "B1":  "https://fbref.com/en/comps/37/shooting/Belgian-Pro-League-Stats",
    "SC0": "https://fbref.com/en/comps/40/shooting/Scottish-Premiership-Stats",
    "T1":  "https://fbref.com/en/comps/26/shooting/Super-Lig-Stats",
    "G1":  "https://fbref.com/en/comps/27/shooting/Super-League-1-Stats",
}
# También URLs para schedule (H2H histórico multi-temporada)
FBREF_SCHEDULE_URLS = {
    "E0":  ("https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
            "https://fbref.com/en/comps/9/2024-2025/schedule/2024-2025-Premier-League-Scores-and-Fixtures"),
    "D1":  ("https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
            "https://fbref.com/en/comps/20/2024-2025/schedule/2024-2025-Bundesliga-Scores-and-Fixtures"),
    "I1":  ("https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
            "https://fbref.com/en/comps/11/2024-2025/schedule/2024-2025-Serie-A-Scores-and-Fixtures"),
    "SP1": ("https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
            "https://fbref.com/en/comps/12/2024-2025/schedule/2024-2025-La-Liga-Scores-and-Fixtures"),
    "F1":  ("https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures",
            "https://fbref.com/en/comps/13/2024-2025/schedule/2024-2025-Ligue-1-Scores-and-Fixtures"),
}
FBREF_TTL_H = 48  # horas entre actualizaciones
FBREF_RATE_DELAY = 12  # segundos entre requests (máx 5/min respetando límite)

# ── Copas europeas y locales para cálculo de fatiga ─────────────────────
CUP_LEAGUES = {
    # Europeas
    2:   "UCL",       # Champions League
    3:   "UEL",       # Europa League
    848: "UECL",      # Conference League
    # Inglaterra
    45:  "FA_Cup",
    48:  "EFL_Cup",
    # España
    143: "Copa_Rey",
    # Alemania
    529: "DFB_Pokal",
    # Italia
    137: "Coppa_Italia",
    # Francia
    66:  "Coupe_France",
    # Portugal
    96:  "Taca_Portugal",
    # Bélgica
    144: "Belgian_Cup",
    # Países Bajos
    90:  "KNVB_Beker",
    # Turquía
    156: "Turkish_Cup",
    # Grecia
    528: "Greek_Cup",
    # Escocia
    322: "Scottish_Cup",
}

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
        label TEXT, marke