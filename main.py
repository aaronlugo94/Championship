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
ODDS_API_KEY     = os.getenv("ODDS_API_KEY", "")   # the-odds-api.com

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

# ── The Odds API — mapeo de ligas a sport keys ───────────────────────────────
ODDS_API_SPORT_MAP = {
    "E0": "soccer_epl", "E1": "soccer_efl_champ", "E2": "soccer_league_one",
    "SP1": "soccer_spain_la_liga", "SP2": "soccer_spain_segunda_division",
    "D1": "soccer_germany_bundesliga", "D2": "soccer_germany_bundesliga2",
    "I1": "soccer_italy_serie_a", "I2": "soccer_italy_serie_b",
    "F1": "soccer_france_ligue_one", "F2": "soccer_france_ligue_two",
    "N1": "soccer_netherlands_eredivisie", "P1": "soccer_portugal_primeira_liga",
    "B1": "soccer_belgium_first_div", "T1": "soccer_turkey_super_league",
    "G1": "soccer_greece_super_league", "SC0": "soccer_scotland_premiership",
    "CUP_2": "soccer_uefa_champs_league",
    "CUP_3": "soccer_uefa_europa_league",
    "CUP_848": "soccer_uefa_europa_conference_league",
    "MEX": "soccer_mexico_ligamx",
    "BSA": "soccer_brazil_campeonato",
}

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
# BACKTEST 13,615 picks × 17 ligas × 4 temporadas:
# DNB:   ROI +40.9% | 17/17 ligas ROI+ ✅ ÚNICO mercado robusto
# DC:    ROI -5.0%  |  3/17 ligas ROI+ ❌
# OVER:  ROI -7.6%  |  2/17 ligas ROI+ ❌
# UNDER: ROI -7.4%  |  3/17 ligas ROI+ ❌
# → Solo DNB activo. DC/OVER/UNDER en modo monitor (EV muy alto para no disparar)
# BACKTEST 13,615 picks × 17 ligas × 4 temporadas:
# DNB:   ROI +40.9% | 17/17 ligas ROI+ → ÚNICO mercado robusto
# DC:    ROI  -5.0% |  3/17 ligas ROI+ → negativo cross-liga
# OVER:  ROI  -7.6% |  2/17 ligas ROI+ → negativo cross-liga
# UNDER: ROI  -7.4% |  3/17 ligas ROI+ → negativo cross-liga
MIN_EV_MKT = {
    "UNDER":   0.999,  # ❌ DESACTIVADO
    "OVER":    0.999,  # ❌ DESACTIVADO
    "DC":      0.999,  # ❌ DESACTIVADO
    "1X2":     0.999,  # ❌ DESACTIVADO
    "BTTS":    0.999,  # ❌ DESACTIVADO
    "BTTS_NO": 0.999,  # ❌ DESACTIVADO
    "DNB":     0.020,  # ✅ ACTIVO — ROI +40.9%, 17/17 ligas
}
MAX_EV  = 0.15
KELLY   = 0.10  # Backtest: Kelly 10% = mejor Sharpe para DNB
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
        label TEXT, market TEXT, odd REAL, ev REAL, reason TEXT, logged_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS closing_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id TEXT, market TEXT, selection TEXT,
        odd_open REAL, odd_close REAL, clv_pct REAL, captured_at TEXT,
        odd_close_ps REAL,    -- Pinnacle cierre (benchmark sharp)
        clv_pct_ps REAL,      -- CLV vs Pinnacle cierre
        odd_close_maxc REAL,  -- MaxC cierre (mejor precio disponible)
        clv_pct_maxc REAL     -- CLV vs MaxC cierre
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS fbref_xg_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        div TEXT NOT NULL,
        team_normalized TEXT NOT NULL,
        team_original TEXT,
        mp INTEGER,
        xg_for REAL,
        xg_against REAL,
        npxg_for REAL,
        xg_for_home REAL,
        xg_against_home REAL,
        xg_for_away REAL,
        xg_against_away REAL,
        updated_at TEXT,
        season TEXT,
        UNIQUE(div, team_normalized, season)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_fbref ON fbref_xg_cache(div, team_normalized)")
    c.execute("""CREATE TABLE IF NOT EXISTS cup_fixtures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_normalized TEXT NOT NULL,
        competition TEXT NOT NULL,
        match_date TEXT NOT NULL,
        opponent TEXT,
        updated_at TEXT,
        UNIQUE(team_normalized, competition, match_date)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cup ON cup_fixtures(team_normalized, match_date)")
    # Tabla para partidos de copa próximos (calendario)
    c.execute("""CREATE TABLE IF NOT EXISTS cup_calendar (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id TEXT,
        league_id INTEGER,
        competition TEXT,
        match_date TEXT NOT NULL,
        home_team TEXT,
        away_team TEXT,
        status TEXT,
        odd_h REAL, odd_d REAL, odd_a REAL,
        updated_at TEXT,
        UNIQUE(league_id, match_date, home_team, away_team)
    )""")
    # Migración: agregar columnas nuevas si no existen (ALTER TABLE seguro)
    _migrations = [
        ("picks_log",    "ht_alerted",    "INTEGER DEFAULT 0"),
        ("picks_log",    "clv_captured",  "INTEGER DEFAULT 0"),
        ("closing_lines","odd_close_ps",  "REAL"),
        ("closing_lines","clv_pct_ps",    "REAL"),
        ("closing_lines","odd_close_maxc","REAL"),
        ("closing_lines","clv_pct_maxc",  "REAL"),
    ]
    for table, col, typedef in _migrations:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")
            conn.commit()
        except Exception:
            pass  # columna ya existe — ignorar

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
    Log.rej(label, market, odd, ev, reason)
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
                Log.csv_ok(div, len(r.content)//1024)
            else:
                Log.warn(f"{div}: respuesta no es CSV válido — conservando archivo anterior", "CSV")
        elif r.status_code != 200:
            Log.warn(f"{div}: HTTP {r.status_code} — conservando", "CSV")
        return path
    except Exception as e:
        Log.err(f"CSV {div}: {e}", "CSV")
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
        Log.info(f"fixtures.csv: {len(df)} partidos", "DATA")
        return df
    except Exception as e:
        Log.err(f"fixtures.csv: {e}", "DATA"); return None

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
    # AH: handicap asiático de la línea principal
    # Negativo = local favorito (ej -0.5, -1.0), positivo = visitante favorito
    def best_ah(*cols):
        for c in cols:
            try:
                v = row.get(c) if hasattr(row, 'get') else getattr(row, c, None)
                if v is not None and not (isinstance(v, float) and math.isnan(v)):
                    return float(v)
            except: pass
        return None

    return {
        "H":      best("PSH",    "B365H",    "AvgH",    "MaxH"),
        "D":      best("PSD",    "B365D",    "AvgD",    "MaxD"),
        "A":      best("PSA",    "B365A",    "AvgA",    "MaxA"),
        # O/U: prioridad Pinnacle cierre → MaxC (mejor precio cierre) → B365 → Avg
        # MaxC>2.5 = mejor precio disponible en el mercado de cierre
        # Evidencia CSV I1: CLV O/U promedio +3.49%, EV+3.6% con MaxC>B365 3%+
        "O25":    best("PC>2.5", "P>2.5", "MaxC>2.5", "Max>2.5", "B365C>2.5", "B365>2.5", "AvgC>2.5", "Avg>2.5"),
        "U25":    best("PC<2.5", "P<2.5", "MaxC<2.5", "Max<2.5", "B365C<2.5", "B365<2.5", "AvgC<2.5", "Avg<2.5"),
        # También guardar apertura B365 para CLV
        "O25_open": best("B365>2.5", "Avg>2.5"),
        "U25_open": best("B365<2.5", "Avg<2.5"),
        # MaxC para calcular EV real vs mejor precio del mercado
        "MaxCO25": best("MaxC>2.5", "Max>2.5"),
        "MaxCU25": best("MaxC<2.5", "Max<2.5"),
        "BTTS_Y": best("BbAvBBTS","B365BTTSY"),
        # Asian Handicap — línea de la casa (negativo = local favorito)
        "AH":     best_ah("AHh", "AHCh", "BbAHh"),
        # Cuota máxima para usar en EV real (mejor precio disponible)
        "MaxH":   best("MaxH"),
        "MaxD":   best("MaxD"),
        "MaxA":   best("MaxA"),
        "MaxO25": best("Max>2.5"),
        "MaxU25": best("Max<2.5"),
        # Avg del mercado para calibración
        "AvgH":   best("AvgH"),
        "AvgO25": best("Avg>2.5"),
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

def get_team_stats(df, team_name, cfg, depth=10, perspective="all"):
    """
    Extrae xG del equipo desde el DataFrame.
    MEJORADO:
    - perspective: "home" | "away" | "all" — calcula xG según dónde juega
    - shot_quality: ajuste por ratio HST/HS (calidad de tiros, no solo cantidad)
    - momentum: últimos 3 partidos pesan más que la media histórica
    - conv_rate dinámico: conv_home para partidos local, conv_away para visitante
    """
    import pandas as pd
    teams = pd.concat([df["HomeTeam"], df["AwayTeam"]]).dropna().unique().tolist()
    match = difflib.get_close_matches(team_name, teams, n=1, cutoff=0.55)
    if not match: return None, None, [], [], "LOW"

    name   = match[0]
    played = df.dropna(subset=["FTHG","FTAG"]).copy()
    if "Date" in played.columns:
        played = played.sort_values("Date")

    # Filtrar por perspectiva home/away o todos
    if perspective == "home":
        rows = played[played["HomeTeam"]==name].tail(depth)
    elif perspective == "away":
        rows = played[played["AwayTeam"]==name].tail(depth)
    else:
        rows = played[(played["HomeTeam"]==name)|(played["AwayTeam"]==name)].tail(depth)

    if len(rows) < 2: return None, None, [], [], "LOW"

    gf_l, ga_l, sf_l, sa_l, sq_l = [], [], [], [], []
    for _, row in rows.iterrows():
        ih = (row["HomeTeam"] == name)
        gf_l.append(min(float(row["FTHG"] if ih else row["FTAG"]), 3.5))
        ga_l.append(min(float(row["FTAG"] if ih else row["FTHG"]), 3.5))
        if cfg["has_shots"]:
            try:
                hst = float(row.get("HST", float("nan")))
                ast_ = float(row.get("AST", float("nan")))
                hs   = float(row.get("HS",  float("nan")))
                as_  = float(row.get("AS",  float("nan")))
                if not any(math.isnan(x) for x in [hst, ast_]):
                    sf_val = hst if ih else ast_
                    sa_val = ast_ if ih else hst
                    sf_l.append(sf_val)
                    sa_l.append(sa_val)
                    # Shot quality: ratio tiros a puerta / total tiros
                    # Un HST/HS alto = mejor calidad de tiros → xG real mayor
                    if not any(math.isnan(x) for x in [hs, as_]):
                        hs_val = hs if ih else as_
                        sq = hst/max(hs_val, 1) if ih else ast_/max(as_ if ih else hs, 1)
                        sq_l.append(min(sq, 1.0))
            except: pass

    # ── Momentum: últimos 3 con peso extra ───────────────────────────────
    def _momentum(series, n_recent=3):
        """Si los últimos n_recent son mejores que el promedio → factor>1."""
        if len(series) < 4: return 1.0
        recent = sum(series[-n_recent:]) / n_recent
        hist   = sum(series[:-n_recent]) / max(len(series)-n_recent, 1)
        if hist < 0.1: return 1.0
        ratio = recent / hist
        return max(0.85, min(ratio, 1.20))

    mom_f = _momentum(gf_l)
    mom_a = _momentum(ga_l)

    # ── Forma blend ──────────────────────────────────────────────────────
    ff_f = _form(gf_l); ff_a = _form(ga_l)
    fp_f = _form_pts(gf_l, ga_l, n=5)
    fp_a = _form_pts(ga_l, gf_l, n=5)
    ff_f_final = (ff_f * 0.50 + fp_f * 0.35 + mom_f * 0.15)
    ff_a_final = (ff_a * 0.50 + fp_a * 0.35 + mom_a * 0.15)

    # ── Shot quality factor ───────────────────────────────────────────────
    # Liga promedio: ~35% de tiros van a puerta (HST/HS ≈ 0.35)
    # Si el equipo tiene SQ > 0.35 → mejor calidad → multiplicar xG
    sq_avg = sum(sq_l)/len(sq_l) if sq_l else 0.35
    sq_factor = max(0.85, min(sq_avg / 0.35, 1.25))

    # ── conv_rate según perspectiva ───────────────────────────────────────
    if perspective == "home":
        conv_f = cfg.get("conv_home", 0.30)
        conv_a = cfg.get("conv_away", 0.31)
    elif perspective == "away":
        conv_f = cfg.get("conv_away", 0.31)
        conv_a = cfg.get("conv_home", 0.30)
    else:
        conv_f = (cfg.get("conv_home",0.30) + cfg.get("conv_away",0.31)) / 2
        conv_a = conv_f

    if cfg["has_shots"] and sf_l:
        xgf = _wavg(sf_l) * conv_f * ff_f_final * sq_factor
        xga = _wavg(sa_l) * conv_a * ff_a_final
        src = f"shots+sq({sq_factor:.2f})"
    else:
        xgf = _wavg(gf_l) * ff_f_final
        xga = _wavg(ga_l) * ff_a_final
        src = "goals_proxy"

    conf = "HIGH" if len(gf_l)>=6 else "MED" if len(gf_l)>=3 else "LOW"
    
    # ── FBref xG override — si disponible reemplaza el proxy HST ─────────────
    # Solo override el xGF (ataque), mantener xGA del CSV (más granular)
    # porque FBref da xG a nivel de equipo, no partido a partido
    fbref_data = get_fbref_xg(team_name, cfg.get("_div", ""))
    if fbref_data and fbref_data.get("xg_for"):
        fb = fbref_data
        # Usar xG de FBref según perspectiva
        if perspective == "home" and fb.get("xg_for_home"):
            xgf_fbref = fb["xg_for_home"]
            xga_fbref = fb.get("xg_against_home")
        elif perspective == "away" and fb.get("xg_for_away"):
            xgf_fbref = fb["xg_for_away"]
            xga_fbref = fb.get("xg_against_away")
        else:
            xgf_fbref = fb["xg_for"]
            xga_fbref = fb.get("xg_against")
        
        # Blend: 70% FBref real + 30% proxy HST (para capturar forma reciente)
        # FBref es más preciso pero refleja toda la temporada
        # HST proxy captura los últimos 8 partidos con decay
        xgf_final = xgf_fbref * 0.70 + xgf * 0.30
        xga_final = (xga_fbref * 0.70 + xga * 0.30) if xga_fbref else xga
        
        Log.info(f"  {name}[{perspective}]: xGF={xgf_final:.2f} (fbref={xgf_fbref:.2f} proxy={xgf:.2f}) xGA={xga_final:.2f} {conf}", "xG")
        return xgf_final, xga_final, gf_l, ga_l, conf

    Log.info(f"  {name}[{perspective}]: xGF={xgf:.2f} xGA={xga:.2f} {conf} sq={sq_factor:.2f} mom={mom_f:.2f} [proxy]", "xG")
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

    # Clave diferenciada por perspectiva home/away
    kh = f"{div}:{home_name}:home"; ka = f"{div}:{away_name}:away"
    kh_all = f"{div}:{home_name}"; ka_all = f"{div}:{away_name}"

    # Intentar cache con perspectiva específica, fallback a cache general
    hf, ha, hc = from_cache(kh)
    if hf is None: hf, ha, hc = from_cache(kh_all)
    if hf is None and df is not None:
        # MEJORA: calcular xG del local COMO LOCAL (últimos partidos en casa)
        cfg["_div"] = div  # pasar div para lookup FBref
        hf, ha, hg, hga, hc = get_team_stats(df, home_name, cfg, perspective="home")
        if hf is None:
            hf, ha, hg, hga, hc = get_team_stats(df, home_name, cfg, perspective="all")
        if hf: to_cache(kh, home_name, hf, ha, hg if hg else [], hga if hga else [], hc)

    af, aa, ac = from_cache(ka)
    if af is None: af, aa, ac = from_cache(ka_all)
    if af is None and df is not None:
        # MEJORA: calcular xG del visitante COMO VISITANTE
        af, aa, ag, aga, ac = get_team_stats(df, away_name, cfg, perspective="away")
        if af is None:
            af, aa, ag, aga, ac = get_team_stats(df, away_name, cfg, perspective="all")
        if af: to_cache(ka, away_name, af, aa, ag if ag else [], aga if aga else [], ac)

    if hf is None or af is None:
        Log.warn(f"DEFAULT xG para {home_name} vs {away_name}", "xG")
        return 1.20, 1.20, 2.40, "LOW", "default"

    # xG del partido: ataque local vs defensa visitante / ataque visitante vs defensa local
    xh = (hf + aa) / 2   # ataque de home + debilidad defensiva de away
    xa = (af + ha) / 2   # ataque de away + debilidad defensiva de home
    xh *= (1 - min(inj_h*0.015, 0.08))
    xa *= (1 - min(inj_a*0.015, 0.08))

    # ── FATIGA: ajuste por días de descanso ──────────────────────────────
    # Evidencia CSV I1: ≤4 días → 2.33 goles/PJ, 5-7 → 2.46, >7 → 2.50
    # Factor escala xG según cuánto descansó cada equipo
    def _fatigue_factor(days_rest):
        if days_rest is None or days_rest <= 0: return 1.0
        if days_rest <= 3:   return 0.92   # ≤3 días: fatiga severa (-8%)
        if days_rest <= 4:   return 0.95   # 4 días: fatiga moderada (-5%)
        if days_rest <= 7:   return 1.00   # 5-7 días: normal
        return 1.03                         # >7 días: frescura (+3%)

    # Obtener días de descanso desde el cache de partidos
    # (se pasa como parámetro opcional desde run_daily_scan)
    if hasattr(build_xg, '_rest_h') and build_xg._rest_h is not None:
        ff_h = _fatigue_factor(build_xg._rest_h)
        ff_a = _fatigue_factor(build_xg._rest_a)
        if ff_h != 1.0 or ff_a != 1.0:
            Log.info(f"  Fatiga: H={ff_h:.2f} (rest={build_xg._rest_h}d) A={ff_a:.2f} (rest={build_xg._rest_a}d)", "xG")
        xh *= ff_h; xa *= ff_a

    lf  = cfg.get("league_factor", 1.0)
    xh  = max(0.60, min(xh*lf, 3.80))
    xa  = max(0.60, min(xa*lf, 3.80))
    conf = "HIGH" if (hc=="HIGH" and ac=="HIGH") else \
           "MED"  if (hc!="LOW" and ac!="LOW") else "LOW"
    src  = "csv_shots" if cfg["has_shots"] else "csv_goals_proxy"
    Log.info(f"  xG: H={xh:.2f} A={xa:.2f} T={xh+xa:.2f} {conf}", "xG")
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
            Log.warn(f"Trends {date_str} HTTP {r.status_code}", "DATA"); return {}
        result = {}
        for m in r.json().get("matches", []):
            h_id = m.get("homeTeam",{}).get("id")
            a_id = m.get("awayTeam",{}).get("id")
            if h_id and a_id: result[f"{h_id}_{a_id}"] = m
        _TREND_MEM[key] = result
        Log.info(f"Trends {date_str}: {len(result)} partidos", "DATA")
        return result
    except Exception as e:
        Log.err(f"Trends: {e}", "DATA"); return {}

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

# Flag global: si FBref da 403, desactivar para esta sesión
_FBREF_BLOCKED = False

def _fbref_request(url: str) -> str | None:
    """
    Request a FBref page respetando rate limit (<= 5 req/min).
    Si recibe 403, marca FBref como bloqueado para esta sesión.
    Railway/servidores cloud son bloqueados frecuentemente por FBref.
    """
    global _FBREF_BLOCKED
    if _FBREF_BLOCKED:
        return None  # ya sabemos que está bloqueado
    import time, random
    fbref_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "https://fbref.com/",
    }
    try:
        time.sleep(FBREF_RATE_DELAY + random.uniform(0, 3))
        r = requests.get(url, headers=fbref_headers, timeout=20)
        if r.status_code == 200:
            return r.text
        elif r.status_code == 403:
            _FBREF_BLOCKED = True
            Log.warn("FBref bloqueado (403) en este servidor — usando xG proxy. "
                     "FBref bloquea IPs de cloud/Railway.", "FBREF")
            return None
        elif r.status_code == 429:
            Log.warn("FBref rate limit — esperando 60s", "FBREF")
            time.sleep(60)
            return None
        else:
            Log.warn(f"FBref {url}: status={r.status_code}", "FBREF")
            return None
    except Exception as e:
        Log.warn(f"FBref request failed: {e}", "FBREF")
        return None


def _normalize_team_fbref(name: str) -> str:
    """Normaliza nombres de equipos de FBref para matching con co.uk."""
    import unicodedata
    if not name: return ""
    # Remover acentos
    n = ''.join(c for c in unicodedata.normalize('NFD', str(name))
                if unicodedata.category(c) != 'Mn')
    n = n.lower().strip()
    # Abreviaciones comunes FBref → co.uk
    replacements = {
        "manchester utd": "man united",
        "manchester city": "man city",
        "newcastle utd": "newcastle",
        "tottenham": "tottenham",
        "nott'ham forest": "nott'm forest",
        "nottingham forest": "nott'm forest",
        "sheffield utd": "sheffield united",
        "west bromwich albion": "west brom",
        "queens park rangers": "qpr",
        "wolverhampton wanderers": "wolves",
        "brighton & hove albion": "brighton",
        "huddersfield town": "huddersfield",
        "stoke city": "stoke",
        "swansea city": "swansea",
        "cardiff city": "cardiff",
        "bayer leverkusen": "leverkusen",
        "rb leipzig": "leipzig",
        "borussia dortmund": "dortmund",
        "borussia monchengladbach": "m'gladbach",
        "atletico madrid": "ath madrid",
        "athletic bilbao": "ath bilbao",
        "atletico bilbao": "ath bilbao",
        "paris saint-germain": "psg",
        "paris s-g": "psg",
        "internazionale": "inter",
        "inter milan": "inter",
        "hellas verona": "verona",
        "venezia": "venezia",
        "ac milan": "milan",
    }
    for fbref_name, cuk_name in replacements.items():
        if fbref_name in n:
            n = n.replace(fbref_name, cuk_name)
    return n.strip()


def fetch_fbref_xg(divs: list | None = None, force: bool = False) -> dict:
    """
    Descarga xG real de FBref para las ligas especificadas.
    
    Retorna dict: {div: {team_normalized: {xg_for, xg_against, mp, ...}}}
    
    Los datos se cachean en SQLite con TTL de 48h.
    Si FBref no está accesible, retorna {} silenciosamente (fallback a HST proxy).
    
    Rate limit: máx 5 req/min (12s delay entre ligas).
    """
    import difflib as _dl
    
    if divs is None:
        divs = list(FBREF_SHOOTING_URLS.keys())
    
    now = datetime.now(timezone.utc)
    results = {}
    conn = sqlite3.connect(DB_PATH)
    season = "2025-26"

    for div in divs:
        # Si FBref está bloqueado (403), salir inmediatamente
        if _FBREF_BLOCKED:
            Log.warn("FBref bloqueado — saltando todas las ligas", "FBREF")
            break

        url = FBREF_SHOOTING_URLS.get(div)
        if not url:
            continue

        # Verificar TTL del cache
        if not force:
            cached = conn.execute("""
                SELECT team_normalized, mp, xg_for, xg_against, npxg_for,
                       xg_for_home, xg_against_home, xg_for_away, xg_against_away,
                       updated_at
                FROM fbref_xg_cache
                WHERE div=? AND season=?
                ORDER BY updated_at DESC LIMIT 100
            """, (div, season)).fetchall()
            
            if cached:
                # Verificar si el más reciente tiene TTL válido
                last_updated = cached[0][9] if cached else None
                if last_updated:
                    try:
                        age_h = (now - datetime.fromisoformat(
                            last_updated.replace("+00:00","")
                        ).replace(tzinfo=timezone.utc)).total_seconds() / 3600
                        if age_h < FBREF_TTL_H:
                            # Cache válido
                            results[div] = {
                                r[0]: {
                                    "mp": r[1], "xg_for": r[2], "xg_against": r[3],
                                    "npxg_for": r[4],
                                    "xg_for_home": r[5], "xg_against_home": r[6],
                                    "xg_for_away": r[7], "xg_against_away": r[8],
                                }
                                for r in cached
                            }
                            Log.info(f"FBref {div}: cache válido ({age_h:.0f}h)", "FBREF")
                            continue
                    except:
                        pass

        # Descargar de FBref
        Log.info(f"FBref {div}: descargando shooting stats...", "FBREF")
        html = _fbref_request(url)
        if not html:
            Log.warn(f"FBref {div}: no disponible, usando xG proxy", "FBREF")
            continue

        try:
            import pandas as pd
            # FBref esconde tablas en comentarios HTML — descomentarlas
            html_clean = html.replace('<!--', '').replace('-->', '')
            tables = pd.read_html(html_clean)
            
            if not tables:
                Log.warn(f"FBref {div}: sin tablas", "FBREF")
                continue

            # Buscar la tabla de shooting por equipo (tiene columna 'Squad')
            shoot_df = None
            for t in tables:
                cols = [str(c).lower() for c in t.columns]
                # Buscar tabla con Squad, Gls, xG
                if any('squad' in c for c in cols) and any('xg' in c for c in cols):
                    # Aplanar MultiIndex si existe
                    if hasattr(t.columns, 'levels'):
                        t.columns = [' '.join(str(c) for c in col).strip() 
                                      if isinstance(col, tuple) else str(col)
                                      for col in t.columns]
                    shoot_df = t
                    break

            if shoot_df is None:
                Log.warn(f"FBref {div}: tabla shooting no encontrada", "FBREF")
                continue

            # Limpiar columnas
            shoot_df.columns = [str(c).strip().lower().replace(' ', '_') 
                                  for c in shoot_df.columns]
            
            # Quitar filas de separación/totales
            if 'squad' in shoot_df.columns:
                shoot_df = shoot_df[
                    shoot_df['squad'].notna() & 
                    (shoot_df['squad'] != 'Squad') &
                    (~shoot_df['squad'].astype(str).str.contains('Unnamed|Total|Liga', na=True))
                ].copy()
            else:
                Log.warn(f"FBref {div}: columna 'squad' no encontrada en {list(shoot_df.columns[:8])}", "FBREF")
                continue

            def safe_float(val):
                try:
                    f = float(val)
                    return f if not (f != f) else None  # NaN check
                except:
                    return None

            div_results = {}
            saved = 0
            
            for _, row in shoot_df.iterrows():
                squad = str(row.get('squad', '')).strip()
                if not squad or squad == 'nan':
                    continue
                
                team_norm = _normalize_team_fbref(squad)
                
                # Extraer columnas — FBref puede tener nombres variables
                # Buscar xG (for y against) y MP
                mp = safe_float(row.get('mp') or row.get('90s'))
                
                # xG for (goals expected, goles esperados del equipo)
                xg_for = safe_float(
                    row.get('xg') or row.get('xg_expected') or 
                    row.get('xg_for') or row.get('expected_xg')
                )
                # xGA (goals expected against)
                xg_ag = safe_float(
                    row.get('xga') or row.get('xg_against') or 
                    row.get('xgagainst') or row.get('xg_allowed')
                )
                # npxG (sin penales)
                npxg = safe_float(
                    row.get('npxg') or row.get('np:xg') or row.get('npxg_expected')
                )
                
                if xg_for is None:
                    continue  # sin xG no sirve
                
                # Normalizar por partido si tenemos MP
                if mp and mp > 0:
                    xg_for_pp = round(xg_for / mp, 3)
                    xg_ag_pp = round(xg_ag / mp, 3) if xg_ag else None
                    npxg_pp = round(npxg / mp, 3) if npxg else xg_for_pp
                else:
                    xg_for_pp = xg_for
                    xg_ag_pp = xg_ag
                    npxg_pp = npxg

                data = {
                    "mp": int(mp) if mp else None,
                    "xg_for": xg_for_pp,
                    "xg_against": xg_ag_pp,
                    "npxg_for": npxg_pp,
                    # Home/away por separado se obtendrán de otra tabla
                    # Por ahora usar overall con factor home advantage
                    "xg_for_home": round(xg_for_pp * 1.10, 3) if xg_for_pp else None,
                    "xg_against_home": round(xg_ag_pp * 0.92, 3) if xg_ag_pp else None,
                    "xg_for_away": round(xg_for_pp * 0.91, 3) if xg_for_pp else None,
                    "xg_against_away": round(xg_ag_pp * 1.08, 3) if xg_ag_pp else None,
                }
                
                div_results[team_norm] = data
                
                # Guardar en cache
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO fbref_xg_cache
                        (div, team_normalized, team_original, mp,
                         xg_for, xg_against, npxg_for,
                         xg_for_home, xg_against_home,
                         xg_for_away, xg_against_away,
                         updated_at, season)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (div, team_norm, squad, data["mp"],
                          data["xg_for"], data["xg_against"], data["npxg_for"],
                          data["xg_for_home"], data["xg_against_home"],
                          data["xg_for_away"], data["xg_against_away"],
                          now.isoformat(), season))
                    saved += 1
                except Exception as e:
                    Log.warn(f"FBref save {squad}: {e}", "FBREF")

            conn.commit()
            results[div] = div_results
            Log.ok(f"FBref {div}: {len(div_results)} equipos, {saved} guardados en cache", "FBREF")

        except Exception as e:
            import traceback
            Log.err(f"FBref parse {div}: {e}\n{traceback.format_exc()[:300]}", "FBREF")
            continue

    conn.close()
    return results


def get_fbref_xg(team_name: str, div: str) -> dict | None:
    """
    Obtiene xG real de FBref para un equipo desde el cache SQLite.
    Usa fuzzy matching para nombres de equipos.
    Retorna None si no hay datos (fallback a xG proxy).
    """
    import difflib as _dl
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("""
            SELECT team_normalized, team_original, mp, xg_for, xg_against, 
                   npxg_for, xg_for_home, xg_against_home, xg_for_away, xg_against_away
            FROM fbref_xg_cache
            WHERE div=? AND season='2025-26'
        """, (div,)).fetchall()
        conn.close()
        
        if not rows:
            return None
        
        all_teams = [r[0] for r in rows]
        team_norm = _normalize_team_fbref(team_name)
        
        # Buscar exacto primero
        for r in rows:
            if r[0] == team_norm:
                return {
                    "xg_for": r[3], "xg_against": r[4],
                    "npxg_for": r[5],
                    "xg_for_home": r[6], "xg_against_home": r[7],
                    "xg_for_away": r[8], "xg_against_away": r[9],
                    "mp": r[2], "source": "fbref"
                }
        
        # Fuzzy match
        matches = _dl.get_close_matches(team_norm, all_teams, n=1, cutoff=0.60)
        if matches:
            for r in rows:
                if r[0] == matches[0]:
                    return {
                        "xg_for": r[3], "xg_against": r[4],
                        "npxg_for": r[5],
                        "xg_for_home": r[6], "xg_against_home": r[7],
                        "xg_for_away": r[8], "xg_against_away": r[9],
                        "mp": r[2], "source": "fbref_fuzzy"
                    }
        return None
    except Exception as e:
        Log.warn(f"get_fbref_xg {team_name}: {e}", "FBREF")
        return None



def fetch_live_odds(divs=None):
    """
    Obtiene cuotas en tiempo real de Pinnacle via The Odds API.
    Guarda en SQLite tabla live_odds para consulta del bot y dashboard.
    Costo: 1 request por liga. Con 500/mes gratis = 17 ligas/día × 29 días.
    
    The Odds API: https://the-odds-api.com
    Env: ODDS_API_KEY (Railway variable)
    """
    if not ODDS_API_KEY:
        return {}
    
    now = datetime.now(timezone.utc)
    # ODDS_DAILY: ligas con más picks, corridas diarias (17 req/día = 510/mes)  
    # Usar subset para scheduler diario y lista completa solo al arrancar
    ODDS_DAILY = ["E0","E1","SP1","D1","I1","F1","N1","P1","B1","T1",
                  "MEX","BSA","CUP_2","CUP_3","CUP_848"]  # 15 req/día = 450/mes ✅
    divs = divs or ODDS_DAILY
    results = {}  # {(home, away, div): {h: odd, d: odd, a: odd, ou: {over: odd, under: odd}}}
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS live_odds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        div TEXT, sport_key TEXT, home_team TEXT, away_team TEXT,
        commence_time TEXT,
        pin_h REAL, pin_d REAL, pin_a REAL,
        pin_over REAL, pin_under REAL,
        b365_h REAL, b365_d REAL, b365_a REAL,
        updated_at TEXT,
        UNIQUE(sport_key, home_team, away_team, commence_time)
    )""")
    # Historial de cuotas Pinnacle — guardamos cada snapshot
    # Permite detectar movimiento de línea (sharp money signal)
    conn.execute("""CREATE TABLE IF NOT EXISTS odds_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        div TEXT, sport_key TEXT,
        home_team TEXT, away_team TEXT,
        commence_time TEXT,
        pin_h REAL, pin_d REAL, pin_a REAL,
        pin_dnb_h REAL, pin_dnb_a REAL,
        snapshot_at TEXT
    )""")
    conn.commit()

    for div, sport_key in ODDS_API_SPORT_MAP.items():
        if div not in divs: continue
        try:
            r = requests.get(
                f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/",
                params={
                    "apiKey": ODDS_API_KEY,
                    "regions": "eu",
                    "markets": "h2h,totals",
                    "bookmakers": "pinnacle,betfair_ex_eu,bet365",
                    "oddsFormat": "decimal",
                },
                timeout=15
            )
            remaining = r.headers.get("x-requests-remaining", "?")
            if r.status_code != 200:
                Log.warn(f"Odds API {div}: {r.status_code}", "ODDS")
                continue
            
            games = r.json()
            for game in games:
                home = game.get("home_team", "")
                away = game.get("away_team", "")
                commence = game.get("commence_time", "")[:19]
                
                pin_h = pin_d = pin_a = pin_over = pin_under = None
                b365_h = b365_d = b365_a = None
                
                for bk in game.get("bookmakers", []):
                    bk_key = bk.get("key", "")
                    for mkt in bk.get("markets", []):
                        if mkt["key"] == "h2h":
                            odds_map = {o["name"]: o["price"] for o in mkt.get("outcomes", [])}
                            if bk_key == "pinnacle":
                                pin_h = odds_map.get(home)
                                pin_d = odds_map.get("Draw")
                                pin_a = odds_map.get(away)
                            elif bk_key == "bet365":
                                b365_h = odds_map.get(home)
                                b365_d = odds_map.get("Draw")
                                b365_a = odds_map.get(away)
                        elif mkt["key"] == "totals" and bk_key == "pinnacle":
                            for o in mkt.get("outcomes", []):
                                if o["name"] == "Over": pin_over = o["price"]
                                elif o["name"] == "Under": pin_under = o["price"]
                
                conn.execute("""
                    INSERT OR REPLACE INTO live_odds
                    (div, sport_key, home_team, away_team, commence_time,
                     pin_h, pin_d, pin_a, pin_over, pin_under,
                     b365_h, b365_d, b365_a, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (div, sport_key, home, away, commence,
                      pin_h, pin_d, pin_a, pin_over, pin_under,
                      b365_h, b365_d, b365_a, now.isoformat()))
                # Snapshot histórico de Pinnacle (para line monitoring)
                if pin_h and pin_d and pin_a:
                    try:
                        _dh = round(1/(1/pin_h - 1/pin_d), 3) if (1/pin_h - 1/pin_d) > 0.05 else None
                        _da = round(1/(1/pin_a - 1/pin_d), 3) if (1/pin_a - 1/pin_d) > 0.05 else None
                    except:
                        _dh = _da = None
                    conn.execute("""
                        INSERT INTO odds_history
                        (div, sport_key, home_team, away_team, commence_time,
                         pin_h, pin_d, pin_a, pin_dnb_h, pin_dnb_a, snapshot_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (div, sport_key, home, away, commence,
                          pin_h, pin_d, pin_a, _dh, _da, now.isoformat()))
                
                results[(home, away, div)] = {
                    "pin_h": pin_h, "pin_d": pin_d, "pin_a": pin_a,
                    "pin_over": pin_over, "pin_under": pin_under,
                }
            Log.ok(f"Odds API {div}: {len(games)} partidos (remaining={remaining})", "ODDS")
            # Guardar partidos de copa en cup_calendar para persistencia
            if div.startswith("CUP_"):
                try:
                    league_id_save = int(div.replace("CUP_",""))
                    comp_name_save = {2:"🏆 UCL", 3:"🥈 UEL", 848:"🥉 UECL"}.get(league_id_save, div)
                    conn_cc = sqlite3.connect(DB_PATH)
                    for game in games:
                        fix_date = game.get("commence_time","")[:10]
                        if not fix_date: continue
                        conn_cc.execute("""
                            INSERT OR IGNORE INTO cup_calendar
                            (fixture_id, league_id, competition, match_date,
                             home_team, away_team, status, updated_at)
                            VALUES (?,?,?,?,?,?,?,?)
                        """, (game.get("id",""), league_id_save, comp_name_save, fix_date,
                              game.get("home_team",""), game.get("away_team",""),
                              "SCHEDULED", datetime.now(timezone.utc).isoformat()))
                    conn_cc.commit(); conn_cc.close()
                except Exception: pass
        except Exception as e:
            Log.warn(f"Odds API {div}: {e}", "ODDS")
    
    conn.commit(); conn.close()
    return results


def fetch_cup_calendar(headers):
    """
    Descarga partidos próximos de copas europeas usando football-data.org (free).
    UCL/UEL via /competitions/CL|EL/matches — sin límite diario, 10 req/min.
    Copas nacionales via api-football como fallback.
    Se corre al arrancar y cada mañana a las 05:45 UTC.
    """
    now = datetime.now(timezone.utc)
    date_to = (now + timedelta(days=8)).strftime("%Y-%m-%d")
    date_from = now.strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    total = 0

    # ── FUENTE A: football-data.org (UCL + UEL + ligas principales) ──
    # Free tier incluye CL (UCL) y EL (UEL) sin restricciones
    FD_CUPS = {
        "CL":  (2,   "🏆 UCL"),
        "EL":  (3,   "🥈 UEL"),
        "EC":  (848, "🥉 UECL"),  # solo en torneos grandes
    }
    if FD_ORG_TOKEN:
        for fd_code, (league_id, comp_name) in FD_CUPS.items():
            try:
                r = requests.get(
                    f"https://api.football-data.org/v4/competitions/{fd_code}/matches",
                    headers={"X-Auth-Token": FD_ORG_TOKEN},
                    params={"dateFrom": date_from, "dateTo": date_to,
                            "status": "SCHEDULED,LIVE,IN_PLAY,PAUSED,FINISHED"},
                    timeout=12
                )
                if r.status_code != 200:
                    Log.warn(f"fd.org {fd_code}: {r.status_code}", "CAL")
                    continue
                data = r.json()
                matches_fd = data.get("matches", [])
                for m in matches_fd:
                    try:
                        utc_date = m["utcDate"][:10]
                        fix_id   = str(m["id"])
                        status   = m["status"]
                        home     = m["homeTeam"]["name"] or m["homeTeam"].get("shortName","")
                        away     = m["awayTeam"]["name"] or m["awayTeam"].get("shortName","")
                        conn.execute("""
                            INSERT OR REPLACE INTO cup_calendar
                            (fixture_id, league_id, competition, match_date,
                             home_team, away_team, status, odd_h, odd_d, odd_a, updated_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)
                        """, (fix_id, league_id, comp_name, utc_date,
                              home, away, status, None, None, None, now.isoformat()))
                        total += 1
                    except Exception:
                        continue
                Log.ok(f"cup_calendar fd.org {fd_code}: {len(matches_fd)} partidos", "CAL")
                time.sleep(0.5)  # respetar 10 req/min
            except Exception as e:
                Log.warn(f"cup_calendar fd.org {fd_code}: {e}", "CAL")

    # ── FUENTE B: api-football para copas nacionales ──────────────────
    CUP_APIF = {
        45:  "🏴󠁧󠁢󠁥󠁮󠁧󠁿 FA Cup",
        143: "🇪🇸 Copa Rey",
        137: "🇮🇹 Coppa Italia",
        529: "🇩🇪 DFB Pokal",
        66:  "🇫🇷 Coupe France",
    }
    for league_id, comp_name in CUP_APIF.items():
        try:
            res = apif_get("fixtures", {
                "league": league_id, "season": 2025,
                "from": date_from, "to": date_to,
            }, headers)
            for fix in (res or []):
                try:
                    fix_date = fix["fixture"]["date"][:10]
                    fix_id   = str(fix["fixture"]["id"])
                    status   = fix["fixture"]["status"]["short"]
                    home     = fix["teams"]["home"]["name"]
                    away     = fix["teams"]["away"]["name"]
                    conn.execute("""
                        INSERT OR REPLACE INTO cup_calendar
                        (fixture_id, league_id, competition, match_date,
                         home_team, away_team, status, odd_h, odd_d, odd_a, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (fix_id, league_id, comp_name, fix_date,
                          home, away, status, None, None, None, now.isoformat()))
                    total += 1
                except Exception:
                    continue
            Log.ok(f"cup_calendar apif {comp_name}: {len(res or [])} partidos", "CAL")
        except Exception as e:
            Log.warn(f"cup_calendar apif {comp_name}: {e}", "CAL")

    conn.commit(); conn.close()
    Log.ok(f"cup_calendar: {total} partidos próximos cacheados ({date_from}→{date_to})", "CAL")
    return total



def detect_line_moves(div=None, min_move_pct=0.04, hours_window=24):
    """
    Detecta movimientos de línea Pinnacle en las últimas N horas.
    Movimiento > 4% = señal de dinero sharp.

    Retorna dict {(home,away,div): {move_h, move_d, signal, confidence, ...}}

    Señales:
      sharp_home  → cuota local bajó  → sharp money en local
      sharp_away  → cuota visitante bajó
      sharp_draw  → cuota empate bajó
      steam       → movimiento brusco en ambos lados (línea bajo presión)
      none        → mercado estable
    """
    try:
        from datetime import timedelta
        conn = sqlite3.connect(DB_PATH)
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_window)).isoformat()

        query = """
            SELECT home_team, away_team, div,
                   MIN(pin_h)     as open_h,  MAX(pin_h)     as close_h,
                   MIN(pin_d)     as open_d,  MAX(pin_d)     as close_d,
                   MIN(pin_a)     as open_a,  MAX(pin_a)     as close_a,
                   MIN(pin_dnb_h) as open_dh, MAX(pin_dnb_h) as close_dh,
                   MIN(pin_dnb_a) as open_da, MAX(pin_dnb_a) as close_da,
                   COUNT(*)       as snaps,
                   MIN(snapshot_at) as first_t, MAX(snapshot_at) as last_t
            FROM odds_history
            WHERE snapshot_at >= ?
        """
        params = [cutoff]
        if div:
            query += " AND div=?"
            params.append(div)
        query += " GROUP BY home_team, away_team, div HAVING snaps >= 2"

        rows = conn.execute(query, params).fetchall()
        conn.close()

        moves = {}
        for (home, away, d,
             oh, ch, od, cd, oa, ca,
             odh, cdh, oda, cda,
             snaps, first, last) in rows:

            if not oh or not ch or oh <= 1.01: continue

            # Cambio porcentual (negativo = cuota bajó = equipo más valorado)
            def pct(old, new):
                if not old or not new or old <= 1.0: return 0.0
                return round((new - old) / old * 100, 2)

            mh = pct(oh, ch)  # local
            md = pct(od, cd)  # empate
            ma = pct(oa, ca)  # visitante
            mdh = pct(odh, cdh)  # DNB local
            mda = pct(oda, cda)  # DNB visitante

            # Señal
            thr = min_move_pct * 100
            if   mh < -thr: signal = "sharp_home";  conf = min(abs(mh)/10, 1.0)
            elif ma < -thr: signal = "sharp_away";  conf = min(abs(ma)/10, 1.0)
            elif md < -thr: signal = "sharp_draw";  conf = min(abs(md)/10, 1.0)
            elif abs(mh) > 8 and abs(ma) > 8:
                             signal = "steam";       conf = 0.5
            else:            signal = "none";        conf = 0.0

            hours_span = 0.0
            try:
                t1 = datetime.fromisoformat(first.replace("Z","+00:00"))
                t2 = datetime.fromisoformat(last.replace("Z","+00:00"))
                hours_span = round((t2-t1).total_seconds()/3600, 1)
            except: pass

            moves[(home, away, d)] = {
                "move_h": mh, "move_d": md, "move_a": ma,
                "move_dnb_h": mdh, "move_dnb_a": mda,
                "signal": signal, "confidence": round(conf,2),
                "open_h": oh,  "close_h": ch,
                "open_d": od,  "close_d": cd,
                "open_a": oa,  "close_a": ca,
                "snaps": snaps, "hours_span": hours_span,
            }
        return moves
    except Exception as e:
        Log.warn(f"detect_line_moves: {e}", "LINES")
        return {}


def fetch_cup_fixtures(headers, full_season=False):
    """
    Descarga partidos de copas europeas y locales usando api-football.
    
    full_season=True  → temporada completa (agosto 2025 → hoy). 
                        Se corre UNA VEZ al arrancar si la DB está vacía.
                        Costo: 14 requests (una por copa).
    full_season=False → últimos 10 días solamente.
                        Se corre cada lunes a las 05:00 UTC.
                        Costo: 14 requests.
    
    Total plan free: 100 req/día. Esto usa ≤14 req por ejecución.
    """
    now = datetime.now(timezone.utc)

    if full_season:
        # Temporada completa: desde inicio agosto 2025 hasta hoy
        date_from = "2025-07-01"
        date_to   = now.strftime("%Y-%m-%d")
        Log.info("Descargando temporada COMPLETA de copas...", "CUPS")
    else:
        date_from = (now - timedelta(days=10)).strftime("%Y-%m-%d")
        date_to   = now.strftime("%Y-%m-%d")

    conn = sqlite3.connect(DB_PATH)
    total_saved = total_fixtures = 0

    def norm(n):
        """Normaliza nombre de equipo para matching fuzzy."""
        import unicodedata
        n = n.lower().strip()
        # Remover acentos
        n = ''.join(c for c in unicodedata.normalize('NFD', n)
                    if unicodedata.category(c) != 'Mn')
        # Abreviaciones comunes
        replacements = {
            "fc ": "", " fc": "", "afc ": "", " afc": "",
            "cf ": "", " cf": "", "ac ": "", " ac": "",
            "internazionale": "inter milan",
            "atletico": "atletico",
            "manchester united": "man united",
            "manchester city": "man city",
            "paris saint-germain": "psg",
            "paris saint germain": "psg",
        }
        for old, new_v in replacements.items():
            n = n.replace(old, new_v)
        return n.strip()

    for league_id, comp_name in CUP_LEAGUES.items():
        try:
            res = apif_get("fixtures", {
                "league":  league_id,
                "season":  2025,
                "from":    date_from,
                "to":      date_to,
                "status":  "FT-AET-PEN"   # solo partidos terminados
            }, headers)

            if not res:
                Log.warn(f"  {comp_name}: sin datos", "CUPS")
                continue

            copa_saved = 0
            for fix in res:
                try:
                    fix_date  = fix["fixture"]["date"][:10]
                    status    = fix["fixture"]["status"]["short"]
                    if status not in ("FT","AET","PEN"):
                        continue
                    home_name = fix["teams"]["home"]["name"]
                    away_name = fix["teams"]["away"]["name"]
                    total_fixtures += 1

                    for team, opp in [(home_name, away_name), (away_name, home_name)]:
                        try:
                            conn.execute("""
                                INSERT OR IGNORE INTO cup_fixtures
                                (team_normalized, competition, match_date, opponent, updated_at)
                                VALUES (?, ?, ?, ?, ?)
                            """, (norm(team), comp_name, fix_date,
                                  norm(opp), now.isoformat()))
                            copa_saved += 1
                        except Exception:
                            pass
                except Exception:
                    continue

            total_saved += copa_saved
            Log.ok(f"  {comp_name}: {len(res)} partidos → {copa_saved} equipos registrados", "CUPS")

        except Exception as e:
            Log.warn(f"Copa {comp_name} (id={league_id}): {e}", "CUPS")

    conn.commit()
    conn.close()

    mode = "temporada completa" if full_season else f"{date_from}→{date_to}"
    Log.ok(f"cup_fixtures [{mode}]: {total_fixtures} partidos, {total_saved} registros", "CUPS")

    # Enviar resumen por Telegram si es temporada completa
    if full_season and total_saved > 0:
        try:
            send_msg(
                f"⚽ <b>Cup fixtures cargados</b>\n"
                f"📅 Temporada 2025-26 completa\n"
                f"🏆 {len(CUP_LEAGUES)} copas • {total_fixtures} partidos\n"
                f"✅ {total_saved} registros en DB para cálculo de fatiga"
            )
        except Exception:
            pass

    return total_saved


def _cup_db_is_empty():
    """Verifica si la tabla cup_fixtures está vacía."""
    try:
        conn = sqlite3.connect(DB_PATH)
        n = conn.execute("SELECT COUNT(*) FROM cup_fixtures").fetchone()[0]
        conn.close()
        return n == 0
    except:
        return True


def get_true_rest_days(team_name: str, before_date, div: str, df_league=None) -> int | None:
    """
    Calcula los días reales de descanso de un equipo considerando:
    1. Partidos de liga (CSV co.uk)
    2. Partidos de copas europeas y locales (cup_fixtures SQLite)
    Retorna los días desde el último partido en CUALQUIER competición.
    """
    import difflib as _dl
    import pandas as pd  # necesario para pd.Timestamp, pd.concat, etc.
    candidates = []

    # ── Fuente 1: CSV de liga ─────────────────────────────────────────
    if df_league is not None:
        try:
            df = df_league.copy()
            if "Date" not in df.columns and "date" in df.columns:
                df = df.rename(columns={"date":"Date"})
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
                df = df.rename(columns={"Home":"HomeTeam","Away":"AwayTeam",
                                        "HG":"FTHG","AG":"FTAG"})
                # Fuzzy match del nombre
                all_teams = pd.concat([df.get("HomeTeam", pd.Series()),
                                       df.get("AwayTeam", pd.Series())]).dropna().unique()
                matches = _dl.get_close_matches(team_name, all_teams, n=1, cutoff=0.50)
                if matches:
                    nm = matches[0]
                    past = df[
                        ((df.get("HomeTeam")==nm) | (df.get("AwayTeam")==nm)) &
                        (df["Date"] < pd.Timestamp(before_date)) &
                        df.get("FTHG", pd.Series(dtype=float)).notna()
                    ].sort_values("Date")
                    if not past.empty:
                        candidates.append(past.iloc[-1]["Date"])
        except Exception as e:
            Log.warn(f"rest_days CSV {team_name}: {e}", "FAT")

    # ── Fuente 2: cup_fixtures (copas europeas + locales) ─────────────
    try:
        norm_name = team_name.lower().strip()
        before_str = pd.Timestamp(before_date).strftime("%Y-%m-%d")
        conn = sqlite3.connect(DB_PATH)

        # Buscar por nombre exacto normalizado
        rows = conn.execute("""
            SELECT match_date FROM cup_fixtures
            WHERE team_normalized = ? AND match_date < ?
            ORDER BY match_date DESC LIMIT 5
        """, (norm_name, before_str)).fetchall()

        if not rows:
            # Intentar fuzzy match con los equipos en la DB
            all_cup_teams = [r[0] for r in conn.execute(
                "SELECT DISTINCT team_normalized FROM cup_fixtures").fetchall()]
            fuzzy = _dl.get_close_matches(norm_name, all_cup_teams, n=3, cutoff=0.65)
            for fn in fuzzy:
                cup_rows = conn.execute("""
                    SELECT match_date FROM cup_fixtures
                    WHERE team_normalized = ? AND match_date < ?
                    ORDER BY match_date DESC LIMIT 3
                """, (fn, before_str)).fetchall()
                rows.extend(cup_rows)

        conn.close()

        for (match_date_str,) in rows:
            try:
                candidates.append(pd.Timestamp(match_date_str))
            except:
                pass
    except Exception as e:
        Log.warn(f"rest_days cups {team_name}: {e}", "FAT")

    # ── Calcular días desde el partido más reciente ───────────────────
    if not candidates:
        return None

    last_match = max(candidates)
    before_ts  = pd.Timestamp(before_date)
    days = (before_ts - last_match).days
    return max(0, days)


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
                    Log.ok(f"Liga MX season: {yr}", "MEX")
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

def adjust_strong_favorite(ph, pd_, pa):
    """
    Corrección para favoritos fuertes.
    Evidencia CSV: cuando MaxH implica >70%, el mercado sobreestima 5.1%.
    Cuando MaxA implica >70%, similar sesgo.
    Fix: shrinkage adicional proporcional a cuánto supera el 65%.
    """
    def shrink_fav(p, threshold=0.65, max_correction=0.05):
        if p <= threshold: return p
        excess = p - threshold          # cuánto supera el umbral
        correction = min(excess * 0.35, max_correction)  # máx 5%
        return p - correction

    ph_adj  = shrink_fav(ph)
    pa_adj  = shrink_fav(pa)

    # Si se ajustó alguno, redistribuir la diferencia al empate
    delta_h = ph - ph_adj
    delta_a = pa - pa_adj
    pd_adj  = min(pd_ + delta_h + delta_a, 0.45)  # cap empate a 45%

    # Renormalizar
    total = ph_adj + pd_adj + pa_adj
    if total > 0:
        ph_adj  = ph_adj  / total
        pd_adj  = pd_adj  / total
        pa_adj  = pa_adj  / total

    return ph_adj, pd_adj, pa_adj


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
    """
    NegBinom O/U con std calibrado empíricamente por liga.
    CORRECCIÓN POISSON: el modelo subestima partidos de 3+ goles.
    Evidencia CSV I1 2025-26: 3 goles Poisson=21.1% vs Real=24.3% (+3.2%)
                               4 goles Poisson=12.9% vs Real=15.0% (+2.1%)
    Fix: corrección +0.03 a P(Over2.5) cuando xG total > 2.0
    """
    mu=xg_total; var=max(std**2, mu)
    def nb(k):
        if mu<=0: return 0.0
        if var<=mu*1.01: return _pmf(mu,k)
        r=mu**2/(var-mu); p=r/(r+mu)
        try: return exp(lgamma(k+r)-lgamma(r)-lgamma(k+1)+r*log(p)+k*log(1-p))
        except: return 0.0
    pu = sum(nb(k) for k in range(int(np.floor(line))+1))
    po = 1 - pu
    # Corrección empírica: Poisson subestima colas altas de goles
    # Se aplica solo cuando xG sugiere partido con goles (xG > 2.0)
    # Factor decrece a 0 cuando xG < 1.5 (partidos defensivos, corrección no aplica)
    if line == 2.5 and mu > 1.5:
        correction = 0.03 * min((mu - 1.5) / 1.0, 1.0)  # rampa 0→0.03 entre xG 1.5-2.5
        po = min(po + correction, 0.95)
        pu = 1 - po
    return round(po,4), round(pu,4)

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

    # Corrección favoritos fuertes — evidencia CSV: P(home)>70% sobreestimado 5.1%
    ph, pd_, pa = adjust_strong_favorite(ph, pd_, pa)

    # 1X2: umbral bajado para acumular track record en rango betterFly-style (>2.0)
    # Evidencia externa: 60% WR con cuota 2.05 = +16% ROI (algobetting community)
    # Ligas menos líquidas (liq < 0.88): umbral 2.00
    # Ligas top líquidas: umbral 2.50 (mercado más eficiente)
    # BACKTEST: 1X2 sin filtro CLV tiene BR 22% — ilusorio
    # Requerir cuota mínima más alta para compensar eficiencia del mercado
    MIN_ODD_1X2 = 2.20 if liq < 0.88 else 2.80
    for prob, odd_val, pick in [(ph,oh,f"Gana {h_n}"),
                                 (pd_,od,"Empate"),
                                 (pa,oa,f"Gana {a_n}")]:
        if odd_val and odd_val >= MIN_ODD_1X2:
            out.append({"mkt":"1X2","pick":pick,"odd":odd_val,"prob":prob,
                        "model_gap":round(prob-1/(odd_val*1.05),4),
                        "odd_range":"2.0-2.5" if odd_val < 2.5 else "2.5+"})

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
                # BACKTEST Serie A: DC 1.25-1.40 → ROI +0.9%, p=0.0001
                # DC 1.40-1.52 → ROI -18% → eliminar ese rango
                if dc_odd and 1.01 < dc_odd < 1.40:
                    # FILTRO AH: validar con Asian Handicap del mercado
                    # Evidencia CSV: AH=0 (neutro) → local gana solo 22.5%
                    # DC home (1X) tiene sentido solo si AH ≤ -0.5 (mercado confirma favoritismo)
                    ah_val = odds.get("AH")
                    ah_ok = True
                    if ah_val is not None:
                        if "o Empate" in dc_pick:  # DC home: 1X
                            # Requiere que mercado diga local es favorito (AH ≤ 0)
                            if ah_val > 0.25:
                                ah_ok = False
                                Log.rej(f"DC home rechazado: AH={ah_val:+.2f} (mercado no confirma favoritismo)", "AH")
                        elif dc_pick.startswith("DC: Empate o"):  # DC away: X2
                            # Requiere que mercado diga visitante es favorito (AH ≥ 0)
                            if ah_val < -0.25:
                                ah_ok = False
                                Log.rej(f"DC away rechazado: AH={ah_val:+.2f} (mercado no confirma favoritismo visit.)", "AH")
                    if ah_ok:
                        out.append({"mkt":"DC","pick":dc_pick,"odd":round(dc_odd,2),
                                    "prob":dc_prob,"ah":ah_val,
                                    "model_gap":round(dc_prob-1/(dc_odd*vig),4)})

    # ── DRAW NO BET ──────────────────────────────────────────────────────
    # DNB: si empata devuelven. EV real = ph/(ph+pa) para local,  pa/(ph+pa) para visitante.
    # Cuota fair derivada de las probabilidades Dixon-Coles sin empate.
    # Solo en ligas con liq < 0.88 (mercado menos eficiente) y conf HIGH.
    if conf != "LOW" and oh and oa:  # DNB en todas las ligas, no solo baja liquidez
        ph_dnb = ph / max(ph + pa, 0.01)   # prob local sin empate
        pa_dnb = pa / max(ph + pa, 0.01)   # prob visitante sin empate
        # Cuota fair DNB desde las cuotas 1X2 del mercado (sin vig)
        # DNB_H fair = 1 / (1/oh - 1/od) cuando od es la cuota empate
        try:
            dnb_h_odd = round(1 / (1/oh - 1/od), 2) if oh and od and (1/oh - 1/od) > 0.05 else None
            dnb_a_odd = round(1 / (1/oa - 1/od), 2) if oa and od and (1/oa - 1/od) > 0.05 else None
        except (ZeroDivisionError, ValueError):
            dnb_h_odd = dnb_a_odd = None
        # DNB solo con cuotas 1.40-3.50 y favorito claro (prob > 55%)
        # Fuera de ese rango el edge del vig doble es menos pronunciado
        if dnb_h_odd and 1.40 < dnb_h_odd < 3.50 and ph_dnb > 0.55:
            out.append({"mkt":"DNB","pick":f"DNB: Gana {h_n}",
                        "odd":dnb_h_odd,"prob":ph_dnb,
                        "model_gap":round(ph_dnb - 1/(dnb_h_odd*1.05), 4)})
        if dnb_a_odd and 1.40 < dnb_a_odd < 3.50 and pa_dnb > 0.55:
            out.append({"mkt":"DNB","pick":f"DNB: Gana {a_n}",
                        "odd":dnb_a_odd,"prob":pa_dnb,
                        "model_gap":round(pa_dnb - 1/(dnb_a_odd*1.05), 4)})

    # ── O/U 2.5 ──────────────────────────────────────────────────────────
    has_trend = ts.get("pct_o25") is not None
    po_raw, _ = negbinom_ou(xh+xa, std)
    po = shrink(blend(po_raw, ts.get("pct_o25")))
    pu = 1 - po

    if not has_trend:
        pu = shrink(pu, a=0.45)
        po = 1 - pu
    else:
        pu = shrink(pu, a=0.60)
        po = 1 - pu

    # ── MEJORA 2: AH ajuste O/U — CALIBRADO CON DATOS REALES ──────────────
    # Datos CSV I1 2025-26 (280 partidos, calibración empírica):
    #   AH ≤ -1.25: Over 59.4% — local muy fav JUEGA ABIERTO → +5% Over
    #   AH [-1.0,-0.5): Over 47.6% — neutro → 0%
    #   AH [-0.5,0.0): Over 48.1% — partido competido → +3% Over
    #   AH [0.0,+0.5): Over 44.8% — neutro-bajo → 0%
    #   AH ≥ +0.5: Over 40.4% — visitante fav DEFIENDE → -8% Over
    #
    # NOTA: AH muy negativo → LOCAL MUY FAVORITO → juega abierto sin defender
    # Contra-intuitivo pero confirmado por datos reales
    ah_val = odds.get("AH")
    ah_ou_adjustment = 0.0
    ah_label = ""
    if ah_val is not None:
        if ah_val <= -1.25:
            # Local muy favorito (ej. -1.5, -2.0)
            # Equipo dominante juega abierto, rival también arriesga → más goles
            ah_ou_adjustment = +0.05
            ah_label = f"AH{ah_val:+.1f}→+5%Over(local dominante)"
        elif ah_val <= -0.75:
            # Local favorito moderado (-1.0) → neutro
            ah_ou_adjustment = 0.0
            ah_label = ""
        elif ah_val < 0.0:
            # Partido levemente favorable al local (-0.5) → competido → +3%
            ah_ou_adjustment = +0.03
            ah_label = f"AH{ah_val:+.1f}→+3%Over(partido disputado)"
        elif ah_val < 0.5:
            # Neutro / visita leve favorita → sin ajuste
            ah_ou_adjustment = 0.0
            ah_label = ""
        else:
            # Visitante favorito (≥ +0.5) → equipo visitante defiende → -8%
            ah_ou_adjustment = -0.08
            ah_label = f"AH{ah_val:+.1f}→-8%Over(visit defiende)"

    if ah_ou_adjustment != 0.0:
        po = max(0.10, min(po + ah_ou_adjustment, 0.92))
        pu = 1 - po
        if ah_label:
            Log.info(f"  O/U AH ajuste: {ah_label} po={po:.3f}", "AH")

    # Cuota Over: CSV primero, Trend como fallback
    o25 = odds.get("O25") or ts.get("ou25_odd")
    u25 = odds.get("U25")

    # EV real: usar MaxC si disponible (mejor precio de cierre del mercado)
    o25_ev = odds.get("MaxCO25") or o25  # para calcular EV
    u25_ev = odds.get("MaxCU25") or u25

    if o25 and o25>1.01:
        xg_total = xh + xa
        # BACKTEST: Over cuota 1.80-3.50 → edge claro (cuota >2.50: ROI +53%)
        # Excluir rango xG 2.5-3.2 donde el modelo es menos confiable
        over_odd_ok = 1.80 <= o25 <= 3.50
        if over_odd_ok and not (2.50 <= xg_total <= 3.20):
            ev_ref_o = o25_ev if o25_ev and o25_ev > o25 else o25
            model_gap_o = round(po - 1/(ev_ref_o * 1.05), 4)
            out.append({"mkt":"OVER","pick":"Over 2.5 Goles","odd":o25,"prob":po,
                        "model_gap":model_gap_o,
                        "ah_adj":round(ah_ou_adjustment,3),
                        "best_close":o25_ev,
                        "ev_best":round(po*ev_ref_o-1,4)})
    if u25 and u25>1.01:
        xg_total = xh + xa
        # BACKTEST: Under solo en cuota 1.70-2.10
        # <1.70: ROI -18.5% | >2.10: sin edge estadístico
        under_odd_ok = 1.70 <= u25 <= 2.10
        if under_odd_ok and not (1.80 <= xg_total <= 2.20):
            ev_ref_u = u25_ev if u25_ev and u25_ev > u25 else u25
            model_gap_u = round(pu - 1/(ev_ref_u * 1.05), 4)
            out.append({"mkt":"UNDER","pick":"Under 2.5 Goles","odd":u25,"prob":pu,
                        "model_gap":model_gap_u,
                        "ah_adj":round(-ah_ou_adjustment,3),
                        "best_close":u25_ev,
                        "ev_best":round(pu*ev_ref_u-1,4)})

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

def validate_xg(xh, xa, oh, oa, o25, mkt_hint=None):
    """
    Valida que los xG son razonables y no son valores por defecto.
    Para DNB (mkt_hint="DNB") relajamos los filtros porque el edge
    viene del vig doble, no de la precisión del xG.
    """
    if oh and oa:
        mo=min(oh,oa); rat=max(xh,xa)/min(xh,xa) if min(xh,xa)>0 else 1
        # Solo aplicar filtros estrictos cuando NO es para DNB
        if mkt_hint != "DNB":
            if mo<1.40 and rat<1.50: return False,"XG_DEFAULT_DETECTED"
            if mo<1.65 and rat<1.20: return False,"XG_FLAT_ON_FAV"
            if 1.30<=xh<=1.50 and 1.30<=xa<=1.50 and mo<1.60: return False,"XG_LIKELY_DEFAULT"
        else:
            # Para DNB: solo rechazar si xG son idénticos (default literal)
            if xh == xa and mo < 1.40: return False,"XG_DEFAULT_DETECTED"
    if o25 and mkt_hint != "DNB":
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
        # DNB: empate = PUSH (devuelven la apuesta, profit=0)
        if hg==ag: return "PUSH"   # empate → PUSH, profit=0
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
                                if res in("WIN","LOSS","PUSH"):
                                    if res == "PUSH":
                                        # DNB empate → devuelven la apuesta, profit=0
                                        profit = 0.0
                                        res_db = "PUSH"
                                    else:
                                        profit = round(stake*odd-stake if res=="WIN" else -stake, 4)
                                        res_db = res
                                    row[9]=res_db; row[14]=str(fthg); row[15]=str(ftag)
                                    row[11]=str(profit)
                                    wins+=res=="WIN"; losses+=res=="LOSS"; resolved+=1
                                    # Sincronizar resultado a picks_log DB
                                    db_updates.append((res_db, profit, fthg, ftag,
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
                        UPDATE picks_log SET result=?, profit=?,
                               clv_captured=CASE WHEN ? = 'PUSH' THEN 1 ELSE clv_captured END
                        WHERE home_team LIKE ? AND away_team LIKE ?
                          AND market=? AND result='PENDING'
                    """, (res, profit, res,
                          f"%{home[:8]}%", f"%{away[:8]}%", mkt))
                conn_a.commit(); conn_a.close()
            except Exception as db_e:
                print(f"  ⚠️ audit DB sync: {db_e}", flush=True)
        Log.audit_end(resolved, wins, losses)
        if resolved:
            pushes = resolved - wins - losses
            push_str = f" | 🔄{pushes} PUSH" if pushes else ""
            send_msg(f"🔬 <b>Auditoría V7.2</b>\nResueltos: {resolved} | ✅{wins} WIN | ❌{losses} LOSS{push_str}")
    except Exception as e:
        Log.err(f"audit: {e}", "AUDIT")

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
        Log.err(f"pnl: {e}", "PNL")

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
            Log.err(f"KILL-SWITCH: drawdown {drawdown*100:.1f}% — PAUSANDO SISTEMA", "RISK")
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
            Log.warn("burn_in_evaluator.py no encontrado", "BURNIN")
        except Exception as e:
            Log.err(f"burn-in eval: {e}", "BURNIN")

    def _filter(self, probs, label, fid, h_n, a_n, ko, xh, xa, xt, conf, src, div, ts):
        """
        Aplica filtros y retorna candidatos válidos.
        Incluye filtro CLV en tiempo real: si Pinnacle actual < cuota justa → SKIP.
        """
        cands=[]

        # Cargar cuotas Pinnacle actuales para este partido (1 query SQLite)
        pin_odds = {}
        if ODDS_API_KEY:
            try:
                import difflib as _dlf
                conn_f = sqlite3.connect(DB_PATH)
                pin_rows = conn_f.execute("""
                    SELECT pin_h, pin_d, pin_a, pin_over, pin_under, home_team, away_team
                    FROM live_odds WHERE div=?
                    AND updated_at >= datetime('now', '-18 hours')
                    ORDER BY updated_at DESC LIMIT 20
                """, (div,)).fetchall()
                conn_f.close()
                for ph,pd,pa,po,pu,lh,la in pin_rows:
                    sh = _dlf.SequenceMatcher(None,h_n.lower(),lh.lower()).ratio()
                    sa = _dlf.SequenceMatcher(None,a_n.lower(),la.lower()).ratio()
                    if sh > 0.55 and sa > 0.55:
                        pin_odds = {"H":ph,"D":pd,"A":pa,"O25":po,"U25":pu}
                        # Calcular Pinnacle DNB derivado de H/D/A
                        # Pinnacle tiene vig ~2% → DNB calculado es el precio más justo
                        try:
                            if ph and pd and ph > 1.01 and pd > 1.01:
                                _dh = 1/(1/ph - 1/pd)
                                pin_odds["DNB_H"] = round(_dh, 3) if _dh > 1.01 else None
                            if pa and pd and pa > 1.01 and pd > 1.01:
                                _da = 1/(1/pa - 1/pd)
                                pin_odds["DNB_A"] = round(_da, 3) if _da > 1.01 else None
                        except:
                            pass
                        break
            except Exception:
                pass

        for item in probs:
            ev=(item["prob"]*item["odd"])-1
            ok2,fail=sanity(item["prob"],item["mkt"],item["odd"])
            if not ok2:     log_rej(label,item["mkt"],item["odd"],ev,fail); continue
            min_ev_mkt = MIN_EV_MKT.get(item["mkt"], MIN_EV)
            if ev<min_ev_mkt: log_rej(label,item["mkt"],item["odd"],ev,"LOW_EV"); continue
            # DNB puede tener EV muy alto por vig doble del mercado — permitir hasta 80%
            max_ev_this = 0.80 if item["mkt"]=="DNB" else MAX_EV
            if ev>max_ev_this: log_rej(label,item["mkt"],item["odd"],ev,"EV_ALUCINATION"); continue
            k,urs,rej=kelly_urs(ev,item["odd"],item["mkt"])
            if k==0.0:      log_rej(label,item["mkt"],item["odd"],ev,rej); continue

            # ── FILTRO CLV EN TIEMPO REAL ────────────────────────────────
            # Cuota justa del modelo = 1 / prob
            # Si Pinnacle actual < cuota justa → mercado ya lo descuenta → SKIP
            if pin_odds:
                fair_odd = round(1 / item["prob"], 3) if item["prob"] > 0.01 else None
                mkt = item["mkt"]
                pin_ref = None
                if mkt == "DNB":
                    sel = item.get("pick","")
                    if h_n in sel:
                        # Comparar cuota B365_DNB calculada vs Pinnacle_DNB calculada
                        # Si B365_DNB > Pinnacle_DNB → B365 paga más → edge real
                        # Si B365_DNB < Pinnacle_DNB → Pinnacle ya ajustó → skip
                        pin_ref = pin_odds.get("DNB_H")
                    else:
                        pin_ref = pin_odds.get("DNB_A")
                elif mkt in ("1X2","DC"):
                    sel = item.get("pick","")
                    if h_n in sel or "1X" in sel:
                        pin_ref = pin_odds.get("H")
                    elif a_n in sel or "X2" in sel:
                        pin_ref = pin_odds.get("A")
                    else:
                        pin_ref = pin_odds.get("D")
                elif mkt == "OVER":
                    pin_ref = pin_odds.get("O25")
                elif mkt == "UNDER":
                    pin_ref = pin_odds.get("U25")

                if pin_ref and fair_odd and pin_ref < fair_odd * 0.90:
                    # Pinnacle está 10%+ por debajo de nuestro fair value
                    # Backtest: CLV >+10% = ROI +7.9% | CLV 0-10% = neutral
                    clv_now = round((item["odd"] / pin_ref - 1) * 100, 1)
                    log_rej(label, mkt, item["odd"], ev,
                            f"PIN_BAJO: Pinnacle={pin_ref:.2f} < fair={fair_odd:.2f} CLV={clv_now:+.1f}%")
                    print(f"     ⛔ {mkt} CLV negativo vs Pinnacle ({pin_ref:.2f} < {fair_odd:.2f}) → SKIP",
                          flush=True)
                    continue
                elif pin_ref and fair_odd:
                    clv_live = round((item["odd"] / pin_ref - 1) * 100, 1)
                    print(f"     📊 {mkt} CLV live vs PIN: {clv_live:+.1f}%", flush=True)

            # ── Line monitoring: señal de movimiento ──────────────────
            line_signal = ""
            if item["mkt"] == "DNB" and ODDS_API_KEY:
                try:
                    moves = detect_line_moves(div=div, hours_window=48)
                    mk = next(((h,a,d) for (h,a,d) in moves
                               if h_n[:6].lower() in h.lower() or
                                  a_n[:6].lower() in a.lower()), None)
                    if mk:
                        mv = moves[mk]
                        sig = mv["signal"]
                        pick_dir = "home" if h_n in item.get("pick","") else "away"
                        if sig == f"sharp_{pick_dir}":
                            # Sharp money en la misma dirección → refuerza el pick
                            line_signal = f" 🔥SHARP+{mv['confidence']:.0%}"
                        elif sig in ("sharp_home","sharp_away") and sig != f"sharp_{pick_dir}":
                            # Sharp money en dirección contraria → cuidado
                            line_signal = f" ⚠️CONTRA-SHARP"
                        elif sig == "steam":
                            line_signal = " ⚡STEAM"
                        elif mv["snaps"] < 3:
                            line_signal = " 📊SIN-HIST"
                        else:
                            mh = mv["move_h"]; ma = mv["move_a"]
                            line_signal = f" Δ{mh:+.1f}%/{ma:+.1f}%"
                except Exception:
                    pass
            print(f"     ✅ {item['mkt']} @{item['odd']:.2f} EV={ev*100:.1f}% URS={urs:.3f}{line_signal}",flush=True)
            cands.append({**item,"ev":ev,"base_stake":k,"urs":urs,
                          "conf":conf,"xg_src":src,
                          "fid":fid,"h_n":h_n,"a_n":a_n,"ko":ko,
                          "xh":xh,"xa":xa,"xt":xt,"div":div,
                          "trend_pct_o25":ts.get("pct_o25"),
                          "trend_pct_bts":ts.get("pct_bts")})
        return cands

    def _save_pick(self, p, today_str, conn_c):
        """
        Guarda pick en DB y CSV de auditoría.
        Mientras burn-in incompleto (p-value > 0.10): stake máximo 0.5% flat.
        Kelly completo solo cuando p < 0.10 y BR > 52%.
        """
        cfg_p=TARGET_LEAGUES.get(p["div"],{})
        # Limitar stake durante burn-in
        if not LIVE_TRADING:
            # Dry run: usar stake calculado por Kelly para calibrar
            final_stake = p.get("final_stake", p.get("base_stake", 0) * KELLY)
        else:
            # Live pero burn-in incompleto → stake flat conservador
            final_stake = min(p.get("final_stake", 0.005), 0.005)  # máx 0.5%
        p = {**p, "final_stake": final_stake}

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
        # Obtener señal de línea para incluir en Telegram
        _line_tag = ""
        try:
            _lm = detect_line_moves(div=p["div"], hours_window=48)
            _mk = next(((h,a,d) for (h,a,d) in _lm
                        if p["h_n"][:5].lower() in h.lower()), None)
            if _mk:
                _mv = _lm[_mk]
                _s = _mv["signal"]
                _pick_dir = "home" if p["h_n"] in p.get("pick","") else "away"
                if _s == f"sharp_{_pick_dir}":
                    _line_tag = "\n🔥 Sharp money CONFIRMA el pick"
                elif _s in ("sharp_home","sharp_away") and _s != f"sharp_{_pick_dir}":
                    _line_tag = "\n⚠️ Sharp money EN CONTRA — precaución"
                elif _mv["snaps"] >= 2:
                    _mh = _mv["move_h"]; _ma = _mv["move_a"]
                    _line_tag = f"\n📈 Línea: local{_mh:+.1f}% visita{_ma:+.1f}%"
        except: pass

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

        now_utc   = datetime.now(timezone.utc)
        today     = now_utc.strftime("%Y-%m-%d")
        tomorrow  = (now_utc+timedelta(days=1)).strftime("%Y-%m-%d")
        day_after = (now_utc+timedelta(days=2)).strftime("%Y-%m-%d")
        today_str = now_utc.strftime("%d/%m/%Y")
        # scan_dates: HOY + D+1 + D+2 siempre
        # Jueves/Viernes: + D+3 (sábado) y D+4 (domingo) para cubrir fin de semana
        # Lunes/Martes: + D+3 para cubrir jueves de UEL/Champions siguiente
        scan_dates = [today, tomorrow, day_after]
        wd = now_utc.weekday()  # 0=lun, 1=mar, 2=mié, 3=jue, 4=vie, 5=sáb, 6=dom
        if wd in (3, 4):  # jueves o viernes — cubrir fin de semana completo
            scan_dates.append((now_utc+timedelta(days=3)).strftime("%Y-%m-%d"))
        if wd == 3:       # jueves — también D+4 para el domingo
            scan_dates.append((now_utc+timedelta(days=4)).strftime("%Y-%m-%d"))
        if wd in (0, 1):  # lunes o martes — agregar D+3 para jueves de copa
            scan_dates.append((now_utc+timedelta(days=3)).strftime("%Y-%m-%d"))
        # Evitar duplicados manteniendo orden
        scan_dates = list(dict.fromkeys(scan_dates))
        preliminary = []

        Log.scan_start(scan_dates)
        dias_str = " / ".join(
            datetime.strptime(d, "%Y-%m-%d").strftime("%a %d")
            for d in scan_dates
        )
        send_msg(
            f"🔍 <b>V7.2 Scan</b>\n"
            f"🗓 {dias_str}\n"
            f"[1] CSV co.uk + [2] fd.org + [3] api-football + Pinnacle live"
        )

        # ── [2] Trend Resource — 1 req por fecha ─────────────────────────
        fd_codes = [cfg["fbd_code"] for cfg in TARGET_LEAGUES.values()
                    if cfg.get("fbd_code")]
        all_trends = {}
        for d in scan_dates:
            all_trends.update(fetch_trends(d, fd_codes))
            time.sleep(7.0)

        # ── [1] fixtures.csv → europeas ───────────────────────────────────
        fix_df = get_fixtures_csv()

        euro_divs = [d for d,c in TARGET_LEAGUES.items()
                     if c["source"]=="csv_euro" and not c.get("_disabled", False)]

        # ── Construir lista de partidos desde AMBAS fuentes ─────────────────
        # Fuente A: fixtures.csv co.uk (ligas principales con cuotas)
        # Fuente B: CSVs históricos (filas sin resultado = partidos futuros)
        # Esto garantiza cobertura de las 6 ligas nuevas y fines de semana

        import pandas as pd

        candidate_rows = []  # list of (div, home, away, ko_str, odds_row)

        # Fuente A — fixtures.csv
        if fix_df is not None:
            for scan_d in scan_dates:
                target_date = datetime.strptime(scan_d, "%Y-%m-%d").date()
                daily = fix_df[
                    (fix_df["Date"].dt.date==target_date) &
                    (fix_df["Div"].isin(euro_divs))
                ]
                for _, row in daily.iterrows():
                    div=row.get("Div","")
                    if div not in TARGET_LEAGUES: continue
                    h_n=str(row.get("HomeTeam","")).strip()
                    a_n=str(row.get("AwayTeam","")).strip()
                    if not h_n or not a_n: continue
                    candidate_rows.append((div, h_n, a_n, str(row.get("Date","")), row, "fixtures"))

        # Fuente B — CSVs históricos: filas SIN resultado son partidos futuros
        for div in euro_divs:
            if div not in TARGET_LEAGUES: continue
            cfg_d = TARGET_LEAGUES[div]
            df_div = load_csv(div)
            if df_div is None: continue
            try:
                # Filas sin FTHG/FTAG = partidos no jugados aún
                future = df_div[df_div["FTHG"].isna() | df_div["FTAG"].isna()].copy()
                if future.empty: continue
                future["Date"] = pd.to_datetime(future["Date"], dayfirst=True, errors="coerce")
                for scan_d in scan_dates:
                    target_date = datetime.strptime(scan_d, "%Y-%m-%d").date()
                    daily_f = future[future["Date"].dt.date==target_date]
                    for _, row in daily_f.iterrows():
                        h_n=str(row.get("HomeTeam","")).strip()
                        a_n=str(row.get("AwayTeam","")).strip()
                        if not h_n or not a_n: continue
                        # Evitar duplicados con fixtures.csv
                        already = any(
                            r[0]==div and
                            difflib.SequenceMatcher(None,r[1].lower(),h_n.lower()).ratio()>0.80 and
                            difflib.SequenceMatcher(None,r[2].lower(),a_n.lower()).ratio()>0.80
                            for r in candidate_rows
                        )
                        if not already:
                            candidate_rows.append((div, h_n, a_n, str(row.get("Date","")), row, "csv_hist"))
            except Exception as e:
                Log.warn(f"CSV hist future {div}: {e}", "SCAN")

        Log.info(f"Candidatos encontrados: {len(candidate_rows)} partidos en {len(scan_dates)} fechas", "SCAN")
        if len(candidate_rows) == 0:
            Log.warn("0 candidatos — verificar fixtures.csv y CSVs históricos", "SCAN")
            Log.warn(f"euro_divs activos ({len(euro_divs)}): {euro_divs[:5]}", "SCAN")
            Log.warn(f"scan_dates: {scan_dates}", "SCAN")
            # Verificar fixtures.csv
            if fix_df is not None:
                Log.warn(f"fixtures.csv: {len(fix_df)} filas, fechas: {fix_df['Date'].dt.date.unique()[:5].tolist() if 'Date' in fix_df.columns else 'SIN FECHA'}", "SCAN")
                Log.warn(f"fixtures.csv divs: {fix_df['Div'].unique()[:10].tolist() if 'Div' in fix_df.columns else 'SIN DIV'}", "SCAN")
            else:
                Log.warn("fixtures.csv: None (no se descargó)", "SCAN")
            # Verificar CSVs históricos
            for _dv in euro_divs[:3]:
                _df_chk = load_csv(_dv)
                if _df_chk is not None:
                    fut = _df_chk[_df_chk["FTHG"].isna()].copy() if "FTHG" in _df_chk.columns else pd.DataFrame()
                    Log.warn(f"CSV {_dv}: {len(_df_chk)} filas, {len(fut)} futuros sin resultado", "SCAN")
                else:
                    Log.warn(f"CSV {_dv}: no disponible", "SCAN")
        else:
            # Log de los primeros 5 candidatos
            for _c in candidate_rows[:5]:
                Log.info(f"  Candidato: {_c[1]} vs {_c[2]} ({_c[0]}) [{_c[5]}] ko={_c[3][:10]}", "SCAN")

        # ── Procesar todos los candidatos ─────────────────────────────────
        for div, h_n, a_n, ko, row, row_src in candidate_rows:
            cfg=TARGET_LEAGUES[div]
            label=f"{h_n} vs {a_n} ({cfg['name']})"
            fid=f"{div}_{h_n}_{a_n}"
            Log.info(f"── {label} [{row_src}]", "SCAN")

            # xG desde CSV histórico
            df_hist=load_csv(div)

            # ── Fatiga: calcular días de descanso de cada equipo ──────────
            try:
                import difflib as _dlf
                ko_date = pd.Timestamp(ko)
                # Fatiga real: liga + copas europeas/locales
                build_xg._rest_h = get_true_rest_days(h_n, ko_date, div, df_hist)
                build_xg._rest_a = get_true_rest_days(a_n, ko_date, div, df_hist)
                if build_xg._rest_h is not None or build_xg._rest_a is not None:
                    Log.info(f"  Descanso real (liga+copas): {h_n}={build_xg._rest_h}d {a_n}={build_xg._rest_a}d", "FAT")
            except:
                build_xg._rest_h = None; build_xg._rest_a = None

            xh,xa,xt,conf,src=build_xg(h_n,a_n,div,cfg,df_hist)

            # Cuotas — fixtures.csv + Pinnacle live (The Odds API)
            odds=get_odds_from_row(row,cfg)

            # Enriquecer con Pinnacle en tiempo real si está disponible
            try:
                import difflib as _dl_scan
                conn_lo_scan = sqlite3.connect(DB_PATH)
                lo_scan = conn_lo_scan.execute("""
                    SELECT pin_h, pin_d, pin_a, pin_over, pin_under
                    FROM live_odds
                    WHERE div=? AND updated_at >= datetime('now', '-12 hours')
                    ORDER BY updated_at DESC
                """, (div,)).fetchall()
                conn_lo_scan.close()
                # Fuzzy match por nombres de equipo
                for (ph_pin, pd_pin, pa_pin, po_pin, pu_pin) in lo_scan:
                    # Buscar si alguna fila de live_odds coincide con este partido
                    # (live_odds tiene home_team/away_team en nombres de Odds API)
                    pass  # El match se hace por div — tomamos la primera fila relevante
                # Alternativa más simple: buscar directamente por home+away
                conn_lo2 = sqlite3.connect(DB_PATH)
                pin_row = conn_lo2.execute("""
                    SELECT pin_h, pin_d, pin_a, pin_over, pin_under
                    FROM live_odds
                    WHERE div=?
                    AND updated_at >= datetime('now', '-12 hours')
                    AND (
                        (lower(home_team) LIKE ? OR lower(home_team) LIKE ?)
                        AND (lower(away_team) LIKE ? OR lower(away_team) LIKE ?)
                    )
                    ORDER BY updated_at DESC LIMIT 1
                """, (div,
                      f"%{h_n[:5].lower()}%", f"%{h_n[-4:].lower()}%",
                      f"%{a_n[:5].lower()}%", f"%{a_n[-4:].lower()}%"
                )).fetchone()
                conn_lo2.close()
                if pin_row and pin_row[0]:
                    ph_p, pd_p, pa_p, po_p, pu_p = pin_row
                    # Usar Pinnacle como cuota de referencia principal
                    # Pinnacle es la línea más eficiente del mercado
                    if ph_p and ph_p > 1.01: odds["H_PIN"] = ph_p
                    if pd_p and pd_p > 1.01: odds["D_PIN"] = pd_p
                    if pa_p and pa_p > 1.01: odds["A_PIN"] = pa_p
                    if po_p and po_p > 1.01: odds["O25_PIN"] = po_p
                    if pu_p and pu_p > 1.01: odds["U25_PIN"] = pu_p
                    # Si no hay cuota de B365 en fixtures.csv, usar Pinnacle
                    if not odds.get("H") and ph_p: odds["H"] = ph_p
                    if not odds.get("D") and pd_p: odds["D"] = pd_p
                    if not odds.get("A") and pa_p: odds["A"] = pa_p
                    if not odds.get("O25") and po_p: odds["O25"] = po_p
                    if not odds.get("U25") and pu_p: odds["U25"] = pu_p
            except Exception as _e_pin:
                pass  # Pinnacle no disponible — usar co.uk

            # Trend fd.org por nombre
            ts={}
            for te in all_trends.values():
                hn_t=te.get("homeTeam",{}).get("name","")
                an_t=te.get("awayTeam",{}).get("name","")
                if (difflib.SequenceMatcher(None,h_n,hn_t).ratio()>0.60 and
                    difflib.SequenceMatcher(None,a_n,an_t).ratio()>0.60):
                    ts=extract_trend(te); break

            # Para DNB relajamos validación de xG (el edge es estructural)
            active_mkts_now = set(k for k,v in MIN_EV_MKT.items() if v < 0.50)
            mkt_hint = "DNB" if active_mkts_now == {"DNB"} else None
            ok,reason=validate_xg(xh,xa,odds.get("H"),odds.get("A"),odds.get("O25"),
                                   mkt_hint=mkt_hint)
            if not ok:
                log_rej(label,"ALL",0,0,reason)
                print(f"     ⛔ {reason} xH={xh:.2f} xA={xa:.2f} oH={odds.get('H')} oA={odds.get('A')}", flush=True)
                continue
            if conf=="LOW":
                log_rej(label,"ALL",0,0,"XG_LOW_SKIP")
                print(f"     ⛔ XG_LOW_SKIP — historial insuficiente", flush=True)
                continue

            probs=build_probs(xh,xa,conf,h_n,a_n,cfg,odds,ts)
            cands=self._filter(probs,label,fid,h_n,a_n,ko,xh,xa,xt,conf,src,div,ts)
            if cands:
                cands.sort(key=lambda x:x["ev"]*x["urs"],reverse=True)
                preliminary.append(cands[0])

        # ── [1]+[2] BSA — CSV goles proxy + fd.org fixtures + Trend ──────
        df_bra=load_csv("BSA")
        cfg_bra=TARGET_LEAGUES["BSA"]

        for d in scan_dates:
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
                            Log.info("BSA odds via api-football", "BSA")
                    if not odds.get("H"):
                        # Fallback: The Odds API para BSA
                        try:
                            conn_bsa = sqlite3.connect(DB_PATH)
                            pin_bsa = conn_bsa.execute("""
                                SELECT pin_h, pin_d, pin_a, pin_over, pin_under
                                FROM live_odds WHERE div='BSA'
                                AND (lower(home_team) LIKE ? OR lower(home_team) LIKE ?)
                                ORDER BY updated_at DESC LIMIT 1
                            """, (f"%{h_n[:5].lower()}%", f"%{h_n[-4:].lower()}%")).fetchone()
                            conn_bsa.close()
                            if pin_bsa and pin_bsa[0]:
                                odds.update({"H": pin_bsa[0], "D": pin_bsa[1], "A": pin_bsa[2],
                                             "O25": pin_bsa[3], "U25": pin_bsa[4]})
                                Log.info(f"BSA odds via Pinnacle live: {h_n}", "BSA")
                        except Exception: pass
                    if not odds.get("H"):
                        Log.warn("BSA sin cuotas — skip", "BSA")
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
        mx_fixtures=get_mx_fixtures(scan_dates, self.apif_h)

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

            # [3] Cuotas: api-football primero, The Odds API como fallback
            odds_mx=get_mx_odds(fid, self.apif_h)
            if not odds_mx:
                # Fallback: buscar en live_odds de The Odds API
                try:
                    conn_mx = sqlite3.connect(DB_PATH)
                    pin_mx = conn_mx.execute("""
                        SELECT pin_h, pin_d, pin_a, pin_over, pin_under
                        FROM live_odds WHERE div='MEX'
                        AND (lower(home_team) LIKE ? OR lower(home_team) LIKE ?)
                        ORDER BY updated_at DESC LIMIT 1
                    """, (f"%{h_n[:5].lower()}%", f"%{h_n[-4:].lower()}%")).fetchone()
                    conn_mx.close()
                    if pin_mx and pin_mx[0]:
                        odds_mx = {"H": pin_mx[0], "D": pin_mx[1], "A": pin_mx[2],
                                   "O25": pin_mx[3], "U25": pin_mx[4]}
                        Log.info(f"MEX odds via Pinnacle live: {h_n}", "MEX")
                except Exception: pass
            if not odds_mx:
                print(f"     ⚠️ MEX sin cuotas — skip", flush=True)
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

        # ── Un pick máximo por partido (evitar correlación DNB_H + DNB_A) ──
        seen_matches = {}
        for p in sorted(preliminary, key=lambda x: x["ev"]*x["urs"], reverse=True):
            match_key = f"{p['h_n']}|{p['a_n']}|{p['div']}"
            if match_key not in seen_matches:
                seen_matches[match_key] = p
        preliminary = list(seen_matches.values())

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

    def check_ht_alerts(self):
        """
        Revisa picks Under/Over activos y genera alertas HT.
        Evidencia CSV I1:
          HT 0-0 → Over 2.5 FT = 15.2%  | ALERTA si pick es OVER
          HT 1 gol → neutro (47.1%)
          HT 2+ goles → Over 2.5 FT = 83.9% | ALERTA si pick es UNDER

        Llama a api-football /fixtures para obtener marcadores en vivo.
        Solo activo durante ventana de partidos (12:00-23:00 UTC).
        """
        try:
            now_utc = datetime.now(timezone.utc)
            # Solo entre 12:00 y 23:00 UTC (ventana de partidos europeos)
            if not (12 <= now_utc.hour <= 23):
                return

            conn = sqlite3.connect(DB_PATH)
            # Picks Under/Over PENDING de hoy
            pending = conn.execute("""
                SELECT id, home_team, away_team, market, selection, div,
                       kickoff_time, odd_open
                FROM picks_log
                WHERE result = 'PENDING'
                  AND market IN ('UNDER','OVER')
                  AND date(kickoff_time) = date('now')
                  AND (ht_alerted IS NULL OR ht_alerted = 0)
            """).fetchall()
            conn.close()

            if not pending:
                return

            alerts_sent = 0
            for pid, home, away, mkt, sel, div, ko, odd in pending:
                try:
                    # Obtener fixture en vivo desde api-football
                    res = apif_get("fixtures", {
                        "date": now_utc.strftime("%Y-%m-%d")
                    }, self.apif_h)

                    for fix in res:
                        fh = fix["teams"]["home"]["name"]
                        fa = fix["teams"]["away"]["name"]
                        # Fuzzy match
                        import difflib as _dl
                        if (_dl.SequenceMatcher(None, home.lower(), fh.lower()).ratio() > 0.60 and
                            _dl.SequenceMatcher(None, away.lower(), fa.lower()).ratio() > 0.60):

                            status = fix["fixture"]["status"]["short"]
                            # Solo alertar en el descanso (HT)
                            if status != "HT":
                                continue

                            score = fix.get("score",{}).get("halftime",{})
                            ht_h = score.get("home", 0) or 0
                            ht_a = score.get("away", 0) or 0
                            ht_total = ht_h + ht_a

                            # Generar alerta según evidencia CSV
                            alert_msg = None
                            ht_score = f"{home} {ht_h}-{ht_a} {away} (HT)"
                            if mkt == "OVER" and ht_total == 0:
                                alert_msg = (
                                    "⚠️ <b>ALERTA HT — OVER en riesgo</b>\n"
                                    f"🏟️ {ht_score}\n"
                                    f"Pick: {sel} @{odd:.2f}\n"
                                    "📊 Histórico: 0-0 HT → Over 2.5 FT solo <b>15.2%</b>\n"
                                    "💡 Mercado ha repriced probablemente"
                                )
                            elif mkt == "UNDER" and ht_total == 0:
                                alert_msg = (
                                    "✅ <b>CONFIRMACIÓN HT — UNDER bien posicionado</b>\n"
                                    f"🏟️ {ht_score}\n"
                                    f"Pick: {sel} @{odd:.2f}\n"
                                    "📊 Histórico: 0-0 HT → Over 2.5 FT solo <b>15.2%</b> → Under muy probable"
                                )
                            elif mkt == "UNDER" and ht_total >= 2:
                                alert_msg = (
                                    "⚠️ <b>ALERTA HT — UNDER en riesgo</b>\n"
                                    f"🏟️ {ht_score}\n"
                                    f"Pick: {sel} @{odd:.2f}\n"
                                    f"📊 Histórico: {ht_total} goles HT → Over 2.5 FT <b>83.9%</b>\n"
                                    "💡 Under está en peligro"
                                )
                            elif mkt == "OVER" and ht_total >= 2:
                                alert_msg = (
                                    "✅ <b>CONFIRMACIÓN HT — OVER excelente posición</b>\n"
                                    f"🏟️ {ht_score}\n"
                                    f"Pick: {sel} @{odd:.2f}\n"
                                    f"📊 Histórico: {ht_total} goles HT → Over 2.5 FT <b>83.9%</b>"
                                )
                            if alert_msg:
                                send_msg(alert_msg)
                                alerts_sent += 1
                                # Marcar como alertado para no repetir
                                conn2 = sqlite3.connect(DB_PATH)
                                try:
                                    conn2.execute(
                                        "UPDATE picks_log SET ht_alerted=1 WHERE id=?",
                                        (pid,))
                                    conn2.commit()
                                except Exception:
                                    pass
                                conn2.close()
                            break
                except Exception as e:
                    Log.warn(f"HT alert {home} vs {away}: {e}", "HT")

            if alerts_sent:
                Log.ok(f"HT alerts enviadas: {alerts_sent}", "HT")

        except Exception as e:
            Log.err(f"check_ht_alerts: {e}", "HT")

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
                            # CLV benchmark: Pinnacle CIERRE (PSCH) = más sharp del mundo
                            # Luego MaxC (mejor precio cierre), luego B365 cierre
                            co={
                                "H":   best_close("PSCH","PSH","MaxCH","B365CH","AvgCH"),
                                "D":   best_close("PSCD","PSD","MaxCD","B365CD","AvgCD"),
                                "A":   best_close("PSCA","PSA","MaxCA","B365CA","AvgCA"),
                                "O25": best_close("PC>2.5","P>2.5","MaxC>2.5","B365C>2.5","AvgC>2.5"),
                                "U25": best_close("PC<2.5","P<2.5","MaxC<2.5","B365C<2.5","AvgC<2.5"),
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
                    # Calcular CLV vs Pinnacle cierre y MaxC cierre
                    def _co_odd(market, sel_str, co_dict):
                        if market=="OVER":   return co_dict.get("O25")
                        elif market=="UNDER": return co_dict.get("U25")
                        elif market=="1X2":  return _clv_1x2(co_dict, home, away, sel_str)
                        elif market=="BTTS": return co_dict.get("BTTS_Y")
                        elif market=="DC":
                            oh,od,oa=co_dict.get("H"),co_dict.get("D"),co_dict.get("A")
                            if oh and od and oa:
                                if "o Empate" in sel_str and not sel_str.startswith("DC: Empate"):
                                    return round(1/(1/oh+1/od),2)
                                elif "Empate o" in sel_str:
                                    return round(1/(1/od+1/oa),2)
                                return round(1/(1/oh+1/oa),2)
                        return None

                    # Pinnacle cierre benchmark
                    co_ps = {}
                    co_maxc = {}
                    try:
                        # Buscar en CSV histórico la fila del partido
                        path_csv = os.path.join(DATA_DIR, f"{div}.csv")
                        if os.path.exists(path_csv):
                            import pandas as _pd2
                            try:    _df2 = _pd2.read_csv(path_csv, encoding="utf-8-sig")
                            except: _df2 = _pd2.read_csv(path_csv, encoding="latin-1")
                            _df2.columns = _df2.columns.str.strip()
                            _df2 = _df2.rename(columns={"Home":"HomeTeam","Away":"AwayTeam"})
                            import difflib as _dl2
                            _teams_h = _df2["HomeTeam"].dropna().unique()
                            _teams_a = _df2["AwayTeam"].dropna().unique()
                            rh2 = _dl2.get_close_matches(home, _teams_h, n=1, cutoff=0.55)
                            ra2 = _dl2.get_close_matches(away, _teams_a, n=1, cutoff=0.55)
                            if rh2 and ra2:
                                _m2 = _df2[(_df2["HomeTeam"]==rh2[0])&(_df2["AwayTeam"]==ra2[0])]
                                if not _m2.empty:
                                    _r2 = _m2.iloc[0]
                                    def _bc2(*cs):
                                        for c in cs:
                                            try:
                                                v=_r2.get(c) if hasattr(_r2,"get") else getattr(_r2,c,None)
                                                if v is not None:
                                                    f=float(v)
                                                    if f>1.01 and f==f: return f
                                            except: pass
                                        return None
                                    co_ps={
                                        "H":_bc2("PSCH","PSH"),"D":_bc2("PSCD","PSD"),"A":_bc2("PSCA","PSA"),
                                        "O25":_bc2("PC>2.5","P>2.5"),"U25":_bc2("PC<2.5","P<2.5"),
                                    }
                                    co_maxc={
                                        "H":_bc2("MaxCH","MaxH"),"D":_bc2("MaxCD","MaxD"),"A":_bc2("MaxCA","MaxA"),
                                        "O25":_bc2("MaxC>2.5","Max>2.5"),"U25":_bc2("MaxC<2.5","Max<2.5"),
                                    }
                    except Exception: pass

                    oc_ps   = _co_odd(mkt, sel, co_ps)   if co_ps   else None
                    oc_maxc = _co_odd(mkt, sel, co_maxc) if co_maxc else None
                    clv_ps   = round((odd_open/oc_ps   - 1)*100, 2) if oc_ps   and oc_ps   > 1.01 else None
                    clv_maxc = round((odd_open/oc_maxc - 1)*100, 2) if oc_maxc and oc_maxc > 1.01 else None

                    conn.execute("""INSERT INTO closing_lines
                        VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?)""",
                        (fid,mkt,sel,odd_open,odd_close,clv_pct,now_utc.isoformat(),
                         oc_ps, clv_ps, oc_maxc, clv_maxc))
                    conn.execute("UPDATE picks_log SET clv_captured=1 WHERE id=?",(pid,))
                    conn.commit(); conn.close()
                    # Construir línea CLV con los 3 benchmarks
                clv_line = f"{sel} @{odd_open:.2f}"
                clv_line += f" vs B365={clv_pct:+.1f}%"
                if clv_ps is not None:
                    clv_line += f" PS={clv_ps:+.1f}%"
                if clv_maxc is not None:
                    clv_line += f" MaxC={clv_maxc:+.1f}%"
                clv_lines.append(clv_line)

            Log.clv_end(len(clv_lines), sum(1 for l in clv_lines if "CLV=+" in l)/max(len(clv_lines),1)*100)
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
            Log.err(f"CLV: {e}", "CLV")

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

STATS_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>V7.2 · Stats</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Barlow:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#07080d;--s1:#0c0e17;--s2:#11141f;
  --border:#1c2035;--border2:#252840;
  --accent:#4f6ef7;--green:#22c55e;--red:#ef4444;--amber:#f59e0b;
  --text:#e8eaf6;--muted:#5a5f80;
  --mono:'IBM Plex Mono',monospace;--sans:'Barlow',sans-serif;
}
body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh}
a{color:inherit;text-decoration:none}

header{display:flex;align-items:center;justify-content:space-between;padding:1.25rem 2rem;border-bottom:1px solid var(--border);background:var(--s1)}
.logo{display:flex;align-items:center;gap:12px}
.logo-mark{width:28px;height:28px;background:var(--accent);border-radius:6px;display:flex;align-items:center;justify-content:center}
.logo-mark svg{width:16px;height:16px;fill:#fff}
.logo h1{font-size:1rem;font-weight:600}
.logo span{font-family:var(--mono);font-size:.62rem;color:var(--muted)}
.nav-link{font-family:var(--mono);font-size:.65rem;color:var(--muted);padding:6px 12px;border:1px solid var(--border);border-radius:6px;transition:all .15s}
.nav-link:hover{color:var(--text);border-color:var(--border2)}

.league-bar{display:flex;gap:6px;padding:1rem 2rem;background:var(--s1);border-bottom:1px solid var(--border);overflow-x:auto;flex-wrap:wrap}
.league-btn{font-family:var(--mono);font-size:.62rem;padding:5px 11px;border:1px solid var(--border);background:transparent;color:var(--muted);border-radius:5px;cursor:pointer;white-space:nowrap;transition:all .15s}
.league-btn:hover{color:var(--text)}
.league-btn.active{background:var(--accent);color:#fff;border-color:var(--accent)}

.main{padding:1.5rem 2rem;display:grid;gap:1.5rem}

/* Liga stats cards */
.league-cards{display:grid;grid-template-columns:repeat(7,1fr);gap:8px}
.lcard{background:var(--s1);border:1px solid var(--border);border-radius:8px;padding:.75rem;text-align:center}
.lcard-label{font-family:var(--mono);font-size:.55rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:.35rem}
.lcard-val{font-size:1.1rem;font-weight:600}

/* Secciones */
.section-title{font-family:var(--mono);font-size:.6rem;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:.75rem}

/* Tabla de posiciones */
.table-wrap{overflow-x:auto;overflow-y:visible}
table{width:100%;border-collapse:collapse;font-size:.75rem}
thead th{font-family:var(--mono);font-size:.55rem;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);padding:8px 12px;text-align:left;border-bottom:1px solid var(--border);background:var(--s1);cursor:pointer;white-space:nowrap}
thead th:hover{color:var(--text)}
thead th.sorted{color:var(--accent)}
tbody tr{border-bottom:1px solid var(--border);transition:background .1s}
tbody tr:hover{background:var(--s2)}
tbody td{padding:9px 12px;font-family:var(--mono);font-size:.72rem;vertical-align:middle;white-space:nowrap}
td.team-name{font-family:var(--sans);font-size:.82rem;font-weight:500;min-width:140px}
td.pos-num{color:var(--muted);width:32px;text-align:center}
td.num{text-align:right}
td.green{color:var(--green)} td.red{color:var(--red)} td.amber{color:var(--amber)} td.muted{color:var(--muted)}

/* Forma */
.form-row{display:flex;gap:3px;align-items:center}
.f-dot{width:17px;height:17px;border-radius:3px;display:inline-flex;align-items:center;justify-content:center;font-size:.55rem;font-weight:600}
.f-w{background:rgba(34,197,94,.15);color:var(--green)}
.f-d{background:rgba(245,158,11,.15);color:var(--amber)}
.f-l{background:rgba(239,68,68,.15);color:var(--red)}

/* Próximos partidos */
.fixtures-grid{display:grid;gap:6px}
.fixture-row{display:grid;grid-template-columns:60px 1fr auto 1fr 180px;gap:12px;align-items:center;padding:10px 14px;background:var(--s1);border:1px solid var(--border);border-radius:8px;transition:border-color .15s}
.fixture-row:hover{border-color:var(--border2)}
.fix-date{font-family:var(--mono);font-size:.6rem;color:var(--muted)}
.fix-home{text-align:right;font-weight:500;font-size:.82rem}
.fix-away{text-align:left;font-weight:500;font-size:.82rem}
.fix-vs{font-family:var(--mono);font-size:.65rem;color:var(--muted);text-align:center}
.fix-odds{display:flex;gap:6px;justify-content:flex-end}
.odd-pill{font-family:var(--mono);font-size:.62rem;padding:3px 8px;border-radius:4px;border:1px solid var(--border)}
.odd-h{color:var(--green);border-color:rgba(34,197,94,.2);background:rgba(34,197,94,.05)}
.odd-d{color:var(--amber);border-color:rgba(245,158,11,.2);background:rgba(245,158,11,.05)}
.odd-a{color:var(--red);border-color:rgba(239,68,68,.2);background:rgba(239,68,68,.05)}

/* xG chart bars */
.xg-bar-wrap{display:flex;align-items:center;gap:6px}
.xg-bar-fill{height:6px;border-radius:3px;background:var(--accent);min-width:2px}
.xg-val{font-family:var(--mono);font-size:.62rem;color:var(--muted)}

.tabs{display:flex;gap:2px;background:var(--s2);border:1px solid var(--border);border-radius:8px;padding:2px;width:fit-content;margin-bottom:1rem}
.tab-btn{font-family:var(--mono);font-size:.62rem;padding:5px 14px;border:none;background:transparent;color:var(--muted);border-radius:6px;cursor:pointer;transition:all .15s}
.tab-btn:hover{color:var(--text)}
.tab-btn.active{background:var(--accent);color:#fff}

.loading{text-align:center;padding:3rem;font-family:var(--mono);font-size:.75rem;color:var(--muted)}

@media(max-width:800px){
  .league-cards{grid-template-columns:repeat(3,1fr)}
  .fixture-row{grid-template-columns:1fr;gap:4px}
  header{padding:1rem}
  .main{padding:1rem}
}
</style>
</head>
<body>

<header>
  <div class="logo">
    <div class="logo-mark"><svg viewBox="0 0 16 16"><path d="M2 12 L8 4 L14 12 Z"/></svg></div>
    <div>
      <h1>V7.2 Stats</h1>
      <span>Triple League Specialist</span>
    </div>
  </div>
  <a href="/" class="nav-link">← dashboard</a>
</header>

<div class="league-bar" id="league-bar"></div>

<div class="main">
  <div id="league-cards" class="league-cards"></div>

  <div class="tabs">
    <button class="tab-btn active" onclick="showTab('table')">Tabla</button>
    <button class="tab-btn" onclick="showTab('xg')">xG</button>
    <button class="tab-btn" onclick="showTab('fixtures')">Próximos</button>
  </div>

  <div id="tab-table">
    <p class="section-title">Tabla de posiciones</p>
    <div class="table-wrap">
      <table id="standings-table">
        <thead>
          <tr>
            <th class="pos-num">#</th>
            <th>Equipo</th>
            <th class="num" data-col="pj">PJ</th>
            <th class="num" data-col="pg">PG</th>
            <th class="num" data-col="pe">PE</th>
            <th class="num" data-col="pp">PP</th>
            <th class="num" data-col="gf">GF</th>
            <th class="num" data-col="ga">GA</th>
            <th class="num" data-col="gd">DG</th>
            <th class="num" data-col="pts">PTS</th>
            <th>Forma</th>
            <th class="num" data-col="btts_pct">BTTS%</th>
            <th class="num" data-col="over25_pct">O2.5%</th>
            <th class="num" data-col="avg_gf">xGF</th>
            <th class="num" data-col="avg_ga">xGA</th>
          </tr>
        </thead>
        <tbody id="standings-body"><tr><td colspan="15" class="loading">cargando...</td></tr></tbody>
      </table>
    </div>
  </div>

  <div id="tab-xg" style="display:none">
    <p class="section-title">xG por equipo — últimos 8 partidos</p>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>#</th><th>Equipo</th>
            <th class="num">xGF</th><th style="min-width:120px">ataque</th>
            <th class="num">xGA</th><th style="min-width:120px">defensa</th>
            <th class="num">neto</th>
          </tr>
        </thead>
        <tbody id="xg-body"><tr><td colspan="7" class="loading">cargando...</td></tr></tbody>
      </table>
    </div>
  </div>

  <div id="tab-fixtures" style="display:none">
    <p class="section-title">Próximos partidos</p>
    <div class="fixtures-grid" id="fixtures-grid"><div class="loading">cargando...</div></div>
  </div>
</div>

<script>
const LEAGUES = {
  "E0":"🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier","E1":"🏴󠁧󠁢󠁥󠁮󠁧󠁿 Championship","E2":"🏴󠁧󠁢󠁥󠁮󠁧󠁿 League One",
  "SP1":"🇪🇸 La Liga","SP2":"🇪🇸 Segunda",
  "D1":"🇩🇪 Bundesliga","D2":"🇩🇪 Bundesliga 2",
  "I1":"🇮🇹 Serie A","I2":"🇮🇹 Serie B",
  "F1":"🇫🇷 Ligue 1","F2":"🇫🇷 Ligue 2",
  "N1":"🇳🇱 Eredivisie","P1":"🇵🇹 Primeira",
  "B1":"🇧🇪 Jupiler","SC0":"🏴󠁧󠁢󠁳󠁣󠁴 Premiership",
  "T1":"🇹🇷 Süper Lig","G1":"🇬🇷 Super League",
  // Copas europeas y nacionales
  "CUP_2":"🏆 UCL","CUP_3":"🥈 UEL","CUP_848":"🥉 UECL",
  "CUP_45":"🏴󠁧󠁢󠁥󠁮󠁧󠁿 FA Cup","CUP_143":"🇪🇸 Copa Rey","CUP_137":"🇮🇹 Coppa Italia",
};
let currentLeague = "E1", tableData = [], sortCol = "pts", sortDir = -1;

// Build league bar
const bar = document.getElementById("league-bar");
Object.entries(LEAGUES).forEach(([code, name]) => {
  const btn = document.createElement("button");
  btn.className = "league-btn" + (code===currentLeague?" active":"");
  btn.textContent = name;
  btn.onclick = () => { currentLeague=code; loadLeague(code);
    document.querySelectorAll(".league-btn").forEach(b=>b.classList.remove("active"));
    btn.classList.add("active");
  };
  bar.appendChild(btn);
});

function showTab(t) {
  ["table","xg","fixtures"].forEach(id=>{
    document.getElementById("tab-"+id).style.display = id===t?"block":"none";
  });
  document.querySelectorAll(".tab-btn").forEach((b,i)=>{
    b.classList.toggle("active", ["table","xg","fixtures"][i]===t);
  });
}

function formDot(r){ const c=r==="W"?"f-w":r==="D"?"f-d":"f-l"; return `<span class="f-dot ${c}">${r}</span>`; }
function pctColor(v,lo,hi){ return v>=hi?"green":v>=lo?"amber":"red"; }

function renderTable(data) {
  const sorted = [...data].sort((a,b)=>{
    let av=a[sortCol]??0, bv=b[sortCol]??0;
    return av<bv?sortDir:av>bv?-sortDir:0;
  });
  document.getElementById("standings-body").innerHTML = sorted.map((t,i)=>`
    <tr>
      <td class="pos-num muted">${i+1}</td>
      <td class="team-name">${t.team}</td>
      <td class="num muted">${t.pj}</td>
      <td class="num green">${t.pg}</td>
      <td class="num amber">${t.pe}</td>
      <td class="num red">${t.pp}</td>
      <td class="num">${t.gf}</td>
      <td class="num">${t.ga}</td>
      <td class="num ${t.gd>=0?"green":"red"}">${t.gd>=0?"+":""}${t.gd}</td>
      <td class="num" style="font-weight:600;color:var(--text)">${t.pts}</td>
      <td><div class="form-row">${(t.form||[]).map(formDot).join("")}</div></td>
      <td class="num ${pctColor(t.btts_pct,45,55)}">${t.btts_pct}%</td>
      <td class="num ${pctColor(t.over25_pct,45,55)}">${t.over25_pct}%</td>
      <td class="num ${t.xgf?"":"muted"}">${t.xgf??"-"}</td>
      <td class="num ${t.xga?"":"muted"}">${t.xga??"-"}</td>
    </tr>`).join("");

  // xG tab
  const xgSorted = [...data].filter(t=>t.xgf).sort((a,b)=>b.xgf-a.xgf);
  const maxXgf = Math.max(...xgSorted.map(t=>t.xgf||0), 0.1);
  const maxXga = Math.max(...xgSorted.map(t=>t.xga||0), 0.1);
  document.getElementById("xg-body").innerHTML = xgSorted.map((t,i)=>`
    <tr>
      <td class="pos-num muted">${i+1}</td>
      <td class="team-name">${t.team}</td>
      <td class="num green">${t.xgf}</td>
      <td><div class="xg-bar-wrap"><div class="xg-bar-fill" style="width:${t.xgf/maxXgf*120}px;background:var(--green)"></div></div></td>
      <td class="num red">${t.xga}</td>
      <td><div class="xg-bar-wrap"><div class="xg-bar-fill" style="width:${t.xga/maxXga*120}px;background:var(--red)"></div></div></td>
      <td class="num ${(t.xgf-t.xga)>=0?"green":"red"}">${(t.xgf-t.xga)>=0?"+":""}${(t.xgf-t.xga).toFixed(2)}</td>
    </tr>`).join("");
}

function renderFixtures(fixtures) {
  if(!fixtures.length){ document.getElementById("fixtures-grid").innerHTML='<div class="loading">No hay próximos partidos en el CSV</div>'; return; }
  document.getElementById("fixtures-grid").innerHTML = fixtures.map(f=>`
    <div class="fixture-row">
      <span class="fix-date">${f.date}</span>
      <span class="fix-home">${f.home}</span>
      <span class="fix-vs">${f.xg_h||"?"} — ${f.xg_a||"?"}</span>
      <span class="fix-away">${f.away}</span>
      <div class="fix-odds">
        ${f.ph?`<span class="odd-pill odd-h">${(f.ph*100).toFixed(0)}%</span>`:""}
        ${f.pd?`<span class="odd-pill odd-d">${(f.pd*100).toFixed(0)}%</span>`:""}
        ${f.pa?`<span class="odd-pill odd-a">${(f.pa*100).toFixed(0)}%</span>`:""}
        ${f.b365h?`<span class="odd-pill" style="color:var(--muted)">@${f.b365h}/${f.b365d}/${f.b365a}</span>`:""}
      </div>
    </div>`).join("");
}

function renderLeagueCards(ls) {
  document.getElementById("league-cards").innerHTML = [
    ["Partidos",ls.total_games,"white"],
    ["Goles/PJ",ls.avg_goals,"amber"],
    ["BTTS",ls.btts_pct+"%",ls.btts_pct>=50?"green":"red"],
    ["Over 2.5",ls.over25_pct+"%",ls.over25_pct>=50?"green":"red"],
    ["Local",ls.home_win_pct+"%","green"],
    ["Empate",ls.draw_pct+"%","amber"],
    ["Visitante",ls.away_win_pct+"%","red"],
  ].map(([l,v,c])=>`
    <div class="lcard">
      <div class="lcard-label">${l}</div>
      <div class="lcard-val ${c}">${v}</div>
    </div>`).join("");
}

async function loadLeague(div) {
  document.getElementById("standings-body").innerHTML = '<tr><td colspan="15" class="loading">cargando...</td></tr>';
  document.getElementById("fixtures-grid").innerHTML = '<div class="loading">cargando...</div>';
  try {
    const r = await fetch(`/api/stats?league=${div}`);
    const d = await r.json();
    if(d.error){ alert(d.error); return; }
    tableData = d.table;
    renderTable(tableData);
    renderFixtures(d.upcoming||[]);
    renderLeagueCards(d.league_stats);
  } catch(e){ alert("Error: "+e.message); }
}

document.querySelectorAll("thead th[data-col]").forEach(th=>{
  th.addEventListener("click",()=>{
    const col=th.dataset.col;
    if(sortCol===col){ sortDir*=-1; th.classList.toggle("sorted"); }
    else{ sortCol=col; sortDir=-1; document.querySelectorAll("thead th").forEach(t=>t.classList.remove("sorted")); }
    th.classList.add("sorted"); renderTable(tableData);
  });
});

loadLeague(currentLeague);
</script>
</body>
</html>
"""

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>V7.2 · Quant Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Barlow:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#07080d;--s1:#0c0e17;--s2:#11141f;
  --border:#1c2035;--border2:#252840;
  --accent:#4f6ef7;--green:#22c55e;--red:#ef4444;--amber:#f59e0b;--sky:#38bdf8;--purple:#a78bfa;
  --text:#e8eaf6;--muted:#5a5f80;--muted2:#3a3f5c;
  --mono:'IBM Plex Mono',monospace;--sans:'Barlow',sans-serif;
}
body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;overflow-x:hidden}

/* ── HEADER ── */
header{display:flex;align-items:center;gap:12px;padding:1rem 2rem;border-bottom:1px solid var(--border);background:var(--s1);position:sticky;top:0;z-index:100}
.logo-mark{width:26px;height:26px;background:var(--accent);border-radius:6px;display:grid;place-items:center;flex-shrink:0}
.logo-mark svg{width:14px;height:14px;fill:#fff}
.logo-text{font-size:.9rem;font-weight:600;letter-spacing:.02em}
.logo-sub{font-family:var(--mono);font-size:.58rem;color:var(--muted)}
.header-tabs{display:flex;gap:2px;margin-left:auto;background:var(--s2);border:1px solid var(--border);border-radius:8px;padding:2px}
.htab{font-family:var(--mono);font-size:.65rem;padding:6px 16px;border:none;background:transparent;color:var(--muted);border-radius:6px;cursor:pointer;transition:all .15s;letter-spacing:.04em}
.htab:hover{color:var(--text)}
.htab.active{background:var(--accent);color:#fff}
.live-pill{display:flex;align-items:center;gap:5px;font-family:var(--mono);font-size:.6rem;color:var(--green);background:rgba(34,197,94,.08);border:1px solid rgba(34,197,94,.2);padding:4px 10px;border-radius:99px;margin-left:12px}
.live-dot{width:5px;height:5px;border-radius:50%;background:var(--green);animation:blink 1.8s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}

/* ── TABS ── */
.tab-pane{display:none}
.tab-pane.active{display:block}

/* ── TAB 0: CALENDARIO ── */
.cal-toolbar{display:flex;align-items:center;gap:8px;padding:.85rem 2rem;background:var(--s1);border-bottom:1px solid var(--border);flex-wrap:wrap}
.league-scroll{display:flex;gap:5px;overflow-x:auto;flex:1;scrollbar-width:none}
.league-scroll::-webkit-scrollbar{display:none}
.l-chip{font-family:var(--mono);font-size:.6rem;padding:4px 11px;border:1px solid var(--border);border-radius:99px;cursor:pointer;white-space:nowrap;color:var(--muted);transition:all .15s;flex-shrink:0}
.l-chip:hover{color:var(--text)}
.l-chip.active{background:var(--accent);color:#fff;border-color:var(--accent)}
.date-nav{display:flex;gap:4px;align-items:center;flex-shrink:0}
.dday{font-family:var(--mono);font-size:.62rem;padding:4px 10px;border:1px solid var(--border);border-radius:6px;cursor:pointer;color:var(--muted);transition:all .15s}
.dday:hover{color:var(--text)}
.dday.active{background:var(--s2);color:var(--text);border-color:var(--border2)}

.cal-body{padding:1.5rem 2rem}
.league-group{margin-bottom:1.5rem}
.lg-header{display:flex;align-items:center;gap:8px;margin-bottom:.5rem}
.lg-name{font-family:var(--mono);font-size:.6rem;text-transform:uppercase;letter-spacing:.1em;color:var(--muted)}
.lg-count{font-family:var(--mono);font-size:.55rem;color:var(--muted2);background:var(--s2);border:1px solid var(--border);padding:1px 7px;border-radius:99px}

.match-row{display:grid;grid-template-columns:55px 1fr 90px 1fr 140px 90px;gap:10px;align-items:center;padding:10px 14px;border:1px solid var(--border);border-radius:8px;margin-bottom:5px;cursor:pointer;transition:all .15s;background:var(--s1)}
.match-row:hover{border-color:var(--border2);background:var(--s2)}
.match-row.has-pick{border-color:rgba(79,110,247,.3);background:rgba(79,110,247,.04)}
.mr-time{font-family:var(--mono);font-size:.62rem;color:var(--muted)}
.mr-home{text-align:right}
.mr-away{text-align:left}
.team-name{font-size:.85rem;font-weight:500}
.form-row{display:flex;gap:2px;margin-top:3px}
.form-row.right{justify-content:flex-end}
.fd{width:14px;height:14px;border-radius:2px;display:flex;align-items:center;justify-content:center;font-size:.52rem;font-weight:600}
.fw{background:rgba(34,197,94,.15);color:var(--green)}
.fl{background:rgba(239,68,68,.15);color:var(--red)}
.fdr{background:rgba(245,158,11,.15);color:var(--amber)}
.mr-probs{display:flex;gap:3px;justify-content:center}
.prob-pill{font-family:var(--mono);font-size:.6rem;padding:3px 7px;border-radius:4px;border:1px solid var(--border)}
.pp-h{color:var(--green);border-color:rgba(34,197,94,.2);background:rgba(34,197,94,.05)}
.pp-d{color:var(--amber);border-color:rgba(245,158,11,.2);background:rgba(245,158,11,.05)}
.pp-a{color:var(--red);border-color:rgba(239,68,68,.2);background:rgba(239,68,68,.05)}
.mr-pick{text-align:right}
.pick-badge{font-family:var(--mono);font-size:.58rem;padding:3px 9px;border-radius:4px;border:1px solid rgba(79,110,247,.3);color:var(--accent);background:rgba(79,110,247,.08)}
.mr-xg{font-family:var(--mono);font-size:.65rem;color:var(--muted);text-align:center}

/* Match detail expandido */
.match-detail{background:var(--s2);border:1px solid var(--border2);border-radius:10px;padding:1.25rem;margin-top:4px;margin-bottom:8px;display:none}
.match-detail.open{display:block}
.md-header{display:grid;grid-template-columns:1fr 160px 1fr;gap:1rem;align-items:center;margin-bottom:1rem}
.md-team{text-align:center}
.md-team-name{font-size:1.1rem;font-weight:600;margin-bottom:.4rem}
.md-center{text-align:center;background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:.75rem}
.md-xg{font-size:1.4rem;font-weight:600;letter-spacing:.05em;color:var(--accent)}
.prob-bar-wrap{margin:.5rem 0}
.prob-bar{display:flex;height:5px;border-radius:3px;overflow:hidden}
.pb-h{background:var(--green)}
.pb-d{background:var(--amber)}
.pb-a{background:var(--red)}
.prob-labels{display:flex;justify-content:space-between;font-family:var(--mono);font-size:.58rem;color:var(--muted);margin-top:3px}
.stats-grid{display:grid;grid-template-columns:1fr minmax(110px,auto) 1fr;border:1px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:1rem}
.sg-col{display:flex;flex-direction:column}
.sg-row{display:grid;grid-template-columns:1fr minmax(110px,auto) 1fr;border-bottom:1px solid var(--border)}
.sg-row:last-child{border-bottom:none}
.sg-val{padding:6px 12px;font-family:var(--mono);font-size:.7rem;text-align:right;background:var(--s1)}
.sg-val.left{text-align:left;background:var(--s1)}
.sg-lbl{padding:6px 10px;font-family:var(--mono);font-size:.58rem;color:var(--muted);text-align:center;background:var(--bg);white-space:nowrap}
.sg-val.winner{color:var(--green);font-weight:500}
.md-markets{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:1rem}
.mkt-card{background:var(--bg);border:1px solid var(--border);border-radius:7px;padding:.7rem;text-align:center}
.mkt-label{font-family:var(--mono);font-size:.55rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:.3rem}
.mkt-pct{font-size:1.1rem;font-weight:600}
.mkt-fair{font-family:var(--mono);font-size:.58rem;color:var(--muted);margin-top:.2rem}
.h2h-section{margin-top:.75rem}
.h2h-title{font-family:var(--mono);font-size:.58rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:.5rem}
.h2h-summary{display:flex;gap:6px;margin-bottom:.5rem;flex-wrap:wrap}
.h2h-stat{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:5px 12px;text-align:center;flex:1}
.h2h-stat-val{font-size:.9rem;font-weight:600}
.h2h-stat-lbl{font-family:var(--mono);font-size:.55rem;color:var(--muted)}
.h2h-match{display:grid;grid-template-columns:70px 1fr auto 1fr;gap:6px;align-items:center;padding:5px 8px;background:var(--bg);border-radius:5px;margin-bottom:3px;font-size:.75rem}
.h2h-score{font-family:var(--mono);font-weight:600;text-align:center;padding:0 6px}

/* ── TAB 1: PICKS JORNADA ── */
.picks-toolbar{display:flex;align-items:center;gap:8px;padding:.85rem 2rem;background:var(--s1);border-bottom:1px solid var(--border);flex-wrap:wrap}
.seg{display:flex;gap:2px;background:var(--s2);border:1px solid var(--border);border-radius:8px;padding:2px}
.seg-btn{font-family:var(--mono);font-size:.62rem;padding:5px 12px;border:none;background:transparent;color:var(--muted);border-radius:6px;cursor:pointer;transition:all .15s}
.seg-btn:hover{color:var(--text)}
.seg-btn.active{background:var(--accent);color:#fff}
.spacer{flex:1}
.resolve-btn{font-family:var(--mono);font-size:.62rem;padding:7px 14px;background:rgba(79,110,247,.1);border:1px solid rgba(79,110,247,.3);color:var(--accent);border-radius:7px;cursor:pointer}

/* Stats cards row */
.stats-strip{display:grid;grid-template-columns:repeat(8,minmax(0,1fr));border-bottom:1px solid var(--border)}
.scard{padding:1rem 1.25rem;border-right:1px solid var(--border)}
.scard:last-child{border-right:none}
.scard-label{font-family:var(--mono);font-size:.55rem;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:.4rem}
.scard-val{font-size:1.5rem;font-weight:600;line-height:1}
.scard-sub{font-family:var(--mono);font-size:.58rem;color:var(--muted);margin-top:.25rem}
.c-green{color:var(--green)}.c-red{color:var(--red)}.c-blue{color:var(--accent)}.c-amber{color:var(--amber)}.c-white{color:var(--text)}

/* Tabla de picks */
.table-wrap{overflow-x:auto;overflow-y:visible}
table{width:100%;border-collapse:collapse;font-size:.75rem}
thead th{font-family:var(--mono);font-size:.56rem;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);padding:9px 14px;text-align:left;border-bottom:1px solid var(--border);background:var(--s1);cursor:pointer;white-space:nowrap}
thead th:hover{color:var(--text)}
thead th.sorted{color:var(--accent)}
tbody tr{border-bottom:1px solid var(--border);transition:background .1s;cursor:pointer}
tbody tr:hover{background:var(--s2)}
tbody td{padding:9px 14px;font-family:var(--mono);font-size:.7rem;vertical-align:middle;white-space:nowrap}
td.party{font-family:var(--sans);font-size:.82rem;font-weight:500;white-space:normal;min-width:150px}
td.muted-td{color:var(--muted)}
.empty{text-align:center;padding:4rem;color:var(--muted);font-family:var(--mono);font-size:.75rem}

/* badges */
.badge{display:inline-flex;align-items:center;padding:2px 7px;border-radius:4px;font-size:.58rem;font-weight:500;letter-spacing:.04em}
.b-win{background:rgba(34,197,94,.1);color:var(--green);border:1px solid rgba(34,197,94,.2)}
.b-loss{background:rgba(239,68,68,.1);color:var(--red);border:1px solid rgba(239,68,68,.2)}
.b-pend{background:rgba(79,110,247,.1);color:var(--accent);border:1px solid rgba(79,110,247,.2)}
.b-under{background:rgba(56,189,248,.08);color:var(--sky);border:1px solid rgba(56,189,248,.15)}
.b-over{background:rgba(251,146,60,.08);color:#fb923c;border:1px solid rgba(251,146,60,.15)}
.b-dc{background:rgba(167,139,250,.08);color:var(--purple);border:1px solid rgba(167,139,250,.15)}
.b-1x2{background:rgba(250,204,21,.08);color:#facc15;border:1px solid rgba(250,204,21,.15)}
.b-btts{background:rgba(34,197,94,.08);color:var(--green);border:1px solid rgba(34,197,94,.15)}
.b-dnb{background:rgba(245,158,11,.08);color:var(--amber);border:1px solid rgba(245,158,11,.15)}
.pos{color:var(--green)}.neg{color:var(--red)}.neu{color:var(--muted)}
.ev-h{color:var(--green)}.ev-m{color:var(--amber)}.ev-l{color:var(--muted)}
.xg-mini{display:flex;gap:3px;align-items:center}
.xg-seg{height:4px;border-radius:2px;min-width:3px}

/* ── TAB 2: HISTORICO ── */
.hist-body{padding:1.5rem 2rem}
.section-title{font-family:var(--mono);font-size:.6rem;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:.75rem}
.chart-container{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:1.25rem;margin-bottom:1.5rem;position:relative}
.by-mkt-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:1.5rem}
.bm-card{background:var(--s1);border:1px solid var(--border);border-radius:8px;padding:1rem}
.bm-name{font-family:var(--mono);font-size:.6rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:.5rem}
.bm-br{font-size:1.3rem;font-weight:600}
.bm-n{font-family:var(--mono);font-size:.6rem;color:var(--muted);margin-top:.2rem}
.bm-form{display:flex;gap:3px;margin-top:.5rem}
.by-league-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}
.bl-card{background:var(--s1);border:1px solid var(--border);border-radius:8px;padding:.85rem}
.bl-name{font-size:.78rem;font-weight:500;margin-bottom:.4rem}
.bl-stats{font-family:var(--mono);font-size:.65rem;color:var(--muted)}

@media(max-width:900px){
  .stats-strip{grid-template-columns:repeat(4,1fr)}
  .scard{border-bottom:1px solid var(--border)}
  .match-row{grid-template-columns:45px 1fr 80px 1fr;gap:6px}
  .mr-pick,.mr-xg{display:none}
  header{padding:.75rem 1rem}
  .cal-body,.hist-body{padding:1rem}
}
</style>
</head>
<body>

<header>
  <div class="logo-mark"><svg viewBox="0 0 16 16"><path d="M2 12 L8 4 L14 12 Z"/></svg></div>
  <div><div class="logo-text">V7.2 Quant</div><div class="logo-sub">Triple League Specialist</div></div>
  <div class="header-tabs">
    <button class="htab active" onclick="showMainTab(0,this)">Calendario</button>
    <button class="htab" onclick="showMainTab(1,this)">Jornada</button>
    <button class="htab" onclick="showMainTab(2,this)">Historial</button>
    <button class="htab" onclick="showMainTab(4,this)">📈 Líneas</button>
    <button class="htab" onclick="showMainTab(3,this)">Dashboard</button>
  </div>
  <div class="live-pill"><span class="live-dot"></span><span id="last-upd">live</span></div>
</header>

<!-- ══════════ TAB 0: CALENDARIO ══════════ -->
<div id="tab0" class="tab-pane active">
  <div class="cal-toolbar">
    <div class="league-scroll" id="league-chips"></div>
    <div class="date-nav" id="date-nav"></div>
  </div>
  <div class="cal-body" id="cal-body"><div class="empty">cargando partidos...</div></div>
</div>

<!-- ══════════ TAB 1: PICKS JORNADA ══════════ -->
<div id="tab1" class="tab-pane">
  <!-- JORNADA: solo picks PENDING -->
  <div class="picks-toolbar" style="border-bottom:1px solid var(--border);padding:.75rem 1.5rem;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
    <span style="font-family:var(--mono);font-size:.6rem;color:var(--muted);text-transform:uppercase;letter-spacing:.08em">Picks activos — pendientes de resolución</span>
    <div class="spacer"></div>
    <div class="seg" id="mkt-seg-j">
      <button class="seg-btn active" data-m="all">Todos</button>
      <button class="seg-btn" data-m="UNDER">Under</button>
      <button class="seg-btn" data-m="OVER">Over</button>
      <button class="seg-btn" data-m="DC">DC</button>
      <button class="seg-btn" data-m="1X2">1X2</button>
      <button class="seg-btn" data-m="BTTS">BTTS</button>
      <button class="seg-btn" data-m="DNB">DNB</button>
    </div>
    <button class="resolve-btn" onclick="resolveData()">resolver picks</button>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Fecha</th><th>Liga</th><th>Partido</th><th>Mkt</th>
        <th>Cuota</th><th>EV</th><th>Prob</th><th>Stake</th><th>xG</th>
      </tr></thead>
      <tbody id="jornada-body"><tr><td colspan="9" class="empty">cargando...</td></tr></tbody>
    </table>
  </div>
</div>

<!-- ══════════ TAB 2: HISTORIAL RESUELTOS ══════════ -->
<div id="tab2" class="tab-pane">
  <!-- ══ HISTORIAL: picks resueltos con CLV ══ -->
  <div class="stats-strip" id="stats-strip">
    <div class="scard"><div class="scard-label">Picks totales</div><div class="scard-val c-white" id="s-total">—</div><div class="scard-sub" id="s-sub-total"></div></div>
    <div class="scard"><div class="scard-label">Win</div><div class="scard-val c-green" id="s-win">—</div><div class="scard-sub" id="s-sub-win"></div></div>
    <div class="scard"><div class="scard-label">Loss</div><div class="scard-val c-red" id="s-loss">—</div><div class="scard-sub" id="s-sub-loss"></div></div>
    <div class="scard"><div class="scard-label">Pending</div><div class="scard-val c-blue" id="s-pend">—</div><div class="scard-sub">esperando</div></div>
    <div class="scard"><div class="scard-label">Beat Rate</div><div class="scard-val" id="s-br">—</div><div class="scard-sub">mín 52%</div></div>
    <div class="scard"><div class="scard-label">Avg EV</div><div class="scard-val c-blue" id="s-ev">—</div><div class="scard-sub">apertura</div></div>
    <div class="scard"><div class="scard-label">PnL (u)</div><div class="scard-val" id="s-pnl">—</div><div class="scard-sub">unidades</div></div>
    <div class="scard"><div class="scard-label">Burn-in</div><div class="scard-val" id="s-burn">—</div><div class="scard-sub" id="s-burn-sub">picks</div></div>
  </div>
  <div class="picks-toolbar">
    <div class="seg" id="flt-seg">
      <button class="seg-btn active" data-f="all">Todos</button>
      <button class="seg-btn" data-f="WIN">Win</button>
      <button class="seg-btn" data-f="LOSS">Loss</button>
      <button class="seg-btn" data-f="PENDING">Pending</button>
    </div>
    <div class="seg" id="mkt-seg-h">
      <button class="seg-btn active" data-m="all">Mercados</button>
      <button class="seg-btn" data-m="UNDER">Under</button>
      <button class="seg-btn" data-m="OVER">Over</button>
      <button class="seg-btn" data-m="DC">DC</button>
      <button class="seg-btn" data-m="1X2">1X2</button>
    </div>
    <div class="spacer"></div>
    <input id="picks-search-h" type="text" placeholder="buscar equipo..." style="font-family:var(--mono);font-size:.68rem;padding:7px 12px;background:var(--s2);border:1px solid var(--border);color:var(--text);border-radius:7px;outline:none;width:180px">
    <button class="resolve-btn" onclick="resolveData()">resolver picks</button>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Fecha</th><th>Liga</th><th>Partido</th><th>Mkt</th>
        <th>Cuota</th><th>EV</th>
        <th style="color:var(--amber)" title="CLV vs B365 cierre">CLV B365</th>
        <th style="color:var(--green)" title="CLV vs Pinnacle">CLV PS ★</th>
        <th>xG</th><th>Resultado</th><th>Profit</th>
      </tr></thead>
      <tbody id="hist-body"><tr><td colspan="11" class="empty">cargando...</td></tr></tbody>
    </table>
  </div>
</div>

<!-- ══════════ TAB 3: DASHBOARD ══════════ -->
<div id="tab3" class="tab-pane">
  <div class="hist-body">
    <p class="section-title">PnL acumulado</p>
    <div class="chart-container">
      <canvas id="pnl-chart" height="120"></canvas>
    </div>
    <p class="section-title">por mercado</p>
    <div class="by-mkt-grid" id="by-mkt-grid"></div>
    <p class="section-title">por liga</p>
    <div class="by-league-grid" id="by-league-grid"></div>
  </div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<div id="tab4" class="tab-pane">
  <div style="padding:1rem 1.2rem">
    <div style="font-family:var(--mono);font-size:.63rem;color:var(--muted);margin-bottom:.75rem">
      Movimientos de línea Pinnacle — últimas 48h &nbsp;|&nbsp;
      🔥 Sharp confirma pick &nbsp;|&nbsp; ⚠️ Sharp en contra &nbsp;|&nbsp;
      Verde=cuota bajó (equipo más favorecido) · Rojo=cuota subió
    </div>
    <div id="lines-body" style="font-family:var(--mono);font-size:.65rem;color:var(--muted)">
      Selecciona esta pestaña para cargar...
    </div>
  </div>
</div>

<script>
// ── STATE ──
let allPicks=[], calMatches=[], sortCol='date', sortDir=-1;
let activeFlt='all', activeMkt='all', activeLeague='all', activeDate='';
let pnlChart=null;

// ── TAB SWITCHING ──
function showMainTab(i,btn){
  document.querySelectorAll('.tab-pane').forEach((p,j)=>p.classList.toggle('active',j===i));
  document.querySelectorAll('.htab').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  if(i===1){ loadPicks().then(renderJornada); }
  if(i===2){ loadPicks().then(renderHistorial); }
  if(i===3){ if(!allPicks.length) loadPicks().then(renderHistorico); else renderHistorico(); }
  if(i===4){ loadLineMovements(); }
}

async function loadLineMovements(){
  const body = document.getElementById('lines-body');
  body.innerHTML = '<span style="color:var(--muted)">Cargando...</span>';
  try {
    const r = await fetch('/api/line_moves');
    const data = await r.json();
    const moves = (data.moves||[]);
    if(!moves.length){
      body.innerHTML = '<div style="padding:1rem;color:var(--muted)">Sin movimientos significativos en las últimas 48h.<br>El historial se construye con cada fetch de Pinnacle (cada hora).</div>';
      return;
    }
    const sigLabel = {sharp_home:'🔥 SHARP LOCAL',sharp_away:'🔥 SHARP VISITA',sharp_draw:'🔥 SHARP EMPATE',steam:'⚡ STEAM',none:'→ Estable'};
    const fmtMove = v => {
      if(v===null||v===undefined) return '<span style="color:var(--muted)">—</span>';
      const c = v<-4?'#22c55e':v>4?'#ef4444':'var(--fg)';
      return `<span style="color:${c}">${v>0?'+':''}${v.toFixed(1)}%</span>`;
    };
    let h = `<table style="width:100%;border-collapse:collapse">
      <thead><tr style="color:var(--muted);border-bottom:1px solid var(--s3)">
        <th style="text-align:left;padding:6px 8px">Partido</th>
        <th style="text-align:left;padding:6px 4px">Liga</th>
        <th style="text-align:right;padding:6px 4px">Local Δ</th>
        <th style="text-align:right;padding:6px 4px">Empate Δ</th>
        <th style="text-align:right;padding:6px 4px">Visita Δ</th>
        <th style="text-align:right;padding:6px 4px">DNB_L Δ</th>
        <th style="text-align:right;padding:6px 4px">DNB_V Δ</th>
        <th style="text-align:left;padding:6px 8px">Señal</th>
        <th style="text-align:right;padding:6px 4px">Snaps</th>
      </tr></thead><tbody>`;
    for(const m of moves){
      const bg = m.signal.startsWith('sharp')?'rgba(34,197,94,.05)':m.signal==='steam'?'rgba(245,158,11,.05)':'';
      const sc = m.signal.startsWith('sharp')?'#22c55e':m.signal==='steam'?'#f59e0b':'var(--muted)';
      h += `<tr style="border-bottom:1px solid var(--s2);background:${bg}">
        <td style="padding:7px 8px;font-weight:600">${m.home} <span style="color:var(--muted)">vs</span> ${m.away}</td>
        <td style="padding:7px 4px;color:var(--muted)">${m.league}</td>
        <td style="padding:7px 4px;text-align:right">${fmtMove(m.move_h)}</td>
        <td style="padding:7px 4px;text-align:right">${fmtMove(m.move_d)}</td>
        <td style="padding:7px 4px;text-align:right">${fmtMove(m.move_a)}</td>
        <td style="padding:7px 4px;text-align:right">${fmtMove(m.move_dnb_h)}</td>
        <td style="padding:7px 4px;text-align:right">${fmtMove(m.move_dnb_a)}</td>
        <td style="padding:7px 8px;color:${sc};font-weight:700">${sigLabel[m.signal]||m.signal}</td>
        <td style="padding:7px 4px;text-align:right;color:var(--muted)">${m.snaps}</td>
      </tr>`;
    }
    h += `</tbody></table><div style="padding:.4rem .5rem 0;color:var(--muted);font-size:.58rem">${moves.length} partidos · verde=cuota bajó (sharp money) · rojo=cuota subió · snaps=número de snapshots</div>`;
    body.innerHTML = h;
  } catch(e){ body.innerHTML = `<div style="color:#ef4444;padding:1rem">Error: ${e.message}</div>`; }
}

// ════════════════════════════════════════
// TAB 0: CALENDARIO
// ════════════════════════════════════════
const LEAGUES = {
  "E0":"🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier","E1":"🏴󠁧󠁢󠁥󠁮󠁧󠁿 Championship","E2":"🏴󠁧󠁢󠁥󠁮󠁧󠁿 League One",
  "SP1":"🇪🇸 La Liga","SP2":"🇪🇸 Segunda",
  "D1":"🇩🇪 Bundesliga","D2":"🇩🇪 Bundesliga 2",
  "I1":"🇮🇹 Serie A","I2":"🇮🇹 Serie B",
  "F1":"🇫🇷 Ligue 1","F2":"🇫🇷 Ligue 2",
  "N1":"🇳🇱 Eredivisie","P1":"🇵🇹 Primeira",
  "B1":"🇧🇪 Jupiler","SC0":"🏴󠁧󠁢󠁳󠁣󠁴 Premiership",
  "T1":"🇹🇷 Süper Lig","G1":"🇬🇷 Super League",
  // Copas europeas y nacionales
  "CUP_2":"🏆 UCL","CUP_3":"🥈 UEL","CUP_848":"🥉 UECL",
  "CUP_45":"🏴󠁧󠁢󠁥󠁮󠁧󠁿 FA Cup","CUP_143":"🇪🇸 Copa Rey","CUP_137":"🇮🇹 Coppa Italia",
};

function fd(r){
  const c=r==='W'?'fw':r==='D'?'fdr':'fl';
  return `<span class="fd ${c}">${r}</span>`;
}

function renderCalendar(){
  const body=document.getElementById('cal-body');
  let matches=calMatches.filter(m=>{
    if(activeDate && m.date!==activeDate) return false;
    if(activeLeague!=='all' && m.div!==activeLeague) return false;
    return true;
  });
  if(!matches.length){body.innerHTML='<div class="empty">Sin partidos para este filtro</div>';return;}

  // Agrupar por liga
  const byLeague={};
  matches.forEach(m=>{
    const key=m.div;
    if(!byLeague[key]) byLeague[key]=[];
    byLeague[key].push(m);
  });

  body.innerHTML=Object.entries(byLeague).map(([div,ms])=>`
    <div class="league-group">
      <div class="lg-header">
        <span class="lg-name">${LEAGUES[div]||div}</span>
        <span class="lg-count">${ms.length} partidos</span>
      </div>
      ${ms.map(m=>matchRowHTML(m)).join('')}
    </div>`).join('');

  // Event listeners para expandir
  document.querySelectorAll('.match-row').forEach(row=>{
    row.addEventListener('click',()=>{
      const det=row.nextElementSibling;
      if(det && det.classList.contains('match-detail')){
        det.classList.toggle('open');
        if(det.classList.contains('open') && !det.dataset.loaded){
          det.dataset.loaded='1';
          const matchData = row.dataset.match ? JSON.parse(row.dataset.match) : {};
          loadMatchDetail(det, row.dataset.home, row.dataset.away, row.dataset.div, matchData);
        }
      }
    });
  });
}

function matchRowHTML(m){
  const hasPick=!!m.pick;
  const ph=m.ph?`${(m.ph*100).toFixed(0)}%`:'—';
  const pd=m.pd?`${(m.pd*100).toFixed(0)}%`:'—';
  const pa=m.pa?`${(m.pa*100).toFixed(0)}%`:'—';
  const xg=m.xg_h&&m.xg_a?`${m.xg_h}—${m.xg_a}`:'';
  const pickBadge=hasPick?`<span class="pick-badge">🎯 ${m.pick.market} @${toUS(m.pick.odd)}</span>`:'';
  const fh=(m.form_h||[]).map(fd).join('');
  const fa=(m.form_a||[]).map(fd).join('');
  // Pinnacle live odds
  const pinH=m.pin_h||null, pinA=m.pin_a||null;
  const pinHus=pinH?toUS(pinH):null, pinAus=pinA?toUS(pinA):null;
  const fairH=m.ph>0?1/m.ph:null, fairA=m.pa>0?1/m.pa:null;
  const hasValueH=!!(pinH&&fairH&&pinH>fairH);
  const hasValueA=!!(pinA&&fairA&&pinA>fairA);
  return `
    <div class="match-row${hasPick?' has-pick':''}" data-home="${m.home}" data-away="${m.away}" data-div="${m.div}" data-match='${JSON.stringify({b365h:m.b365h,b365d:m.b365d,b365a:m.b365a,rest_h:m.rest_h,rest_a:m.rest_a,home_pos:m.home_pos,away_pos:m.away_pos,n_teams:m.n_teams,ph:m.ph,pd:m.pd,pa:m.pa,xg_h:m.xg_h,xg_a:m.xg_a,pin_h:m.pin_h,pin_d:m.pin_d,pin_a:m.pin_a,pin_over:m.pin_over,pin_under:m.pin_under,odds_updated:m.odds_updated})}'>
      <div class="mr-time">${m.time||m.date?.slice(5)||''}</div>
      <div class="mr-home">
        <div class="team-name">${m.home}</div>
        <div class="form-row right">${fh}</div>
      </div>
      <div class="mr-probs">
        <span class="prob-pill pp-h">${ph}</span>
        <span class="prob-pill pp-d">${pd}</span>
        <span class="prob-pill pp-a">${pa}</span>
      </div>
      <div class="mr-away">
        <div class="team-name">${m.away}</div>
        <div class="form-row">${fa}</div>
      </div>
      <div class="mr-pick">${pickBadge}</div>
      <div class="mr-xg">
        ${m.home_pos&&m.away_pos?`<span style="font-family:var(--mono);font-size:.55rem;color:var(--muted)">#${m.home_pos} vs #${m.away_pos}</span><br>`:''}
        ${xg}
        ${pinH?`<div style="margin-top:3px;font-size:.58rem;font-family:var(--mono);display:flex;gap:4px;flex-wrap:wrap">
          <span style="padding:1px 5px;border-radius:3px;background:${hasValueH?'#22c55e22':'#ffffff0a'};color:${hasValueH?'#22c55e':'#64748b'}">${hasValueH?'▲ ':''}${pinHus}</span>
          ${pinA?`<span style="padding:1px 5px;border-radius:3px;background:${hasValueA?'#22c55e22':'#ffffff0a'};color:${hasValueA?'#22c55e':'#64748b'}">${hasValueA?'▲ ':''}${pinAus}</span>`:''}
        </div>`:''}
        ${(m.rest_h||m.rest_a)?`<div style="font-family:var(--mono);font-size:.52rem;margin-top:2px">
          <span style="color:${m.rest_h<=4?'var(--red)':m.rest_h>7?'var(--green)':'var(--muted)'}">${m.rest_h?m.rest_h+'d':'-'}</span>
          <span style="color:var(--muted)">vs</span>
          <span style="color:${m.rest_a<=4?'var(--red)':m.rest_a>7?'var(--green)':'var(--muted)'}">${m.rest_a?m.rest_a+'d':'-'}</span>
        </div>`:''}
      </div>
    </div>
    <div class="match-detail" data-home="${m.home}" data-away="${m.away}" data-div="${m.div}" data-match='${JSON.stringify({b365h:m.b365h,b365d:m.b365d,b365a:m.b365a,rest_h:m.rest_h,rest_a:m.rest_a,home_pos:m.home_pos,away_pos:m.away_pos,n_teams:m.n_teams,ph:m.ph,pd:m.pd,pa:m.pa,xg_h:m.xg_h,xg_a:m.xg_a,pin_h:m.pin_h,pin_d:m.pin_d,pin_a:m.pin_a,pin_over:m.pin_over,pin_under:m.pin_under,odds_updated:m.odds_updated})}'>
      <div style="font-family:var(--mono);font-size:.65rem;color:var(--muted)">cargando análisis...</div>
    </div>`;
}

async function loadMatchDetail(detEl, home, away, div, m={}){
  try{
    const r=await fetch(`/api/analyze?home=${encodeURIComponent(home)}&away=${encodeURIComponent(away)}&div=${encodeURIComponent(div)}`);
    const d=await r.json();
    if(d.error){detEl.innerHTML=`<div style="color:var(--muted);font-family:var(--mono);font-size:.7rem">${d.error}</div>`;return;}
    const hs=d.home_stats, as_=d.away_stats;
    const hh=hs.home, ah=as_.away;
    function pct(v){return v!=null?`${(v*100).toFixed(1)}%`:'—';}
    function fair(v){return v?`${fmtOddS(v)}<span style='opacity:.5;font-size:.8em'> fair</span>`:'' }
    function statRow(lbl,hv,av,hib=true){
      const hN=parseFloat(hv)||0, aN=parseFloat(av)||0;
      const hW=hib?(hN>aN):(hN<aN&&hN!==aN);
      const aW=hib?(aN>hN):(aN<hN&&hN!==aN);
      return `<div class="sg-row">
        <div class="sg-val${hW?' winner':''}">${hv}</div>
        <div class="sg-lbl">${lbl}</div>
        <div class="sg-val left${aW?' winner':''}">${av}</div>
      </div>`;
    }
    let html = '';
    // ── RECOMENDACIÓN DE APUESTA ──────────────────────────────────────
    const rec = d.bet_rec;
    if(rec && rec.has_rec){
      const top = rec.top;
      const strColors = {'FUERTE':'#22c55e','BUENA':'#86efac','MODERADA':'#f59e0b','CONSERVADORA':'#94a3b8'};
      const strColor = strColors[top.strength] || '#94a3b8';
      html += `<div style="margin:1rem 0;border-radius:12px;overflow:hidden;border:1px solid ${strColor}44">
        <div style="background:${strColor}18;padding:.7rem 1rem;display:flex;align-items:center;gap:8px">
          <span style="font-size:1.1rem">${top.emoji||'📌'}</span>
          <span style="font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--text)">Recomendación del modelo</span>
          <span style="margin-left:auto;font-family:var(--mono);font-size:.6rem;padding:2px 8px;border-radius:4px;background:${strColor}33;color:${strColor};font-weight:700">${top.strength}</span>
        </div>
        <div style="padding:1rem">
          <div style="font-size:1.05rem;font-weight:800;color:${strColor};margin-bottom:.3rem">
            ${top.selection} <span style="font-family:var(--mono);font-size:.75rem;color:var(--muted);font-weight:400">${fmtOddS(top.fair_odd)}<span style="opacity:.5"> fair</span></span>
          </div>
          <div style="font-family:var(--mono);font-size:.65rem;color:var(--muted);margin-bottom:.75rem;line-height:1.6">${top.reason}</div>

          <!-- REGLA DEL CLV — la más importante -->
          <div style="background:var(--s1);border-radius:8px;padding:.6rem .85rem;margin-bottom:.75rem;border-left:3px solid ${strColor}">
            <div style="font-family:var(--mono);font-size:.6rem;color:var(--muted);margin-bottom:.2rem">CÓMO APOSTARLO</div>
            <div style="font-family:var(--mono);font-size:.72rem;color:var(--text);font-weight:600">${top.clv_rule}</div>
            <div style="font-family:var(--mono);font-size:.6rem;color:var(--muted);margin-top:.3rem">El mercado tiene 66% accuracy → si la cuota bajó (mercado confirmó) → apuesta.</div>
          </div>

          <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <div style="font-family:var(--mono);font-size:.72rem;padding:5px 14px;background:${strColor};color:#000;border-radius:6px;font-weight:800">
              ${top.stake_pct}% bankroll
            </div>
            <div style="font-family:var(--mono);font-size:.62rem;color:var(--muted)">${top.action}</div>
          </div>

          ${rec.alternatives && rec.alternatives.length ? `
          <div style="margin-top:.7rem;padding-top:.6rem;border-top:1px solid var(--border)">
            <div style="font-family:var(--mono);font-size:.58rem;color:var(--muted);margin-bottom:.3rem">ALTERNATIVAS</div>
            <div style="display:flex;gap:6px;flex-wrap:wrap">
              ${rec.alternatives.map(a=>`<span style="font-family:var(--mono);font-size:.62rem;padding:2px 8px;background:var(--s1);border-radius:4px;color:var(--text)">${a.selection} @${a.fair_odd} (${a.prob}%)</span>`).join('')}
            </div>
          </div>` : ''}

          ${rec.tip ? `<div style="margin-top:.6rem;font-family:var(--mono);font-size:.6rem;color:var(--accent);line-height:1.5">${rec.tip}</div>` : ''}
          <button id="btn-log-pick"
            onclick="logPick(this,'${encodeURIComponent(JSON.stringify({
              home:d.home,away:d.away,div:d.div||'',league:d.league||'',
              market:top.market,selection:top.selection,
              odd:top.fair_odd,prob:top.prob/100
            }))}',${d.xg_home||0},${d.xg_away||0},${top.prob/100},${top.fair_odd})"
            style="margin-top:.75rem;width:100%;padding:9px;background:var(--accent);color:#fff;border:none;border-radius:8px;font-family:var(--mono);font-size:.72rem;font-weight:700;cursor:pointer;letter-spacing:.03em;transition:opacity .15s">
            📝 Registrar Pick en historial
          </button>
        </div>
      </div>`;
    } else if(rec && !rec.has_rec){
      html += `<div style="margin:.5rem 0;padding:.7rem 1rem;background:var(--s2);border-radius:8px;border-left:3px solid var(--muted);font-family:var(--mono);font-size:.65rem;color:var(--muted);line-height:1.6">${rec.message}</div>`;
    }

    // ── RESULTADO DE IDA (copa, doble partido) ───────────────────────
    if(d.leg1 && d.leg1.home_goals !== null){
      const l1h = d.leg1.home_goals, l1a = d.leg1.away_goals;
      const agg_h = l1h, agg_a = l1a; // ida ya es desde perspectiva del home actual
      // Situación para la vuelta
      let situation = '';
      if(l1h > l1a) situation = `<span style="color:var(--green)">Gana ${d.home} ${l1h}-${l1a} en el global</span>`;
      else if(l1h < l1a) situation = `<span style="color:var(--green)">Gana ${d.away} ${l1a}-${l1h} en el global</span>`;
      else situation = `<span style="color:var(--amber)">Empate ${l1h}-${l1a} — decide el de visitante</span>`;

      // DC y DNB ajustados al agregado
      const ph = d.probs?.home||0, pa = d.probs?.away||0, pd_ = d.probs?.draw||0;
      const dc_h = ph + pd_; // DC local (gana o empata en 90min)
      const fair_dc = dc_h > 0 ? toUS(1/dc_h) : '—';
      const fair_dnb = ph > 0 ? toUS(1/ph) : '—';

      html += `<div style="margin:.5rem 0 1rem;padding:.85rem 1rem;background:var(--s2);border-radius:10px;border-left:3px solid var(--amber)">
        <div style="font-family:var(--mono);font-size:.58rem;color:var(--muted);margin-bottom:.4rem;text-transform:uppercase;letter-spacing:.06em">⚽ Doble partido — Resultado Ida</div>
        <div style="font-size:1rem;font-weight:700;margin-bottom:.3rem">${d.home} ${l1h} – ${l1a} ${d.away}</div>
        <div style="font-family:var(--mono);font-size:.68rem;margin-bottom:.6rem">${situation}</div>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          ${l1h < l1a || l1h === l1a ? `<div style="font-family:var(--mono);font-size:.65rem;padding:4px 10px;background:var(--s1);border-radius:5px">
            DC local ${fmtOddS(fair_dc)} — ${d.home} gana o empata en 90min → pasa en global
          </div>` : ''}
          ${l1h < l1a ? `<div style="font-family:var(--mono);font-size:.65rem;padding:4px 10px;background:var(--s1);border-radius:5px">
            DNB local ${fmtOddS(fair_dnb)} — ${d.home} necesita ganar (sin devolución en empate)
          </div>` : ''}
          ${l1h > l1a ? `<div style="font-family:var(--mono);font-size:.65rem;padding:4px 10px;background:var(--s1);border-radius:5px">
            DC visita ${fmtOddS(1/(pa+pd_))} — ${d.away} remonta empata o gana
          </div>` : ''}
        </div>
      </div>`;
    }

    const h2h=d.h2h||{};
    detEl.innerHTML= html + `
      <div class="md-header">
        <div class="md-team">
          <div class="md-team-name">${d.home}</div>
          <div class="form-row" style="justify-content:center">${(hs.overall?.form||[]).map(fd).join('')}</div>
          <div style="font-family:var(--mono);font-size:.62rem;color:var(--muted);margin-top:.3rem">PPG ${hs.overall?.ppg||'—'}</div>
        </div>
        <div class="md-center">
          <div style="font-family:var(--mono);font-size:.55rem;color:var(--muted);margin-bottom:.2rem">
            xG estimado
            ${d.xg_source==='fbref'?'<span style="color:var(--green);font-size:.5rem;margin-left:4px">● FBref real</span>':'<span style="color:var(--muted2);font-size:.5rem;margin-left:4px">● proxy</span>'}
          </div>
          <div class="md-xg">${d.xg_home} — ${d.xg_away}</div>
          <div class="prob-bar-wrap">
            <div class="prob-bar">
              <div class="pb-h" style="width:${(d.probs.home*100).toFixed(0)}%"></div>
              <div class="pb-d" style="width:${(d.probs.draw*100).toFixed(0)}%"></div>
              <div class="pb-a" style="width:${(d.probs.away*100).toFixed(0)}%"></div>
            </div>
            <div class="prob-labels"><span>${pct(d.probs.home)}</span><span>${pct(d.probs.draw)}</span><span>${pct(d.probs.away)}</span></div>
          </div>
          ${(()=>{
            // VALUE vs MERCADO
            const fo=d.fair_odds||{};
            const bh=m.b365h, bd=m.b365d, ba=m.b365a;
            if(!bh) return '';
            function ev(fair,book){if(!fair||!book)return null;return ((book/fair)-1)*100;}
            const evH=ev(fo.home,bh), evD=ev(fo.draw,bd), evA=ev(fo.away,ba);
            function pill(label,evVal,odd){
              if(evVal===null) return '';
              const c=evVal>3?'var(--green)':evVal>0?'#86efac':evVal>-3?'var(--amber)':'var(--red)';
              const sign=evVal>0?'+':'';
              return `<span style="font-family:var(--mono);font-size:.58rem;padding:2px 7px;border-radius:4px;border:1px solid ${c}22;background:${c}11;color:${c}">${label} @${odd} ${sign}${evVal.toFixed(1)}%</span>`;
            }
            return `<div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:.5rem;justify-content:center">
              ${pill('L',evH,bh)}${pill('E',evD,bd)}${pill('V',evA,ba)}
            </div>`;
          })()}
          ${d.home_pos?.pos?`<div style="font-family:var(--mono);font-size:.58rem;color:var(--muted);margin-top:.4rem">${d.home} #${d.home_pos.pos} · ${d.away} #${d.away_pos?.pos||'?'} de ${d.home_pos.total}</div>`:''}
        </div>
        <div class="md-team">
          <div class="md-team-name">${d.away}</div>
          <div class="form-row" style="justify-content:center">${(as_.overall?.form||[]).map(fd).join('')}</div>
          <div style="font-family:var(--mono);font-size:.62rem;color:var(--muted);margin-top:.3rem">PPG ${as_.overall?.ppg||'—'}</div>
        </div>
      </div>
      <div class="stats-grid">
        ${m.rest_h||m.rest_a?`
        <div class="sg-row" style="background:rgba(79,110,247,.04)">
          <div class="sg-val" style="color:${m.rest_h<=4?'var(--red)':m.rest_h>7?'var(--green)':'var(--text)'}">${m.rest_h?m.rest_h+' días':'-'}</div>
          <div class="sg-lbl">Descanso</div>
          <div class="sg-val left" style="color:${m.rest_a<=4?'var(--red)':m.rest_a>7?'var(--green)':'var(--text)'}">${m.rest_a?m.rest_a+' días':'-'}</div>
        </div>`:''}
        ${statRow('PPG overall', hs.overall?.ppg||'—', as_.overall?.ppg||'—')}
        ${statRow('PPG local/visit', hs.home?.ppg||'—', as_.away?.ppg||'—')}
        ${statRow('Win%', (hs.overall?.win_pct||'—')+'%', (as_.overall?.win_pct||'—')+'%')}
        ${statRow('Goles/PJ', hs.home?.avg_scored||'—', as_.away?.avg_scored||'—')}
        ${statRow('Conc/PJ', hs.home?.avg_conceded||'—', as_.away?.avg_conceded||'—', false)}
        ${statRow('BTTS%', (hs.home?.btts_pct||hs.overall?.btts_pct||'—')+'%', (as_.away?.btts_pct||as_.overall?.btts_pct||'—')+'%')}
        ${statRow('CS%', (hs.home?.cs_pct||hs.overall?.cs_pct||'—')+'%', (as_.away?.cs_pct||as_.overall?.cs_pct||'—')+'%')}
        ${statRow('FTS%', (hs.home?.fts_pct||hs.overall?.fts_pct||'—')+'%', (as_.away?.fts_pct||as_.overall?.fts_pct||'—')+'%', false)}
        ${statRow('Over 1.5%', (hs.overall?.over15_pct||'—')+'%', (as_.overall?.over15_pct||'—')+'%')}
        ${statRow('Over 2.5%', (hs.home?.over25_pct||hs.overall?.over25_pct||'—')+'%', (as_.away?.over25_pct||as_.overall?.over25_pct||'—')+'%')}
        ${statRow('Over 3.5%', (hs.overall?.over35_pct||'—')+'%', (as_.overall?.over35_pct||'—')+'%')}
      </div>
      <div class="md-markets">
        <div class="mkt-card"><div class="mkt-label">Over 2.5</div><div class="mkt-pct" style="color:var(--sky)">${pct(d.ou.over)}</div><div class="mkt-fair">${fair(d.fair_odds.over)}</div></div>
        <div class="mkt-card"><div class="mkt-label">Under 2.5</div><div class="mkt-pct" style="color:var(--purple)">${pct(d.ou.under)}</div><div class="mkt-fair">${fair(d.fair_odds.under)}</div></div>
        <div class="mkt-card"><div class="mkt-label">BTTS Sí</div><div class="mkt-pct" style="color:var(--green)">${pct(d.btts.yes)}</div><div class="mkt-fair">${fair(d.fair_odds.btts_y)}</div></div>
        <div class="mkt-card"><div class="mkt-label">BTTS No</div><div class="mkt-pct" style="color:var(--red)">${pct(d.btts.no)}</div><div class="mkt-fair"></div></div>
      </div>
      ${(()=>{
        // RACHA O/U últimos 5
        function ouRow(label, streak){
          if(!streak||!streak.length) return '';
          const dots=streak.map(s=>{
            const c=s.result==='O'?'var(--sky)':'var(--purple)';
            return `<span style="display:inline-flex;flex-direction:column;align-items:center;gap:1px">
              <span style="width:22px;height:22px;border-radius:4px;background:${c}22;border:1px solid ${c}44;color:${c};font-family:var(--mono);font-size:.58rem;font-weight:600;display:flex;align-items:center;justify-content:center">${s.result}</span>
              <span style="font-family:var(--mono);font-size:.48rem;color:var(--muted)">${s.goals}</span>
            </span>`;
          }).join('');
          const overs=streak.filter(s=>s.result==='O').length;
          const btts_c=streak.filter(s=>s.btts).length;
          return `<div style="display:flex;align-items:center;gap:10px;padding:6px 0">
            <span style="font-family:var(--mono);font-size:.6rem;color:var(--muted);width:70px">${label}</span>
            <div style="display:flex;gap:4px">${dots}</div>
            <span style="font-family:var(--mono);font-size:.6rem;color:var(--muted);margin-left:4px">${overs}/5 Over · ${btts_c}/5 BTTS</span>
          </div>`;
        }
        const homeOu=d.home_ou||[]; const awayOu=d.away_ou||[];
        if(!homeOu.length&&!awayOu.length) return '';
        return `<div style="background:var(--s2);border:1px solid var(--border);border-radius:8px;padding:.75rem 1rem;margin-top:.75rem">
          <div style="font-family:var(--mono);font-size:.58rem;color:var(--muted);letter-spacing:.08em;text-transform:uppercase;margin-bottom:.5rem">Racha Over/Under — últimos 5</div>
          ${ouRow(d.home, homeOu)}
          ${ouRow(d.away, awayOu)}
        </div>`;
      })()}
      <div style="display:flex;gap:5px;flex-wrap:wrap;margin-top:.5rem">
        ${(()=>{
          if(!m.b365h||!d.probs) return '';
          function ev(p,odd){return p>0?((odd*p-1)*100):null;}
          function pill(lbl,p,odd){
            if(!odd||!p) return '';
            const e=ev(p,odd);
            const c=e>3?'var(--green)':e>0?'#86efac':e>-3?'var(--amber)':'var(--red)';
            return `<span style="font-family:var(--mono);font-size:.6rem;padding:3px 8px;border-radius:5px;border:1px solid ${c}33;color:${c};background:${c}0d">${lbl} @${odd} ${e>0?'+':''}${e.toFixed(1)}% EV</span>`;
          }
          return pill('Local',d.probs.home,m.b365h)+pill('Empate',d.probs.draw,m.b365d)+pill('Visitante',d.probs.away,m.b365a);
        })()}
      </div>
      ${h2h.total>0?`
      <div class="h2h-section">
        <div class="h2h-title">Head to Head — ${h2h.total} enfrentamientos</div>
        <div class="h2h-summary">
          <div class="h2h-stat"><div class="h2h-stat-val c-green">${h2h.home_wins}</div><div class="h2h-stat-lbl">${d.home.split(' ').pop()}</div></div>
          <div class="h2h-stat"><div class="h2h-stat-val c-amber">${h2h.draws}</div><div class="h2h-stat-lbl">empates</div></div>
          <div class="h2h-stat"><div class="h2h-stat-val c-red">${h2h.away_wins}</div><div class="h2h-stat-lbl">${d.away.split(' ').pop()}</div></div>
          <div class="h2h-stat"><div class="h2h-stat-val" style="color:var(--sky)">${h2h.over25}/${h2h.total}</div><div class="h2h-stat-lbl">over 2.5</div></div>
          <div class="h2h-stat"><div class="h2h-stat-val" style="color:var(--purple)">${h2h.btts}/${h2h.total}</div><div class="h2h-stat-lbl">btts</div></div>
        </div>
        ${(h2h.matches||[]).slice(0,5).map(m=>`
          <div class="h2h-match">
            <span style="color:var(--muted);font-family:var(--mono);font-size:.62rem">${m.date}</span>
            <span style="text-align:right;font-size:.78rem">${m.home_team}</span>
            <span class="h2h-score">${m.home_goals} - ${m.away_goals}</span>
            <span style="font-size:.78rem">${m.away_team}</span>
          </div>`).join('')}
      </div>`:''}`;
  }catch(e){
    detEl.innerHTML=`<div style="color:var(--red);font-family:var(--mono);font-size:.7rem">Error: ${e.message}</div>`;
  }
}

async function logPick(btn, dataStr, xgH, xgA, prob, odd){
  btn.disabled = true;
  btn.textContent = '⏳ Registrando...';
  try{
    const data = JSON.parse(decodeURIComponent(dataStr));
    const ev = Math.max(0, (prob * odd) - 1);
    const params = new URLSearchParams({
      home:      data.home,
      away:      data.away,
      div:       data.div||'',
      market:    data.market,
      selection: data.selection,
      odd:       odd,
      prob:      prob,
      ev:        ev,
      xg_h:      xgH,
      xg_a:      xgA,
    });
    const r = await fetch('/api/log_pick?' + params.toString());
    const d = await r.json();
    if(d.ok){
      btn.textContent = '✅ ' + d.msg;
      btn.style.background = 'var(--green)';
    } else {
      btn.textContent = '⚠️ ' + d.msg;
      btn.style.background = 'var(--amber)';
      btn.disabled = false;
    }
  } catch(e){
    btn.textContent = '❌ Error: ' + e.message;
    btn.disabled = false;
  }
}

async function loadCalendar(){
  try{
    const r=await fetch('/api/calendar?days=7');
    const d=await r.json();
    calMatches=d.matches||[];
    const dates=[...new Set(calMatches.map(m=>m.date))].sort();

    // Date nav
    const nav=document.getElementById('date-nav');
    nav.innerHTML=dates.map(dt=>{
      const label=new Date(dt+'T12:00:00').toLocaleDateString('es-MX',{weekday:'short',day:'numeric'});
      return `<button class="dday${activeDate===dt?' active':''}" onclick="setDate('${dt}',this)">${label}</button>`;
    }).join('');

    // League chips
    const divs=[...new Set(calMatches.map(m=>m.div))];
    const chips=document.getElementById('league-chips');
    chips.innerHTML=`<div class="l-chip active" onclick="setLeague('all',this)">Todos (${calMatches.length})</div>`+
      divs.map(d=>{
        const n=calMatches.filter(m=>m.div===d).length;
        return `<div class="l-chip" onclick="setLeague('${d}',this)">${LEAGUES[d]||d} (${n})</div>`;
      }).join('');

    if(dates.length) setDate(dates[0], nav.querySelector('.dday'));
    else renderCalendar();
  }catch(e){
    document.getElementById('cal-body').innerHTML=`<div class="empty">Error: ${e.message}</div>`;
  }
}

function setDate(dt, btn){
  activeDate=dt;
  document.querySelectorAll('.dday').forEach(b=>b.classList.remove('active'));
  if(btn) btn.classList.add('active');
  renderCalendar();
}
function setLeague(div, btn){
  activeLeague=div;
  document.querySelectorAll('.l-chip').forEach(b=>b.classList.remove('active'));
  if(btn) btn.classList.add('active');
  renderCalendar();
}

// ════════════════════════════════════════
// TAB 1: PICKS
// ════════════════════════════════════════
const mktBadge=m=>{
  const map={UNDER:'b-under',OVER:'b-over',DC:'b-dc','1X2':'b-1x2',
             BTTS:'b-btts',BTTS_NO:'b-over',DNB:'b-dnb'};
  return `<span class="badge ${map[m]||'b-1x2'}">${m}</span>`;
};
const statusBadge=s=>{
  const map={WIN:'b-win',LOSS:'b-loss',PENDING:'b-pend'};
  const icon={WIN:'▲',LOSS:'▼',PENDING:'◎'};
  return `<span class="badge ${map[s]||''}">${icon[s]||''}${s}</span>`;
};
const evClass=v=>v>=0.10?'ev-h':v>=0.05?'ev-m':'ev-l';
const evCls=evClass;  // alias usado en renderJornada y renderHistorial
const resBadge=statusBadge;  // alias usado en renderHistorial
// Conversión decimal → americana
const toUS=d=>{
  if(!d||isNaN(d))return'—';
  const f=parseFloat(d);
  if(f<=1.01)return'—';
  if(f>=2.0) return'+'+(Math.round((f-1)*100));
  return ''+(Math.round(-100/(f-1)));
};
// Mostrar cuota: americana principal + decimal pequeño
const fmtOdd=d=>{
  if(!d||isNaN(d))return'—';
  const us=toUS(d);
  return `${us}<span style="font-size:.7em;opacity:.5;margin-left:2px">(${parseFloat(d).toFixed(2)})</span>`;
};
// Solo americana sin decimal (para espacios pequeños)
const fmtOddS=d=>toUS(d);

const fmtDate=d=>{
  if(!d||d==='null'||d==='undefined')return'—';
  const s=String(d);
  // Intentar parsear ISO: "2026-04-11T06:31..." o "2026-04-11"
  const iso=s.match(/^([0-9]{4})-([0-9]{2})-([0-9]{2})/);
  if(iso) return `${iso[3]}/${iso[2]}/${iso[1].slice(2)}`;
  // Fallback: mostrar primeros 10 chars
  return s.slice(0,10);
};
const xgMini=(h,a)=>{
  const t=(h||0)+(a||0);if(!t)return'—';
  const hw=Math.round((h/t)*50);
  return `<div class="xg-mini"><span style="font-size:.58rem;color:var(--muted)">${(h||0).toFixed(1)}</span><div class="xg-seg" style="width:${hw}px;background:var(--accent)"></div><div class="xg-seg" style="width:${50-hw}px;background:var(--red)"></div><span style="font-size:.58rem;color:var(--muted)">${(a||0).toFixed(1)}</span></div>`;
};

function updateStats(s){
  if(!s) return;  // guard contra null/undefined
  const res=(s.wins||0)+(s.losses||0);
  const br=res?s.wins/res:0;
  document.getElementById('s-total').textContent=s.total;
  document.getElementById('s-sub-total').textContent=`${res} resueltos`;
  document.getElementById('s-win').textContent=s.wins;
  document.getElementById('s-sub-win').textContent=res?`${(s.wins/res*100).toFixed(1)}%`:'';
  document.getElementById('s-loss').textContent=s.losses;
  document.getElementById('s-sub-loss').textContent=res?`${(s.losses/res*100).toFixed(1)}%`:'';
  document.getElementById('s-pend').textContent=s.pending;
  const brEl=document.getElementById('s-br');
  brEl.textContent=res?(br*100).toFixed(1)+'%':'—';
  brEl.className='scard-val '+(br>=0.55?'c-green':br>=0.50?'c-amber':'c-red');
  document.getElementById('s-ev').textContent='+'+(s.avg_ev||0).toFixed(1)+'%';
  const pEl=document.getElementById('s-pnl');
  pEl.textContent=(s.pnl>=0?'+':'')+s.pnl.toFixed(4);
  pEl.className='scard-val '+(s.pnl>0?'c-green':s.pnl<0?'c-red':'c-white');
  const bEl=document.getElementById('s-burn');
  bEl.textContent=s.resolved+'/30';
  bEl.className='scard-val '+(s.resolved>=30?'c-green':'c-blue');
  document.getElementById('s-burn-sub').textContent=s.resolved>=30?'¡listo para live!':`faltan ${30-s.resolved}`;
  document.getElementById('last-upd').textContent=new Date().toLocaleTimeString('es-MX',{hour:'2-digit',minute:'2-digit'});
}

function renderPicks(){
  const searchEl=document.getElementById('picks-search');
  const search=searchEl?searchEl.value.toLowerCase():'';
  let data=allPicks.filter(p=>{
    if(activeFlt!=='all'&&p.status!==activeFlt)return false;
    if(activeMkt!=='all'&&p.market!==activeMkt)return false;
    if(search){const hay=((p.home||'')+' '+(p.away||'')+' '+(p.div||'')+' '+(p.market||'')).toLowerCase();if(!hay.includes(search))return false;}
    return true;
  });
  data.sort((a,b)=>{
    let av=a[sortCol]??'',bv=b[sortCol]??'';
    if(typeof av==='string')av=av.toLowerCase();
    if(typeof bv==='string')bv=bv.toLowerCase();
    return av<bv?sortDir:av>bv?-sortDir:0;
  });
  if(!data.length){document.getElementById('picks-body').innerHTML=`<tr><td colspan="11" class="empty">sin picks</td></tr>`;return;}
  document.getElementById('picks-body').innerHTML=data.map(p=>{
    const ev=parseFloat(p.ev||0);
    const prob=parseFloat(p.prob||0)*100;
    const stake=parseFloat(p.stake||0)*100;
    const profit=parseFloat(p.profit||0);
    const profitStr=p.status==='PENDING'?`<span class="neu">—</span>`:`<span class="${profit>=0?'pos':'neg'}">${profit>=0?'+':''}${profit.toFixed(4)}</span>`;
    return`<tr>
      <td class="muted-td">${fmtDate(p.date)}</td>
      <td class="muted-td">${p.div||'—'}</td>
      <td class="party">${p.home||''} <span style="color:var(--muted);font-weight:300">vs</span> ${p.away||''}</td>
      <td>${mktBadge(p.market)}</td>
      <td>${fmtOddS(p.odd)}</td>
      <td class="${evClass(ev)}">+${(ev*100).toFixed(1)}%</td>
      <td class="muted-td">${prob.toFixed(1)}%</td>
      <td class="muted-td">${stake.toFixed(2)}%</td>
      <td>${xgMini(p.xg_h,p.xg_a)}</td>
      <td>${statusBadge(p.status)}</td>
      <td>${profitStr}</td>
    </tr>`;
  }).join('');
}

async function loadPicks(){
  try{
    const r=await fetch('/api/picks');
    const d=await r.json();
    allPicks=d.picks||[];
    if(d.stats) updateStats(d.stats);
  }catch(e){
    console.error('loadPicks error:',e);
    // Mostrar error en tablas para debugging
    ['hist-body','jornada-body'].forEach(id=>{
      const el=document.getElementById(id);
      if(el) el.innerHTML=`<tr><td colspan="11" style="color:red;font-family:monospace;font-size:.7rem">Error: ${e.message}</td></tr>`;
    });
  }
}

function renderJornada(){
  const segJ=document.getElementById('mkt-seg-j');
  let mktF='all';
  if(segJ){const a=segJ.querySelector('.seg-btn.active');if(a)mktF=a.dataset.m||'all';}
  const pending=allPicks.filter(p=>p.status==='PENDING'&&(mktF==='all'||p.market===mktF));
  const tbody=document.getElementById('jornada-body');
  if(!tbody)return;
  if(!pending.length){tbody.innerHTML='<tr><td colspan="9" class="empty">Sin picks pendientes esta jornada</td></tr>';return;}
  tbody.innerHTML=pending.map(p=>{
    const ev=(parseFloat(p.ev||0)*100).toFixed(1);
    const prob=(parseFloat(p.prob||0)*100).toFixed(1);
    const stake=(parseFloat(p.stake||0)*100).toFixed(2);
    const d=fmtDate(p.date);
    return `<tr>
      <td style="color:var(--muted);font-family:var(--mono);font-size:.7rem">${d}</td>
      <td style="color:var(--muted);font-family:var(--mono);font-size:.7rem">${p.div||'—'}</td>
      <td style="font-size:.82rem;font-weight:500">${p.home||''} <span style="color:var(--muted)">vs</span> ${p.away||''}</td>
      <td>${mktBadge(p.market)}</td>
      <td style="font-family:var(--mono)">${fmtOddS(p.odd)}<br><span style="font-size:.65em;opacity:.45">${parseFloat(p.odd||0).toFixed(2)}</span></td>
      <td class="${evCls(parseFloat(ev))}">+${ev}%</td>
      <td style="color:var(--muted);font-family:var(--mono)">${prob}%</td>
      <td style="color:var(--muted);font-family:var(--mono)">${stake}%</td>
      <td>${xgMini(p.xg_h,p.xg_a)}</td>
    </tr>`;
  }).join('');
}

function renderHistorial(){
  const segFlt=document.getElementById('flt-seg');
  const segMkt=document.getElementById('mkt-seg-h');
  const srch=document.getElementById('picks-search-h');
  let fltF='all',mktF='all',searchF='';
  if(segFlt){const a=segFlt.querySelector('.seg-btn.active');if(a)fltF=a.dataset.f||'all';}
  if(segMkt){const a=segMkt.querySelector('.seg-btn.active');if(a)mktF=a.dataset.m||'all';}
  if(srch)searchF=srch.value.toLowerCase();
  let data=allPicks.filter(p=>{
    // Si el filtro activo es 'all', mostrar todos incluyendo PENDING
    // Si es un filtro específico, aplicar
    if(fltF==='all'&&p.status==='PENDING')return false; // ocultar PENDING en vista todos
    if(fltF!=='all'&&p.status!==fltF)return false;
    if(mktF!=='all'&&p.market!==mktF)return false;
    if(searchF){const hay=((p.home||'')+' '+(p.away||'')+' '+(p.div||'')).toLowerCase();if(!hay.includes(searchF))return false;}
    return true;
  });
  data.sort((a,b)=>new Date(b.date)-new Date(a.date));
  const tbody=document.getElementById('hist-body');
  if(!tbody)return;
  if(!data.length){tbody.innerHTML='<tr><td colspan="11" class="empty">sin picks resueltos</td></tr>';return;}
  // KPI CLV Pinnacle promedio
  const clvPsVals = data.filter(p=>p.clv_ps!=null).map(p=>p.clv_ps);
  const avgClvPs  = clvPsVals.length ? (clvPsVals.reduce((a,b)=>a+b,0)/clvPsVals.length) : null;
  const clvKpiEl  = document.getElementById('clv-kpi-ps');
  if(clvKpiEl && avgClvPs!=null){
    const sign = avgClvPs >= 0 ? '+' : '';
    clvKpiEl.textContent = `CLV Pinnacle: ${sign}${avgClvPs.toFixed(1)}%`;
    clvKpiEl.style.color = avgClvPs > 0 ? 'var(--green)' : avgClvPs > -2 ? 'var(--amber)' : 'var(--red)';
  }

  tbody.innerHTML=data.map(p=>{
    const ev=(parseFloat(p.ev||0)*100).toFixed(1);
    const prob=(parseFloat(p.prob||0)*100).toFixed(1);
    const stake=(parseFloat(p.stake||0)*100).toFixed(2);
    const profit=parseFloat(p.profit||0);
    const d=fmtDate(p.date);
    // CLV columns
    function clvCell(val){
      if(val==null) return '<td style="color:var(--muted2);font-family:var(--mono);font-size:.7rem">—</td>';
      const c = val>2?'var(--green)':val>0?'#86efac':val>-2?'var(--amber)':'var(--red)';
      return `<td style="color:${c};font-family:var(--mono);font-size:.75rem;font-weight:600">${val>0?'+':''}${val.toFixed(1)}%</td>`;
    }
    return `<tr>
      <td style="color:var(--muted);font-family:var(--mono);font-size:.7rem">${d}</td>
      <td style="color:var(--muted);font-family:var(--mono);font-size:.7rem">${p.div||'—'}</td>
      <td style="font-size:.82rem;font-weight:500">${p.home||''} <span style="color:var(--muted)">vs</span> ${p.away||''}</td>
      <td>${mktBadge(p.market)}</td>
      <td style="font-family:var(--mono)">${fmtOddS(p.odd)}<br><span style="font-size:.65em;opacity:.45">${parseFloat(p.odd||0).toFixed(2)}</span></td>
      <td class="${evCls(parseFloat(ev))}">${parseFloat(ev)>0?'+':''}${ev}%</td>
      ${clvCell(p.clv_b365)}
      ${clvCell(p.clv_ps)}
      <td>${xgMini(p.xg_h,p.xg_a)}</td>
      <td>${resBadge(p.status)}</td>
      <td class="${profit>0?'c-green':profit<0?'c-red':''}">${profit>=0?'+':''}${profit.toFixed(4)}</td>
    </tr>`;
  }).join('');
}

async function resolveData(){
  const btn=document.querySelector('.resolve-btn');
  btn.textContent='resolviendo...';btn.disabled=true;
  try{const r=await fetch('/api/resolve');const d=await r.json();
    btn.textContent=d.error?'error':`✓ ${d.resolved||0} resueltos`;
    await loadPicks();
  }catch(e){btn.textContent='error';}
  setTimeout(()=>{btn.textContent='resolver picks';btn.disabled=false;},4000);
}

document.querySelectorAll('#flt-seg .seg-btn').forEach(btn=>{
  btn.addEventListener('click',()=>{
    document.querySelectorAll('#flt-seg .seg-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');activeFlt=btn.dataset.f;renderPicks();
  });
});
document.querySelectorAll('#mkt-seg .seg-btn').forEach(btn=>{
  btn.addEventListener('click',()=>{
    document.querySelectorAll('#mkt-seg .seg-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');activeMkt=btn.dataset.m;renderPicks();
  });
});
document.querySelectorAll('thead th[data-col]').forEach(th=>{
  th.addEventListener('click',()=>{
    const col=th.dataset.col;
    if(sortCol===col){sortDir*=-1;}else{sortCol=col;sortDir=-1;document.querySelectorAll('thead th').forEach(t=>t.classList.remove('sorted'));}
    th.classList.add('sorted');renderPicks();
  });
});
const psEl=document.getElementById('picks-search'); if(psEl) psEl.addEventListener('input',renderPicks);

// ════════════════════════════════════════
// TAB 2: HISTÓRICO
// ════════════════════════════════════════
function renderHistorico(){
  if(!allPicks.length){loadPicks().then(renderHistorico);return;}

  // PnL chart
  const resolved=allPicks.filter(p=>p.status!=='PENDING').reverse();
  let cum=0;
  const labels=resolved.map((_,i)=>i+1);
  const data=resolved.map(p=>{cum+=parseFloat(p.profit||0);return Math.round(cum*10000)/10000;});
  const colors=data.map((v,i)=>i>0&&data[i]<data[i-1]?'#ef4444':'#22c55e');

  if(pnlChart) pnlChart.destroy();
  const ctx=document.getElementById('pnl-chart').getContext('2d');
  pnlChart=new Chart(ctx,{
    type:'line',
    data:{
      labels,
      datasets:[{
        data,borderColor:'#4f6ef7',backgroundColor:'rgba(79,110,247,.08)',
        borderWidth:2,pointRadius:2,pointBackgroundColor:'#4f6ef7',
        fill:true,tension:.3
      }]
    },
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:{
        callbacks:{label:c=>`PnL: ${c.parsed.y>=0?'+':''}${c.parsed.y.toFixed(4)}U`}
      }},
      scales:{
        x:{ticks:{color:'#5a5f80',font:{size:10}},grid:{color:'#1c2035'}},
        y:{ticks:{color:'#5a5f80',font:{size:10}},grid:{color:'#1c2035'}}
      }
    }
  });

  // Por mercado
  const mkts={};
  allPicks.filter(p=>p.status!=='PENDING').forEach(p=>{
    if(!mkts[p.market]) mkts[p.market]={w:0,l:0,pnl:0,form:[]};
    if(p.status==='WIN'){mkts[p.market].w++;mkts[p.market].pnl+=parseFloat(p.profit||0);}
    else{mkts[p.market].l++;mkts[p.market].pnl-=parseFloat(p.stake||0)*0; mkts[p.market].pnl+=parseFloat(p.profit||0);}
    mkts[p.market].form.push(p.status==='WIN'?'W':'L');
  });
  document.getElementById('by-mkt-grid').innerHTML=Object.entries(mkts).map(([m,d])=>{
    const tot=d.w+d.l;
    const br=tot?d.w/tot:0;
    const c=br>=0.55?'var(--green)':br>=0.50?'var(--amber)':'var(--red)';
    return`<div class="bm-card">
      <div class="bm-name">${m}</div>
      <div class="bm-br" style="color:${c}">${(br*100).toFixed(1)}%</div>
      <div class="bm-n">${tot} picks · ${d.w}W/${d.l}L</div>
      <div class="bm-form">${d.form.slice(-8).map(r=>`<span class="fd ${r==='W'?'fw':'fl'}">${r}</span>`).join('')}</div>
    </div>`;
  }).join('');

  // Por liga
  const divs={};
  allPicks.filter(p=>p.status!=='PENDING').forEach(p=>{
    if(!divs[p.div]) divs[p.div]={w:0,l:0};
    if(p.status==='WIN') divs[p.div].w++;
    else divs[p.div].l++;
  });
  document.getElementById('by-league-grid').innerHTML=Object.entries(divs)
    .filter(([,d])=>d.w+d.l>=2)
    .sort((a,b)=>(b[1].w+b[1].l)-(a[1].w+a[1].l))
    .map(([div,d])=>{
      const tot=d.w+d.l;
      const br=d.w/tot;
      const c=br>=0.55?'var(--green)':br>=0.50?'var(--amber)':'var(--red)';
      return`<div class="bl-card">
        <div class="bl-name">${LEAGUES[div]||div}</div>
        <div style="font-size:1.1rem;font-weight:600;color:${c}">${(br*100).toFixed(0)}%</div>
        <div class="bl-stats">${tot} picks · ${d.w}W/${d.l}L</div>
      </div>`;
    }).join('');
}

// ── INIT ──
loadCalendar();
loadPicks().then(()=>{
  const wire=(id,fn)=>{const seg=document.getElementById(id);if(seg)seg.querySelectorAll('.seg-btn').forEach(b=>b.addEventListener('click',()=>{seg.querySelectorAll('.seg-btn').forEach(x=>x.classList.remove('active'));b.classList.add('active');fn();}));};
  wire('flt-seg', renderHistorial);
  wire('mkt-seg-h', renderHistorial);
  wire('mkt-seg-j', renderJornada);
  const srch=document.getElementById('picks-search-h');
  if(srch) srch.addEventListener('input', renderHistorial);
});
loadCalendario();
setInterval(()=>{
  loadPicks();
  loadCalendario();
  const el=document.getElementById('last-upd');
  if(el) el.textContent=new Date().toLocaleTimeString('es-MX',{hour:'2-digit',minute:'2-digit'});
},120000);
</script>
</body>
</html>
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

def _dec_to_us(dec):
    """Decimal → americana: 2.15 → +114, 1.65 → -153"""
    if not dec or dec <= 1.0: return "—"
    if dec >= 2.0: return f"+{int((dec-1)*100)}"
    return f"{int(-100/(dec-1))}"


def _build_bet_rec(xh, xa, ph, pd_, pa, po, pu, py, pn):
    """
    Recomendación de apuesta basada en el 66% de accuracy del mercado.

    LÓGICA CENTRAL:
    - El mercado tiene 66% de accuracy prediciendo el resultado
    - Pero el vig (~6%) destruye ese edge si apuestas ciegamente
    - La ventaja real: apostar cuando TU precio > precio justo del mercado
      = CLV positivo = el mercado confirmó tu dirección bajando la cuota

    REGLA DE ORO:
    El modelo calcula la cuota justa. TÚ comparas con la cuota actual.
    Si cuota actual en B365/Pinnacle > cuota justa modelo → HAY VALUE → APUESTA
    Si cuota actual < cuota justa modelo → mercado ya lo descuenta → PASA

    EVIDENCIA CSV I1 2025-26:
    - Favorito con CLV>0 (apertura mejor que cierre): WR 56.6%, ROI +3.7%
    - Visitante con CLV>0, cuota 2.5-3.5: ROI +43.6% (N=17)
    - Apostar siempre sin filtro: ROI -3.9% (el vig destruye el edge)
    """
    recs = []
    total_xg = round(xh + xa, 2)

    # ── IDENTIFICAR FAVORITO DEL MODELO ─────────────────────────────────
    # El mercado tiene 66% accuracy → seguimos al favorito cuando el precio es bueno
    sides = [
        (ph, "Local",    "1X2",   round(1/ph,   2) if ph   > 0.01 else 99, 2.0),
        (pa, "Visitante","1X2",   round(1/pa,   2) if pa   > 0.01 else 99, 2.0),
        (po, "Over 2.5", "OVER",  round(1/po,   2) if po   > 0.01 else 99, 1.5),
        (pu, "Under 2.5","UNDER", round(1/pu,   2) if pu   > 0.01 else 99, 1.5),
        (py, "BTTS: Sí", "BTTS",  round(1/py,   2) if py   > 0.01 else 99, 1.0),
    ]

    for prob, label, mkt, fair_odd, base_stake in sides:
        if prob < 0.38: continue  # sin confianza mínima

        # EV vs vig del mercado (~5.5%)
        # Si el mercado da cuota X y nuestra prob es P:
        # EV = P * X - 1. Para EV>0 necesitamos X > 1/P
        # Cuota justa = 1/P. Necesitamos cuota real > cuota justa para tener EV+

        # Nivel de confianza
        if prob >= 0.65:
            strength = "FUERTE"
            stake = base_stake * 1.5
            emoji = "🔥"
        elif prob >= 0.55:
            strength = "BUENA"
            stake = base_stake
            emoji = "✅"
        elif prob >= 0.45:
            strength = "MODERADA"
            stake = base_stake * 0.5
            emoji = "⚡"
        else:
            continue

        # Razón específica por mercado
        if mkt == "1X2" and label == "Local":
            reason = (f"Modelo: {prob*100:.0f}% prob local. xG {xh:.2f}vs{xa:.2f}. "
                      f"El mercado da 66% accuracy → sigue al favorito cuando el precio es justo.")
        elif mkt == "1X2" and label == "Visitante":
            reason = (f"Modelo: {prob*100:.0f}% prob visitante. xG {xa:.2f} vs local {xh:.2f}. "
                      f"Visita con valor: histórico +34% ROI cuando mercado confirma.")
        elif mkt == "OVER":
            reason = f"xG total {total_xg} — modelo Over {prob*100:.0f}%. Partido con goles esperados."
        elif mkt == "UNDER":
            reason = f"xG total {total_xg} — modelo Under {prob*100:.0f}%. Partido cerrado esperado."
        elif mkt == "BTTS":
            reason = f"xG local {xh:.2f} / visita {xa:.2f} — ambos equipos con amenaza goleadora."
        else:
            reason = f"Prob modelo: {prob*100:.0f}%"

        recs.append({
            "market": mkt, "selection": label,
            "prob": round(prob*100, 1),
            "fair_odd": fair_odd,
            "strength": strength, "emoji": emoji,
            "reason": reason,
            "stake_pct": round(min(stake, 3.0), 1),
            "priority": prob * (1.3 if prob >= 0.55 else 1.0),
            # La regla de oro para el apostador:
            "action": f"Busca {label} a cuota > {fair_odd} en B365 o Pinnacle",
            "clv_rule": f"Si ves {_dec_to_us(fair_odd)} o mejor → APUESTA. Si no llega → PASA.",
        })

    # DC como protección cuando hay favorito claro pero cuota baja
    dc_1x = round(ph + pd_, 3)
    dc_x2 = round(pd_ + pa, 3)
    if ph >= 0.52 and dc_1x >= 0.72:
        fair_dc = round(1/dc_1x, 2)
        recs.append({
            "market": "DC", "selection": "DC: Local o Empate",
            "prob": round(dc_1x*100, 1), "fair_odd": fair_dc,
            "strength": "CONSERVADORA", "emoji": "🛡️",
            "reason": f"Local {ph*100:.0f}% ganar. DC elimina riesgo empate. Cuota justa: {fair_dc}",
            "stake_pct": 1.0,
            "priority": dc_1x * 0.65,
            "action": f"Busca DC Local a cuota > {fair_dc}",
            "clv_rule": f"Si cuota actual > {fair_dc} → APUESTA. Si < {fair_dc} → PASA.",
        })

    if not recs:
        return {
            "has_rec": False,
            "message": (
                "⚖️ Partido muy equilibrado — sin edge claro. "
                f"xG: {xh:.2f} vs {xa:.2f}. "
                "El modelo no identifica ventaja suficiente para recomendar apuesta."
            ),
        }

    recs.sort(key=lambda x: -x["priority"])
    top = recs[0]
    return {
        "has_rec": True,
        "top": {
            "market":    top["market"],
            "selection": top["selection"],
            "prob":      top["prob"],
            "fair_odd":  top["fair_odd"],
            "strength":  top["strength"],
            "emoji":     top["emoji"],
            "reason":    top["reason"],
            "stake_pct": top["stake_pct"],
            "action":    top["action"],
            "clv_rule":  top["clv_rule"],
        },
        "alternatives": [
            {"market": r["market"], "selection": r["selection"],
             "prob": r["prob"], "fair_odd": r["fair_odd"],
             "strength": r["strength"], "action": r["action"]}
            for r in recs[1:3]
        ],
        "tip": (
            "💡 CÓMO USAR: Abre B365 o Pinnacle. "
            f"Si la cuota de {top['selection']} es mayor que {top['fair_odd']} → APUESTA {top['stake_pct']}% del bankroll. "
            "Si es menor → el mercado ya lo descuenta, PASA."
        ),
    }


class DashboardHandler(BaseHTTPRequestHandler):
    """HTTP handler del dashboard V7.2."""

    def log_message(self, format, *args): pass  # suprimir logs HTTP

    def _serve_analyze(self):
        """API /api/analyze — análisis de partido estilo footystats."""
        try:
            from urllib.parse import urlparse, parse_qs
            import pandas as pd
            qs   = parse_qs(urlparse(self.path).query)
            home = qs.get("home",[""])[0].strip()
            away = qs.get("away",[""])[0].strip()
            div  = qs.get("div", [""])[0].strip().upper()
            if not home or not away: self._json_err("Faltan home/away"); return
            found_div=found_home=found_away=None

            # Partidos de copa: buscar equipos en todas las ligas con cutoff bajo
            is_cup = div.startswith("CUP_")
            cup_league_id_orig = int(div.replace("CUP_","")) if is_cup else None

            # Normalizar nombres de copa (fd.org usa nombres largos)
            _CUP_ALIASES = {
                "paris saint-germain fc":"PSG","paris saint-germain":"PSG","paris sg":"PSG",
                "fc barcelona":"Barcelona","fc bayern münchen":"Bayern Munich",
                "fc bayern munich":"Bayern Munich","bayern münchen":"Bayern Munich","fc bayern":"Bayern Munich",
                "real madrid cf":"Real Madrid","club atlético de madrid":"Atletico Madrid",
                "club atletico de madrid":"Atletico Madrid","atletico de madrid":"Atletico Madrid",
                "sporting cp":"Sporting","sporting clube de portugal":"Sporting",
                "borussia dortmund":"Dortmund","manchester city fc":"Man City",
                "manchester united fc":"Man United","manchester united":"Man United",
                "tottenham hotspur fc":"Tottenham","tottenham hotspur":"Tottenham",
                "ac milan":"Milan","fc internazionale":"Inter","inter milan":"Inter",
                "internazionale fc":"Inter","juventus fc":"Juventus",
                "sevilla fc":"Sevilla","real betis":"Betis","real betis balompié":"Betis",
                "villarreal cf":"Villarreal","real sociedad":"Sociedad",
                "athletic bilbao":"Athletic Club","athletic club bilbao":"Athletic Club",
                "rb leipzig":"RB Leipzig","bayer 04 leverkusen":"Leverkusen","bayer leverkusen":"Leverkusen",
                "eintracht frankfurt":"Frankfurt","vfb stuttgart":"Stuttgart",
                "sc freiburg":"Freiburg","freiburg sc":"Freiburg",
                "as monaco":"Monaco","olympique marseille":"Marseille",
                "olympique de marseille":"Marseille","olympique lyonnais":"Lyon",
                "porto fc":"Porto","fc porto":"Porto","sl benfica":"Benfica",
                "sc braga":"Braga","sporting de braga":"Braga",
                "celtic fc":"Celtic","rangers fc":"Rangers",
                "fenerbahce sk":"Fenerbahce","fenerbahçe sk":"Fenerbahce",
                "galatasaray sk":"Galatasaray","galatasaray as":"Galatasaray",
                "ss lazio":"Lazio","as roma":"Roma","acf fiorentina":"Fiorentina",
                "bologna fc":"Bologna","bologna fc 1909":"Bologna",
                "ogc nice":"Nice","stade rennais fc":"Rennes","rc strasbourg":"Strasbourg",
                "az alkmaar":"AZ Alkmaar","az":"AZ Alkmaar",
                "psv eindhoven":"PSV","ajax amsterdam":"Ajax",
                "feyenoord rotterdam":"Feyenoord","club brugge kv":"Bruges",
                "celta vigo":"Celta Vigo","rc celta":"Celta Vigo",
                "aston villa fc":"Aston Villa","nottingham forest fc":"Nottingham Forest",
                "west ham united fc":"West Ham","crystal palace fc":"Crystal Palace",
                "rayo vallecano":"Rayo Vallecano","fsv mainz 05":"Mainz",
                "fc midtjylland":"Midtjylland","aek athens fc":"AEK Athens",
                "shakhtar donetsk":"Shakhtar",
            }
            def norm_team(name):
                alias = _CUP_ALIASES.get(name.lower().strip())
                if alias: return alias
                for prefix in ["FC ","AC ","AS ","SS ","SC ","US ","RC ","VfB ","FSV "]:
                    if name.startswith(prefix):
                        name = name[len(prefix):]; break
                for suffix in [" FC"," CF"," SC"," AC"," BC"," CP",
                               " City"," Town"," United"," Athletic",
                               " Atlético"," Saint-Germain"," de Madrid"," 05"," 04"]:
                    if name.lower().endswith(suffix.lower()):
                        name = name[:-len(suffix)]; break
                return name.strip()
            home_search = norm_team(home) if is_cup else home
            away_search = norm_team(away) if is_cup else away
            cutoff = 0.40 if is_cup else 0.45

            _CUP_HOME_LEAGUES = {
                "Liverpool":"E0","Arsenal":"E0","Man City":"E0","Man United":"E0",
                "Chelsea":"E0","Tottenham":"E0","Newcastle":"E0","Aston Villa":"E0",
                "Nottingham Forest":"E0","West Ham":"E0",
                "Real Madrid":"SP1","Barcelona":"SP1","Atletico Madrid":"SP1",
                "Sevilla":"SP1","Villarreal":"SP1","Sociedad":"SP1",
                "Betis":"SP1","Athletic Club":"SP1","Celta Vigo":"SP1","Osasuna":"SP1",
                "Bayern Munich":"D1","Dortmund":"D1","Leverkusen":"D1",
                "RB Leipzig":"D1","Frankfurt":"D1","Stuttgart":"D1","Freiburg":"D1",
                "Inter":"I1","Milan":"I1","Juventus":"I1","Napoli":"I1",
                "Roma":"I1","Lazio":"I1","Atalanta":"I1","Fiorentina":"I1","Bologna":"I1",
                "PSG":"F1","Monaco":"F1","Marseille":"F1","Lyon":"F1",
                "Lille":"F1","Nice":"F1","Strasbourg":"F1","Rennes":"F1",
                "Ajax":"N1","PSV":"N1","Feyenoord":"N1","AZ Alkmaar":"N1",
                "Porto":"P1","Benfica":"P1","Sporting":"P1","Braga":"P1",
                "Rangers":"SC0","Celtic":"SC0","Galatasaray":"T1","Fenerbahce":"T1",
            }
            if is_cup:
                h_div = _CUP_HOME_LEAGUES.get(home_search)
                a_div = _CUP_HOME_LEAGUES.get(away_search)
                priority_divs = list(dict.fromkeys(
                    [d for d in [h_div, a_div] if d] +
                    list(TARGET_LEAGUES.keys())
                ))
                search_divs = priority_divs
            else:
                search_divs = [div] if div in TARGET_LEAGUES else list(TARGET_LEAGUES.keys())

            for d in search_divs:
                if d in ("BSA","MEX"): continue
                path=os.path.join(DATA_DIR,f"{d}.csv")
                if not os.path.exists(path): continue
                try:
                    try:    df_t=pd.read_csv(path,encoding="utf-8-sig")
                    except: df_t=pd.read_csv(path,encoding="latin-1")
                    df_t.columns=df_t.columns.str.strip()
                    df_t=df_t.rename(columns={"Home":"HomeTeam","Away":"AwayTeam","HG":"FTHG","AG":"FTAG"})
                    teams=pd.concat([df_t["HomeTeam"],df_t["AwayTeam"]]).dropna().unique()
                    rh=difflib.get_close_matches(home_search,teams,n=1,cutoff=cutoff)
                    ra=difflib.get_close_matches(away_search,teams,n=1,cutoff=cutoff)
                    if rh and ra: found_div=d; found_home=rh[0]; found_away=ra[0]; break
                except: continue
            if not found_div:
                # Para copa: buscar cada equipo en su liga doméstica por separado
                # PSG → F1, Bayern → D1 — no necesitan estar en el mismo CSV
                if is_cup:
                    home_div = _CUP_HOME_LEAGUES.get(home_search)
                    away_div = _CUP_HOME_LEAGUES.get(away_search)
                    if home_div and away_div and home_div != away_div:
                        # Ligas diferentes — usar la del local para el análisis principal
                        found_div = home_div
                        # Buscar home en su liga
                        path_h = os.path.join(DATA_DIR, f"{home_div}.csv")
                        if os.path.exists(path_h):
                            try:
                                df_h = pd.read_csv(path_h, encoding="utf-8-sig")
                                df_h.columns = df_h.columns.str.strip()
                                teams_h = pd.concat([df_h["HomeTeam"],df_h["AwayTeam"]]).dropna().unique()
                                rh2 = difflib.get_close_matches(home_search, teams_h, n=1, cutoff=0.45)
                                if rh2: found_home = rh2[0]
                            except: pass
                        # Buscar away en su liga
                        path_a = os.path.join(DATA_DIR, f"{away_div}.csv")
                        if os.path.exists(path_a):
                            try:
                                df_a = pd.read_csv(path_a, encoding="utf-8-sig")
                                df_a.columns = df_a.columns.str.strip()
                                teams_a = pd.concat([df_a["HomeTeam"],df_a["AwayTeam"]]).dropna().unique()
                                ra2 = difflib.get_close_matches(away_search, teams_a, n=1, cutoff=0.45)
                                if ra2: found_away = ra2[0]
                            except: pass
                        if found_home and found_away:
                            Log.info(f"Copa multi-liga: {found_home}({home_div}) vs {found_away}({away_div})", "ANA")
                        else:
                            self._json_err(f"No se encontró '{home_search}' o '{away_search}'"); return
                    else:
                        self._json_err(f"No se encontró '{home_search}' o '{away_search}' en ligas disponibles"); return
                else:
                    self._json_err(f"No se encontró '{home_search}' o '{away_search}' en ligas disponibles"); return
            div=found_div; home=found_home; away=found_away
            cfg=TARGET_LEAGUES[div]
            try:    df=pd.read_csv(os.path.join(DATA_DIR,f"{div}.csv"),encoding="utf-8-sig")
            except: df=pd.read_csv(os.path.join(DATA_DIR,f"{div}.csv"),encoding="latin-1")
            df.columns=df.columns.str.strip()
            df=df.rename(columns={"Home":"HomeTeam","Away":"AwayTeam","HG":"FTHG","AG":"FTAG"})
            df["Date"]=pd.to_datetime(df["Date"],dayfirst=True,errors="coerce")
            played=df.dropna(subset=["FTHG","FTAG"]).copy()
            played["FTHG"]=played["FTHG"].astype(float)
            played["FTAG"]=played["FTAG"].astype(float)
            has_shots=cfg.get("has_shots") and "HST" in played.columns
            import numpy as _np_a
            def side_stats_full(rows_h, rows_a, name_t):
                """Stats completos para un equipo: overall + home + away."""
                def calc(rows, gf_col, ga_col):
                    if rows.empty: return {}
                    gf=rows[gf_col].values.astype(float)
                    ga=rows[ga_col].values.astype(float)
                    n2=len(gf)
                    if n2==0: return {}
                    w=int((gf>ga).sum()); d=int((gf==ga).sum())
                    return {
                        "pj":n2,"ppg":round((w*3+d)/n2,2),
                        "win_pct":round(w/n2*100,1),
                        "avg_scored":round(float(gf.mean()),2),
                        "avg_conceded":round(float(ga.mean()),2),
                        "btts_pct":round(int(((gf>0)&(ga>0)).sum())/n2*100,1),
                        "cs_pct":round(int((ga==0).sum())/n2*100,1),
                        "fts_pct":round(int((gf==0).sum())/n2*100,1),
                        "over15_pct":round(int(((gf+ga)>1.5).sum())/n2*100,1),
                        "over25_pct":round(int(((gf+ga)>2.5).sum())/n2*100,1),
                        "over35_pct":round(int(((gf+ga)>3.5).sum())/n2*100,1),
                    }
                home_s=calc(rows_h,"FTHG","FTAG")
                away_s=calc(rows_a,"FTAG","FTHG")
                # Overall
                gf_o=_np_a.concatenate([rows_h["FTHG"].values.astype(float), rows_a["FTAG"].values.astype(float)])
                ga_o=_np_a.concatenate([rows_h["FTAG"].values.astype(float), rows_a["FTHG"].values.astype(float)])
                n_o=len(gf_o)
                w_o=int((gf_o>ga_o).sum()); d_o=int((gf_o==ga_o).sum())
                # Forma global ordenada por fecha
                form_o=[]
                all_r=pd.concat([rows_h,rows_a])
                if "Date" in all_r.columns:
                    all_r=all_r.sort_values("Date",ascending=True)
                for _,rr in all_r.tail(5).iterrows():
                    ih2=rr["HomeTeam"]==name_t
                    gx=float(rr["FTHG"] if ih2 else rr["FTAG"])
                    gcx=float(rr["FTAG"] if ih2 else rr["FTHG"])
                    form_o.append("W" if gx>gcx else "D" if gx==gcx else "L")
                overall={
                    "pj":n_o,"ppg":round((w_o*3+d_o)/n_o,2) if n_o>0 else 0,
                    "win_pct":round(w_o/n_o*100,1) if n_o>0 else 0,
                    "avg_scored":round(float(gf_o.mean()),2) if n_o>0 else 0,
                    "avg_conceded":round(float(ga_o.mean()),2) if n_o>0 else 0,
                    "btts_pct":round(int(((gf_o>0)&(ga_o>0)).sum())/n_o*100,1) if n_o>0 else 0,
                    "cs_pct":round(int((ga_o==0).sum())/n_o*100,1) if n_o>0 else 0,
                    "fts_pct":round(int((gf_o==0).sum())/n_o*100,1) if n_o>0 else 0,
                    "over25_pct":round(int(((gf_o+ga_o)>2.5).sum())/n_o*100,1) if n_o>0 else 0,
                    "form":form_o
                }
                return {"overall":overall,"home":home_s,"away":away_s}

            h_rows_a=played[played["HomeTeam"]==home].copy()
            a_rows_a=played[played["AwayTeam"]==home].copy()
            h_rows_b=played[played["HomeTeam"]==away].copy()
            a_rows_b=played[played["AwayTeam"]==away].copy()
            hs=side_stats_full(h_rows_a, a_rows_a, home)
            as_=side_stats_full(h_rows_b, a_rows_b, away)

            # xG usando perspectiva correcta: local como local, visitante como visitante
            h_ov=hs.get("home",hs.get("overall",{}))
            a_ov=as_.get("away",as_.get("overall",{}))
            xh=round((h_ov.get("avg_scored",1.2)+a_ov.get("avg_conceded",1.2))/2,2)
            xa=round((a_ov.get("avg_scored",1.0)+h_ov.get("avg_conceded",1.0))/2,2)
            xt=round(xh+xa,2)
            try: ph,pd_,pa=dixon_coles(xh,xa); ph,pd_,pa=round(ph,3),round(pd_,3),round(pa,3)
            except: ph,pd_,pa=0.4,0.25,0.35
            std=cfg.get("xg_std",1.55)
            po_raw,pu_raw=negbinom_ou(xt,std)
            po=round(shrink(po_raw,a=0.65),3); pu=round(1-po,3)
            py,pn=btts_prob(xh,xa); py=round(py or 0,3); pn=round(1-py,3)
            # ── Racha O/U últimos 5 por equipo ──────────────────────────────
            def ou_streak(rows_h, rows_a, name_t, line=2.5):
                """Últimos 5 partidos del equipo: Over/Under y BTTS."""
                all_r = pd.concat([rows_h, rows_a])
                if "Date" in all_r.columns:
                    all_r = all_r.sort_values("Date", ascending=True)
                streak = []
                for _, r in all_r.tail(5).iterrows():
                    total = float(r["FTHG"]) + float(r["FTAG"])
                    btts_r = float(r["FTHG"]) > 0 and float(r["FTAG"]) > 0
                    streak.append({
                        "result": "O" if total > line else "U",
                        "goals": int(r["FTHG"]) + int(r["FTAG"]),
                        "btts": btts_r,
                        "home": r["HomeTeam"],
                        "away": r["AwayTeam"],
                        "score": f"{int(r['FTHG'])}-{int(r['FTAG'])}",
                    })
                return streak

            home_ou = ou_streak(h_rows_a, a_rows_a, home)
            away_ou = ou_streak(h_rows_b, a_rows_b, away)

            # ── Posición en tabla ─────────────────────────────────────────
            def get_league_table(played_df, team_name):
                """Posición, PJ, PTS del equipo en la tabla actual."""
                table = {}
                for _, r in played_df.iterrows():
                    for side, gf_col, ga_col in [("HomeTeam","FTHG","FTAG"),("AwayTeam","FTAG","FTHG")]:
                        t = r[side]; gf = float(r[gf_col]); ga = float(r[ga_col])
                        if t not in table:
                            table[t] = {"pj":0,"pts":0,"gf":0,"ga":0,"pg":0,"pe":0,"pp":0}
                        table[t]["pj"] += 1; table[t]["gf"] += gf; table[t]["ga"] += ga
                        if gf > ga: table[t]["pts"] += 3; table[t]["pg"] += 1
                        elif gf == ga: table[t]["pts"] += 1; table[t]["pe"] += 1
                        else: table[t]["pp"] += 1
                sorted_table = sorted(table.items(), key=lambda x: (-x[1]["pts"], -(x[1]["gf"]-x[1]["ga"]), -x[1]["gf"]))
                pos_map = {t: i+1 for i, (t, _) in enumerate(sorted_table)}
                n_teams = len(sorted_table)
                def info(name):
                    import difflib as dl
                    teams = list(table.keys())
                    match = dl.get_close_matches(name, teams, n=1, cutoff=0.50)
                    if not match: return {}
                    d = table[match[0]]
                    pos = pos_map.get(match[0], 0)
                    return {"pos": pos, "total": n_teams, "pj": d["pj"], "pts": d["pts"],
                            "gf": int(d["gf"]), "ga": int(d["ga"]),
                            "gd": int(d["gf"]-d["ga"])}
                return info(home), info(away)

            home_pos, away_pos = get_league_table(played, home)

            h2h_rows=played[((played["HomeTeam"]==home)&(played["AwayTeam"]==away))|
                ((played["HomeTeam"]==away)&(played["AwayTeam"]==home))].sort_values("Date",ascending=False).head(10)
            hwins=hdraws=awins=0; h2h_list=[]
            for _,r in h2h_rows.iterrows():
                ih=(r["HomeTeam"]==home); gf=r["FTHG"] if ih else r["FTAG"]; ga=r["FTAG"] if ih else r["FTHG"]
                if gf>ga: hwins+=1
                elif gf==ga: hdraws+=1
                else: awins+=1
                h2h_list.append({"date":r["Date"].strftime("%d/%m/%Y") if pd.notna(r["Date"]) else "?",
                    "home_team":r["HomeTeam"],"away_team":r["AwayTeam"],
                    "home_goals":int(r["FTHG"]),"away_goals":int(r["FTAG"])})
            h2h_tot=hwins+hdraws+awins

            # ── H2H real de copa + resultado IDA via football-data.org ───
            leg1_home_goals = leg1_away_goals = None
            if is_cup and FD_ORG_TOKEN:
                try:
                    # cup_league_id_orig se guardó antes de que div cambiara a found_div
                    if cup_league_id_orig is None:
                        raise ValueError("no es partido de copa")
                    league_id_cup = cup_league_id_orig
                    fd_comp = {2:"CL", 3:"EL", 848:"EC"}.get(league_id_cup)
                    if fd_comp:
                        r_fd = requests.get(
                            f"https://api.football-data.org/v4/competitions/{fd_comp}/matches",
                            headers={"X-Auth-Token": FD_ORG_TOKEN},
                            params={"season": 2025, "status": "FINISHED"},
                            timeout=10
                        )
                        if r_fd.status_code == 200:
                            import difflib as _dl2
                            for fm in r_fd.json().get("matches", []):
                                fh = fm["homeTeam"]["name"] or ""
                                fa = fm["awayTeam"]["name"] or ""
                                fh_n = norm_team(fh); fa_n = norm_team(fa)
                                h_n = norm_team(home); a_n = norm_team(away)
                                # Mismo enfrentamiento en cualquier orden
                                same_ha = (_dl2.get_close_matches(h_n,[fh_n],n=1,cutoff=0.4) and
                                           _dl2.get_close_matches(a_n,[fa_n],n=1,cutoff=0.4))
                                same_ah = (_dl2.get_close_matches(h_n,[fa_n],n=1,cutoff=0.4) and
                                           _dl2.get_close_matches(a_n,[fh_n],n=1,cutoff=0.4))
                                if not (same_ha or same_ah): continue
                                ft = fm.get("score",{}).get("fullTime",{})
                                gh, ga_s = ft.get("home"), ft.get("away")
                                if gh is None or ga_s is None: continue
                                # Orientar desde perspectiva del home actual
                                hg = int(gh) if same_ha else int(ga_s)
                                ag = int(ga_s) if same_ha else int(gh)
                                h2h_list.insert(0, {
                                    "date": fm["utcDate"][:10],
                                    "home_team": fh, "away_team": fa,
                                    "home_goals": int(gh), "away_goals": int(ga_s),
                                    "competition": fd_comp
                                })
                                if hg > ag: hwins += 1
                                elif hg == ag: hdraws += 1
                                else: awins += 1
                                h2h_tot += 1
                                if leg1_home_goals is None:
                                    leg1_home_goals = hg
                                    leg1_away_goals = ag
                except Exception as _ec:
                    Log.warn(f"H2H copa: {_ec}", "ANALYZE")

            payload=json.dumps({"home":home,"away":away,"div":div,"league":cfg.get("name",""),
                "home_stats":hs,"away_stats":as_,
                "xg_home":xh,"xg_away":xa,"xg_total":xt,
                "probs":{"home":ph,"draw":pd_,"away":pa},"ou":{"over":po,"under":pu},
                "btts":{"yes":py,"no":pn},
                "fair_odds":{"home":round(1/ph,2) if ph>0 else None,"draw":round(1/pd_,2) if pd_>0 else None,
                    "away":round(1/pa,2) if pa>0 else None,"over":round(1/po,2) if po>0 else None,
                    "under":round(1/pu,2) if pu>0 else None,"btts_y":round(1/py,2) if py>0 else None},
                "xg_source": "fbref" if get_fbref_xg(home, div) else "proxy",
                "bet_rec": _build_bet_rec(xh, xa, ph, pd_, pa, po, pu, py, pn),
                "h2h":{"total":h2h_tot,"home_wins":hwins,"draws":hdraws,"away_wins":awins,
                    "over25":sum(1 for r in h2h_list if r["home_goals"]+r["away_goals"]>2),
                    "btts":sum(1 for r in h2h_list if r["home_goals"]>0 and r["away_goals"]>0),
                    "matches":h2h_list},
                "leg1":{"home_goals":leg1_home_goals,"away_goals":leg1_away_goals} if leg1_home_goals is not None else None,
                "home_ou":home_ou, "away_ou":away_ou,
                "home_pos":home_pos, "away_pos":away_pos}).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        except Exception as e:
            import traceback; Log.err(f"analyze: {e}", "ANALYZE")
            self._json_err(str(e))

    def _serve_fbref_status(self):
        """API /api/fbref — estado del cache de xG real de FBref."""
        try:
            conn = sqlite3.connect(DB_PATH)
            total = conn.execute("SELECT COUNT(*) FROM fbref_xg_cache").fetchone()[0]
            by_div = conn.execute("""
                SELECT div, COUNT(*) as teams,
                       AVG(xg_for) as avg_xg_for, AVG(xg_against) as avg_xg_ag,
                       MAX(updated_at) as last_update
                FROM fbref_xg_cache
                WHERE season='2025-26'
                GROUP BY div ORDER BY last_update DESC
            """).fetchall()
            # Top equipos por xG
            top_xg = conn.execute("""
                SELECT div, team_original, xg_for, xg_against, mp
                FROM fbref_xg_cache
                WHERE season='2025-26' AND xg_for IS NOT NULL
                ORDER BY xg_for DESC LIMIT 20
            """).fetchall()
            conn.close()
            payload = json.dumps({
                "total_teams": total,
                "by_league": [{"div":r[0],"teams":r[1],
                    "avg_xg_for":round(r[2],3) if r[2] else None,
                    "avg_xg_ag":round(r[3],3) if r[3] else None,
                    "last_update":r[4]} for r in by_div],
                "top_xg_teams": [{"div":r[0],"team":r[1],
                    "xg_for":r[2],"xg_against":r[3],"mp":r[4]} for r in top_xg],
                "source": "fbref.com (StatsBomb data)",
                "blend": "70% FBref real + 30% HST proxy"
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        except Exception as e: self._json_err(str(e))

    def _serve_cups_status(self):
        """API /api/cups — estado de la tabla cup_fixtures."""
        try:
            conn = sqlite3.connect(DB_PATH)
            total = conn.execute("SELECT COUNT(*) FROM cup_fixtures").fetchone()[0]
            by_comp = conn.execute("""
                SELECT competition, COUNT(DISTINCT team_normalized) as teams,
                       COUNT(*) as matches, MAX(match_date) as last_date
                FROM cup_fixtures GROUP BY competition ORDER BY last_date DESC
            """).fetchall()
            recent = conn.execute("""
                SELECT team_normalized, competition, match_date, opponent
                FROM cup_fixtures ORDER BY match_date DESC LIMIT 20
            """).fetchall()
            conn.close()
            payload = json.dumps({
                "total_records": total,
                "competitions": [{"name":r[0],"teams":r[1],"matches":r[2],"last":r[3]}
                                  for r in by_comp],
                "recent": [{"team":r[0],"comp":r[1],"date":r[2],"vs":r[3]}
                            for r in recent]
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        except Exception as e: self._json_err(str(e))

    def _serve_calendar(self):
        """API /api/calendar?days=N — partidos próximos usando fixtures.csv + CSVs históricos."""
        global _CUP_CAL_CACHE, _CUP_CAL_TS
        try:
            import pandas as pd
            from urllib.parse import urlparse, parse_qs
            qs   = parse_qs(urlparse(self.path).query)
            days = int(qs.get("days", ["7"])[0])
            now  = datetime.now(timezone.utc)
            dates = [(now + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days+1)]
            matches = []; seen = set()

            # ── FUENTE A: fixtures.csv de co.uk (siempre tiene próximos partidos) ──
            fix_df = None
            try:
                fix_r = requests.get(FIXTURES_URL,
                    headers={"User-Agent":"Mozilla/5.0"}, timeout=15)
                if fix_r.status_code == 200:
                    try:    fix_df = pd.read_csv(pd.io.common.StringIO(fix_r.content.decode("utf-8-sig")))
                    except: fix_df = pd.read_csv(pd.io.common.StringIO(fix_r.content.decode("latin-1")))
                    fix_df.columns = fix_df.columns.str.strip()
                    fix_df["Date"] = pd.to_datetime(fix_df["Date"], dayfirst=True, errors="coerce")
                    Log.info(f"fixtures.csv: {len(fix_df)} partidos próximos", "CAL")
            except Exception as e:
                Log.warn(f"fixtures.csv: {e}", "CAL")

            # Helper: stats rápidas de un equipo desde su CSV histórico
            def get_stats_from_csv(div, name):
                path = os.path.join(DATA_DIR, f"{div}.csv")
                if not os.path.exists(path): return {}, []
                try:
                    try:    df_h = pd.read_csv(path, encoding="utf-8-sig")
                    except: df_h = pd.read_csv(path, encoding="latin-1")
                    df_h.columns = df_h.columns.str.strip()
                    df_h = df_h.rename(columns={"Home":"HomeTeam","Away":"AwayTeam","HG":"FTHG","AG":"FTAG"})
                    played_h = df_h.dropna(subset=["FTHG","FTAG"]).copy()
                    played_h["FTHG"] = played_h["FTHG"].astype(float)
                    played_h["FTAG"] = played_h["FTAG"].astype(float)
                    # fuzzy match nombre
                    all_teams = pd.concat([played_h["HomeTeam"],played_h["AwayTeam"]]).dropna().unique()
                    match = difflib.get_close_matches(name, all_teams, n=1, cutoff=0.50)
                    if not match: return {}, []
                    nm = match[0]
                    h_r = played_h[played_h["HomeTeam"]==nm]
                    a_r = played_h[played_h["AwayTeam"]==nm]
                    import numpy as _np
                    gf = _np.concatenate([h_r["FTHG"].values, a_r["FTAG"].values])
                    ga = _np.concatenate([h_r["FTAG"].values, a_r["FTHG"].values])
                    n = len(gf)
                    if n == 0: return {}, []
                    wins = int((gf>ga).sum()); draws = int((gf==ga).sum())
                    btts = int(((gf>0)&(ga>0)).sum())
                    over25 = int(((gf+ga)>2.5).sum())
                    # Forma: últimos 5 partidos ordenados por fecha REAL
                    form = []
                    if "Date" in played_h.columns:
                        played_h["Date"] = pd.to_datetime(played_h["Date"], dayfirst=True, errors="coerce")
                        rows_all = pd.concat([h_r, a_r])
                        rows_all = rows_all.sort_values("Date", ascending=True).dropna(subset=["Date"])
                        for _, r in rows_all.tail(5).iterrows():
                            ih = r["HomeTeam"] == nm
                            g  = r["FTHG"] if ih else r["FTAG"]
                            gc = r["FTAG"] if ih else r["FTHG"]
                            form.append("W" if g > gc else "D" if g == gc else "L")
                    # Stats home específicos del equipo
                    gf_h=h_r["FTHG"].values.astype(float); ga_h=h_r["FTAG"].values.astype(float)
                    gf_a=a_r["FTAG"].values.astype(float); ga_a=a_r["FTHG"].values.astype(float)
                    import numpy as _np2
                    gf_all=_np2.concatenate([gf_h,gf_a]); ga_all=_np2.concatenate([ga_h,ga_a])
                    n_all=len(gf_all)
                    def _s(gf,ga):
                        n2=len(gf); w=int((gf>ga).sum()); d=int((gf==ga).sum())
                        if n2==0: return {}
                        return {"pj":n2,"ppg":round((w*3+d)/n2,2),
                            "avg_scored":round(float(gf.mean()),2),
                            "avg_conceded":round(float(ga.mean()),2),
                            "btts_pct":round(int(((gf>0)&(ga>0)).sum())/n2*100,1),
                            "cs_pct":round(int((ga==0).sum())/n2*100,1),
                            "over25_pct":round(int(((gf+ga)>2.5).sum())/n2*100,1)}
                    stats = {"overall":_s(gf_all,ga_all),"home":_s(gf_h,ga_h),"away":_s(gf_a,ga_a),
                        # Compatibilidad directa con código antiguo
                        "ppg":round((int((gf_all>ga_all).sum())*3+int((gf_all==ga_all).sum()))/n_all,2) if n_all>0 else 0,
                        "avg_scored":round(float(gf_all.mean()),2) if n_all>0 else 0,
                        "avg_conceded":round(float(ga_all.mean()),2) if n_all>0 else 0,
                        "btts_pct":round(int(((gf_all>0)&(ga_all>0)).sum())/n_all*100,1) if n_all>0 else 0,
                        "over25_pct":round(int(((gf_all+ga_all)>2.5).sum())/n_all*100,1) if n_all>0 else 0,
                        "pj":n_all}
                    return stats, form
                except: return {}, []

            def safe_float(v):
                try: f=float(v); return round(f,2) if f>1.01 else None
                except: return None

            # ── Tabla de posiciones para mostrar en el calendario ───────────
            _table_cache = {}
            def get_pos(div_key, team_name, played_df):
                if div_key not in _table_cache:
                    tbl = {}
                    for _, r in played_df.iterrows():
                        for side, gf_col, ga_col in [("HomeTeam","FTHG","FTAG"),("AwayTeam","FTAG","FTHG")]:
                            t=r[side]; gf=float(r[gf_col]); ga=float(r[ga_col])
                            if t not in tbl: tbl[t]={"pts":0,"gf":0,"ga":0}
                            tbl[t]["gf"]+=gf; tbl[t]["ga"]+=ga
                            if gf>ga: tbl[t]["pts"]+=3
                            elif gf==ga: tbl[t]["pts"]+=1
                    sorted_t=sorted(tbl.items(),key=lambda x:(-x[1]["pts"],-(x[1]["gf"]-x[1]["ga"]),-x[1]["gf"]))
                    _table_cache[div_key]={t:i+1 for i,(t,_) in enumerate(sorted_t)}
                    _table_cache[div_key+"__n"]=len(sorted_t)
                pos_map=_table_cache.get(div_key,{})
                n=_table_cache.get(div_key+"__n",0)
                import difflib as _dl
                match=_dl.get_close_matches(team_name,list(pos_map.keys()),n=1,cutoff=0.50)
                return pos_map.get(match[0],0) if match else 0, n

            def add_match(div, h, a, date_str, row):
                key = f"{div}_{date_str}_{h}_{a}"
                if key in seen: return
                seen.add(key)
                cfg = TARGET_LEAGUES.get(div,{})
                oh=safe_float(row.get("B365H") or row.get("PSH") or row.get("BbAvH"))
                od=safe_float(row.get("B365D") or row.get("PSD") or row.get("BbAvD"))
                oa=safe_float(row.get("B365A") or row.get("PSA") or row.get("BbAvA"))
                hs_q, hf = get_stats_from_csv(div, h)
                as_q, af = get_stats_from_csv(div, a)
                xh=xa=ph=pd_=pa=None
                try:
                    xh=round((hs_q.get("avg_scored",1.2)+as_q.get("avg_conceded",1.2))/2,2)
                    xa=round((as_q.get("avg_scored",1.0)+hs_q.get("avg_conceded",1.0))/2,2)
                    ph,pd_,pa=dixon_coles(xh,xa)
                    ph,pd_,pa=round(ph,3),round(pd_,3),round(pa,3)
                except: pass
                picks_m=[]
                try:
                    conn_p=sqlite3.connect(DB_PATH)
                    pk=conn_p.execute(
                        "SELECT market,selection,odd_open,ev,stake_pct,result FROM picks_log "
                        "WHERE home_team LIKE ? AND away_team LIKE ? AND date(kickoff_time)=?",
                        (f"%{h[:5]}%",f"%{a[:5]}%",date_str)).fetchall()
                    conn_p.close()
                    picks_m=[{"market":r[0],"selection":r[1],"odd":r[2],"ev":r[3],"stake":r[4],"result":r[5]} for r in pk]
                except: pass
                # Posición en tabla
                h_pos=a_pos=n_teams=0
                try:
                    path_p=os.path.join(DATA_DIR,f"{div}.csv")
                    if os.path.exists(path_p):
                        try:    df_p=pd.read_csv(path_p,encoding="utf-8-sig")
                        except: df_p=pd.read_csv(path_p,encoding="latin-1")
                        df_p.columns=df_p.columns.str.strip()
                        df_p=df_p.rename(columns={"Home":"HomeTeam","Away":"AwayTeam","HG":"FTHG","AG":"FTAG"})
                        pl_p=df_p.dropna(subset=["FTHG","FTAG"]).copy()
                        pl_p["FTHG"]=pl_p["FTHG"].astype(float); pl_p["FTAG"]=pl_p["FTAG"].astype(float)
                        h_pos,n_teams=get_pos(div,h,pl_p)
                        a_pos,_=get_pos(div,a,pl_p)
                except: pass
                # Días de descanso para el calendario
                try:
                    path_r=os.path.join(DATA_DIR,f"{div}.csv")
                    rest_h_c=rest_a_c=None
                    if os.path.exists(path_r):
                        try:    df_r=pd.read_csv(path_r,encoding="utf-8-sig")
                        except: df_r=pd.read_csv(path_r,encoding="latin-1")
                        df_r.columns=df_r.columns.str.strip()
                        df_r=df_r.rename(columns={"Home":"HomeTeam","Away":"AwayTeam"})
                        df_r["Date"]=pd.to_datetime(df_r["Date"],dayfirst=True,errors="coerce")
                        match_dt=pd.Timestamp(date_str)
                        # Fatiga real: liga + copas
                        rest_h_c = get_true_rest_days(h, match_dt, div, df_r)
                        rest_a_c = get_true_rest_days(a, match_dt, div, df_r)
                except: rest_h_c=rest_a_c=None
                matches.append({"date":date_str,"div":div,"league":cfg.get("name",div),
                    "home":h,"away":a,"home_form":hf,"away_form":af,
                    "home_stats":hs_q,"away_stats":as_q,
                    "xg_h":xh,"xg_a":xa,"ph":ph,"pd":pd_,"pa":pa,
                    "b365h":oh,"b365d":od,"b365a":oa,"picks":picks_m,
                    "home_pos":h_pos,"away_pos":a_pos,"n_teams":n_teams,
                    "rest_h":rest_h_c,"rest_a":rest_a_c})

            # Procesar fixtures.csv
            if fix_df is not None:
                for date_str in dates:
                    target = datetime.strptime(date_str, "%Y-%m-%d").date()
                    day = fix_df[fix_df["Date"].dt.date==target]
                    for _, row in day.iterrows():
                        div = str(row.get("Div","")).strip()
                        if div not in TARGET_LEAGUES: continue
                        h = str(row.get("HomeTeam","")).strip()
                        a = str(row.get("AwayTeam","")).strip()
                        if not h or not a: continue
                        add_match(div, h, a, date_str, row)

            # ── FUENTE B: CSVs históricos (filas sin resultado = partidos futuros) ──
            for div in TARGET_LEAGUES:
                if div in ("BSA","MEX"): continue
                path = os.path.join(DATA_DIR, f"{div}.csv")
                if not os.path.exists(path): continue
                try:
                    try:    df = pd.read_csv(path, encoding="utf-8-sig")
                    except: df = pd.read_csv(path, encoding="latin-1")
                    df.columns = df.columns.str.strip()
                    df = df.rename(columns={"Home":"HomeTeam","Away":"AwayTeam","HG":"FTHG","AG":"FTAG"})
                    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
                    future = df[df["FTHG"].isna() | df["FTAG"].isna()].copy()
                    if future.empty: continue
                    for date_str in dates:
                        target = datetime.strptime(date_str, "%Y-%m-%d").date()
                        for _, row in future[future["Date"].dt.date==target].iterrows():
                            h=str(row.get("HomeTeam","")).strip()
                            a=str(row.get("AwayTeam","")).strip()
                            if not h or not a: continue
                            add_match(div, h, a, date_str, row)
                except Exception as e:
                    Log.warn(f"calendar CSV {div}: {e}", "CAL")
            # ── FUENTE C: cup_fixtures (UCL/UEL/UECL + copas nacionales) ──────
            # Usar api-football /fixtures?date=X para partidos de copa hoy y próximos
            try:
                for date_str in dates:
                    # Buscar en cup_fixtures SQLite partidos YA jugados (para H2H/stats)
                    # Para partidos FUTUROS de copa, usar api-football
                    pass  # placeholder — ver abajo
            except Exception as e:
                Log.warn(f"cup calendar: {e}", "CAL")

            # ── FUENTE C: copa calendar desde SQLite (cacheado al arrancar) ────
            try:
                cup_conn = sqlite3.connect(DB_PATH)
                cup_rows = cup_conn.execute("""
                    SELECT league_id, competition, match_date, home_team, away_team,
                           status, odd_h, odd_d, odd_a
                    FROM cup_calendar
                    WHERE match_date >= ?
                    ORDER BY match_date, competition
                """, (dates[0],)).fetchall()
                cup_conn.close()
                for (lid, comp, fix_date, home, away, status, oh, od, oa) in cup_rows:
                    if fix_date not in dates: continue
                    div_key = f"CUP_{lid}"
                    key = f"{div_key}_{fix_date}_{home}_{away}"
                    if key in seen: continue
                    seen.add(key)
                    matches.append({
                        "date": fix_date, "div": div_key,
                        "league": comp, "home": home, "away": away,
                        "home_form": [], "away_form": [],
                        "home_stats": {}, "away_stats": {},
                        "xg_h": None, "xg_a": None,
                        "ph": None, "pd": None, "pa": None,
                        "b365h": oh, "b365d": od, "b365a": oa,
                        "picks": [], "home_pos": 0, "away_pos": 0, "n_teams": 0,
                        "rest_h": None, "rest_a": None,
                        "is_cup": True, "cup_name": comp,
                        "fixture_id": str(lid), "league_id": lid,
                    })
                Log.info(f"cup_calendar: {len(cup_rows)} partidos de copa en calendario", "CAL")
            except Exception as e:
                Log.warn(f"cup_calendar serve: {e}", "CAL")

            # ── FUENTE D: football-data.org directo si cup_calendar vacío ────
            # Request en tiempo real solo si la tabla está vacía o sin datos de hoy
            try:
                cup_conn2 = sqlite3.connect(DB_PATH)
                today_str = dates[0]
                n_today = cup_conn2.execute(
                    "SELECT COUNT(*) FROM cup_calendar WHERE match_date=?", (today_str,)
                ).fetchone()[0]
                cup_conn2.close()

                # Siempre refrescar de fd.org al abrir el calendario
                # Es rápido (3 requests) y garantiza datos actuales
                # Usar cache si es reciente (< 60 min)
                cache_ok = (today_str in _CUP_CAL_TS and
                            (now - _CUP_CAL_TS[today_str]).total_seconds() < 3600)

                if cache_ok:
                    for m in _CUP_CAL_CACHE.get(today_str, []):
                        key = f"{m['div']}_{m['date']}_{m['home']}_{m['away']}"
                        if key not in seen:
                            seen.add(key)
                            matches.append(m)
                    Log.info(f"cup_calendar: {len(_CUP_CAL_CACHE.get(today_str,[]))} partidos desde cache", "CAL")
                elif FD_ORG_TOKEN:
                    Log.info(f"Actualizando partidos de copa de hoy ({today_str})...", "CAL")
                    FD_LIVE = {
                        "CL":  (2,   "🏆 UCL"),
                        "EL":  (3,   "🥈 UEL"),
                        "EC":  (848, "🥉 UECL"),
                    }
                    for fd_code, (league_id, comp_name) in FD_LIVE.items():
                        try:
                            r_fd = requests.get(
                                f"https://api.football-data.org/v4/competitions/{fd_code}/matches",
                                headers={"X-Auth-Token": FD_ORG_TOKEN},
                                params={"dateFrom": today_str, "dateTo": today_str},
                                timeout=10
                            )
                            if r_fd.status_code != 200:
                                continue
                            for m in r_fd.json().get("matches", []):
                                try:
                                    fix_date = m["utcDate"][:10]
                                    home = m["homeTeam"]["name"]
                                    away = m["awayTeam"]["name"]
                                    if not home or home == "None": continue
                                    div_key = f"CUP_{league_id}"
                                    key = f"{div_key}_{fix_date}_{home}_{away}"
                                    if key in seen: continue
                                    seen.add(key)
                                    matches.append({
                                        "date": fix_date, "div": div_key,
                                        "league": comp_name,
                                        "home": home, "away": away,
                                        "home_form": [], "away_form": [],
                                        "home_stats": {}, "away_stats": {},
                                        "xg_h": None, "xg_a": None,
                                        "ph": None, "pd": None, "pa": None,
                                        "b365h": None, "b365d": None, "b365a": None,
                                        "picks": [], "home_pos": 0, "away_pos": 0,
                                        "n_teams": 0, "rest_h": None, "rest_a": None,
                                        "is_cup": True, "cup_name": comp_name,
                                        "fixture_id": str(m["id"]),
                                        "league_id": league_id,
                                    })
                                    # Cachear para próximos requests
                                    try:
                                        conn_c = sqlite3.connect(DB_PATH)
                                        conn_c.execute("""
                                            INSERT OR IGNORE INTO cup_calendar
                                            (fixture_id, league_id, competition, match_date,
                                             home_team, away_team, status, updated_at)
                                            VALUES (?,?,?,?,?,?,?,?)
                                        """, (str(m["id"]), league_id, comp_name, fix_date,
                                              home, away, m.get("status","SCHEDULED"),
                                              now.isoformat()))
                                        conn_c.commit(); conn_c.close()
                                    except Exception: pass
                                except Exception:
                                    continue
                            Log.ok(f"fd.org live {fd_code}: partidos de hoy cargados", "CAL")
                        except Exception as e:
                            Log.warn(f"fd.org live {fd_code}: {e}", "CAL")
                    # Guardar en cache después de todos los fetches
                    today_cup = [m for m in matches if m.get("is_cup") and m.get("date")==today_str]
                    _CUP_CAL_CACHE[today_str] = today_cup
                    _CUP_CAL_TS[today_str] = now
            except Exception as e:
                Log.warn(f"cup fallback: {e}", "CAL")

            # ── Enriquecer con cuotas Pinnacle actuales de live_odds ────
            if ODDS_API_KEY:
                try:
                    conn_lo = sqlite3.connect(DB_PATH)
                    lo_rows = conn_lo.execute("""
                        SELECT home_team, away_team, div, sport_key,
                               pin_h, pin_d, pin_a, pin_over, pin_under, updated_at
                        FROM live_odds
                        WHERE updated_at >= datetime('now', '-6 hours')
                        ORDER BY updated_at DESC
                    """).fetchall()
                    conn_lo.close()
                    # Índice fuzzy: (home_norm, away_norm) → odds
                    import difflib as _dl_lo
                    lo_idx = {}
                    for row in lo_rows:
                        lo_idx[(row[0], row[1])] = {
                            "pin_h": row[4], "pin_d": row[5], "pin_a": row[6],
                            "pin_over": row[7], "pin_under": row[8],
                            "odds_updated": row[9],
                        }
                    for m in matches:
                        # Buscar match fuzzy en live_odds
                        best = None; best_score = 0
                        for (lh, la), odds in lo_idx.items():
                            sh = _dl_lo.SequenceMatcher(None, m["home"].lower(), lh.lower()).ratio()
                            sa = _dl_lo.SequenceMatcher(None, m["away"].lower(), la.lower()).ratio()
                            score = (sh + sa) / 2
                            if score > best_score and score > 0.6:
                                best_score = score; best = odds
                        if best:
                            m.update(best)
                except Exception as _elo:
                    Log.warn(f"live_odds calendar: {_elo}", "ODDS")

            # ── FUENTE E: live_odds → UEL/UECL y copas sin fd.org ────────
            # The Odds API ya descargó estos partidos con cuotas Pinnacle
            # Usarlos como fuente de fixtures cuando fd.org da 403
            if ODDS_API_KEY:
                try:
                    _CUP_SPORT_MAP = {
                        "soccer_uefa_europa_league":             (3,   "🥈 UEL"),
                        "soccer_uefa_europa_conference_league":  (848, "🥉 UECL"),
                        "soccer_uefa_champs_league":             (2,   "🏆 UCL"),
                    }
                    conn_lo2 = sqlite3.connect(DB_PATH)
                    lo_cup = conn_lo2.execute("""
                        SELECT sport_key, home_team, away_team, commence_time,
                               pin_h, pin_d, pin_a
                        FROM live_odds
                        WHERE sport_key IN (
                            'soccer_uefa_europa_league',
                            'soccer_uefa_europa_conference_league',
                            'soccer_uefa_champs_league'
                        )
                        ORDER BY commence_time
                    """).fetchall()
                    conn_lo2.close()

                    for (sk, home, away, commence, pin_h, pin_d, pin_a) in lo_cup:
                        if not home or not away: continue
                        fix_date = commence[:10] if commence else None
                        if not fix_date or fix_date not in dates: continue
                        league_id, comp_name = _CUP_SPORT_MAP.get(sk, (0, "Copa"))
                        div_key = f"CUP_{league_id}"
                        key = f"{div_key}_{fix_date}_{home}_{away}"
                        if key in seen: continue
                        seen.add(key)
                        matches.append({
                            "date": fix_date,
                            "div": div_key,
                            "league": comp_name,
                            "home": home, "away": away,
                            "home_form": [], "away_form": [],
                            "home_stats": {}, "away_stats": {},
                            "xg_h": None, "xg_a": None,
                            "ph": None, "pd": None, "pa": None,
                            "b365h": None, "b365d": None, "b365a": None,
                            "pin_h": pin_h, "pin_d": pin_d, "pin_a": pin_a,
                            "picks": [], "home_pos": 0, "away_pos": 0,
                            "n_teams": 0, "rest_h": None, "rest_a": None,
                            "is_cup": True, "cup_name": comp_name,
                            "fixture_id": None, "league_id": league_id,
                            "odds_updated": None,
                        })
                    Log.info(f"live_odds copa: {len(lo_cup)} partidos UEL/UECL/UCL", "CAL")
                    # Si live_odds está vacío, intentar fetch inmediato
                    if len(lo_cup) == 0 and ODDS_API_KEY:
                        Log.info("live_odds vacío — fetching UEL/UECL ahora...", "CAL")
                        try:
                            fetch_live_odds(["CUP_2","CUP_3","CUP_848"])
                            # Re-intentar query
                            conn_lo3 = sqlite3.connect(DB_PATH)
                            lo_cup = conn_lo3.execute("""
                                SELECT sport_key, home_team, away_team, commence_time,
                                       pin_h, pin_d, pin_a
                                FROM live_odds
                                WHERE sport_key IN (
                                    'soccer_uefa_europa_league',
                                    'soccer_uefa_europa_conference_league',
                                    'soccer_uefa_champs_league'
                                )
                                ORDER BY commence_time
                            """).fetchall()
                            conn_lo3.close()
                            Log.ok(f"live_odds copa retry: {len(lo_cup)} partidos", "CAL")
                            # Agregar los partidos recién fetched
                            for (sk, home, away, commence, pin_h, pin_d, pin_a) in lo_cup:
                                if not home or not away: continue
                                fix_date = commence[:10] if commence else None
                                if not fix_date or fix_date not in dates: continue
                                league_id2, comp_name2 = _CUP_SPORT_MAP.get(sk, (0,"Copa"))
                                div_key2 = f"CUP_{league_id2}"
                                key2 = f"{div_key2}_{fix_date}_{home}_{away}"
                                if key2 in seen: continue
                                seen.add(key2)
                                matches.append({
                                    "date": fix_date, "div": div_key2,
                                    "league": comp_name2, "home": home, "away": away,
                                    "home_form": [], "away_form": [],
                                    "home_stats": {}, "away_stats": {},
                                    "xg_h": None, "xg_a": None,
                                    "ph": None, "pd": None, "pa": None,
                                    "b365h": None, "b365d": None, "b365a": None,
                                    "pin_h": pin_h, "pin_d": pin_d, "pin_a": pin_a,
                                    "picks": [], "home_pos": 0, "away_pos": 0,
                                    "n_teams": 0, "rest_h": None, "rest_a": None,
                                    "is_cup": True, "cup_name": comp_name2,
                                    "fixture_id": None, "league_id": league_id2,
                                    "odds_updated": None,
                                })
                        except Exception as _er:
                            Log.warn(f"live_odds retry: {_er}", "CAL")
                except Exception as _elo2:
                    Log.warn(f"live_odds copa calendar: {_elo2}", "CAL")

            matches.sort(key=lambda x:(x["date"],x["league"]))
            payload=json.dumps({"matches":matches,"dates":dates}).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        except Exception as e:
            import traceback; Log.err(f"calendar: {e}\n{traceback.format_exc()}", "CAL")
            self._json_err(str(e))

    def _serve_picks_summary(self):
        """API /api/picks_summary — stats por mercado y curva PnL."""
        try:
            conn = sqlite3.connect(DB_PATH)
            mkt_stats = conn.execute("""
                SELECT market, COUNT(*) as n,
                       SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses,
                       AVG(ev) as avg_ev, SUM(profit) as total_pnl
                FROM picks_log WHERE result IN ('WIN','LOSS')
                GROUP BY market ORDER BY n DESC
            """).fetchall()
            pnl_curve = conn.execute("""
                SELECT profit, result, date(kickoff_time) as dt
                FROM picks_log WHERE result IN ('WIN','LOSS')
                ORDER BY kickoff_time ASC
            """).fetchall()
            conn.close()
            mkt_list=[{"market":r[0],"n":r[1],"wins":r[2],"losses":r[3],
                "br":round(r[2]/(r[2]+r[3])*100,1) if (r[2]+r[3])>0 else 0,
                "avg_ev":round(float(r[4] or 0)*100,1),"pnl":round(float(r[5] or 0),4)}
                for r in mkt_stats]
            cum=0; curve=[]
            for profit,result,dt in pnl_curve:
                cum+=float(profit or 0)
                curve.append({"pnl":round(cum,4),"result":result,"date":dt})
            payload=json.dumps({"mkt_stats":mkt_list,"pnl_curve":curve}).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        except Exception as e: self._json_err(str(e))


    def do_GET(self):
        try:
            if self.path == "/" or self.path == "/dashboard":
                self._serve_html()
            elif self.path == "/api/picks":
                self._serve_api()
            elif self.path == "/api/sync":
                self._serve_sync()
            elif self.path == "/api/resolve":
                self._serve_resolve()
            elif self.path.startswith("/api/analyze"):
                self._serve_analyze()
            elif self.path == "/stats":
                self._serve_stats_html()
            elif self.path.startswith("/api/stats"):
                self._serve_stats_api()
            elif self.path.startswith("/api/calendar"):
                self._serve_calendar()
            elif self.path == "/api/cups":
                self._serve_cups_status()
            elif self.path == "/api/fbref":
                self._serve_fbref_status()
            elif self.path.startswith("/api/picks_summary"):
                self._serve_picks_summary()
            elif self.path.startswith("/api/log_pick"):
                self._serve_log_pick()
            elif self.path.startswith("/api/line_moves"):
                self._serve_line_moves()
            else:
                self.send_response(404); self.end_headers()
        except Exception as _e:
            import traceback as _tb
            Log.err(f"do_GET {self.path}: {_e}\n{_tb.format_exc()}", "HTTP")
            try:
                self._json_err(str(_e))
            except Exception:
                pass

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
            # Query principal — columnas fijas, sin JOIN para evitar errores
            c.execute("""
                SELECT id, pick_time, div, home_team, away_team,
                       market, selection, odd_open, prob_model, ev_open,
                       result, stake_pct, profit, xg_home, xg_away
                FROM picks_log ORDER BY id DESC LIMIT 500
            """)
            rows = c.fetchall()

            # CLV lookup — dos fuentes:
            # A) closing_lines (capturado en tiempo real el día del partido)
            # B) CSV histórico de co.uk (B365C, PSC, MaxC para picks pasados)
            clv_map = {}
            try:
                # Fuente A: closing_lines ya capturadas
                clv_rows = conn.execute("""
                    SELECT p.id, cl.clv_pct, cl.clv_pct_ps, cl.clv_pct_maxc
                    FROM picks_log p
                    JOIN closing_lines cl
                        ON cl.fixture_id = p.fixture_id
                        AND cl.market    = p.market
                        AND cl.selection = p.selection
                    WHERE p.clv_captured = 1
                """).fetchall()
                for row in clv_rows:
                    clv_map[row[0]] = (row[1], row[2], row[3])
            except Exception:
                pass

            # Fuente B: CSV histórico — calcular CLV para picks sin closing_lines
            try:
                import pandas as pd, os, difflib as _dl
                no_clv_picks = conn.execute("""
                    SELECT id, div, home_team, away_team, market, selection,
                           odd_open, pick_time
                    FROM picks_log
                    WHERE result IN ('WIN','LOSS')
                    AND id NOT IN (SELECT p.id FROM picks_log p
                                   JOIN closing_lines cl
                                   ON cl.fixture_id=p.fixture_id
                                   AND cl.market=p.market
                                   AND cl.selection=p.selection)
                """).fetchall()

                # Cargar CSVs ya descargados
                csv_cache = {}
                for pid, div, home, away, mkt, sel, odd_open, pick_time in no_clv_picks:
                    if pid in clv_map: continue
                    if div not in csv_cache:
                        path = os.path.join(DATA_DIR, f"{div}.csv")
                        if not os.path.exists(path): continue
                        try:
                            try:    df_c = pd.read_csv(path, encoding="utf-8-sig")
                            except: df_c = pd.read_csv(path, encoding="latin-1")
                            df_c.columns = df_c.columns.str.strip()
                            df_c = df_c.rename(columns={"Home":"HomeTeam","Away":"AwayTeam",
                                                        "HG":"FTHG","AG":"FTAG"})
                            df_c["Date"] = pd.to_datetime(df_c["Date"], dayfirst=True, errors="coerce")
                            csv_cache[div] = df_c.dropna(subset=["FTHG","FTAG"])
                        except: continue
                    df = csv_cache.get(div)
                    if df is None: continue
                    # Buscar partido por equipos (fuzzy)
                    teams_h = df["HomeTeam"].unique()
                    teams_a = df["AwayTeam"].unique()
                    mh = _dl.get_close_matches(home, teams_h, n=1, cutoff=0.6)
                    ma = _dl.get_close_matches(away, teams_a, n=1, cutoff=0.6)
                    if not mh or not ma: continue
                    match_rows = df[(df["HomeTeam"]==mh[0]) & (df["AwayTeam"]==ma[0])]
                    if match_rows.empty: continue
                    row_m = match_rows.iloc[-1]  # último enfrentamiento

                    def _safe(col):
                        try: v = float(row_m.get(col, 0) or 0); return v if v > 1.01 else None
                        except: return None

                    # Cuota cierre B365
                    if mkt in ("1X2","DC"):
                        if "Empate" in sel or sel == "D":
                            oc_b  = _safe("B365CD")
                            oc_ps = _safe("PSCD")   # Pinnacle cierre empate
                            oc_mx = _safe("MaxCD")
                        elif home in sel or sel == "H":
                            oc_b  = _safe("B365CH")
                            oc_ps = _safe("PSCH")   # Pinnacle cierre
                            oc_mx = _safe("MaxCH")
                        else:
                            oc_b  = _safe("B365CA")
                            oc_ps = _safe("PSCA")   # Pinnacle cierre
                            oc_mx = _safe("MaxCA")
                    elif mkt in ("OVER","UNDER"):
                        if mkt == "OVER":
                            oc_b  = _safe("B365C>2.5")
                            oc_ps = _safe("MaxC>2.5")   # mejor proxy para Pinnacle O/U
                            oc_mx = _safe("MaxC>2.5")
                        else:
                            oc_b  = _safe("B365C<2.5")
                            oc_ps = _safe("MaxC<2.5")
                            oc_mx = _safe("MaxC<2.5")
                    else:
                        oc_b = oc_ps = oc_mx = None

                    if not oc_b and not oc_ps: continue
                    oc_ref = oc_b or oc_ps
                    clv_b365 = round((odd_open/oc_b  - 1)*100, 1) if oc_b  and oc_b  > 1.01 else None
                    clv_ps_v = round((odd_open/oc_ps - 1)*100, 1) if oc_ps and oc_ps > 1.01 else None
                    clv_mx   = round((odd_open/oc_mx - 1)*100, 1) if oc_mx and oc_mx > 1.01 else None
                    if clv_b365 is not None or clv_ps_v is not None:
                        clv_map[pid] = (clv_b365, clv_ps_v, clv_mx)
            except Exception as _eclv:
                Log.warn(f"CLV CSV fallback: {_eclv}", "CLV")

            conn.close()

            picks = []
            wins = losses = pending = 0
            total_ev = total_pnl = 0.0
            resolved = 0

            for r in rows:
                # r: id(0) pick_time(1) div(2) home(3) away(4)
                #    market(5) selection(6) odd_open(7) prob_model(8) ev_open(9)
                #    result(10) stake_pct(11) profit(12) xg_home(13) xg_away(14)
                pid    = r[0]
                status = r[10] or "PENDING"
                if status == "WIN":  wins += 1; resolved += 1
                elif status == "LOSS": losses += 1; resolved += 1
                else: pending += 1
                ev = float(r[9] or 0)
                total_ev += ev
                total_pnl += float(r[12] or 0)
                clv_tuple = clv_map.get(pid, (None, None, None))
                def _cf(v):
                    try: return round(float(v), 1) if v is not None else None
                    except: return None
                picks.append({
                    "date":    r[1],  "div":    r[2],
                    "home":    r[3],  "away":   r[4],
                    "market":  r[5],  "pick":   r[6],
                    "odd":     r[7],  "prob":   r[8],
                    "ev":      ev,  # decimal (0.094) — JS multiplica por 100
                    "status":  status,
                    "stake":   r[11], "profit": r[12],
                    "xg_h":    r[13], "xg_a":   r[14],
                    "clv_b365": _cf(clv_tuple[0]),
                    "clv_ps":   _cf(clv_tuple[1]),
                    "clv_maxc": _cf(clv_tuple[2]),
                })

            n = len(picks)
            avg_ev = (total_ev / n * 100) if n else 0
            # KPI CLV Pinnacle — la métrica más importante del modelo
            clv_ps_vals  = [p["clv_ps"]   for p in picks if p.get("clv_ps")   is not None]
            clv_b365_vals= [p["clv_b365"] for p in picks if p.get("clv_b365") is not None]
            clv_maxc_vals= [p["clv_maxc"] for p in picks if p.get("clv_maxc") is not None]
            avg_clv_ps   = round(sum(clv_ps_vals)  /len(clv_ps_vals),  2) if clv_ps_vals   else None
            avg_clv_b365 = round(sum(clv_b365_vals)/len(clv_b365_vals),2) if clv_b365_vals else None
            avg_clv_maxc = round(sum(clv_maxc_vals)/len(clv_maxc_vals),2) if clv_maxc_vals else None
            payload = json.dumps({
                "picks": picks,
                "clv_summary": {
                    "avg_clv_pinnacle": avg_clv_ps,
                    "avg_clv_b365":     avg_clv_b365,
                    "avg_clv_maxc":     avg_clv_maxc,
                    "n_with_clv":       len(clv_ps_vals),
                    "edge_confirmed":   avg_clv_ps is not None and avg_clv_ps > 0,
                },
                "stats": {
                    "total":    int(n),
                    "wins":     int(wins),
                    "losses":   int(losses),
                    "pending":  int(pending),
                    "avg_ev":   float(avg_ev),
                    "pnl":      float(total_pnl),
                    "resolved": int(resolved)
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

    def _json_err(self, msg):
        payload = json.dumps({"error": msg}).encode()
        self.send_response(500)
        self.send_header("Content-Type","application/json")
        self.send_header("Access-Control-Allow-Origin","*")
        self.end_headers(); self.wfile.write(payload)

    def _serve_line_moves(self):
        """GET /api/line_moves — movimientos de línea detectados."""
        try:
            hours = 48
            moves = detect_line_moves(hours_window=hours)
            result = []
            for (home, away, div), mv in moves.items():
                if mv["signal"] == "none" and mv["snaps"] < 3: continue
                cfg = TARGET_LEAGUES.get(div, {})
                result.append({
                    "home": home, "away": away,
                    "div": div, "league": cfg.get("name", div),
                    "move_h":     mv["move_h"],
                    "move_d":     mv["move_d"],
                    "move_a":     mv["move_a"],
                    "move_dnb_h": mv["move_dnb_h"],
                    "move_dnb_a": mv["move_dnb_a"],
                    "signal":     mv["signal"],
                    "confidence": mv["confidence"],
                    "open_h":     mv["open_h"],
                    "close_h":    mv["close_h"],
                    "snaps":      mv["snaps"],
                    "hours_span": mv["hours_span"],
                })
            # Ordenar: sharps primero, luego por magnitud de movimiento
            result.sort(key=lambda x: (
                0 if "sharp" in x["signal"] else 1,
                -abs(x["move_h"])
            ))
            payload = json.dumps({"moves": result, "hours": hours}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(payload)
        except Exception as e:
            self._json_err(str(e))

    def _serve_log_pick(self):
        """POST /api/log_pick — registra pick manual desde panel de análisis."""
        try:
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            def g(k, default=None):
                v = qs.get(k, [default])[0]
                return v if v not in (None,"","null","undefined") else default
            home=g("home"); away=g("away"); div=g("div","")
            market=g("market"); selection=g("selection")
            odd=float(g("odd",0) or 0); prob=float(g("prob",0) or 0)
            ev=float(g("ev",0) or 0); ko_str=g("ko","")
            xg_h=float(g("xg_h",0) or 0); xg_a=float(g("xg_a",0) or 0)
            if not home or not away or not market or odd<=1.0:
                self._json_err("Faltan datos"); return
            ev_dec = ev if ev<1 else ev/100
            stake  = kelly_urs(ev_dec, odd, market)
            now = datetime.now(timezone.utc)
            conn = sqlite3.connect(DB_PATH)
            existing = conn.execute(
                "SELECT id FROM picks_log WHERE home_team=? AND away_team=? AND market=? AND result='PENDING'",
                (home, away, market)).fetchone()
            if existing:
                conn.close()
                payload = json.dumps({"ok":False,"msg":"Pick ya registrado","id":existing[0]}).encode()
            else:
                cfg = TARGET_LEAGUES.get(div,{})
                c = conn.cursor()
                c.execute("""INSERT INTO picks_log
                    (fixture_id,league,div,home_team,away_team,market,selection,
                     odd_open,prob_model,ev_open,stake_pct,xg_home,xg_away,xg_total,
                     xg_source,pick_time,kickoff_time,clv_captured,result,profit)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,'PENDING',0)""",
                    (None,cfg.get("name",div),div,home,away,market,selection,
                     odd,prob,ev_dec,stake,xg_h,xg_a,round(xg_h+xg_a,2),
                     "proxy",now.isoformat(),ko_str))
                pid = c.lastrowid
                conn.commit(); conn.close()
                try:
                    nl = "\n"
                    msg = (f"📝 Pick Manual{nl}⚽ {home} vs {away}{nl}"
                           f"🎯 {market}: {selection} @ {_dec_to_us(odd)} ({odd:.2f}){nl}"
                           f"📊 EV: {ev_dec*100:.1f}% | Stake: {stake*100:.2f}%{nl}"
                           f"🔢 Pick #{pid}")
                    send_msg(msg)
                except: pass
                Log.ok(f"Pick manual #{pid}: {home} vs {away} {market} @{odd:.2f}","PICK")
                payload = json.dumps({"ok":True,"id":pid,
                    "msg":f"{market} {selection} @ {_dec_to_us(odd)} registrado"}).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        except Exception as e:
            import traceback; Log.err(f"log_pick: {e} " + traceback.format_exc(),"PICK")
            self._json_err(str(e))

    def _serve_stats_html(self):
        self.send_response(200)
        self.send_header("Content-Type","text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(STATS_HTML.encode("utf-8"))

    def _serve_stats_api(self):
        """API /api/stats?league=E1"""
        try:
            import pandas as pd
            from urllib.parse import urlparse, parse_qs
            qs  = parse_qs(urlparse(self.path).query)
            div = qs.get("league",["E1"])[0].upper()
            cfg = TARGET_LEAGUES.get(div)
            if not cfg: self._json_err(f"Liga {div} no encontrada"); return
            path = os.path.join(DATA_DIR, f"{div}.csv")
            if not os.path.exists(path): self._json_err(f"CSV {div} no disponible"); return
            try:    df = pd.read_csv(path, encoding="utf-8-sig")
            except: df = pd.read_csv(path, encoding="latin-1")
            df.columns = df.columns.str.strip()
            df = df.rename(columns={"Home":"HomeTeam","Away":"AwayTeam","HG":"FTHG","AG":"FTAG"})
            df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
            played = df.dropna(subset=["FTHG","FTAG"]).copy()
            played["FTHG"] = played["FTHG"].astype(float)
            played["FTAG"] = played["FTAG"].astype(float)
            if played.empty: self._json_err("Sin partidos jugados"); return
            teams = {}
            for _, r in played.iterrows():
                for side in ["home","away"]:
                    t = r["HomeTeam"] if side=="home" else r["AwayTeam"]
                    gf= r["FTHG"] if side=="home" else r["FTAG"]
                    ga= r["FTAG"] if side=="home" else r["FTHG"]
                    if t not in teams:
                        teams[t]={"pj":0,"pg":0,"pe":0,"pp":0,"gf":0,"ga":0,"pts":0,"form":[],"btts":0,"over25":0}
                    teams[t]["pj"]+=1; teams[t]["gf"]+=gf; teams[t]["ga"]+=ga
                    teams[t]["btts"] += 1 if (r["FTHG"]>0 and r["FTAG"]>0) else 0
                    teams[t]["over25"] += 1 if (r["FTHG"]+r["FTAG"])>2.5 else 0
                    if gf>ga: teams[t]["pg"]+=1; teams[t]["pts"]+=3; teams[t]["form"].append("W")
                    elif gf==ga: teams[t]["pe"]+=1; teams[t]["pts"]+=1; teams[t]["form"].append("D")
                    else: teams[t]["pp"]+=1; teams[t]["form"].append("L")
            table = sorted([{"team":t,"pj":d["pj"],"pg":d["pg"],"pe":d["pe"],"pp":d["pp"],
                "gf":int(d["gf"]),"ga":int(d["ga"]),"gd":int(d["gf"]-d["ga"]),"pts":d["pts"],
                "form":d["form"][-5:],"btts_pct":round(d["btts"]/d["pj"]*100,1) if d["pj"] else 0,
                "over25_pct":round(d["over25"]/d["pj"]*100,1) if d["pj"] else 0,
                "avg_gf":round(d["gf"]/d["pj"],2) if d["pj"] else 0,
                "avg_ga":round(d["ga"]/d["pj"],2) if d["pj"] else 0,
                "xgf":None,"xga":None}
                for t,d in teams.items() if d["pj"]>0],
                key=lambda x:(-x["pts"],-x["gd"],-x["gf"]))
            future = df[df["FTHG"].isna()].copy()
            future["Date"] = pd.to_datetime(future["Date"], dayfirst=True, errors="coerce")
            upcoming = []
            for _, r in future.sort_values("Date").head(15).iterrows():
                h=str(r.get("HomeTeam","")).strip(); a=str(r.get("AwayTeam","")).strip()
                if not h or not a: continue
                upcoming.append({"date":r["Date"].strftime("%d/%m") if pd.notna(r["Date"]) else "?",
                    "home":h,"away":a,"xg_h":None,"xg_a":None,"ph":None,"pd":None,"pa":None,
                    "b365h":float(r["B365H"]) if "B365H" in r and pd.notna(r.get("B365H")) else None,
                    "b365d":float(r["B365D"]) if "B365D" in r and pd.notna(r.get("B365D")) else None,
                    "b365a":float(r["B365A"]) if "B365A" in r and pd.notna(r.get("B365A")) else None})
            total=len(played)
            league_stats={"name":cfg.get("name",""),"total_games":total,
                "avg_goals":round((played["FTHG"]+played["FTAG"]).mean(),2),
                "btts_pct":round(((played["FTHG"]>0)&(played["FTAG"]>0)).mean()*100,1),
                "over25_pct":round(((played["FTHG"]+played["FTAG"])>2.5).mean()*100,1),
                "home_win_pct":round((played["FTHG"]>played["FTAG"]).mean()*100,1),
                "draw_pct":round((played["FTHG"]==played["FTAG"]).mean()*100,1),
                "away_win_pct":round((played["FTHG"]<played["FTAG"]).mean()*100,1)}
            payload=json.dumps({"table":table,"upcoming":upcoming,"league_stats":league_stats,"div":div}).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        except Exception as e: self._json_err(str(e))

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
            Log.ok(f"Auto-resolve: {resolved} picks ({wins}W/{losses}L)", "RESOLVE")
    except Exception as e:
        Log.err(f"auto_resolve: {e}", "RESOLVE")

def start_dashboard(port=8080):
    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    Log.ok(f"Dashboard en http://0.0.0.0:{port}", "DASH")
    return server

if __name__ == "__main__":
    # Arrancar dashboard web en puerto 8080
    start_dashboard(port=int(os.getenv("PORT", "8080")))
    auto_resolve()   # resolver picks PENDING contra CSVs al arrancar
    # Cargar copa fixtures al arrancar
    _apif_h = {
        "x-apisports-key": API_SPORTS_KEY,
        "x-rapidapi-host": "v3.football.api-sports.io"
    }
    try:
        is_empty = _cup_db_is_empty()
        n = fetch_cup_fixtures(_apif_h, full_season=is_empty)
        mode = "temporada completa" if is_empty else "últimos 10 días"
        Log.ok(f"Cup fixtures [{mode}]: {n} registros", "CUPS")
    except Exception as e:
        Log.warn(f"cup_fixtures startup: {e}", "CUPS")
    # Cargar partidos próximos de copa para el calendario
    try:
        nc = fetch_cup_calendar(_apif_h)
        Log.ok(f"cup_calendar: {nc} partidos próximos", "CAL")
    except Exception as e:
        Log.warn(f"cup_calendar startup: {e}", "CAL")

    # FBref xG — desactivado: Railway bloqueado (403) por FBref
    Log.warn("FBref xG desactivado — IP de Railway bloqueada por FBref (403). Usando xG proxy.", "FBREF")

    # The Odds API — cuotas Pinnacle en tiempo real
    if ODDS_API_KEY:
        try:
            Log.info("Odds API: descargando cuotas Pinnacle...", "ODDS")
            fetch_live_odds()
            Log.ok("Odds API: cuotas cargadas", "ODDS")
        except Exception as e:
            Log.warn(f"Odds API startup: {e}", "ODDS")
    else:
        Log.warn("ODDS_API_KEY no configurada — sin cuotas Pinnacle en tiempo real", "ODDS")
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

    Log.ok("Scheduler UTC activo — 06:00 CSV / 06:30+10:30+14:00 SCAN / 07:00 AUDIT / 16:00 CLV / 20:00 ODDS", "SCHED")
    while True:
        now = datetime.now(timezone.utc)
        hh, mm = now.hour, now.minute

        # 05:45 — actualizar cup_calendar (partidos próximos de copa)
        if (hh, mm) == (5, 45) and not _ran_today("cup_cal"):
            _mark_ran("cup_cal")
            try:
                _h2 = {"x-apisports-key": API_SPORTS_KEY, "x-rapidapi-host": "v3.football.api-sports.io"}
                nc = fetch_cup_calendar(_h2)
                Log.ok(f"cup_calendar refresh: {nc} partidos", "CAL")
            except Exception as e: Log.err(f"cup_calendar: {e}", "CAL")

        # 20:00 UTC — actualizar cuotas Pinnacle (D-0 antes de cierre de líneas)
        if (hh, mm) == (20, 0) and not _ran_today("odds_live"):
            _mark_ran("odds_live")
            if ODDS_API_KEY:
                try:
                    fetch_live_odds()
                    Log.ok("Odds API: cuotas actualizadas (20:00 UTC)", "ODDS")
                except Exception as e: Log.err(f"Odds API: {e}", "ODDS")

        # 06:00 — refresh CSVs + actualizar Pinnacle pre-scan
        if (hh, mm) == (CSV_H, CSV_M) and not _ran_today("csv"):
            _mark_ran("csv")
            try: bot.refresh_csvs()
            except Exception as e: Log.err(f"refresh_csvs: {e}", "SCHED")
            # Actualizar cuotas Pinnacle justo antes del scan de 06:30
            if ODDS_API_KEY:
                try:
                    fetch_live_odds()
                    Log.ok("Odds Pinnacle pre-scan actualizadas (06:00 UTC)", "ODDS")
                except Exception as e: Log.warn(f"Odds pre-scan: {e}", "ODDS")

        # 07:00 — audit + pnl + auto-resolve
        if (hh, mm) == (AUDIT_H, AUDIT_M) and not _ran_today("audit"):
            _mark_ran("audit")
            try: run_audit()
            except Exception as e: Log.err(f"run_audit: {e}", "SCHED")
            try: auto_resolve()
            except Exception as e: Log.err(f"auto_resolve: {e}", "SCHED")
            try: calc_pnl()
            except Exception as e: Log.err(f"calc_pnl: {e}", "SCHED")
            Log.daily_summary()

        # 06:30 — scan principal (jornada del día con cuotas frescas)
        if (hh, mm) == (SCAN_H, SCAN_M) and not _ran_today("scan"):
            _mark_ran("scan")
            if not kill_switch_check():
                try: bot.run_daily_scan()
                except Exception as e: Log.err(f"run_daily_scan: {e}", "SCHED")
            else:
                Log.warn("Scan omitido — kill-switch activo", "SCHED")

        # 10:30 UTC — scan mediodía + actualizar Pinnacle antes del scan
        if (hh, mm) == (10, 30) and not _ran_today("scan_mid"):
            _mark_ran("scan_mid")
            if not kill_switch_check():
                try:
                    if ODDS_API_KEY: fetch_live_odds()  # cuotas frescas antes del scan
                    bot.run_daily_scan()
                    Log.ok("Scan 10:30 UTC completado", "SCHED")
                except Exception as e: Log.err(f"scan_mid: {e}", "SCHED")

        # 14:00 UTC — scan tarde (partidos nocturnos + Liga MX)
        if (hh, mm) == (14, 0) and not _ran_today("scan_late"):
            _mark_ran("scan_late")
            if not kill_switch_check():
                try:
                    if ODDS_API_KEY: fetch_live_odds()
                    bot.run_daily_scan()
                    Log.ok("Scan 14:00 UTC completado", "SCHED")
                except Exception as e: Log.err(f"scan_late: {e}", "SCHED")

        # 16:00 — capture CLV
        if (hh, mm) == (CLV_H, CLV_M) and not _ran_today("clv"):
            _mark_ran("clv")
            try: bot.capture_clv()
            except Exception as e: Log.err(f"capture_clv: {e}", "SCHED")

        # HT Alerts — cada 30 min durante ventana europea (12:00-23:00 UTC)
        # Detecta marcadores de primer tiempo y avisa si el pick está en riesgo
        if 12 <= hh <= 23 and mm % 30 == 0:
            try: bot.check_ht_alerts()
            except Exception as e: Log.warn(f"HT alerts: {e}", "HT")

        # Martes 09:00 — standings
        if now.weekday() == 1 and (hh, mm) == (STAND_H, STAND_M) and not _ran_this_week("standings"):
            _mark_ran_week("standings")
            try: bot.weekly_standings()
            except Exception as e: Log.err(f"weekly_standings: {e}", "SCHED")
            # Copas: actualizar lunes a las 05:00 UTC
            now_d = datetime.now(timezone.utc)
            if now_d.weekday() == 0 and abs(now_d.hour - 5) < 1:
                try:
                    _h = {"x-apisports-key": API_SPORTS_KEY, "x-rapidapi-host": "v3.football.api-sports.io"}
                    n = fetch_cup_fixtures(_h, full_season=False)
                    Log.ok(f"Cup fixtures actualizados: {n} registros", "CUPS")
                except Exception as e: Log.err(f"cup_fixtures: {e}", "CUPS")

            # FBref xG: desactivado — Railway bloqueado por FBref (403)
            # pass

        time.sleep(30)
