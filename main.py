"""
DIAGNÓSTICO V6.2 — Ejecutar en Railway con:
    python diagnostico.py

Comprueba en orden:
    1. Variables de entorno presentes
    2. Conectividad con api-football
    3. Temporada activa (2025 vs 2024)
    4. Fixtures disponibles próximos 7 días
    5. Cuotas Bet365 disponibles para esos fixtures
    6. Estado de la base de datos
"""

import os
import sqlite3
import requests
from datetime import datetime, timedelta, timezone

API_SPORTS_KEY   = os.getenv("API_SPORTS_KEY", "")
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_DIR           = os.getenv("DB_DIR", "./data")
DB_PATH          = os.path.join(DB_DIR, "quant_v6.db")

CHAMPIONSHIP_ID = 40
HEADERS = {'x-apisports-key': API_SPORTS_KEY}

SEP = "=" * 55

def check(label, ok, detail=""):
    status = "✅" if ok else "❌"
    print(f"  {status} {label}")
    if detail:
        print(f"     → {detail}")

# ── 1. VARIABLES DE ENTORNO ──────────────────────────────────
print(f"\n{SEP}")
print("  1. VARIABLES DE ENTORNO")
print(SEP)
check("API_SPORTS_KEY",   bool(API_SPORTS_KEY),   f"{'*'*6}{API_SPORTS_KEY[-4:]}" if API_SPORTS_KEY else "NO ENCONTRADA")
check("TELEGRAM_TOKEN",   bool(TELEGRAM_TOKEN),   "presente" if TELEGRAM_TOKEN else "NO ENCONTRADA")
check("TELEGRAM_CHAT_ID", bool(TELEGRAM_CHAT_ID), TELEGRAM_CHAT_ID or "NO ENCONTRADA")
check("DB_DIR",           True,                   DB_PATH)

if not API_SPORTS_KEY:
    print("\n  ⛔ Sin API key no hay nada que diagnosticar. Verifica las variables en Railway.")
    exit(1)

# ── 2. CONECTIVIDAD API ──────────────────────────────────────
print(f"\n{SEP}")
print("  2. CONECTIVIDAD API-FOOTBALL")
print(SEP)
try:
    r = requests.get(
        "https://v3.football.api-sports.io/status",
        headers=HEADERS, timeout=10
    )
    data = r.json()
    account = data.get('response', {}).get('account', {})
    sub     = data.get('response', {}).get('subscription', {})
    reqs    = data.get('response', {}).get('requests', {})
    check("Conexión OK", r.status_code == 200)
    check("Plan",        True, sub.get('plan', 'desconocido'))
    check("Requests hoy", True,
          f"{reqs.get('current', '?')} / {reqs.get('limit_day', '?')}")
    active = sub.get('active', False)
    check("Suscripción activa", active, "" if active else "⚠️ SUSCRIPCIÓN INACTIVA")
except Exception as e:
    check("Conexión", False, str(e))
    print("\n  ⛔ No hay conectividad. Verifica la API key y el plan.")
    exit(1)

# ── 3. TEMPORADA ACTIVA ──────────────────────────────────────
print(f"\n{SEP}")
print("  3. TEMPORADA ACTIVA CHAMPIONSHIP")
print(SEP)
active_season = None
for season in [2025, 2024, 2023]:
    try:
        r = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers=HEADERS,
            params={"league": CHAMPIONSHIP_ID, "season": season, "next": 3},
            timeout=10
        )
        fixtures = r.json().get('response', [])
        has_data = len(fixtures) > 0
        check(f"Season {season}", has_data,
              f"{len(fixtures)} próximos fixtures" if has_data else "sin datos")
        if has_data and not active_season:
            active_season = season
    except Exception as e:
        check(f"Season {season}", False, str(e))

if not active_season:
    print("\n  ⛔ Ninguna temporada tiene fixtures. Posibles causas:")
    print("     - Parón internacional o fin de temporada")
    print("     - La API no tiene el calendario cargado aún")
    print("     - La liga ID 40 no está en tu plan")

# ── 4. FIXTURES PRÓXIMOS 7 DÍAS ──────────────────────────────
print(f"\n{SEP}")
print(f"  4. FIXTURES PRÓXIMOS 7 DÍAS (season={active_season or 2024})")
print(SEP)
season = active_season or 2024
all_fixtures = []
for offset in range(7):
    d = (datetime.now() + timedelta(days=offset)).strftime("%Y-%m-%d")
    try:
        r = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers=HEADERS,
            params={"league": CHAMPIONSHIP_ID, "season": season, "date": d},
            timeout=10
        )
        day_fixtures = r.json().get('response', [])
        if day_fixtures:
            print(f"  📅 {d}: {len(day_fixtures)} partidos")
            for f in day_fixtures[:3]:
                h = f['teams']['home']['name']
                a = f['teams']['away']['name']
                ko = f['fixture']['date']
                print(f"     {h} vs {a} — {ko}")
            if len(day_fixtures) > 3:
                print(f"     ... y {len(day_fixtures)-3} más")
        all_fixtures.extend(day_fixtures)
    except Exception as e:
        print(f"  ❌ {d}: error — {e}")

if not all_fixtures:
    print("  ⚠️ Sin fixtures en los próximos 7 días")
    print("     Posible parón. Championship tiene parón internacional en marzo.")

# ── 5. CUOTAS DISPONIBLES ────────────────────────────────────
print(f"\n{SEP}")
print("  5. CUOTAS BET365 (bookmaker=8)")
print(SEP)
if all_fixtures:
    test_fix = all_fixtures[0]
    fid = test_fix['fixture']['id']
    h_n = test_fix['teams']['home']['name']
    a_n = test_fix['teams']['away']['name']
    try:
        r = requests.get(
            "https://v3.football.api-sports.io/odds",
            headers=HEADERS,
            params={"fixture": fid, "bookmaker": 8},
            timeout=10
        )
        odds_data = r.json().get('response', [])
        check(f"Odds para {h_n} vs {a_n}", bool(odds_data),
              f"{len(odds_data[0]['bookmakers'][0]['bets'])} mercados" if odds_data else "sin cuotas")
        if not odds_data:
            print("  ⚠️ Sin cuotas: las cuotas aparecen ~48h antes del partido")
            print(f"     Fixture ID {fid} puede ser demasiado lejano")
    except Exception as e:
        check("Odds request", False, str(e))
else:
    print("  ⏭️  Saltado (sin fixtures)")

# ── 6. BASE DE DATOS ─────────────────────────────────────────
print(f"\n{SEP}")
print("  6. BASE DE DATOS")
print(SEP)
try:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM picks_log")
    n_picks = c.fetchone()[0]
    check("picks_log", True, f"{n_picks} picks totales")

    c.execute("SELECT COUNT(*) FROM decision_log")
    n_rejected = c.fetchone()[0]
    check("decision_log (morgue)", True, f"{n_rejected} rechazos registrados")

    if n_rejected > 0:
        print("\n  Top razones de rechazo:")
        c.execute("SELECT reason, COUNT(*) FROM decision_log GROUP BY reason ORDER BY COUNT(*) DESC LIMIT 8")
        for reason, cnt in c.fetchall():
            print(f"    ❌ {reason}: {cnt}")

    c.execute("SELECT date, count FROM request_log ORDER BY date DESC LIMIT 5")
    req_rows = c.fetchall()
    if req_rows:
        print("\n  Requests por día:")
        for date, cnt in req_rows:
            print(f"    📡 {date}: {cnt} requests")
    else:
        print("\n  ⚠️ Sin registros en request_log — el scan nunca se ejecutó")
        print("     O se ejecutó pero falló antes de hacer el primer request")

    conn.close()
except Exception as e:
    check("Conexión DB", False, str(e))

# ── RESUMEN ──────────────────────────────────────────────────
print(f"\n{SEP}")
print("  RESUMEN Y PRÓXIMO PASO")
print(SEP)
if not all_fixtures:
    print("""
  🔴 CAUSA PROBABLE: Parón internacional de marzo.
     La Championship para cuando hay selecciones internacionales.
     El bot está funcionando correctamente — simplemente no hay
     partidos que escanear esta semana.

  ✅ QUÉ HACER:
     1. Verificar en https://www.bbc.com/sport/football/championship
        cuándo es el próximo partido de Championship
     2. El bot arrancará automáticamente cuando haya fixtures
     3. No es necesario reiniciar ni cambiar nada
""")
elif active_season:
    print(f"""
  🟡 HAY FIXTURES pero puede que sin cuotas aún.
     Temporada activa: {active_season}
     Fixtures próximos 7 días: {len(all_fixtures)}

  ✅ QUÉ HACER:
     1. Esperar a que los fixtures estén a <48h para que
        Bet365 publique cuotas
     2. El scan de mañana 09:00 UTC debería encontrarlos
""")
print(SEP + "\n")
