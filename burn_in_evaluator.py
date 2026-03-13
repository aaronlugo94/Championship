# ============================================================
# BURN-IN EVALUATOR V7.2 — Criterio de Salida a Producción
# Adaptado al schema real de quant_v72.db
#
# HIPÓTESIS NULA (H0): CLV promedio <= 0.
# Si rechazamos H0 con p < 0.10 → edge real contra el mercado.
#
# CRITERIO DE ACTIVACIÓN (todos simultáneos):
#   1. N >= 30 picks con CLV capturado (clv_captured = 1)
#   2. CLV promedio > +1.5%
#   3. p-value < 0.10 (t-test una muestra, cola derecha)
#   4. Beat rate >= 52%
#
# DIFERENCIAS vs burn_in_evaluator V5:
#   - closing_lines no tiene selection_key → usa fixture_id + market + selection
#   - CLV ya está precalculado en closing_lines.clv_pct (como %)
#   - DB: quant_v72.db (no quant_v5.db)
#   - picks_log.selection (no selection_key)
# ============================================================

import sqlite3
import numpy as np
from scipy import stats
from datetime import datetime, timezone
import os

MIN_SAMPLE    = 30
MIN_CLV_MEAN  = 0.015   # +1.5%
MAX_P_VALUE   = 0.10
MIN_BEAT_RATE = 0.52


def get_clv_sample(db_path):
    """
    Extrae muestra de CLVs desde closing_lines JOIN picks_log.

    En V7.2:
      - closing_lines.clv_pct ya está calculado como porcentaje
        (odd_open - odd_close) / odd_open * 100
      - El JOIN usa fixture_id + market + selection (no selection_key)
      - Solo picks con clv_captured = 1
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        SELECT
            p.id,
            p.market,
            p.league,
            p.home_team || ' vs ' || p.away_team AS match,
            p.odd_open,
            cl.odd_close,
            cl.clv_pct / 100.0 AS clv,   -- clv_pct está en %, convertir a decimal
            p.ev_open,
            p.urs,
            p.pick_time
        FROM picks_log p
        JOIN closing_lines cl
            ON  p.fixture_id = cl.fixture_id
            AND p.market     = cl.market
            AND p.selection  = cl.selection
        WHERE p.clv_captured = 1
        ORDER BY p.id ASC
    """)
    rows = c.fetchall()
    conn.close()
    return rows


def evaluate_burn_in(db_path):
    """
    Evalúa si el sistema superó el burn-in estadístico.

    Returns dict con:
        ready_for_live  bool
        n               int
        clv_mean        float   (decimal: 0.023 = +2.3%)
        clv_std         float
        t_stat          float
        p_value         float
        beat_rate       float
        ci_90           tuple
        criteria        dict
        market_breakdown dict
        warnings        list
        message         str
        evaluated_at    str
    """
    result = {
        'ready_for_live': False,
        'n': 0,
        'clv_mean': None,
        'clv_std': None,
        't_stat': None,
        'p_value': None,
        'beat_rate': None,
        'ci_90': None,
        'criteria': {
            'sample_size':              False,
            'clv_magnitude':            False,
            'statistical_significance': False,
            'beat_rate':                False,
        },
        'market_breakdown': {},
        'warnings': [],
        'message': '',
        'evaluated_at': datetime.now(timezone.utc).isoformat()
    }

    if not os.path.exists(db_path):
        result['message'] = f"DB no encontrada: {db_path}"
        return result

    rows = get_clv_sample(db_path)
    n = len(rows)
    result['n'] = n

    # ── MUESTRA INSUFICIENTE ─────────────────────────────────
    if n < MIN_SAMPLE:
        result['message'] = (
            f"BURN-IN EN PROGRESO: {n}/{MIN_SAMPLE} picks con CLV válido. "
            f"Faltan {MIN_SAMPLE - n} picks."
        )
        return result

    # ── CÁLCULO ──────────────────────────────────────────────
    clvs = np.array([row[6] for row in rows], dtype=float)
    # Sanidad: eliminar CLVs corruptos (> 50% o < -50% son errores de datos)
    clvs_clean = clvs[(clvs > -0.50) & (clvs < 0.50)]
    if len(clvs_clean) < n:
        result['warnings'].append(
            f"Se eliminaron {n - len(clvs_clean)} CLVs fuera de rango (±50%) — "
            "probable dato corrupto de API."
        )
    clvs = clvs_clean
    n = len(clvs)
    result['n'] = n

    clv_mean  = float(np.mean(clvs))
    clv_std   = float(np.std(clvs, ddof=1))
    beat_rate = float(np.mean(clvs > 0))

    result['clv_mean']  = clv_mean
    result['clv_std']   = clv_std
    result['beat_rate'] = beat_rate

    # ── T-TEST COLA DERECHA ──────────────────────────────────
    t_stat, p_two = stats.ttest_1samp(clvs, popmean=0)
    p_value = p_two / 2 if t_stat > 0 else 1.0 - p_two / 2

    result['t_stat']  = float(t_stat)
    result['p_value'] = float(p_value)

    # ── IC 90% ───────────────────────────────────────────────
    se = clv_std / np.sqrt(n)
    result['ci_90'] = (round(clv_mean - 1.645 * se, 4),
                       round(clv_mean + 1.645 * se, 4))

    # ── DESGLOSE POR MERCADO ─────────────────────────────────
    mkt_data = {}
    for row in rows:
        mkt = row[1]
        mkt_data.setdefault(mkt, []).append(row[6])
    for mkt, vals in mkt_data.items():
        arr = np.array(vals)
        result['market_breakdown'][mkt] = {
            'n':         len(vals),
            'clv_mean':  round(float(np.mean(arr)), 4),
            'beat_rate': round(float(np.mean(arr > 0)), 3)
        }

    # ── CRITERIOS ────────────────────────────────────────────
    cr = result['criteria']
    cr['sample_size']              = n >= MIN_SAMPLE
    cr['clv_magnitude']            = clv_mean >= MIN_CLV_MEAN
    cr['statistical_significance'] = p_value < MAX_P_VALUE
    cr['beat_rate']                = beat_rate >= MIN_BEAT_RATE

    result['ready_for_live'] = all(cr.values())

    # ── ADVERTENCIAS ─────────────────────────────────────────
    warns = result['warnings']

    if clv_std > 0.12:
        warns.append(
            f"ALTA DISPERSIÓN: std={clv_std:.3f}. "
            "Inconsistencia entre picks — revisar segmentación por mercado."
        )

    recent = np.array([row[6] for row in rows[-10:]])
    if len(recent) >= 5 and float(np.mean(recent)) < clv_mean * 0.5:
        warns.append(
            f"DEGRADACIÓN RECIENTE: últimos 10 picks CLV={np.mean(recent)*100:.1f}% "
            f"vs media global {clv_mean*100:.1f}%. Posible deterioro de edge."
        )

    for mkt in ('OVER', 'UNDER'):
        if mkt in result['market_breakdown']:
            m_clv = result['market_breakdown'][mkt]['clv_mean']
            if m_clv < -0.01:
                warns.append(
                    f"KILL-SWITCH CERCANO: {mkt} CLV={m_clv*100:.1f}%. "
                    "Por debajo de -1.5% se activaría kill-switch automático."
                )

    # Alerta si BSA o MEX tienen n muy bajo vs total
    for div in ('BSA', 'MEX'):
        div_rows = [row for row in rows if div in (row[2] or '')]
        if div_rows and len(div_rows) < 5:
            warns.append(
                f"MUESTRA BAJA {div}: solo {len(div_rows)} picks con CLV. "
                "El desglose de esa liga no es estadísticamente confiable."
            )

    # ── MENSAJE ──────────────────────────────────────────────
    ci_low, ci_high = result['ci_90']
    if result['ready_for_live']:
        result['message'] = (
            f"✅ BURN-IN SUPERADO — SISTEMA LISTO PARA LIVE TRADING\n"
            f"   N={n} | CLV={clv_mean*100:.2f}% | p={p_value:.4f} | "
            f"Beat={beat_rate*100:.1f}%\n"
            f"   IC 90%: [{ci_low*100:.2f}%, {ci_high*100:.2f}%]\n"
            f"   Acción: cambiar LIVE_TRADING=True en main_v72.py y reiniciar."
        )
    else:
        failed = [k for k, v in cr.items() if not v]
        details = {
            'sample_size':              f"N={n} (mínimo {MIN_SAMPLE})",
            'clv_magnitude':            f"CLV={clv_mean*100:.2f}% (mínimo +{MIN_CLV_MEAN*100:.1f}%)",
            'statistical_significance': f"p={p_value:.4f} (máximo {MAX_P_VALUE})",
            'beat_rate':                f"Beat={beat_rate*100:.1f}% (mínimo {MIN_BEAT_RATE*100:.0f}%)"
        }
        result['message'] = (
            f"❌ BURN-IN INCOMPLETO — LIVE_TRADING=False\n"
            f"   Criterios fallidos: {' | '.join(details[f] for f in failed)}"
        )

    return result


def print_burn_in_report(db_path):
    """Imprime reporte legible. Llamar desde arranque del bot."""
    r = evaluate_burn_in(db_path)

    print("\n" + "="*62)
    print("   BURN-IN EVALUATOR V7.2")
    print("="*62)
    print(f"\n{r['message']}\n")

    if r['n'] >= MIN_SAMPLE:
        print(f"  {'CRITERIO':<32} {'VALOR':<22} ESTADO")
        print("  " + "-"*60)
        labels = {
            'sample_size':              f"N = {r['n']}",
            'clv_magnitude':            f"CLV = {r['clv_mean']*100:.2f}%",
            'statistical_significance': f"p-value = {r['p_value']:.4f}",
            'beat_rate':                f"Beat Rate = {r['beat_rate']*100:.1f}%"
        }
        for k, label in labels.items():
            status = "✅ OK" if r['criteria'][k] else "❌ FALLA"
            print(f"  {k:<32} {label:<22} {status}")

        ci_low, ci_high = r['ci_90']
        print(f"\n  IC 90%: [{ci_low*100:.2f}%, {ci_high*100:.2f}%]")

        if r['market_breakdown']:
            print("\n  DESGLOSE POR MERCADO:")
            for mkt, data in r['market_breakdown'].items():
                bar = "▓" * int(abs(data['clv_mean']) * 400)
                sign = "+" if data['clv_mean'] >= 0 else ""
                print(f"    {mkt:<8} N={data['n']:<4} "
                      f"CLV={sign}{data['clv_mean']*100:.2f}%  "
                      f"Beat={data['beat_rate']*100:.0f}%  {bar}")

        if r['warnings']:
            print("\n  ⚠️  ADVERTENCIAS:")
            for w in r['warnings']:
                print(f"    → {w}")

    print("="*62 + "\n")
    return r


# ── INTEGRACIÓN EN main_v72.py ───────────────────────────────
# Al final de __init__ de TripleLeagueV72, después de init_db():
#
#   from burn_in_evaluator import print_burn_in_report
#   r = print_burn_in_report(DB_PATH)
#   if r['ready_for_live'] and not LIVE_TRADING:
#       send_msg("🟢 BURN-IN SUPERADO. Activar LIVE_TRADING manualmente.")
#
# El módulo NO activa LIVE_TRADING automáticamente.
# La decisión final es siempre del operador.
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    db = sys.argv[1] if len(sys.argv) > 1 else "./data/quant_v72.db"
    print_burn_in_report(db)
