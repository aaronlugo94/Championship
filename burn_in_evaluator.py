# ============================================================
# BURN-IN EVALUATOR V7.2 — Criterio de Salida a Producción
#
# MUESTRA: picks con resultado WIN/LOSS (no requiere CLV)
# CLV se usa como métrica adicional cuando está disponible.
#
# CRITERIO DE ACTIVACIÓN (todos simultáneos):
#   1. N >= 30 picks resueltos (WIN o LOSS)
#   2. EV promedio de apertura > +1.5%
#   3. p-value < 0.10 (t-test beat rate vs 50%)
#   4. Beat rate >= 52%
#
# CLV (opcional): si hay >= 10 picks con CLV capturado,
# se reporta como métrica adicional pero no bloquea el burn-in.
# ============================================================

import sqlite3
import numpy as np
from scipy import stats
from datetime import datetime, timezone
import os

MIN_SAMPLE    = 30
MIN_EV_MEAN   = 0.015   # +1.5% EV promedio de apertura
MAX_P_VALUE   = 0.10
MIN_BEAT_RATE = 0.52


def get_resolved_sample(db_path):
    """Picks resueltos WIN/LOSS desde picks_log."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        SELECT
            p.id,
            p.market,
            p.league,
            p.home_team || ' vs ' || p.away_team AS match,
            p.odd_open,
            p.ev_open,
            p.result,
            p.stake_pct,
            p.urs,
            p.pick_time,
            p.clv_captured
        FROM picks_log p
        WHERE p.result IN ('WIN', 'LOSS')
        ORDER BY p.id ASC
    """)
    rows = c.fetchall()
    conn.close()
    return rows


def get_clv_sample(db_path):
    """CLVs capturados — métrica adicional cuando disponible."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    try:
        c.execute("""
            SELECT cl.clv_pct / 100.0
            FROM picks_log p
            JOIN closing_lines cl
                ON  p.fixture_id = cl.fixture_id
                AND p.market     = cl.market
                AND p.selection  = cl.selection
            WHERE p.clv_captured = 1
            ORDER BY p.id ASC
        """)
        rows = [r[0] for r in c.fetchall()]
    except Exception:
        rows = []
    conn.close()
    return rows


def evaluate_burn_in(db_path):
    result = {
        'ready_for_live': False,
        'n': 0,
        'ev_mean': None,
        'beat_rate': None,
        't_stat': None,
        'p_value': None,
        'ci_90': None,
        'clv_n': 0,
        'clv_mean': None,
        'criteria': {
            'sample_size':              False,
            'ev_magnitude':             False,
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

    rows = get_resolved_sample(db_path)
    n = len(rows)
    result['n'] = n

    # ── MUESTRA INSUFICIENTE ─────────────────────────────────
    if n < MIN_SAMPLE:
        result['message'] = (
            f"BURN-IN EN PROGRESO: {n}/{MIN_SAMPLE} picks resueltos. "
            f"Faltan {MIN_SAMPLE - n} picks."
        )
        return result

    # ── CÁLCULO PRINCIPAL ────────────────────────────────────
    evs       = np.array([row[5] for row in rows], dtype=float)
    outcomes  = np.array([1.0 if row[6] == 'WIN' else 0.0 for row in rows])

    # Limpiar EVs corruptos
    mask = (evs > -0.50) & (evs < 0.50)
    if mask.sum() < n:
        result['warnings'].append(
            f"Se eliminaron {n - int(mask.sum())} EVs fuera de rango (±50%)."
        )
    evs      = evs[mask]
    outcomes = outcomes[mask]
    n        = len(evs)
    result['n'] = n

    ev_mean   = float(np.mean(evs))
    beat_rate = float(np.mean(outcomes))

    result['ev_mean']   = ev_mean
    result['beat_rate'] = beat_rate

    # t-test: beat rate vs 50% (H0: win rate <= 0.5)
    t_stat, p_two = stats.ttest_1samp(outcomes, popmean=0.5)
    p_value = p_two / 2 if t_stat > 0 else 1.0 - p_two / 2

    result['t_stat']  = float(t_stat)
    result['p_value'] = float(p_value)

    se = float(np.std(outcomes, ddof=1)) / np.sqrt(n)
    result['ci_90'] = (
        round(beat_rate - 1.645 * se, 4),
        round(beat_rate + 1.645 * se, 4)
    )

    # ── CLV ADICIONAL ────────────────────────────────────────
    clvs = get_clv_sample(db_path)
    clvs = [c for c in clvs if -0.50 < c < 0.50]
    result['clv_n']   = len(clvs)
    if clvs:
        result['clv_mean'] = round(float(np.mean(clvs)), 4)

    # ── DESGLOSE POR MERCADO ─────────────────────────────────
    mkt_data = {}
    for row in rows:
        mkt = row[1]
        mkt_data.setdefault(mkt, {'wins': 0, 'total': 0, 'evs': []})
        mkt_data[mkt]['total'] += 1
        mkt_data[mkt]['evs'].append(row[5])
        if row[6] == 'WIN':
            mkt_data[mkt]['wins'] += 1
    for mkt, d in mkt_data.items():
        result['market_breakdown'][mkt] = {
            'n':         d['total'],
            'ev_mean':   round(float(np.mean(d['evs'])), 4),
            'beat_rate': round(d['wins'] / d['total'], 3)
        }

    # ── CRITERIOS ────────────────────────────────────────────
    cr = result['criteria']
    cr['sample_size']              = n >= MIN_SAMPLE
    cr['ev_magnitude']             = ev_mean >= MIN_EV_MEAN
    cr['statistical_significance'] = p_value < MAX_P_VALUE
    cr['beat_rate']                = beat_rate >= MIN_BEAT_RATE

    result['ready_for_live'] = all(cr.values())

    # ── ADVERTENCIAS ─────────────────────────────────────────
    warns = result['warnings']

    recent = outcomes[-10:]
    if len(recent) >= 5 and float(np.mean(recent)) < beat_rate * 0.7:
        warns.append(
            f"DEGRADACIÓN RECIENTE: últimos 10 picks "
            f"Beat={np.mean(recent)*100:.1f}% vs global {beat_rate*100:.1f}%."
        )

    for mkt, d in result['market_breakdown'].items():
        if d['ev_mean'] < -0.01:
            warns.append(
                f"EV NEGATIVO en {mkt}: {d['ev_mean']*100:.1f}%. Revisar calibración."
            )

    for div in ('BSA', 'MEX'):
        div_rows = [row for row in rows if div in (row[2] or '')]
        if div_rows and len(div_rows) < 5:
            warns.append(
                f"MUESTRA BAJA {div}: solo {len(div_rows)} picks resueltos."
            )

    # ── MENSAJE ──────────────────────────────────────────────
    ci_low, ci_high = result['ci_90']
    if result['ready_for_live']:
        result['message'] = (
            f"✅ BURN-IN SUPERADO — SISTEMA LISTO PARA LIVE TRADING\n"
            f"   N={n} | EV={ev_mean*100:.2f}% | p={p_value:.4f} | "
            f"Beat={beat_rate*100:.1f}%\n"
            f"   IC 90% beat rate: [{ci_low*100:.1f}%, {ci_high*100:.1f}%]\n"
            f"   Acción: cambiar LIVE_TRADING=True en main_v72.py y reiniciar."
        )
    else:
        failed = [k for k, v in cr.items() if not v]
        details = {
            'sample_size':              f"N={n} (mínimo {MIN_SAMPLE})",
            'ev_magnitude':             f"EV={ev_mean*100:.2f}% (mínimo +{MIN_EV_MEAN*100:.1f}%)",
            'statistical_significance': f"p={p_value:.4f} (máximo {MAX_P_VALUE})",
            'beat_rate':                f"Beat={beat_rate*100:.1f}% (mínimo {MIN_BEAT_RATE*100:.0f}%)"
        }
        result['message'] = (
            f"❌ BURN-IN INCOMPLETO — LIVE_TRADING=False\n"
            f"   Criterios fallidos: {' | '.join(details[f] for f in failed)}"
        )

    return result


def print_burn_in_report(db_path):
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
            'ev_magnitude':             f"EV apertura = {r['ev_mean']*100:.2f}%",
            'statistical_significance': f"p-value = {r['p_value']:.4f}",
            'beat_rate':                f"Beat Rate = {r['beat_rate']*100:.1f}%"
        }
        for k, label in labels.items():
            status = "✅ OK" if r['criteria'][k] else "❌ FALLA"
            print(f"  {k:<32} {label:<22} {status}")

        ci_low, ci_high = r['ci_90']
        print(f"\n  IC 90% beat rate: [{ci_low*100:.1f}%, {ci_high*100:.1f}%]")

        if r['clv_n'] >= 10:
            print(f"  CLV capturado (N={r['clv_n']}): {r['clv_mean']*100:.2f}%  ← métrica adicional")

        if r['market_breakdown']:
            print("\n  DESGLOSE POR MERCADO:")
            for mkt, data in r['market_breakdown'].items():
                bar = "▓" * int(data['beat_rate'] * 20)
                print(f"    {mkt:<8} N={data['n']:<4} "
                      f"EV={data['ev_mean']*100:+.2f}%  "
                      f"Beat={data['beat_rate']*100:.0f}%  {bar}")

        if r['warnings']:
            print("\n  ⚠️  ADVERTENCIAS:")
            for w in r['warnings']:
                print(f"    → {w}")

    print("="*62 + "\n")
    return r


if __name__ == "__main__":
    import sys
    db = sys.argv[1] if len(sys.argv) > 1 else "./data/quant_v72.db"
    print_burn_in_report(db)
