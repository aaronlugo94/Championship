"""
cleanup_db.py — Elimina picks duplicados de quant_v72.db

Conserva el pick con resultado WIN/LOSS si ya fue resuelto,
de lo contrario conserva el mayor id (más reciente).
Reconstruye picks_audit_v72.csv limpio desde la DB resultante.
"""
import sqlite3, csv, os

DB_PATH   = "/app/data/quant_v72.db"
AUDIT_CSV = "/app/data/picks_audit_v72.csv"

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

total_before = c.execute("SELECT COUNT(*) FROM picks_log").fetchone()[0]
print(f"Picks antes de limpiar: {total_before}")

c.execute("""
    SELECT fixture_id, market, selection, COUNT(*) as n,
           GROUP_CONCAT(id ORDER BY id ASC) as ids
    FROM picks_log
    GROUP BY fixture_id, market, selection
    HAVING n > 1
""")
dups = c.fetchall()
print(f"Grupos duplicados encontrados: {len(dups)}")

if not dups:
    print("Sin duplicados — nada que limpiar.")
    conn.close()
    exit(0)

for fid, mkt, sel, n, ids_str in dups:
    print(f"  {fid} | {mkt} | {sel} → {n} copias, ids: {ids_str}")

deleted = 0
for fid, mkt, sel, n, ids_str in dups:
    ids = sorted(int(x) for x in ids_str.split(","))
    resolved_id = None
    for pid in ids:
        row = c.execute("SELECT result FROM picks_log WHERE id=?", (pid,)).fetchone()
        if row and row[0] in ("WIN", "LOSS"):
            resolved_id = pid
            break
    keep_id = resolved_id if resolved_id else ids[-1]
    action = "resuelto WIN/LOSS" if resolved_id else "más reciente"
    print(f"    → conservando id={keep_id} ({action})")
    for del_id in ids:
        if del_id != keep_id:
            c.execute("DELETE FROM picks_log WHERE id=?", (del_id,))
            deleted += 1

conn.commit()
total_after = c.execute("SELECT COUNT(*) FROM picks_log").fetchone()[0]
print(f"\nEliminados: {deleted} | Picks después: {total_after}")

c.execute("""
    SELECT pick_time, div, home_team, away_team,
           selection, market, prob_model, odd_open, ev_open,
           result, stake_pct, profit,
           xg_home, xg_away, '', '',
           trend_pct_o25, trend_pct_bts, xg_source
    FROM picks_log ORDER BY id ASC
""")
rows = c.fetchall()
conn.close()

header = ["Date","Div","Home","Away","Pick","Market",
          "Prob","Odd","EV","Status","Stake","Profit",
          "xGH","xGA","FTHG","FTAG","pct_o25","pct_bts","xg_src"]

with open(AUDIT_CSV, "w", newline="", encoding="utf-8") as f:
    csv.writer(f).writerow(header)
    csv.writer(f).writerows(rows)

print(f"Audit CSV reconstruido: {len(rows)} picks únicos")
print("\n✅ Listo.")
