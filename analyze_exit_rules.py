"""청산 규칙 일자내·대조군 대비 검증.

절대 수익률·pooled 비교는 결론이 뒤집힌다(실측). 그래서 모든 판정은
  spread(d) = mean_scan(d) - mean_gated(d)
를 일자별로 구한 뒤 일자 동일가중으로 평균낸다.

핵심 질문: 청산 규칙이 '픽 고유의' 우위를 넓히는가?
같은 규칙을 대조군에도 적용해서, 픽의 개선이 시장 전체의 변동성 수확이
아니라 픽에만 붙는 것인지 가른다.

식별 가능성 한계: 스키마에 mfe5/mfe10/mfe20/mae10만 있고 경로가 없다.
- 익절만: mfe로 식별 가능 (고점을 찍었으면 거기서 나왔다)
- 손절만: mae10으로 식별 가능
- 익절+손절 동시: 어느 쪽을 먼저 찍었는지 알 수 없다 → 계산하지 않는다
"""
import sqlite3, sys, statistics as st
from pathlib import Path

DB_DEFAULT = Path(__file__).resolve().parent / "data" / "scores_history.db"

DB = sys.argv[1] if len(sys.argv) > 1 else str(DB_DEFAULT)
con = sqlite3.connect(DB)

def load(cols, where):
    q = f"SELECT scan_date, origin, {cols} FROM outcomes WHERE {where}"
    rows = {}
    for r in con.execute(q):
        rows.setdefault((r[0], r[1]), []).append(r[2:])
    return rows

def tp(fwd, mfe, k):
    """익절 k%: 구간 중 k에 닿았으면 k에서 청산, 아니면 종가."""
    return k if mfe >= k else fwd

def sl(fwd, mae, j):
    """손절 -j%: 구간 중 -j에 닿았으면 -j에서 청산, 아니면 종가."""
    return -j if mae <= -j else fwd

def per_date_spread(data, rule, pick="scan", ctrl="gated", min_n=3):
    """일자별 (픽평균 - 대조평균). 양쪽 다 min_n건 이상인 일자만."""
    dates = sorted({d for (d, o) in data if o in (pick, ctrl)})
    out = []
    for d in dates:
        p = data.get((d, pick), [])
        c = data.get((d, ctrl), [])
        if len(p) < min_n or len(c) < min_n:
            continue
        mp = st.mean(rule(*x) for x in p)
        mc = st.mean(rule(*x) for x in c)
        out.append((d, mp, mc, mp - mc))
    return out

def summarize(label, rows):
    sp = [r[3] for r in rows]
    mp = [r[1] for r in rows]
    mc = [r[2] for r in rows]
    n = len(sp)
    mean = st.mean(sp)
    sd = st.stdev(sp) if n > 1 else float("nan")
    se = sd / (n ** 0.5) if n > 1 else float("nan")
    t = mean / se if se and se == se and se > 0 else float("nan")
    win = sum(1 for s in sp if s > 0) / n * 100
    return dict(label=label, n=n, pick=st.mean(mp), ctrl=st.mean(mc),
                spread=mean, med=st.median(sp), t=t, win=win)

def table(title, results):
    print(f"\n{title}")
    print(f"{'규칙':<16}{'일자':>5}{'픽':>9}{'대조':>9}{'스프레드':>10}{'중앙값':>9}{'t':>7}{'승률%':>8}")
    print("-" * 73)
    for r in results:
        print(f"{r['label']:<16}{r['n']:>5}{r['pick']:>9.2f}{r['ctrl']:>9.2f}"
              f"{r['spread']:>10.2f}{r['med']:>9.2f}{r['t']:>7.2f}{r['win']:>8.1f}")

# ---------- 5일 지평: 익절만 ----------
d5 = load("fwd5, mfe5", "fwd5 IS NOT NULL AND mfe5 IS NOT NULL AND origin IN ('scan','gated')")
res5 = [summarize("보유(종가)", per_date_spread(d5, lambda f, m: f))]
for k in (3, 5, 7, 10, 15):
    res5.append(summarize(f"익절 +{k}%", per_date_spread(d5, lambda f, m, k=k: tp(f, m, k))))
table("[5일] 익절 규칙 — 일자내 대조(scan vs gated), 일자 동일가중", res5)

# ---------- 10일 지평: 익절 / 손절 ----------
d10 = load("fwd10, mfe10, mae10",
           "fwd10 IS NOT NULL AND mfe10 IS NOT NULL AND mae10 IS NOT NULL "
           "AND origin IN ('scan','gated')")
res10 = [summarize("보유(종가)", per_date_spread(d10, lambda f, mf, ma: f))]
for k in (5, 7, 10, 15, 20):
    res10.append(summarize(f"익절 +{k}%", per_date_spread(d10, lambda f, mf, ma, k=k: tp(f, mf, k))))
for j in (5, 7, 10, 15):
    res10.append(summarize(f"손절 -{j}%", per_date_spread(d10, lambda f, mf, ma, j=j: sl(f, ma, j))))
table("[10일] 익절/손절 규칙 — 일자내 대조", res10)

# ---------- 참고: 반납 규모 ----------
print("\n[참고] 구간최고 대비 반납 (pooled — 방향만 보는 용도)")
for o in ("scan", "gated"):
    v = [x for (d, oo), lst in d5.items() if oo == o for x in lst]
    print(f"  {o:<6} n={len(v):<5} 종가 {st.mean(f for f, m in v):>7.2f}%"
          f"  최고 {st.mean(m for f, m in v):>6.2f}%"
          f"  반납 {st.mean(m - f for f, m in v):>6.2f}%p"
          f"  +5%터치 {sum(1 for f, m in v if m >= 5)/len(v)*100:>5.1f}%")
