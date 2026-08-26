#!/usr/bin/env python3
"""익절×손절 격자 — MAE 동시 판정, 경로 미상은 상·하한으로 가둔다.

outcomes 스키마에는 mfe/mae만 있고 '언제 찍었는지'가 없다. 그래서 익절 +k와
손절 -j를 동시에 걸면 둘 다 닿은 종목의 실제 청산가를 특정할 수 없다.

그렇다고 계산을 포기하면 답이 안 나온다. 대신 **양쪽 극단을 다 계산한다**:
  낙관(TP-first) — 둘 다 닿았으면 익절이 먼저 → +k
  비관(SL-first) — 둘 다 닿았으면 손절이 먼저 → -j
실제 값은 반드시 이 둘 사이에 있다. **두 극단이 같은 결론을 내면 그 결론은
경로와 무관하게 성립한다.** 갈리면 그 칸은 판정 보류다.

판정은 전부 일자내·대조군 대비(픽 scan − 대조 gated, 일자 동일가중)다.
"""
import sqlite3, argparse, statistics as st, random
from pathlib import Path
random.seed(20260825)

DB_DEFAULT = Path(__file__).resolve().parent / "data" / "scores_history.db"
ap = argparse.ArgumentParser()
ap.add_argument("--db", default=str(DB_DEFAULT))
ap.add_argument("--min-n", type=int, default=3)
a = ap.parse_args()
con = sqlite3.connect(a.db)

TP = [5, 7, 10, 15]
SL = [5, 7, 10, 15]

rows = {}
for d, o, f, mf, ma in con.execute(
        "SELECT scan_date,origin,fwd10,mfe10,mae10 FROM outcomes "
        "WHERE fwd10 IS NOT NULL AND mfe10 IS NOT NULL AND mae10 IS NOT NULL "
        "AND origin IN ('scan','gated')"):
    rows.setdefault((d, o), []).append((f, mf, ma))

def outcome(f, mf, ma, k, j, tp_first):
    hit_tp, hit_sl = (mf >= k), (ma <= -j)
    if hit_tp and hit_sl:  return k if tp_first else -j
    if hit_tp:             return k
    if hit_sl:             return -j
    return f

def spread(rule):
    out = []
    for d in sorted({d for d, _ in rows}):
        p, c = rows.get((d, "scan"), []), rows.get((d, "gated"), [])
        if len(p) < a.min_n or len(c) < a.min_n: continue
        out.append(st.mean(rule(*x) for x in p) - st.mean(rule(*x) for x in c))
    return out

def ci(v, B=3000):
    s = sorted(st.mean(random.choices(v, k=len(v))) for _ in range(B))
    return s[int(.025 * B)], s[int(.975 * B)]

hold = spread(lambda f, mf, ma: f)
h_mean = st.mean(hold)
print(f"기준 — 보유(종가): 스프레드 {h_mean:+.2f}  SD {st.stdev(hold):.2f}  "
      f"CI [{ci(hold)[0]:+.2f}, {ci(hold)[1]:+.2f}]  ({len(hold)}일자)\n")

# 모호 비율: 두 임계에 모두 닿은 종목이 얼마나 되나 (넓을수록 상·하한이 벌어진다)
print("두 임계에 모두 닿은 비율 (모호 구간 — 픽 기준)")
pick = [x for (d, o), l in rows.items() if o == "scan" for x in l]
print(f"{'':>6}" + "".join(f"{'SL-'+str(j)+'%':>9}" for j in SL))
for k in TP:
    cells = [sum(1 for f, mf, ma in pick if mf >= k and ma <= -j) / len(pick) * 100 for j in SL]
    print(f"TP+{k:<3}" + "".join(f"{c:>8.0f}%" for c in cells))

print("\n" + "=" * 74)
print("익절×손절 격자 — 낙관(TP우선) / 비관(SL우선) 스프레드")
print("=" * 74)
print(f"{'':>6}" + "".join(f"{'SL-'+str(j)+'%':>17}" for j in SL))
verdicts = {}
for k in TP:
    line = f"TP+{k:<3}"
    for j in SL:
        opt = st.mean(spread(lambda f, mf, ma, k=k, j=j: outcome(f, mf, ma, k, j, True)))
        pes = st.mean(spread(lambda f, mf, ma, k=k, j=j: outcome(f, mf, ma, k, j, False)))
        verdicts[(k, j)] = (opt, pes)
        line += f"{opt:>+8.2f}/{pes:>+7.2f}"
    print(line)

# 판정은 평균 점추정이 아니라 짝지은 차이(규칙−보유)의 CI로 한다. 점추정만
# 비교하면 27일자의 일자 변동에 묻힌 차이를 실재하는 것처럼 읽게 된다.
def paired_pre(rule):
    out = []
    for d in sorted({d for d, _ in rows}):
        p_, c_ = rows.get((d, "scan"), []), rows.get((d, "gated"), [])
        if len(p_) < a.min_n or len(c_) < a.min_n: continue
        out.append((st.mean(rule(*x) for x in p_) - st.mean(rule(*x) for x in c_))
                   - (st.mean(x[0] for x in p_) - st.mean(x[0] for x in c_)))
    return out

print("\n판정 — 짝지은 차이(규칙−보유) CI 기준, 두 극단이 같은 결론일 때만 인정")
print(f"{'':>6}" + "".join(f"{'SL-'+str(j)+'%':>11}" for j in SL))
for k in TP:
    line = f"TP+{k:<3}"
    for j in SL:
        vs = []
        for tf in (True, False):
            lo, hi = ci(paired_pre(lambda f, mf, ma, k=k, j=j, tf=tf: outcome(f, mf, ma, k, j, tf)))
            vs.append("우위" if lo > 0 else "열위" if hi < 0 else "무차")
        v = vs[0] if vs[0] == vs[1] else "경로갈림"
        line += f"{('보유'+v if v != '경로갈림' else v):>9}"
    print(line)
print("\n  보유우위 = 경로와 무관하게 보유보다 낫다 (CI 하단>0)")
print("  보유열위 = 경로와 무관하게 보유보다 못하다 (CI 상단<0)")
print("  보유무차 = 보유와 통계적으로 구별되지 않는다 (CI가 0을 포함)")

# ── 지평별 부호 일치 (익절 단독) ────────────────────────────────
print("\n" + "=" * 74)
print("룰 후보별 지평 부호 일치 — 익절 단독 (손절은 MAE가 10일치만 있어 불가)")
print("=" * 74)
H = {5: ("fwd5", "mfe5"), 10: ("fwd10", "mfe10"), 20: ("fwd20", "mfe20")}
hz = {}
for h, (fc, mc) in H.items():
    r = {}
    for d, o, f, m in con.execute(
            f"SELECT scan_date,origin,{fc},{mc} FROM outcomes "
            f"WHERE {fc} IS NOT NULL AND {mc} IS NOT NULL AND origin IN ('scan','gated')"):
        r.setdefault((d, o), []).append((f, m))
    hz[h] = r

def spread_h(r, rule):
    out = []
    for d in sorted({d for d, _ in r}):
        p, c = r.get((d, "scan"), []), r.get((d, "gated"), [])
        if len(p) < a.min_n or len(c) < a.min_n: continue
        out.append(st.mean(rule(*x) for x in p) - st.mean(rule(*x) for x in c))
    return out

print(f"{'규칙':<12}" + "".join(f"{'fwd'+str(h):>16}" for h in H) + f"{'부호':>10}")
print("-" * 74)
print("  (fwd1은 mfe1 컬럼이 없어 청산 규칙 평가 자체가 불가)")
for lab, rule in [("보유(종가)", lambda f, m: f)] + \
                 [(f"익절 +{k}%", (lambda f, m, k=k: k if m >= k else f)) for k in TP]:
    cells, signs = "", []
    for h in H:
        v = spread_h(hz[h], rule)
        if len(v) < 3: cells += f"{'—':>16}"; continue
        cells += f"{st.mean(v):>+9.2f}({len(v):>2}일)"
        signs.append(st.mean(v) > 0)
    agree = "일치" if len(set(signs)) == 1 and len(signs) == 3 else "★불일치"
    print(f"{lab:<12}{cells}{agree:>10}")

# ── 짝지은 차이 (규칙 − 보유), 같은 일자끼리 ───────────────────────
# 평균만 비교하면 일자 변동에 묻힌다. 같은 일자 안에서 규칙과 보유를 직접 빼면
# 시장 성분이 상쇄돼 검정력이 크게 오른다.
print("\n" + "=" * 74)
print("짝지은 차이 (규칙 − 보유) — 같은 일자끼리, 27일자")
print("=" * 74)
print(f"{'규칙':<18}{'낙관Δ':>9}{'95%CI':>18}{'비관Δ':>9}{'95%CI':>18}")
print("-" * 74)

def paired(rule):
    out = []
    for d in sorted({d for d, _ in rows}):
        p, c = rows.get((d, "scan"), []), rows.get((d, "gated"), [])
        if len(p) < a.min_n or len(c) < a.min_n: continue
        s = st.mean(rule(*x) for x in p) - st.mean(rule(*x) for x in c)
        h = st.mean(x[0] for x in p) - st.mean(x[0] for x in c)
        out.append(s - h)
    return out

for k, j in [(5, 5), (7, 7), (10, 10), (15, 15), (10, 15), (15, 10)]:
    o = paired(lambda f, mf, ma, k=k, j=j: outcome(f, mf, ma, k, j, True))
    p_ = paired(lambda f, mf, ma, k=k, j=j: outcome(f, mf, ma, k, j, False))
    print(f"{'TP+%d%% / SL-%d%%' % (k, j):<18}"
          f"{st.mean(o):>+9.2f}{'[%+.2f, %+.2f]' % ci(o):>18}"
          f"{st.mean(p_):>+9.2f}{'[%+.2f, %+.2f]' % ci(p_):>18}")

print("\n[대조] 익절 단독 (손절 없음)")
for k in TP:
    v = paired(lambda f, mf, ma, k=k: (k if mf >= k else f))
    print(f"{'TP+%d%% 단독' % k:<18}{st.mean(v):>+9.2f}{'[%+.2f, %+.2f]' % ci(v):>18}")

print("\n[대조] 손절 단독 (익절 없음)")
for j in SL:
    v = paired(lambda f, mf, ma, j=j: (-j if ma <= -j else f))
    lo, hi = ci(v)
    tag = "열위확정" if hi < 0 else "우위확정" if lo > 0 else "무차"
    print(f"{'SL-%d%% 단독' % j:<18}{st.mean(v):>+9.2f}{'[%+.2f, %+.2f]' % (lo, hi):>18}{tag:>10}")

print("\n[핵심] 익절 단독의 분산 축소 — 스프레드 수준의 CI로 본다")
print(f"{'규칙':<14}{'스프레드':>9}{'SD':>7}{'95%CI':>18}")
print("-" * 48)
for lab, rule in [("보유(종가)", lambda f, mf, ma: f)] + \
                 [(f"익절 +{k}%", (lambda f, mf, ma, k=k: k if mf >= k else f)) for k in TP]:
    v = spread(rule)
    print(f"{lab:<14}{st.mean(v):>+9.2f}{st.stdev(v):>7.2f}{'[%+.2f, %+.2f]' % ci(v):>18}")
