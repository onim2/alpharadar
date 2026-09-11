"""누적 관찰 이력(watch_n/since/gap)의 성과 검정 — 카드 표식 근거 재검증.

질문: `get_watch_history()` docstring 은 누적 등장이 "점수보다 훨씬 강했다"고
적는다(Spearman +0.187). 카드는 그 근거로 ⟨주목⟩ 누적 4~10회, ⟨적기⟩ 최초 후
8~14일, ⟨눌림후⟩ 4~7일 공백을 표시한다. 그런데 8월 말 분석은 7회+ 가 열세라고
보았다. 어느 쪽이 맞는가?

설계
 1) docstring 의 수치는 **pooled** 이고 지표는 **mfe10**(D+1 시가 진입 후 10일 내
    구간 최고가)이다. watch_n 은 표본 기간에 걸쳐 누적되므로 **달력 시간과
    구조적으로 얽혀 있다** — 스캔 10일째에 watch_n=20 인 종목은 존재할 수 없다.
    그래서 pooled 비교는 "높은 watch_n vs 낮은 watch_n"이 아니라 상당 부분
    "늦은 일자 vs 이른 일자", 즉 시장 국면 비교가 된다. 점수 계열보다 훨씬
    심한 교란이다. 일자내로 봐야 한다.
 2) 단면 단위는 scan_date 다. watch_n 은 scan_date 기준으로 계산되어 하루 두 런이
    같은 값을 가지므로(DISTINCT 로 한 칸으로 접힘) run_type 으로 가르지 않고
    (scan_date, ticker) 로 한 행만 쓴다.
 3) 버킷 비교는 **그 날 나머지 통과 종목과의 스프레드**로 한다. 절대 수익률
    비교는 결론이 뒤집힌다.
 4) 지평을 하나로 보지 않는다. 주력 보유는 1~3일·1~2주이고, docstring 이 쓴
    mfe10 은 실현 불가능한 상한이다. mfe10·fwd1·fwd5·fwd10·fwd20 을 모두 본다.
 5) 진입 바 겹침 제외가 **기본값**이다. IC·순위상관 계열에서는 겹침이 독립 단면
    수를 부풀려 판정을 뒤집는다(2026-09-11 실측, docs/w_text_check_20260911.md).

한계
 * mfe10·fwd10 은 성숙까지 10거래일이 걸려 최근 일자가 빠진다(현재 ~08/27).
 * watch_n 재구성은 `get_watch_history()` 를 그대로 옮긴 것이다. --validate 로
   docstring 의 pooled 수치가 재현되는지 확인할 수 있다.
"""
import argparse
import sqlite3

import numpy as np
import pandas as pd
from scipy.stats import binomtest, spearmanr

ap = argparse.ArgumentParser(description=__doc__,
                             formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--db", default="data/scores_history.db")
ap.add_argument("--origin", default="scan")
ap.add_argument("--horizons", nargs="+",
                default=["mfe10", "fwd1", "fwd5", "fwd10", "fwd20"])
ap.add_argument("--min-cs", type=int, default=8, help="단면 최소 종목 수")
ap.add_argument("--min-out", type=int, default=2, help="스프레드용 대조군 최소 수")
ap.add_argument("--boot", type=int, default=10000)
ap.add_argument("--dedup-bars", action=argparse.BooleanOptionalAction, default=True,
                help="휴장으로 진입 바가 겹친 뒤 일자를 제외 (기본 켜짐)")
ap.add_argument("--validate", action="store_true",
                help="docstring 의 pooled 수치를 재현해 재구성을 검증한다")
ap.add_argument("--seed", type=int, default=0)
a = ap.parse_args()

rng = np.random.default_rng(a.seed)
con = sqlite3.connect(a.db)

# ── watch_n/since/gap 재구성 (get_watch_history() 와 동일 규칙) ────────
sr = pd.read_sql_query("SELECT DISTINCT scan_date, ticker FROM scan_results", con)
days = sorted(sr["scan_date"].unique())
idx = {d: i for i, d in enumerate(days)}
appear = sr.groupby("ticker")["scan_date"].apply(lambda s: sorted(s.unique())).to_dict()
rows = []
for t, lst in appear.items():
    for k, d0 in enumerate(lst):
        cur = idx[d0]                 # get_watch_history: cur = len(scan_date < d0)
        if k == 0:
            rows.append((d0, t, 0, 0, np.nan))
        else:
            rows.append((d0, t, k, cur - idx[lst[0]], cur - idx[lst[k - 1]]))
w = pd.DataFrame(rows, columns=["scan_date", "ticker", "watch_n", "watch_since", "watch_gap"])

o = pd.read_sql_query(
    "SELECT scan_date, ticker, fwd1, fwd5, fwd10, fwd20, mfe5, mfe10 "
    "FROM outcomes WHERE origin = ?", con, params=(a.origin,))
con.close()
d = w.merge(o, on=["scan_date", "ticker"])

# ── 진입 바 겹침 탐지 (연속 두 일자의 fwd5 가 사실상 동일) ────────────
coll = []
ds = sorted(d["scan_date"].unique())
for p, c in zip(ds, ds[1:]):
    x = d[d.scan_date == p].set_index("ticker")["fwd5"]
    y = d[d.scan_date == c].set_index("ticker")["fwd5"]
    b = x.index.intersection(y.index)
    xv, yv = x[b].values, y[b].values
    ok = ~(np.isnan(xv) | np.isnan(yv))      # 미도래 NaN 쌍은 비교에서 뺀다
    if ok.sum() >= 5 and np.isclose(xv[ok], yv[ok], rtol=0, atol=1e-6).mean() > 0.95:
        coll.append(c)
if coll:
    print(f"진입 바 겹침 일자 {len(coll)}개: {', '.join(coll)}")
    if a.dedup_bars:
        d = d[~d.scan_date.isin(coll)]
        print("  → 제외 (기본). 전수로 보려면 --no-dedup-bars")
    else:
        print("  → --no-dedup-bars: 전수 유지. 부호검정이 그만큼 후하다")

print(f"\n표본 {len(d)}행 · 일자 {d.scan_date.nunique()} · 종목 {d.ticker.nunique()}"
      f" · {d.scan_date.min()}~{d.scan_date.max()}")


def cs_ic(col, ret, data=None):
    data = d if data is None else data
    out = {}
    for sd, g in data.groupby("scan_date"):
        m = g[col].notna() & g[ret].notna()
        if m.sum() < a.min_cs or g[col][m].nunique() < 3:
            continue
        ic, _ = spearmanr(g[col][m], g[ret][m])
        if not np.isnan(ic):
            out[sd] = ic
    return pd.Series(out, dtype=float)


def ic_row(lab, ic):
    n = len(ic)
    sd = ic.std(ddof=1) if n > 1 else np.nan
    pos = int((ic > 0).sum())
    return {"대상": lab, "일자": n, "평균IC": round(ic.mean(), 4),
            "중앙IC": round(ic.median(), 4),
            "t": round(ic.mean() / sd * np.sqrt(n), 2) if n > 1 and sd > 0 else np.nan,
            "IC>0": f"{pos}/{n}",
            "부호p": round(binomtest(pos, n, 0.5).pvalue, 4) if n else np.nan}


def spread(mask_fn, ret, label, data=None):
    """그 날 조건군 평균 − 나머지 평균. 일자 동일가중 + 일자 부트스트랩."""
    data = d if data is None else data
    per = {}
    for sd, g in data.groupby("scan_date"):
        g = g[g[ret].notna()]
        if len(g) < a.min_cs:
            continue
        ins, out = g[mask_fn(g)], g[~mask_fn(g)]
        if len(ins) < 1 or len(out) < a.min_out:
            continue
        per[sd] = ins[ret].mean() - out[ret].mean()
    s = pd.Series(per, dtype=float)
    n = len(s)
    if n < 5:
        return {"구간": label, "일자": n, "판정": "표본 부족"}
    boot = np.array([rng.choice(s.values, n, replace=True).mean() for _ in range(a.boot)])
    lo, hi = np.percentile(boot, [2.5, 97.5])
    pos = int((s > 0).sum())
    sp = round(binomtest(pos, n, 0.5).pvalue, 3)
    verdict = "우세" if lo > 0 else "열세" if hi < 0 else "무차"
    if verdict == "무차" and sp < 0.05:
        verdict += f"(부호만 {'우세' if pos*2>n else '열세'})"
    return {"구간": label, "일자": n, "평균차": round(s.mean(), 2),
            "중앙차": round(s.median(), 2), "95%CI": f"[{lo:+.2f},{hi:+.2f}]",
            "우세일자": f"{pos}/{n}", "부호p": sp, "판정": verdict}


if a.validate:
    print("\n══ 0. docstring(pooled, mfe10) 재현 — 재구성 검증 ══")
    m = d[d.mfe10.notna()]
    ic, _ = spearmanr(m.watch_n, m.mfe10)
    print(f"  누적등장 pooled Spearman {ic:+.4f}   (docstring +0.187)")
    for lab, g, ref in [
            ("첫 등장(watch_n=0)", m[m.watch_n == 0], "n=462 +10% 26.6% 평균 6.08"),
            ("4~6회째(watch_n 3~5)", m[(m.watch_n >= 3) & (m.watch_n <= 5)], "n=727 +10% 42.0% 평균 13.52"),
            ("7회째+(watch_n>=6)", m[m.watch_n >= 6], "평균 10.79"),
            ("1~2회째(watch_n<=1)", m[m.watch_n <= 1], "평균 6.91")]:
        print(f"  {lab:<22} n={len(g):<5} +10% {(g.mfe10 >= 10).mean():.1%}  "
              f"평균 {g.mfe10.mean():.2f}   ← {ref}")

print("\n══ 1. watch_n 일자내 IC vs pooled ══")
tab = [ic_row(f"일자내 · {r}", cs_ic("watch_n", r)) for r in a.horizons]
print(pd.DataFrame(tab).to_string(index=False))
print("  pooled 대조:", "  ".join(
    f"{r} {spearmanr(d[d[r].notna()].watch_n, d[d[r].notna()][r])[0]:+.4f}" for r in a.horizons))

print("\n══ 2. 버킷별 일자내 스프레드 (버킷 − 그 날 나머지) ══")
BK = [("0 첫등장", lambda g: g.watch_n == 0),
      ("1~2회째", lambda g: (g.watch_n >= 1) & (g.watch_n <= 2)),
      ("4~6회째 (watch_n 3~5)", lambda g: (g.watch_n >= 3) & (g.watch_n <= 5)),
      ("7~10회째 (watch_n 6~9)", lambda g: (g.watch_n >= 6) & (g.watch_n <= 9)),
      ("11회째+ (watch_n>=10)", lambda g: g.watch_n >= 10)]
for ret in a.horizons:
    print(f"\n  ── {ret} ──")
    print(pd.DataFrame([spread(f, ret, lab) for lab, f in BK]).to_string(index=False))

print("\n══ 3. 카드 표식 검정 — 각 밴드 vs 밴드 밖 ══")
gap = d[d.watch_gap.notna()]
MARKS = [("⟨주목⟩ 누적 4~10회 (watch_n 3~9)",
          lambda g: (g.watch_n >= 3) & (g.watch_n <= 9), None),
         ("⟨적기⟩ 최초 후 8~14일 (watch_since 8~14)",
          lambda g: (g.watch_since >= 8) & (g.watch_since <= 14), None),
         ("⟨눌림후⟩ 공백 4~7 (watch_gap 4~7)",
          lambda g: (g.watch_gap >= 4) & (g.watch_gap <= 7), gap)]
for lab, f, data in MARKS:
    print(f"\n  ── {lab} ──")
    print(pd.DataFrame([spread(f, r, r, data) for r in a.horizons]).to_string(index=False))

print("\n══ 4. docstring 직접 주장: 7회째+(watch_n>=6) vs 1~2회째(watch_n<=1) ══")
sub = d[(d.watch_n >= 6) | (d.watch_n <= 1)]
print(pd.DataFrame([spread(lambda g: g.watch_n >= 6, r, r, sub)
                    for r in a.horizons]).to_string(index=False))

print("\n══ 5. watch_gap 분포 — docstring 의 '공백 0' 은 존재하는가 ══")
vc = d.watch_gap.value_counts(dropna=False).sort_index()
print("  " + " · ".join(f"gap={k if k == k else 'None(첫등장)'}:{v}" for k, v in vc.head(8).items()))
print(f"  최소 gap = {d.watch_gap.min()}  → gap=0 은 구조적으로 나올 수 없다"
      f"(직전 스캔일 등장 = 1). docstring 의 '연속(공백0)' 은 다른 정의다.")
