"""w_text 가중치 재배분의 성과 검정 (작업 4-③).

질문: `scoring.w_text` 를 0.40 → 0.20 으로 내리면 fwd5 순위가 나아지는가?

설계
 1) 일자내 대조. pooled·절대수익률 비교는 결론이 뒤집힌다(8/24 교훈).
    backtest.py 의 grid_search·_weighted_ic 는 전 일자를 한 번에 묶은 pooled
    Spearman 이라 '그날 시장이 어땠는지'가 섞여 들어간다. 여기서는 각 런을
    독립 단면으로 보고 단면별 IC 를 낸 뒤 일자 간으로 집계한다.
 2) 단면의 단위는 (scan_date, run_type) 이다. 하루 두 런은 같은 scan_date 를
    쓰지만 점수가 서로 다르고 fwd 값은 같다 — 한 단면에 같은 종목을 두 번
    넣으면 가중이 틀어지므로 런별로 가른다. outcomes 는 (scan_date, ticker)
    단위라 두 런이 같은 fwd 를 공유하는 것이 맞다.
 3) 두 안의 비교는 **같은 단면 안에서 쌍대로** 한다. 단면마다 IC 를 두 번
    (현행·제안) 재서 차를 구하고, 그 차를 일자 부트스트랩·부호검정에 건다.
    안끼리 평균 IC 를 따로 낸 뒤 비교하면 단면 구성 차이가 섞인다.
 4) 지평을 하나로 보지 않는다. 과열 신호는 지평에 따라 부호가 뒤집힌 전례가
    있고(fwd10 교차), 주력 보유는 1~3일·1~2주다. fwd1/5/10/20 을 모두 본다.

한계
 * 휴장이 끼면 두 scan_date 가 같은 진입 바를 써서 fwd 값이 동일해진다
   (8/17·8/18 실측). 그대로 세면 독립 단면 수가 부풀어 부호검정이 후해진다.
   뒤 일자를 표본에서 뺀다(기본). 전수로 보려면 --no-dedup-bars — 판정은
   제외 기준으로 할 것. IC 계열에서는 이 차이로 유의성이 뒤집힌다.
 * score_t 가 NULL 인 행이 있어 T 축만 단면 수가 적다. 축별 표에 같이 찍는다.
 * s_text 는 뉴스 기반이라 lookahead 의심이 남는다. 그래서 정형만(T+D)을
   따로 찍는다 — 이쪽이 같은 방향이면 해석이 안전하다.
"""
import argparse
import sqlite3

import numpy as np
import pandas as pd
from scipy.stats import binomtest, spearmanr

ap = argparse.ArgumentParser(description=__doc__,
                             formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--db", default="data/scores_history.db")
ap.add_argument("--origin", default="scan",
                help="outcomes.origin (기본 scan = 통과·발송 후보군)")
ap.add_argument("--from-date", default="20260101")
ap.add_argument("--horizons", nargs="+", default=["fwd1", "fwd5", "fwd10", "fwd20"])
ap.add_argument("--min-cs", type=int, default=8,
                help="단면 최소 종목 수 (기본 8 — 이보다 작으면 순위상관이 무의미)")
ap.add_argument("--current",  type=float, default=0.40, help="현행 w_text")
ap.add_argument("--proposed", type=float, default=0.20, help="제안 w_text")
ap.add_argument("--sweep", nargs="+", type=float,
                default=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.8, 1.0])
ap.add_argument("--boot", type=int, default=10000, help="부트스트랩 반복 (기본 10000)")
ap.add_argument("--dedup-bars", action=argparse.BooleanOptionalAction, default=True,
                help="휴장으로 진입 바가 겹친 뒤 일자를 표본에서 뺀다 (기본 켜짐)")
ap.add_argument("--seed", type=int, default=0)
a = ap.parse_args()

RET_ALL = ["fwd1", "fwd5", "fwd10", "fwd20"]
rng = np.random.default_rng(a.seed)

con = sqlite3.connect(a.db)
d = pd.read_sql_query(f"""
    SELECT s.scan_date, s.run_type, s.ticker,
           s.score_t, s.s_text, s.score_d, s.score_total,
           {', '.join('o.' + c for c in RET_ALL)}
    FROM scan_results s
    JOIN outcomes o
      ON o.scan_date = s.scan_date AND o.ticker = s.ticker AND o.origin = ?
    WHERE s.scan_date >= ?
""", con, params=(a.origin, a.from_date))
con.close()
if d.empty:
    raise SystemExit(f"표본이 없다 (origin={a.origin}, from={a.from_date})")

# run_type 은 마이그레이션 전 DB에서 NULL 일 수 있다 — 그 경우 하루 한 단면으로 본다.
d["run_type"] = d["run_type"].fillna("am")
d["cs"] = d["scan_date"] + "/" + d["run_type"]
d["struct"] = d["score_t"].fillna(50) * 0.5 + d["score_d"].fillna(50) * 0.5

# ── 진입 바 겹침 탐지 ─────────────────────────────────────────────────
# 연속한 두 scan_date 가 겹치는 종목에서 fwd5 가 사실상 전부 같으면 같은 바다.
dates = sorted(d["scan_date"].unique())
collided = []
for prev, cur in zip(dates, dates[1:]):
    x = d[d["scan_date"] == prev].groupby("ticker")["fwd5"].first()
    y = d[d["scan_date"] == cur].groupby("ticker")["fwd5"].first()
    both = x.index.intersection(y.index)
    xv, yv = x[both].values, y[both].values
    # 아직 도래하지 않은 fwd 는 양쪽 다 NaN 이라, equal_nan 으로 비교하면
    # 최근 일자가 전부 '겹침'으로 잡힌다. 실측값이 있는 쌍만 본다.
    ok = ~(np.isnan(xv) | np.isnan(yv))
    if ok.sum() < 5:
        continue
    same = np.isclose(xv[ok], yv[ok], rtol=0, atol=1e-6)
    if same.mean() > 0.95:
        collided.append(cur)
if collided:
    print(f"진입 바 겹침 일자 {len(collided)}개: {', '.join(collided)}")
    if a.dedup_bars:
        d = d[~d["scan_date"].isin(collided)]
        print("  → 제외 (기본). 전수로 보려면 --no-dedup-bars")
    else:
        print("  → --no-dedup-bars: 전수 유지. 부호검정이 그만큼 후하다.")

sz = d.groupby("cs").size()
keep = sz[sz >= a.min_cs].index
d = d[d["cs"].isin(keep)]
print(f"\n표본: {len(d)}행 | 단면 {d['cs'].nunique()}개 | 일자 {d['scan_date'].nunique()}개 "
      f"| 종목 {d['ticker'].nunique()}개 | {d['scan_date'].min()}~{d['scan_date'].max()}")
print(f"단면 크기: 중앙 {int(sz[keep].median())} (min {sz[keep].min()}, max {sz[keep].max()})")


def composite(g, wx):
    """w_text=wx, 남은 무게를 T·D 에 균등 배분. 합은 항상 1.0."""
    rest = (1.0 - wx) / 2
    return g["score_t"] * rest + g["s_text"] * wx + g["score_d"] * rest


def cs_ic(series_fn, ret):
    """단면별 Spearman IC → {단면: ic}. 표본·분산이 모자란 단면은 버린다."""
    out = {}
    for cs, g in d.groupby("cs"):
        v, r = series_fn(g), g[ret]
        m = v.notna() & r.notna()
        if m.sum() < a.min_cs or v[m].nunique() < 3:
            continue
        ic, _ = spearmanr(v[m], r[m])
        if not np.isnan(ic):
            out[cs] = ic
    return pd.Series(out, dtype=float)


def summarize(ic, label):
    n = len(ic)
    sd = ic.std(ddof=1) if n > 1 else np.nan
    t = ic.mean() / sd * np.sqrt(n) if n > 1 and sd > 0 else np.nan
    pos = int((ic > 0).sum())
    return {"대상": label, "단면": n, "평균IC": round(ic.mean(), 4),
            "중앙IC": round(ic.median(), 4), "t": round(t, 2),
            "IC>0": f"{pos}/{n}",
            "부호p": round(binomtest(pos, n, 0.5).pvalue, 4) if n else np.nan}


# ── 1. 축별 일자내 IC ─────────────────────────────────────────────────
print(f"\n── 1. 축별 일자내 IC ({a.horizons[min(1, len(a.horizons)-1)]}) ──")
ref = "fwd5" if "fwd5" in a.horizons else a.horizons[0]
axes = [("score_t", "T 기술"), ("s_text", "S_text 감성"), ("score_d", "D 수급"),
        ("score_total", "S_total 종합(현행 발송 기준)"),
        ("struct", "정형만 T+D (lookahead 없음)")]
print(pd.DataFrame([summarize(cs_ic(lambda g, c=c: g[c], ref), lab)
                    for c, lab in axes]).to_string(index=False))

# ── 2. w_text 스윕 ───────────────────────────────────────────────────
print(f"\n── 2. w_text 스윕 · 일자내 IC ({ref}) ──")
print(pd.DataFrame([
    summarize(cs_ic(lambda g, wx=wx: composite(g, wx), ref),
              f"w_text={wx:.2f}" + ("  ← 현행" if wx == a.current else
                                     "  ← 제안" if wx == a.proposed else ""))
    for wx in a.sweep]).to_string(index=False))

# ── 3. 쌍대비교 (지평별) ──────────────────────────────────────────────
print(f"\n── 3. 쌍대비교 w_text {a.proposed:.2f} − {a.current:.2f} · 같은 단면 안에서 ──")
rows = []
for ret in a.horizons:
    cur = cs_ic(lambda g: composite(g, a.current), ret)
    pro = cs_ic(lambda g: composite(g, a.proposed), ret)
    cm = cur.index.intersection(pro.index)
    diff = (pro[cm] - cur[cm]).dropna()
    n = len(diff)
    if n < 5:
        rows.append({"지평": ret, "단면": n, "판정": "표본 부족"})
        continue
    boot = np.array([rng.choice(diff.values, n, replace=True).mean()
                     for _ in range(a.boot)])
    lo, hi = np.percentile(boot, [2.5, 97.5])
    pos = int((diff > 0).sum())
    rows.append({
        "지평": ret, "단면": n,
        f"현행{a.current:.2f}": round(cur[cm].mean(), 4),
        f"제안{a.proposed:.2f}": round(pro[cm].mean(), 4),
        "차": round(diff.mean(), 4),
        "95%CI": f"[{lo:+.4f},{hi:+.4f}]",
        "개선단면": f"{pos}/{n}",
        "부호p": round(binomtest(pos, n, 0.5).pvalue, 3),
        "판정": "제안 우세" if lo > 0 else "현행 우세" if hi < 0 else "무차",
    })
print(pd.DataFrame(rows).to_string(index=False))

# ── 4. pooled 대조 ────────────────────────────────────────────────────
print(f"\n── 4. 참고: 같은 표본을 pooled 로 재면 (backtest.py 방식) ──")
for ret in a.horizons:
    line = []
    for wx in (a.current, a.proposed):
        s, r = composite(d, wx), d[ret]
        m = s.notna() & r.notna()
        ic, pv = spearmanr(s[m], r[m])
        line.append(f"w_text={wx:.2f} IC {ic:+.4f}(p={pv:.4f})")
    print(f"  {ret:<6} n={int(m.sum()):<5} " + "  |  ".join(line))
print("\n※ pooled 는 일자 효과가 섞인다. 판정은 3번 표를 쓸 것.")
