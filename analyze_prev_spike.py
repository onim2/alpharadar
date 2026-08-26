"""전일 급등 → 당일 급락 진입의 성과 검정 (D-5).

질문: 과열 게이트가 당일 스냅샷만 보기 때문에, '전일 +26% → 당일 -18%'처럼
지표가 하루 만에 리셋된 종목이 통과한다. 이렇게 들어온 종목이 같은 날 다른
통과 종목보다 실제로 열세인가?

설계
 1) 일자내 대조. pooled·절대수익률 비교는 결론이 뒤집힌다(8/24 교훈).
    같은 scan_date의 통과군을 조건군/대조군으로 갈라 스프레드를 구하고
    일자 동일가중으로 평균낸다.
 2) 등락률은 DB가 아니라 시세에서 가져온다. change_pct의 의미가 런마다
    다르기 때문이다 — 저녁 런(19:15 KST)은 당일 종가 등락률이지만 아침
    런(07:20 KST)은 장 시작 전이라 직전 거래일 등락률이다. 두 런이 같은
    scan_date를 공유하므로 DB 값을 일자축으로 이어붙이면 하루가 어긋난다.
    그래서 각 행의 change_pct와 일치하는 시세 바를 역으로 찾아('관측 바')
    그 바의 직전 거래일을 prev로 삼는다. 룩어헤드가 생기지 않는다.
 3) 전 기간(2026-05-23~)을 쓴다. 그 사이 점수 체계가 여러 번 바뀌었지만
    일자내 비교라 체계 변화는 조건군·대조군에 똑같이 걸린다.

한계: 관측 바를 등락률 일치로 역추적하므로, 같은 값이 이틀 연속 나오면
가장 늦은 바를 택한다. 일치 실패 행은 --report-unmatched로 볼 수 있다.
"""
import sqlite3, argparse, pickle, statistics as st
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--db", default="data/scores_history.db")
ap.add_argument("--px", required=True, help="종목별 등락률 캐시 pickle")
ap.add_argument("--prev", type=float, default=15.0, help="전일 급등 임계 (기본 +15%)")
ap.add_argument("--cur",  type=float, default=-8.0, help="당일 급락 임계 (기본 -8%)")
ap.add_argument("--from-date", default="20260101")
ap.add_argument("--min-ctrl", type=int, default=2, help="일자당 최소 대조군 수")
ap.add_argument("--tol", type=float, default=0.6, help="관측 바 역추적 허용 오차(%p)")
ap.add_argument("--report-unmatched", action="store_true")
ap.add_argument("--arm", default="both", choices=["both", "cur_only", "prev_only"],
                help="both=전일급등+당일급락(기본) / cur_only=당일급락만(전일 급등 아님) "
                     "/ prev_only=전일급등만(당일 급락 아님). 열세가 '당일 급락'만으로 "
                     "설명되면 prev 항을 새로 넣을 이유가 없다 — 그걸 가르는 갈래다.")
ap.add_argument("--quiet-rows", action="store_true", help="종목별 명세를 찍지 않는다")
a = ap.parse_args()

px = pickle.loads(Path(a.px).read_bytes())
con = sqlite3.connect(a.db)

# ── 관측 바 역추적 ────────────────────────────────────────────────────
rows, unmatched = {}, []
for sd, tk, chg in con.execute(
        "SELECT scan_date, ticker, change_pct FROM scan_results "
        "WHERE scan_date>=? AND change_pct IS NOT NULL", (a.from_date,)):
    ser = px.get(tk) or {}
    bars = sorted(ser)
    # scan_date 이하 바 중 change_pct와 일치하는 가장 늦은 바
    cand = [b for b in bars if b <= sd and abs(ser[b] - chg) <= a.tol]
    if not cand:
        unmatched.append((sd, tk, chg)); continue
    B = cand[-1]
    i = bars.index(B)
    if i == 0: continue
    cur, prev = ser[B], ser[bars[i - 1]]
    # 같은 (scan_date,ticker)에 두 런이 있으면 늦은 관측 바를 채택한다
    k = (sd, tk)
    if k not in rows or B > rows[k][0]:
        rows[k] = (B, cur, prev)

out = {}
for sd, tk, f1, f5, m5, mae in con.execute(
        "SELECT scan_date, ticker, fwd1, fwd5, mfe5, mae10 FROM outcomes "
        "WHERE origin='scan'"):
    out[(sd, tk)] = (f1, f5, m5, mae)

print(f"진입 행 {len(rows) + len(unmatched)}건 중 관측 바 역추적 성공 {len(rows)}건 "
      f"({100 * len(rows) / max(1, len(rows) + len(unmatched)):.1f}%), "
      f"실패 {len(unmatched)}건")
if a.report_unmatched:
    for r in unmatched[:30]: print("   미일치", r)

# ── 조건군/대조군 분할 ────────────────────────────────────────────────
by_date = {}
for (sd, tk), (B, cur, prev) in rows.items():
    if (sd, tk) not in out: continue
    if a.arm == "both":       hit = prev >= a.prev and cur <= a.cur
    elif a.arm == "cur_only": hit = prev <  a.prev and cur <= a.cur
    else:                     hit = prev >= a.prev and cur >  a.cur
    by_date.setdefault(sd, ([], []))[0 if hit else 1].append(
        ((sd, tk), out[(sd, tk)], prev, cur))

hits = [x for v in by_date.values() for x in v[0]]
LAB = {"both": f"전일 >= +{a.prev:g}% AND 당일 <= {a.cur:g}%",
       "cur_only": f"당일 <= {a.cur:g}% 이되 전일 < +{a.prev:g}% (당일 급락만)",
       "prev_only": f"전일 >= +{a.prev:g}% 이되 당일 > {a.cur:g}% (전일 급등만)"}
print(f"\n조건[{a.arm}]: {LAB[a.arm]}")
print(f"조건군 {len(hits)}건 / 대조군 {sum(len(v[1]) for v in by_date.values())}건")

if hits and not a.quiet_rows:
    print(f"\n{'scan_date':<11}{'종목':<8}{'전일':>8}{'당일':>8}{'fwd1':>8}{'fwd5':>8}")
    print("-" * 51)
    for (sd, tk), o, prev, cur in sorted(hits):
        f = lambda v: f"{v:>8.2f}" if v is not None else f"{'—':>8}"
        print(f"{sd:<11}{tk:<8}{prev:>8.2f}{cur:>8.2f}{f(o[0])}{f(o[1])}")

# ── 일자내 스프레드 ───────────────────────────────────────────────────
IDX = {"fwd1": 0, "fwd5": 1, "mfe5": 2, "mae10": 3}
print(f"\n{'지평':<8}{'유효일자':>8}{'조건군n':>8}{'평균차':>9}{'중앙차':>9}"
      f"{'열세일자':>9}{'조건군평균':>11}{'대조군평균':>11}")
print("-" * 74)
for name, i in IDX.items():
    dm, dmd, ns, hv, cv = [], [], 0, [], []
    for sd, (h, c) in by_date.items():
        hs = [x[1][i] for x in h if x[1][i] is not None]
        cs = [x[1][i] for x in c if x[1][i] is not None]
        if not hs or len(cs) < a.min_ctrl: continue
        dm.append(st.mean(hs) - st.mean(cs))
        dmd.append(st.median(hs) - st.median(cs))
        ns += len(hs); hv += hs; cv += cs
    if not dm:
        print(f"{name:<8}{'—':>8}{'—':>8}{'—':>9}{'—':>9}{'—':>9}{'—':>11}{'—':>11}")
        continue
    worse = sum(1 for x in dm if x < 0)
    print(f"{name:<8}{len(dm):>8}{ns:>8}{st.mean(dm):>9.2f}{st.mean(dmd):>9.2f}"
          f"{worse:>6}/{len(dm):<3}{st.mean(hv):>11.2f}{st.mean(cv):>11.2f}")

print(f"\n[읽는 법] 평균차·중앙차는 (조건군 - 같은날 대조군)이다. 음수면 열세.")
print(f"          '열세일자'는 스프레드가 음수인 일자 수 — 몇 건의 큰 손실이")
print(f"          평균을 끄는 것인지 방향이 꾸준한 것인지를 가른다.")
print(f"          mae10은 낙폭이라 음수차가 곧 '더 깊이 밀렸다'는 뜻이다.")
