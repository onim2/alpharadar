"""이격도 상한 재판단 하네스 — 티어맞춤 기준선.

질문: 티어별 이격도 상한을 T0에서 얼마나 올리면(완화하면) 이득인가?

설계 원칙 3가지
 1) 일자내 대조. 절대 수익률·pooled 비교는 결론이 뒤집힌다(실측).
 2) 티어맞춤. 상한이 티어별로 다르므로(large 110 / mid 115 / small 120)
    기준선도 티어 안에서 잡는다. 티어를 섞으면 소형주 분포가 결론을 끌고 간다.
 3) fwd1과 fwd5를 함께 본다. 과거에 fwd1만 보고 올렸다가 fwd5가 반대로
    나와 되돌린 적이 있다. 두 지평의 부호가 갈리면 올리지 않는다.

식별 한계 (필터가 순차 continue이고 reason은 '첫 탈락 사유'다)
   과열 → 이격상한 → 이격하한 → MA추세 → RSI → 거래대금
 f:disp_upper 행은 이격도에서 잘렸을 뿐, 상한을 풀어도 뒤의 MA추세·RSI·거래대금을
 다시 통과해야 실제로 편입된다. pool_history에는 rsi만 있고 ma20/ma120·거래대금이
 없다. 따라서 여기 수치는 '완화 시 편입될 종목'의 상한선(upper bound)이다.
 RSI 조건만은 적용 가능하므로 --rsi-gate로 켤 수 있게 둔다.
"""
import sqlite3, argparse, statistics as st, random
random.seed(20260826)

T0 = {"large": 110, "mid": 115, "small": 120}   # config.yaml 현재 값
BANDS = [(0, 2.5), (2.5, 5), (5, 10), (10, 999)]

ap = argparse.ArgumentParser()
ap.add_argument("--db", default="data/scores_history.db")
ap.add_argument("--rsi-gate", action="store_true",
                help="RSI>70이면 어차피 잘리므로 제외 (실제 편입에 더 근사)")
ap.add_argument("--min-n", type=int, default=2, help="일자당 최소 표본")
ap.add_argument("--min-dates", type=int, default=3,
                help="판정에 필요한 최소 유효 일자 (기본 3). 이 미만이면 '—'")
ap.add_argument("--thin", type=int, default=5,
                help="이 미만이면 얇은 표본으로 보고 '*'를 붙인다 (기본 5)")
ap.add_argument("--exclude-dates", default="20260817,20260818",
                help="표본에서 뺄 scan_date 쉼표 목록. 기본값은 광복절 대체휴장으로 "
                     "진입 바가 겹치는 8/17·8/18이다 — 합쳐서 1일자로 세는 게 아니라 "
                     "둘 다 뺀다(사용자 확정 2026-08-26). 안 빼려면 --exclude-dates ''")
ap.add_argument("--asof", default="20260826",
                help="성숙 판정 기준일 YYYYMMDD. 이 날 종가까지 도래한 fwd5만 성숙으로 센다")
a = ap.parse_args()
EXCL = {x.strip() for x in a.exclude_dates.split(",") if x.strip()}
con = sqlite3.connect(a.db)

rsi_cond = "AND p.rsi <= 70" if a.rsi_gate else ""
rows = con.execute(f"""
    SELECT o.scan_date, p.cap_tier, p.disparity, o.fwd1, o.fwd5
    FROM outcomes o JOIN pool_history p
      ON p.scan_date=o.scan_date AND p.ticker=o.ticker AND p.reason='disp_upper'
    WHERE o.origin='f:disp_upper' AND p.disparity IS NOT NULL {rsi_cond}
""").fetchall()
n_raw = len(rows)
rows = [r for r in rows if r[0] not in EXCL]

# 기준선: 같은 날 같은 티어의 통과군(scan)
base = {}
for d, tier, f1, f5 in con.execute("""
    SELECT s.scan_date, s.cap_tier, o.fwd1, o.fwd5
    FROM scan_results s JOIN outcomes o
      ON o.scan_date=s.scan_date AND o.ticker=s.ticker AND o.origin='scan'
"""):
    if d in EXCL: continue
    base.setdefault((d, tier), []).append((f1, f5))

def band_of(tier, disp):
    ex = disp - T0[tier]
    if ex < 0: return None                    # 상한 아래인데 잘림 → 데이터 이상
    for lo, hi in BANDS:
        if lo <= ex < hi: return (lo, hi)
    return None

buckets = {}
for d, tier, disp, f1, f5 in rows:
    b = band_of(tier, disp)
    if b is None: continue
    buckets.setdefault((tier, b), {}).setdefault(d, []).append((f1, f5))

def spread(per_date, tier, hi_idx):
    """일자별 (완화편입군 평균 - 같은날 같은티어 통과군 평균), 일자 동일가중.

    지평마다 성숙한 일자가 다르므로 일자 수를 지평별로 따로 돌려준다.
    fwd5 승률 100%가 1~2일자에서 나온 값인지 20일자에서 나온 값인지
    구분되지 않으면 표를 잘못 읽는다.
    """
    out = []
    for d, vals in per_date.items():
        v = [x[hi_idx] for x in vals if x[hi_idx] is not None]
        bl = [x[hi_idx] for x in base.get((d, tier), []) if x[hi_idx] is not None]
        if len(v) < a.min_n or len(bl) < a.min_n: continue
        out.append(st.mean(v) - st.mean(bl))
    return out

print(f"이격도 상한 완화 시뮬 — 티어맞춤 기준선  (RSI게이트 {'ON' if a.rsi_gate else 'OFF'})")
print(f"현재 상한: large {T0['large']} / mid {T0['mid']} / small {T0['small']}")
if EXCL:
    print(f"제외 일자: {', '.join(sorted(EXCL))} "
          f"({n_raw - len(rows)}건 제외, 진입 바 겹침)")
print()
print(f"{'티어':<7}{'초과대역':<14}{'표본':>5}"
      f"{'fwd1일자':>9}{'스프레드':>10}{'승률':>7}"
      f"{'fwd5일자':>9}{'스프레드':>10}{'승률':>7}{'부호':>7}")
print("-" * 88)
for tier in ("large", "mid", "small"):
    for b in BANDS:
        pd_ = buckets.get((tier, b))
        if not pd_: continue
        n = sum(len(v) for v in pd_.values())
        s1, s5 = spread(pd_, tier, 0), spread(pd_, tier, 1)
        # 유효 일자가 --min-dates 미만이면 수치를 내지 않는다. 그 이상이라도
        # --thin 미만이면 '*'로 얇다는 것을 표시한다 — 8/13의 fwd5 첫 도래분
        # (1일자, n=66)만 보고 상한을 올렸다 되돌린 전례가 있다.
        def cell(s):
            if len(s) < a.min_dates: return f"{'—':>9}{'—':>10}{'—':>7}"
            w = sum(1 for x in s if x > 0) / len(s) * 100
            mark = "" if len(s) >= a.thin else "*"
            return f"{str(len(s)) + mark:>9}{st.mean(s):>10.2f}{w:>6.0f}%"
        if len(s1) >= a.min_dates and len(s5) >= a.min_dates:
            agree = "일치" if (st.mean(s1) > 0) == (st.mean(s5) > 0) else "★반대"
        else:
            agree = "표본부족"
        lab = f"+{b[0]:g}~{b[1]:g}%p" if b[1] < 999 else f"+{b[0]:g}%p↑"
        print(f"{tier:<7}{lab:<14}{n:>5}{cell(s1)}{cell(s5)}{agree:>7}")

print(f"\n  * = 유효 일자 {a.thin} 미만 — 얇은 표본. 방향만 참고하고 확정하지 않는다.")
print(f"\n[판정 규칙] fwd1·fwd5 스프레드가 둘 다 양수이고 승률>50%이며")
print(f"            양쪽 다 유효 일자 {a.min_dates} 이상인 대역만 완화 후보.")
print("            한쪽이라도 음수면 올리지 않는다 (과거 되돌림의 원인).")
print("\n[주의] 2026-08-17은 휴장이다. scan_date 8/17과 8/18은 진입 바가 같아서")
print("       (둘 다 8/18 종가) fwd 값이 완전히 동일하다 — 독립 관측 2개가 아니라")
print("       1개다. 기본값으로 둘 다 --exclude-dates 처리한다.")
mature5 = sum(1 for r in rows if r[4] is not None)
print(f"\n표본 현황: f:disp_upper {len(rows)}건 중 fwd5 성숙 {mature5}건 "
      f"({len({r[0] for r in rows if r[4] is not None})}일자)")

# ── fwd5 성숙 일정 예보 ────────────────────────────────────────────────
# 휴장이 있으면 scan_date가 서로 달라도 진입 바가 같아진다. 8/17(광복절 대체
# 휴장)과 8/18은 둘 다 8/18 종가로 진입해서 fwd 값이 글자 그대로 동일하다.
# 일자 동일가중은 그 둘을 독립 관측 2개로 세는데, 실제로는 1개다. 유효 일자를
# '진입 바' 기준으로 다시 세지 않으면 표본이 실제보다 커 보인다.
import datetime as _dt
try:
    import FinanceDataReader as _fdr
    _ix = _fdr.DataReader("KS11", "2026-08-01", "2026-09-30").index
    _bars = [d.date() for d in _ix]
except Exception as _e:
    _bars = []

if _bars:
    _scans = [r[0] for r in con.execute(
        "SELECT DISTINCT scan_date FROM pool_history WHERE reason='disp_upper' ORDER BY scan_date")]
    print("\n" + "=" * 62)
    print("fwd5 성숙 일정 — '진입 바' 기준 유효 일자")
    print("=" * 62)
    print(f"{'scan_date':<12}{'진입 바':<12}{'fwd5 도래':<12}{'상태':<10}")
    print("-" * 62)
    _today = _dt.date(int(a.asof[:4]), int(a.asof[4:6]), int(a.asof[6:]))
    _entry_seen, _eff = {}, 0
    for s in _scans:
        sd = _dt.date(int(s[:4]), int(s[4:6]), int(s[6:]))
        nxt = [b for b in _bars if b >= sd]
        if not nxt: continue
        e = nxt[0]
        i = _bars.index(e)
        if i + 5 >= len(_bars): continue
        m = _bars[i + 5]
        dup = e in _entry_seen
        _entry_seen.setdefault(e, s)
        excl = s in EXCL
        if not dup and not excl and m <= _today: _eff += 1
        state = "제외(진입 바 겹침)" if excl else \
                (f"중복({_entry_seen[e]}와 동일 바)" if dup else
                 ("성숙" if m <= _today else f"D-{(m - _today).days}"))
        print(f"{s:<12}{str(e):<12}{str(m):<12}{state:<10}")
    print("-" * 62)
    print(f"오늘({_today}) 종가 기준 독립 유효 일자: {_eff}개")
