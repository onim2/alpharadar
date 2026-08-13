#!/usr/bin/env python3
# ══════════════════════════════════════════════════════════════════════════════
# track_outcomes.py — 스캔·gated 종목의 forward return 추적 (로컬+Actions 겸용, 멱등)
#
# scan_results(발송 후보)와 gated_tickers(과열 배제)의 과거 행에 대해
# FinanceDataReader로 두 종류의 성과를 계산해 outcomes 테이블에 저장한다.
#
#   fwd1/5/10/20   종가 진입 → N거래일 후 종가 수익률(%)
#                  진입가 = scan_date 당일(또는 그 이후 첫 거래일) 종가
#   mfe5/10/20     D+1 시가 진입 → N거래일 '내' 최고가 도달률(%)
#   mae10          같은 진입 기준, 10거래일 '내' 최저가 도달률(%)
#
# 두 벌을 다 두는 이유: 이 시스템의 설계 근거(등급 폐지, 누적 관찰 이력 정렬,
# 발송 하한)는 전부 MFE 정의로 만들어졌는데 저장은 fwd*만 하고 있었다. 서로 다른
# 자를 섞어 재면 결론이 뒤집힌다 — 누적 관찰 횟수는 MFE 기준 Spearman +0.187인데
# 종가 기준 fwd10 일자내로는 -0.117이 나온다. 시장·대조군 대비 비교에는 여전히
# 종가 수익률이 맞으므로 fwd*도 그대로 둔다.
#
# 멱등성: 모든 값 컬럼이 채워진 (scan_date,ticker,origin)은 스킵.
#         미도래(미래) 지평은 NULL로 두고 다음 실행에서 채움. 신규 PK 삽입 0이면 멱등.
#         기존 DB에는 없는 컬럼만 ALTER로 덧붙인다.
#
# ── 사전 등록(pre-registered) 주 지표 ──────────────────────────────────────────
#   [과열필터 판정] median fwd10(origin='gated', reason LIKE 'overheat%')
#                   < median fwd10(origin='scan', grade in ('집중','주시'))
#       → 참이면 "배제한 종목이 실제로 덜 올랐다" = 과열 필터가 옳게 작동.
#   [Shadow 판정]  Spearman IC(score_presurge, fwd10)
#                   > Spearman IC(score_legacy=score_total, fwd10)   (고유 종목 n>=30)
#       → 참이면 물밑 스코어가 legacy보다 예측력 우위 → pre_surge_mode 승격 검토.
#   ※ 판정은 별도 분석에서 수행. 본 스크립트는 outcomes 적재만 담당.
# ══════════════════════════════════════════════════════════════════════════════
import argparse
import logging
import sqlite3
from datetime import timedelta
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] track_outcomes — %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path("data/scores_history.db")
HORIZONS = [1, 5, 10, 20]          # 거래일
COL = {1: "fwd1", 5: "fwd5", 10: "fwd10", 20: "fwd20"}

# ── 구간 최고/최저 (MFE/MAE) ──────────────────────────────────────────────────
# fwd*는 '종가 진입 → N거래일 후 종가'다. 그런데 이 시스템의 설계 근거는 전부
# 'D+1 시가 진입 후 N일 내 구간 최고가'로 만들어졌다 — 등급 폐지, 누적 관찰 이력
# 정렬, 발송 하한 45가 모두 그 지표에서 나왔다(get_watch_history 주석 참조).
# 두 지표를 섞으면 결론이 뒤집힌다. 실제로 누적 관찰 횟수는 구간 최고가 기준
# Spearman +0.187인데, 종가 기준 fwd10으로 재면 일자내 -0.117이 나온다.
# 같은 자로 재기 위해 설계 근거와 동일한 정의를 컬럼으로 들인다. fwd*는 그대로
# 둔다 — 시장·대조군 대비 비교에는 종가 수익률이 맞다.
EXC_HORIZONS = [5, 10, 20]         # 거래일
MFE_COL = {5: "mfe5", 10: "mfe10", 20: "mfe20"}
# 최저가는 10일만 둔다. 보유 지평이 1~3일/1~2주라 실제 의사결정을 가르는 것은
# 이 구간의 하락 폭이다. 최고가만 보면 "갔다"는 사실만 남고 그 전에 얼마나
# 빠졌는지가 지워져 낙관 편향이 생긴다.
MAE_COL = {10: "mae10"}
VAL_COLS = ([COL[h] for h in HORIZONS]
            + [MFE_COL[h] for h in EXC_HORIZONS]
            + [MAE_COL[h] for h in MAE_COL])


def _track_control() -> bool:
    """대조군(pool_history)까지 추적할지 — config의 control_sample.track_outcomes.

    끄고 싶은 이유가 실재한다. 대조군은 하루 ~160종목이 늘고 종목마다 가격 조회가
    붙는데, 이 스크립트는 스캔(32~44분) 뒤 같은 60분 워크플로 안에서 돈다.
    시간이 모자라면 스캔 결과 자체를 잃는 것보다 대조군을 포기하는 편이 낫다.
    설정이 없으면 켠 것으로 본다 — 대조군 없이는 성과 해석이 성립하지 않는다.
    """
    try:
        import yaml
        cfg = yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8"))
        return bool((cfg.get("control_sample") or {}).get("track_outcomes", True))
    except Exception as e:
        logger.warning(f"config 읽기 실패 → 대조군 추적 켬 ({type(e).__name__}: {e})")
        return True


def _init_outcomes(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS outcomes (
            scan_date TEXT, ticker TEXT, origin TEXT,
            fwd1 REAL, fwd5 REAL, fwd10 REAL, fwd20 REAL,
            mfe5 REAL, mfe10 REAL, mfe20 REAL, mae10 REAL,
            PRIMARY KEY (scan_date, ticker, origin)
        )
    """)
    # 이미 있는 DB는 fwd*만 갖고 있다. 없는 컬럼만 덧붙인다(멱등).
    have = {r[1] for r in con.execute("PRAGMA table_info(outcomes)")}
    for c in VAL_COLS:
        if c not in have:
            con.execute(f"ALTER TABLE outcomes ADD COLUMN {c} REAL")
            logger.info(f"outcomes 컬럼 추가: {c}")


def _load_targets(con):
    """(scan_date, ticker, origin) 목록 — scan_results + gated_tickers + pool_history.

    pool_history는 대조군 표본이다. 이게 추적되지 않으면 "스캔을 통과한 종목이
    정말 나은가"를 물을 상대가 없다. 실제로 그 부재 때문에 비교 대상이 무작위
    KRX 표본밖에 없었고, 거기엔 유니버스 필터(주가·시총·거래대금)를 통과하지
    못할 종목이 섞여 결론이 뒤집혔다 — 무작위 대비로는 +30% 급등 2.3배였는데
    유니버스를 맞추자 오히려 열세로 나왔다.

    origin 값:
      scan       스캔 통과 (스코어링·발송 대상)
      gated      과열 배제 (전수)
      ns         유니버스는 통과했으나 신호 없음 (POOL_A 미진입) — 표본
      f:<사유>   신호는 있었으나 하드필터 탈락 — 사유별 표본
                 (ma_trend / turnover / disp_upper / disp_lower / rsi / overheat)

    사유를 origin에 접어 넣는 이유: outcomes 스키마를 바꾸지 않고도 사유별로
    성과를 가를 수 있어야 한다. "MA추세로 자른 종목이 실제로 못 갔는가" 같은
    질문이 여기서 답해진다 — 지금은 근거 없이 하루 600~1,000개를 자르고 있다.
    """
    targets = []
    for row in con.execute("SELECT DISTINCT scan_date, ticker FROM scan_results"):
        targets.append((row[0], row[1], "scan"))
    # gated_tickers는 Task3 이후에만 존재 → 없으면 무시
    try:
        for row in con.execute("SELECT DISTINCT scan_date, ticker FROM gated_tickers"):
            targets.append((row[0], row[1], "gated"))
    except sqlite3.OperationalError:
        logger.info("gated_tickers 테이블 없음 → scan만 추적")
    # pool_history도 마찬가지로 나중에 생긴 테이블이라 없을 수 있다
    if not _track_control():
        logger.info("control_sample.track_outcomes=false → 대조군 추적 생략")
        return targets
    try:
        n = 0
        for row in con.execute(
                "SELECT DISTINCT scan_date, ticker, stage, reason FROM pool_history"):
            origin = "ns" if row[2] == "no_signal" else f"f:{row[3]}"
            targets.append((row[0], row[1], origin))
            n += 1
        if n:
            logger.info(f"pool_history 대조군 {n}건 추적 대상 포함")
    except sqlite3.OperationalError:
        logger.info("pool_history 테이블 없음 → 대조군 추적 생략")
    return targets


def _load_existing(con):
    """{(scan_date,ticker,origin): {값 컬럼들}} — 기존 outcomes."""
    cols = ",".join(VAL_COLS)
    out = {}
    for r in con.execute(f"SELECT scan_date,ticker,origin,{cols} FROM outcomes"):
        out[(r[0], r[1], r[2])] = dict(zip(VAL_COLS, r[3:]))
    return out


def _fetch_prices(tickers, start, end):
    """{ticker: OHLC DataFrame}. MFE/MAE에 고가·저가·시가가 필요해 Close만 담지 않는다."""
    import FinanceDataReader as fdr
    cache = {}
    for tk in sorted(set(tickers)):
        try:
            df = fdr.DataReader(tk, start, end)
            if df is not None and not df.empty and "Close" in df.columns:
                cache[tk] = df
        except Exception as e:
            logger.debug(f"가격 조회 실패 {tk}: {e}")
    return cache


def _entry_idx(index, scan_date):
    """scan_date 당일(또는 그 이후 첫 거래일) 바의 위치. 없으면 None."""
    scan_dt = pd.to_datetime(scan_date, format="%Y%m%d")
    i = index.searchsorted(scan_dt, side="left")
    return None if i >= len(index) else i


def _forward_returns(df, scan_date):
    """진입가=scan_date 당일 이후 첫 거래일 종가. 각 지평 h거래일 후 수익률(%). 미도래→None."""
    close_s = df["Close"].astype(float)
    idx0 = _entry_idx(close_s.index, scan_date)
    if idx0 is None:
        return {COL[h]: None for h in HORIZONS}
    entry = float(close_s.iloc[idx0])
    res = {}
    for h in HORIZONS:
        j = idx0 + h
        if entry > 0 and j < len(close_s):
            res[COL[h]] = round((float(close_s.iloc[j]) / entry - 1) * 100, 4)
        else:
            res[COL[h]] = None          # 미도래
    return res


def _excursions(df, scan_date):
    """D+1 시가 진입 후 N거래일 '내' 최고가(MFE)·최저가(MAE) 도달률(%).

    설계 근거와 같은 정의다 — 진입은 신호 다음 날 시가이고, 구간은 진입 바부터
    N개 바다(idx0+1 .. idx0+h). 구간이 다 차지 않았으면 None으로 둔다. 부분
    구간으로 채우면 최고가가 과소 계상되고, 나중에 채워질 때 값의 의미가 바뀐다.
    """
    none = {**{MFE_COL[h]: None for h in EXC_HORIZONS},
            **{MAE_COL[h]: None for h in MAE_COL}}
    if not {"Open", "High", "Low"} <= set(df.columns):
        return none                      # OHLC가 없는 소스 → 조용히 건너뜀
    idx0 = _entry_idx(df.index, scan_date)
    if idx0 is None:
        return none
    e = idx0 + 1                         # D+1 진입 바
    if e >= len(df):
        return none                      # 진입 자체가 미도래
    entry = float(df["Open"].iloc[e])
    if not entry > 0:
        return none
    high = df["High"].astype(float)
    low = df["Low"].astype(float)
    res = dict(none)
    for h in EXC_HORIZONS:
        end = idx0 + h                   # 포함 구간 e..end → h개 바
        if end >= len(df):
            continue                     # 미도래
        res[MFE_COL[h]] = round((float(high.iloc[e:end + 1].max()) / entry - 1) * 100, 4)
        if h in MAE_COL:
            res[MAE_COL[h]] = round((float(low.iloc[e:end + 1].min()) / entry - 1) * 100, 4)
    return res


def main():
    ap = argparse.ArgumentParser(description="forward return 추적 (멱등)")
    ap.add_argument("--db", default=str(DB_PATH))
    args = ap.parse_args()
    db = Path(args.db)
    if not db.exists():
        logger.error(f"DB 없음: {db}")
        return

    con = sqlite3.connect(db)
    _init_outcomes(con)
    con.commit()

    targets  = _load_targets(con)
    existing = _load_existing(con)

    # 이미 4개 지평 모두 채워진 키는 스킵 (재조회 불필요)
    pending = [t for t in targets
               if any(existing.get(t, {}).get(c) is None for c in VAL_COLS)]
    logger.info(f"대상 {len(targets)}건 | 갱신 필요 {len(pending)}건 | 완료 스킵 {len(targets)-len(pending)}건")
    if not pending:
        logger.info("갱신할 outcomes 없음 (전부 완료) — 멱등 종료")
        con.close()
        return

    # 가격 조회는 고유 종목 수에 비례하고, 이 스크립트는 스캔 뒤 같은 60분 워크플로
    # 안에서 돈다. 대조군이 얼마나 부담을 더하는지 로그로 드러내야 timeout이
    # 닥쳤을 때 무엇을 끌지 판단할 수 있다.
    _ctrl = {t for t in pending if t[2] == "ns" or t[2].startswith("f:")}
    _base_tk = {t[1] for t in pending if t not in _ctrl}
    _ctrl_tk = {t[1] for t in _ctrl} - _base_tk
    logger.info(f"  가격 조회 고유 종목 {len(_base_tk) + len(_ctrl_tk)}개 "
                f"(기존 {len(_base_tk)} + 대조군 추가 {len(_ctrl_tk)})")

    dates = [t[0] for t in pending]
    lo = pd.to_datetime(min(dates), format="%Y%m%d") - timedelta(days=7)
    hi = pd.to_datetime(max(dates), format="%Y%m%d") + timedelta(days=45)
    prices = _fetch_prices([t[1] for t in pending],
                           lo.strftime("%Y-%m-%d"), hi.strftime("%Y-%m-%d"))

    new_pk = 0      # 신규 PK(멱등 측정용)
    updated = 0
    for (scan_date, ticker, origin) in pending:
        if ticker not in prices:
            continue
        df = prices[ticker]
        vals = {**_forward_returns(df, scan_date), **_excursions(df, scan_date)}
        prev = existing.get((scan_date, ticker, origin))
        # 기존 non-null 값은 보존, 새로 도래한 지평만 채움
        if prev:
            merged = {c: (prev.get(c) if prev.get(c) is not None else vals.get(c)) for c in VAL_COLS}
            if merged == prev:            # 변화 없음(전부 미도래 유지 등) → 재기록 생략
                continue
        else:
            merged = vals
            new_pk += 1
        cols = ",".join(VAL_COLS)
        ph = ",".join("?" * len(VAL_COLS))
        con.execute(
            f"INSERT OR REPLACE INTO outcomes (scan_date,ticker,origin,{cols}) "
            f"VALUES (?,?,?,{ph})",
            (scan_date, ticker, origin, *(merged[c] for c in VAL_COLS)))
        updated += 1
    con.commit()
    con.close()
    logger.info(f"outcomes 적재: 신규 {new_pk}건 | 갱신(신규+보완) {updated}건")


if __name__ == "__main__":
    main()
