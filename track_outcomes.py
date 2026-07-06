#!/usr/bin/env python3
# ══════════════════════════════════════════════════════════════════════════════
# track_outcomes.py — 스캔·gated 종목의 forward return 추적 (로컬+Actions 겸용, 멱등)
#
# scan_results(발송 후보)와 gated_tickers(과열 배제)의 과거 행에 대해
# FinanceDataReader로 T+1/T+5/T+10/T+20 '거래일' 종가 수익률을 계산해
# outcomes 테이블에 저장한다. 진입가 = scan_date 당일(또는 그 이후 첫 거래일) 종가.
#
# 멱등성: 이미 4개 지평이 모두 채워진 (scan_date,ticker,origin)은 스킵.
#         미도래(미래) 지평은 NULL로 두고 다음 실행에서 채움. 신규 PK 삽입 0이면 멱등.
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


def _init_outcomes(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS outcomes (
            scan_date TEXT, ticker TEXT, origin TEXT,
            fwd1 REAL, fwd5 REAL, fwd10 REAL, fwd20 REAL,
            PRIMARY KEY (scan_date, ticker, origin)
        )
    """)


def _load_targets(con):
    """(scan_date, ticker, origin) 목록 — scan_results + gated_tickers."""
    targets = []
    for row in con.execute("SELECT DISTINCT scan_date, ticker FROM scan_results"):
        targets.append((row[0], row[1], "scan"))
    # gated_tickers는 Task3 이후에만 존재 → 없으면 무시
    try:
        for row in con.execute("SELECT DISTINCT scan_date, ticker FROM gated_tickers"):
            targets.append((row[0], row[1], "gated"))
    except sqlite3.OperationalError:
        logger.info("gated_tickers 테이블 없음 → scan만 추적")
    return targets


def _load_existing(con):
    """{(scan_date,ticker,origin): {fwd1..fwd20}} — 기존 outcomes."""
    out = {}
    for r in con.execute("SELECT scan_date,ticker,origin,fwd1,fwd5,fwd10,fwd20 FROM outcomes"):
        out[(r[0], r[1], r[2])] = {"fwd1": r[3], "fwd5": r[4], "fwd10": r[5], "fwd20": r[6]}
    return out


def _fetch_prices(tickers, start, end):
    import FinanceDataReader as fdr
    cache = {}
    for tk in sorted(set(tickers)):
        try:
            df = fdr.DataReader(tk, start, end)
            if df is not None and not df.empty and "Close" in df.columns:
                cache[tk] = df["Close"].astype(float)
        except Exception as e:
            logger.debug(f"가격 조회 실패 {tk}: {e}")
    return cache


def _forward_returns(close_s, scan_date):
    """진입가=scan_date 당일 이후 첫 거래일 종가. 각 지평 h거래일 후 수익률(%). 미도래→None."""
    scan_dt = pd.to_datetime(scan_date, format="%Y%m%d")
    idx0 = close_s.index.searchsorted(scan_dt, side="left")  # date >= scan_date 첫 바
    if idx0 >= len(close_s):
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
               if any(existing.get(t, {}).get(COL[h]) is None for h in HORIZONS)]
    logger.info(f"대상 {len(targets)}건 | 갱신 필요 {len(pending)}건 | 완료 스킵 {len(targets)-len(pending)}건")
    if not pending:
        logger.info("갱신할 outcomes 없음 (전부 완료) — 멱등 종료")
        con.close()
        return

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
        vals = _forward_returns(prices[ticker], scan_date)
        prev = existing.get((scan_date, ticker, origin))
        # 기존 non-null 값은 보존, 새로 도래한 지평만 채움
        if prev:
            merged = {c: (prev.get(c) if prev.get(c) is not None else vals.get(c)) for c in COL.values()}
            if merged == prev:            # 변화 없음(전부 미도래 유지 등) → 재기록 생략
                continue
        else:
            merged = vals
            new_pk += 1
        con.execute(
            "INSERT OR REPLACE INTO outcomes (scan_date,ticker,origin,fwd1,fwd5,fwd10,fwd20) "
            "VALUES (?,?,?,?,?,?,?)",
            (scan_date, ticker, origin, merged["fwd1"], merged["fwd5"], merged["fwd10"], merged["fwd20"]))
        updated += 1
    con.commit()
    con.close()
    logger.info(f"outcomes 적재: 신규 {new_pk}건 | 갱신(신규+보완) {updated}건")


if __name__ == "__main__":
    main()
