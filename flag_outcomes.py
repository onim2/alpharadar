#!/usr/bin/env python3
"""flag_outcomes.py — 오염된 outcomes 값을 표시하고, 걸러 읽는 뷰 outcomes_clean 을 만든다.

outcomes 자체는 고치지 않는다(재계산은 KIS J 전환 후 일괄 — 2026-09-18 결정).
분석·대시보드는 outcomes 대신 outcomes_clean 을 읽는다.

    cp data/scores_history.db /tmp/t.db
    python flag_outcomes.py --db /tmp/t.db            # 사본에서 먼저. 제외 건수를 찍는다
    python flag_outcomes.py --db /tmp/t.db --un-cutoff none   # UN 규칙 해제(KIS J 재계산 후)

--db 는 필수다. 실전 DB 에는 승인 후에만 쓴다.

두 가지 오염
  ① 장중 바 고정 (outcomes_flags, reason='intraday_bar')
     track_outcomes 가 당일 바를 거르지 않고 '기존 non-null 보존'으로 병합해,
     아침 런의 그 단계가 09:00 을 넘긴 날 청산 바(창의 끝 바)가 그날인 값이
     장중가로 고정됐다. 대상 끝 바: INTRADAY_END_BARS.
       suspect   = 끝 바가 대상일인 값 전부
       confirmed = 그중 최종 가격으로 재계산한 값과 다른 것
     제외(excluded=1): 9/2 는 confirmed 만, 9/15~9/18 은 suspect 전부.
     (9/2 는 9/14 이전이라 재계산 기준이 J 와 같다 — confirmed 가 곧 장중 오염이다.
      9/15~ 는 재계산 기준 자체가 UN 계열이라 구분이 안 되므로 전부 뺀다.)
  ② 통합가(UN) 계열 (뷰의 날짜 규칙, 상수 UN_CUTOFF)
     2026-09-14 부터 FDR 종가가 UN 계열이다. 창(진입~끝 바)에 UN_CUTOFF 이후 바가
     포함된 값은 뷰에서 NULL. 끝 바는 종목 자기 거래일로 센다(outcomes_window_end).
     scan_date >= UN_CUTOFF 이면 진입 바부터 UN 이라 전부 NULL. 끝 바 기록이 없는
     과거 값(이 스크립트 실행 뒤에 채워진 값)은 끝 바가 반드시 오늘 이후이므로 NULL.
"""
import argparse
import importlib
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

KST = timezone(timedelta(hours=9))

# ── 상수 ────────────────────────────────────────────────────────────────────
UN_CUTOFF = "2026-09-14"          # KIS J 재계산 후 None(--un-cutoff none)으로 해제
INTRADAY_END_BARS = {             # 끝 바 → 제외 수준
    "2026-09-02": "confirmed",
    "2026-09-15": "suspect",
    "2026-09-16": "suspect",
    "2026-09-17": "suspect",
    "2026-09-18": "suspect",
}
TOL = 1e-4                        # 저장값은 소수 4자리 반올림
VIEW = "outcomes_clean"


def _import_track_outcomes():
    """track_outcomes 는 import 시 data/logs/scanner_*.log 를 연다 — 추적 로그를 건드리지 않게
    임시 디렉터리에서 import 한다. 재계산 공식을 복제하지 않고 원본 함수를 쓰기 위해서다."""
    here = Path(__file__).resolve().parent
    sys.path.insert(0, str(here))
    old = os.getcwd()
    with tempfile.TemporaryDirectory() as d:
        os.chdir(d)
        try:
            return importlib.import_module("track_outcomes")
        finally:
            os.chdir(old)


def _horizons(T):
    """{col: h} — track_outcomes 의 정의를 그대로 쓴다. 끝 바 인덱스는 전부 idx0 + h."""
    m = {T.COL[h]: h for h in T.HORIZONS}
    m.update({T.MFE_COL[h]: h for h in T.EXC_HORIZONS})
    m.update({T.MAE_COL[h]: h for h in T.MAE_COL})
    return m


def ensure_schema(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS outcomes_flags (
            scan_date TEXT, ticker TEXT, origin TEXT, col TEXT,
            reason TEXT, end_bar TEXT, level TEXT, stored REAL, recomputed REAL,
            excluded INTEGER, flagged_at TEXT,
            PRIMARY KEY (scan_date, ticker, origin, col, reason)
        );
        CREATE TABLE IF NOT EXISTS outcomes_window_end (
            scan_date TEXT, ticker TEXT, origin TEXT, col TEXT, end_bar TEXT,
            PRIMARY KEY (scan_date, ticker, origin, col)
        );
    """)


def create_view(con, cols, un_cutoff):
    """열마다 CASE 로 NULL 처리 — 오염되지 않은 다른 지평은 살린다."""
    key = "f.scan_date=o.scan_date AND f.ticker=o.ticker AND f.origin=o.origin"
    parts = []
    for c in cols:
        bad = [f"EXISTS (SELECT 1 FROM outcomes_flags f WHERE {key} AND f.col='{c}' AND f.excluded=1)"]
        if un_cutoff:
            ymd = un_cutoff.replace("-", "")
            bad.append(f"o.scan_date >= '{ymd}'")
            bad.append(f"NOT EXISTS (SELECT 1 FROM outcomes_window_end f WHERE {key} "
                       f"AND f.col='{c}' AND f.end_bar < '{un_cutoff}')")
        parts.append(f"CASE WHEN {' OR '.join(bad)} THEN NULL ELSE o.{c} END AS {c}")
    con.execute(f"DROP VIEW IF EXISTS {VIEW}")
    note = f"UN_CUTOFF={un_cutoff}" if un_cutoff else "UN_CUTOFF=해제"
    con.execute(f"CREATE VIEW {VIEW} AS SELECT o.scan_date, o.ticker, o.origin, "
                f"{', '.join(parts)} FROM outcomes o /* {note} */")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", required=True, help="쓸 DB. 실전은 승인 후에만.")
    ap.add_argument("--un-cutoff", default=UN_CUTOFF, help="YYYY-MM-DD 또는 none(해제)")
    ap.add_argument("--view-only", action="store_true", help="표시는 그대로 두고 뷰만 다시 만든다")
    a = ap.parse_args()
    un = None if a.un_cutoff.lower() == "none" else a.un_cutoff
    db = Path(a.db)
    if not db.exists():
        sys.exit(f"DB 없음: {db}")

    T = _import_track_outcomes()
    H = _horizons(T)
    cols = list(T.VAL_COLS)
    con = sqlite3.connect(db)
    ensure_schema(con)

    if not a.view_only:
        o = pd.read_sql_query(f"SELECT scan_date, ticker, origin, {', '.join(cols)} FROM outcomes", con)
        o["ticker"] = o.ticker.astype(str).str.zfill(6)
        # 전 행의 끝 바를 기록한다. 뷰는 끝 바 기록이 없는 값을 NULL 로 두므로(보수적),
        # 일부만 기록하면 멀쩡한 과거 값까지 사라진다.
        need = o
        lo = pd.to_datetime(need.scan_date.min(), format="%Y%m%d") - timedelta(days=7)
        hi = T._price_cutoff() if hasattr(T, "_price_cutoff") else \
            pd.Timestamp(datetime.now(KST).date() - timedelta(days=1))
        print(f"[flag] 가격 조회 {need.ticker.nunique()}종목 {lo:%Y-%m-%d}~{hi:%Y-%m-%d}", flush=True)
        prices = T._fetch_prices(need.ticker.unique().tolist(), lo.strftime("%Y-%m-%d"),
                                 hi.strftime("%Y-%m-%d"))
        # 당일 바가 섞이지 않게 한 번 더 자른다(원본 track_outcomes 에 절단이 없을 때 대비)
        prices = {k: v[pd.to_datetime(v.index) <= hi] for k, v in prices.items()}

        now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        wrows, frows, nodata = [], [], 0
        for r in need.itertuples(index=False):
            df = prices.get(r.ticker)
            if df is None or df.empty:
                nodata += 1
                continue
            i0 = T._entry_idx(df.index, r.scan_date)
            if i0 is None:
                continue
            rec = None
            for c in cols:
                v = getattr(r, c)
                if v is None or pd.isna(v):
                    continue
                j = i0 + H[c]
                if j >= len(df):
                    continue                     # 저장값은 있는데 지금 가격으로는 미도래 — 끝 바 미상
                end = pd.Timestamp(df.index[j]).strftime("%Y-%m-%d")
                wrows.append((r.scan_date, r.ticker, r.origin, c, end))
                lvl = INTRADAY_END_BARS.get(end)
                if lvl is None:
                    continue
                if rec is None:
                    rec = {**T._forward_returns(df, r.scan_date), **T._excursions(df, r.scan_date)}
                rv = rec.get(c)
                confirmed = rv is None or abs(rv - v) > TOL
                level = "confirmed" if confirmed else "suspect"
                excluded = int(confirmed if lvl == "confirmed" else True)
                frows.append((r.scan_date, r.ticker, r.origin, c, "intraday_bar", end, level,
                              float(v), rv, excluded, now))
        con.execute("DELETE FROM outcomes_window_end")
        con.executemany("INSERT OR REPLACE INTO outcomes_window_end VALUES (?,?,?,?,?)", wrows)
        con.execute("DELETE FROM outcomes_flags WHERE reason='intraday_bar'")
        con.executemany("INSERT OR REPLACE INTO outcomes_flags VALUES (?,?,?,?,?,?,?,?,?,?,?)", frows)
        print(f"[flag] 끝 바 기록 {len(wrows)} · 장중 표시 {len(frows)} · 가격 없음 {nodata}행")

    create_view(con, cols, un)
    con.commit()
    report(con, cols, un)
    con.close()


def report(con, cols, un):
    print(f"\n[report] 뷰 {VIEW} (UN_CUTOFF={un or '해제'}) — 열별 값 수")
    print(f"{'col':7} {'원본':>7} {'뷰':>7} {'장중제외':>8} {'UN제외':>7}")
    for c in cols:
        n0 = con.execute(f"SELECT COUNT({c}) FROM outcomes").fetchone()[0]
        n1 = con.execute(f"SELECT COUNT({c}) FROM {VIEW}").fetchone()[0]
        ni = con.execute("SELECT COUNT(*) FROM outcomes_flags WHERE col=? AND excluded=1", (c,)).fetchone()[0]
        print(f"{c:7} {n0:7d} {n1:7d} {ni:8d} {n0 - n1 - ni:7d}")
    print("\n[report] 장중 표시 — 끝 바·수준별 (excluded)")
    for row in con.execute("""SELECT end_bar, level, excluded, COUNT(*), SUM(origin='scan')
                              FROM outcomes_flags WHERE reason='intraday_bar'
                              GROUP BY 1,2,3 ORDER BY 1,2"""):
        print("  ", row)


if __name__ == "__main__":
    main()
