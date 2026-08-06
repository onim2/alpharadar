#!/usr/bin/env python3
"""
weekly_report.py — AlphaRadar 추천 추적 성적표 (S2)

목적: scores_history.db의 전체 스캔 기록(발송 여부 무관)에 대해
      익일 시가 진입 기준 forward return을 계산하고, 시장지수 차감
      초과수익으로 신호 품질을 판정하는 주간 보고서를 생성한다.

설계 원칙:
  - DB는 READ-ONLY. 결과는 reports/ 아래 md + csv 파일로만 출력.
  - 모집단 = scan_results 전체 (sent_history 아님 — 6/29 이후 발송 실패 왜곡 회피).
  - 진입가 규약 = 시그널 익일 시가. fwdN = (진입 후 N번째 거래일 종가 / 진입시가) - 1.
  - 초과수익 = 종목 fwdN − 소속시장 지수 fwdN (동일 규약).
  - 미성숙 표본(창 미완성)은 제외하고 n을 명시.
  - 2026-07-06 업그레이드 전/후 분리 집계. 폭락장 국면 태그 병기.

사용법:
  uv add pykrx pandas   # 최초 1회
  uv run python weekly_report.py                      # 전체
  uv run python weekly_report.py --since 2026-06-01   # 기간 제한
  uv run python weekly_report.py --limit-tickers 30   # 드라이런(소표본)
"""

from __future__ import annotations
import argparse
import pickle
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pykrx import stock

# ----------------------------------------------------------------------
# 설정
# ----------------------------------------------------------------------
DB_PATH = Path("data/scores_history.db")
CACHE_PATH = Path("data/cache/price_cache_wr.pkl")
REPORT_DIR = Path("reports")
UPGRADE_DATE = "2026-07-06"          # 물밑포착 개선(과열 배제 필터 등) 적용일
HORIZONS = [1, 5, 10, 20]            # 거래일 기준
INDEX_CODE = {"KOSPI": "1001", "KOSDAQ": "2001"}
FETCH_SLEEP = 0.25                   # pykrx 예의상 대기(초)

# ----------------------------------------------------------------------
# 데이터 로드
# ----------------------------------------------------------------------

def load_scans(db: Path, since: str | None) -> pd.DataFrame:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)  # 읽기 전용
    q = """SELECT scan_date, ticker, name, cap_tier, grade, source,
                  score_total, n_accel, v_surge
           FROM scan_results"""
    df = pd.read_sql(q, con)
    con.close()
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    df["scan_date"] = df["scan_date"].str[:10]
    if since:
        df = df[df["scan_date"] >= since]
    # 같은 날 같은 종목 중복 스캔(하루 2회) → 첫 기록만
    df = df.sort_values(["scan_date", "ticker"]).drop_duplicates(
        ["scan_date", "ticker"], keep="first")
    return df.reset_index(drop=True)


def market_membership(any_date: str) -> set[str]:
    """KOSPI 티커 집합. 실패 시 빈 set → 전 종목 KOSDAQ 취급 대신 KOSPI 폴백."""
    d = any_date.replace("-", "")
    try:
        return set(stock.get_market_ticker_list(d, market="KOSPI"))
    except Exception as e:
        print(f"[warn] KOSPI 멤버십 조회 실패({e}) — 전 종목 KOSPI 지수 차감으로 폴백")
        return set()


def load_cache() -> dict:
    if CACHE_PATH.exists():
        with open(CACHE_PATH, "rb") as f:
            return pickle.load(f)
    return {}


def save_cache(cache: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, "wb") as f:
        pickle.dump(cache, f)


def fetch_ohlcv(ticker: str, start: str, end: str, cache: dict,
                is_index: bool = False) -> pd.DataFrame | None:
    """일봉(시가/종가) — 캐시 우선. index=True면 지수."""
    key = ("IDX" if is_index else "STK", ticker, start, end)
    if key in cache:
        return cache[key]
    s, e = start.replace("-", ""), end.replace("-", "")
    try:
        if is_index:
            df = stock.get_index_ohlcv_by_date(s, e, ticker)
        else:
            df = stock.get_market_ohlcv_by_date(s, e, ticker)
        time.sleep(FETCH_SLEEP)
    except Exception as ex:
        print(f"[warn] 시세 조회 실패 {ticker}: {ex}")
        cache[key] = None
        return None
    if df is None or df.empty:
        cache[key] = None
        return None
    df = df.rename(columns={"시가": "open", "종가": "close"})[["open", "close"]]
    df.index = pd.to_datetime(df.index)
    cache[key] = df
    return df


# ----------------------------------------------------------------------
# forward return (익일 시가 진입 규약)
# ----------------------------------------------------------------------

def fwd_returns(px: pd.DataFrame, scan_date: str,
                horizons: list[int]) -> dict[int, float | None]:
    """entry = scan_date 이후 첫 거래일 시가.
    fwdN = close[entry_idx + N - 1] / entry_open - 1. 창 미완성이면 None."""
    out: dict[int, float | None] = {h: None for h in horizons}
    d = pd.Timestamp(scan_date)
    after = px.index[px.index > d]
    if len(after) == 0:
        return out
    e_idx = px.index.get_loc(after[0])
    entry = px.iloc[e_idx]["open"]
    if not entry or entry <= 0:
        return out
    for h in horizons:
        j = e_idx + h - 1
        if j < len(px):
            out[h] = px.iloc[j]["close"] / entry - 1.0
    return out


def regime_tag(kospi: pd.DataFrame, scan_date: str) -> str:
    """스캔일 시점 KOSPI 20일 수익률로 국면 태그(간이)."""
    d = pd.Timestamp(scan_date)
    hist = kospi[kospi.index <= d]["close"]
    if len(hist) < 21:
        return "unknown"
    r20 = hist.iloc[-1] / hist.iloc[-21] - 1.0
    if r20 <= -0.10:
        return "crash"       # 폭락 국면 (이번 창의 스트레스 꼬리표)
    if r20 <= -0.03:
        return "down"
    if r20 >= 0.05:
        return "up"
    return "flat"


# ----------------------------------------------------------------------
# 집계
# ----------------------------------------------------------------------

def agg_block(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    rows = []
    for h in HORIZONS:
        col = f"ex{h}"
        sub = df.dropna(subset=[col])
        if sub.empty:
            continue
        g = sub.groupby(by, dropna=False)[col]
        blk = pd.DataFrame({
            "horizon": f"fwd{h}",
            "n": g.size(),
            "median": g.median().round(4),
            "mean": g.mean().round(4),
            "q25": g.quantile(0.25).round(4),
            "q75": g.quantile(0.75).round(4),
            "win_rate": g.apply(lambda s: (s > 0).mean()).round(3),
        }).reset_index()
        rows.append(blk)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def to_md(df: pd.DataFrame, title: str) -> str:
    if df.empty:
        return f"### {title}\n\n(표본 없음)\n"
    return f"### {title}\n\n{df.to_markdown(index=False)}\n"


# ----------------------------------------------------------------------
# 메인
# ----------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None, help="YYYY-MM-DD 이후 스캔만")
    ap.add_argument("--limit-tickers", type=int, default=None,
                    help="드라이런용: 종목 수 제한")
    args = ap.parse_args()

    scans = load_scans(DB_PATH, args.since)
    if scans.empty:
        print("scan_results가 비어 있음"); return 1
    print(f"스캔 기록 {len(scans)}건 / 종목 {scans['ticker'].nunique()}개 / "
          f"{scans['scan_date'].min()} ~ {scans['scan_date'].max()}")

    tickers = sorted(scans["ticker"].unique())
    if args.limit_tickers:
        tickers = tickers[: args.limit_tickers]
        scans = scans[scans["ticker"].isin(tickers)]

    start = (pd.Timestamp(scans["scan_date"].min()) -
             timedelta(days=7)).strftime("%Y-%m-%d")
    end = datetime.now().strftime("%Y-%m-%d")

    cache = load_cache()
    kospi_set = market_membership(scans["scan_date"].max())

    idx_px = {m: fetch_ohlcv(c, start, end, cache, is_index=True)
              for m, c in INDEX_CODE.items()}
    if idx_px["KOSPI"] is None:
        print("KOSPI 지수 조회 실패 — 중단"); save_cache(cache); return 1

    recs = []
    for i, tk in enumerate(tickers, 1):
        px = fetch_ohlcv(tk, start, end, cache)
        if i % 25 == 0:
            print(f"  시세 수집 {i}/{len(tickers)}"); save_cache(cache)
        if px is None:
            continue
        mkt = "KOSPI" if (not kospi_set or tk in kospi_set) else "KOSDAQ"
        bench = idx_px[mkt] if idx_px[mkt] is not None else idx_px["KOSPI"]
        for _, row in scans[scans["ticker"] == tk].iterrows():
            f_stk = fwd_returns(px, row["scan_date"], HORIZONS)
            f_idx = fwd_returns(bench, row["scan_date"], HORIZONS)
            rec = row.to_dict() | {"market": mkt}
            for h in HORIZONS:
                rec[f"fwd{h}"] = f_stk[h]
                rec[f"ex{h}"] = (None if f_stk[h] is None or f_idx[h] is None
                                 else f_stk[h] - f_idx[h])
            recs.append(rec)
    save_cache(cache)

    res = pd.DataFrame(recs)
    res["period"] = res["scan_date"].apply(
        lambda d: "post_upgrade" if d >= UPGRADE_DATE else "pre_upgrade")
    res["regime"] = res["scan_date"].apply(
        lambda d: regime_tag(idx_px["KOSPI"], d))

    REPORT_DIR.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    csv_path = REPORT_DIR / f"tracking_{stamp}.csv"
    res.to_csv(csv_path, index=False, encoding="utf-8-sig")

    md = [f"# AlphaRadar 추천 추적 성적표 — {stamp}",
          "",
          f"- 모집단: scan_results 전체 {len(res)}건 (발송 여부 무관)",
          f"- 규약: 익일 시가 진입, fwdN = N번째 거래일 종가 대비, "
          f"exN = 소속시장 지수 차감 초과수익",
          f"- 업그레이드 기준일: {UPGRADE_DATE}",
          "- 주의: 이번 창은 폭락장 스트레스 국면 표본 포함 — "
          "regime 열 crash 태그 확인",
          ""]
    md.append(to_md(agg_block(res, ["grade"]), "등급별 초과수익"))
    md.append(to_md(agg_block(res, ["period", "grade"]),
                    "업그레이드 전/후 × 등급"))
    md.append(to_md(agg_block(res, ["source"]), "엔진(source)별"))
    md.append(to_md(agg_block(res, ["regime"]), "시장 국면별"))

    # n<30 경고
    mature10 = res.dropna(subset=["ex10"])
    post_n = len(mature10[mature10["period"] == "post_upgrade"])
    md.append(f"\n> post_upgrade fwd10 성숙 표본 n={post_n}"
              + (" — n<30, 본판정 유보 권장" if post_n < 30 else ""))

    md_path = REPORT_DIR / f"weekly_report_{stamp}.md"
    md_path.write_text("\n".join(md), encoding="utf-8")
    print(f"\n완료: {md_path}\n      {csv_path}")
    print(mature10.groupby("grade")["ex10"].median().round(4).to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
