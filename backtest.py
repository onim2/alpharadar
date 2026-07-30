"""
AlphaRadar Backtest — 가중치·임계값 최적화 모듈

전제:
  alpharadar.py를 N영업일 이상 실행해서 data/scores_history.db의
  scan_results 테이블에 시그널이 누적되어 있어야 합니다.
  (권장: 최소 30영업일, 이상적으로 90영업일+)

기능:
  1) 종목별 N일 후 forward return 수집 (캐시)
  2) IC (Information Coefficient) — Spearman 상관
  3) 컴포넌트별 IC — T/S_text/D 각각 + 정형(T+D)만 분리
  4) 엔진별 성과 — engine_a / engine_b / both
  5) 등급(집중/주시/참고)별 평균 수익률·승률
  6) Decile 분석 — 점수 상위 10%가 하위 10%보다 잘하는지
  7) 가중치 그리드 서치 — 최적 (w_tech, w_text, w_cross)
  8) 임계값 추천 — 현재 75/65 기준과 데이터 기반 추천 비교

사용 예시:
  python backtest.py --report                          # 전체 리포트
  python backtest.py --grid-search                     # 가중치 최적화
  python backtest.py --horizons 5 10 20 --report       # 다중 holding 기간
  python backtest.py --start 20250901 --end 20251130   # 기간 제한
"""

import argparse
import logging
import pickle
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

CACHE_DIR     = Path("data/cache")
DB_PATH       = Path("data/scores_history.db")
RETURNS_CACHE = CACHE_DIR / "forward_returns.pkl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("backtest")


# ══════════════════════════════════════════════════════════════════════════════
# DB 로드
# ══════════════════════════════════════════════════════════════════════════════
@contextmanager
def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        yield con
    finally:
        con.close()


def load_signals(start: str = None, end: str = None) -> pd.DataFrame:
    """scan_results 테이블에서 시그널 로드"""
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"DB 없음: {DB_PATH}\n"
            "먼저 alpharadar.py를 며칠 돌려서 시그널을 누적하세요.\n"
            "빠른 시작: python backfill.py --days 30"
        )

    where, params = [], []
    if start:
        where.append("scan_date >= ?"); params.append(start)
    if end:
        where.append("scan_date <= ?"); params.append(end)
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""

    # source: 엔진별 분석 / cap_tier는 스키마 버전에 따라 없을 수 있어 제외
    query = f"""
        SELECT scan_date, ticker, name, sector,
               score_total, score_t, s_text, score_d,
               grade, source
        FROM scan_results
        {where_clause}
        ORDER BY scan_date, ticker
    """
    with _conn() as con:
        df = pd.read_sql_query(query, con, params=params)

    if df.empty:
        raise ValueError(
            "DB에서 시그널을 찾을 수 없습니다.\n"
            f"기간: {start or '전체'} ~ {end or '전체'}"
        )

    df["scan_date"] = df["scan_date"].astype(str)
    logger.info(
        f"시그널 로드: {len(df)}건 | "
        f"{df['scan_date'].min()} ~ {df['scan_date'].max()} | "
        f"종목 {df['ticker'].nunique()}개"
    )
    return df


# ══════════════════════════════════════════════════════════════════════════════
# Forward Return 수집
# ══════════════════════════════════════════════════════════════════════════════
def _load_returns_cache() -> dict:
    if RETURNS_CACHE.exists():
        with open(RETURNS_CACHE, "rb") as f:
            return pickle.load(f)
    return {}


def _save_returns_cache(cache: dict):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(RETURNS_CACHE, "wb") as f:
        pickle.dump(cache, f)


# ── 중앙 datastore(master.db) 연동 — KR 과거가격 우선 소스, FDR은 fallback ──
_DS = None
_MMAX = None
DATASTORE_DIR = "/Users/summer123/Project_2/data_store"

def _datastore():
    """data_store/dataio.py의 DataStore (없으면 None → 전량 FDR)."""
    global _DS
    if _DS is None:
        try:
            if DATASTORE_DIR not in sys.path:
                sys.path.insert(0, DATASTORE_DIR)
            from dataio import DataStore
            _DS = DataStore()
            logger.info("중앙 datastore 연결: master.db (KR 과거가격 우선)")
        except Exception as e:
            logger.info(f"중앙 datastore 미사용 → FDR: {e}")
            _DS = False
    return _DS or None


def _master_max_date(ds):
    """master price의 KR(벤더수정) 최신일 — 이 날짜를 넘는 요청은 FDR로."""
    global _MMAX
    if _MMAX is None:
        try:
            with sqlite3.connect(f"file:{ds.db}?mode=ro", uri=True) as c:
                r = c.execute(
                    "SELECT MAX(date) FROM price WHERE adj_src='vendor frozen'"
                ).fetchone()
            _MMAX = pd.Timestamp(r[0]) if r and r[0] else False
        except Exception:
            _MMAX = False
    return None if _MMAX is False else _MMAX


def _fetch_from_master(ticker: str, start: str, end: str):
    """master.db에서 KR 종목 수정종가(벤더 동결, FDR Close와 동일 의미) → df[['Close']].
    티커 매핑 '005930'→'A005930'. 요청 end가 master 커버리지를 넘으면 None(→FDR)."""
    ds = _datastore()
    if ds is None:
        return None
    mmax = _master_max_date(ds)
    if mmax is None or pd.Timestamp(end) > mmax:   # 최신 구간은 FDR이 더 최신
        return None
    mt = "A" + ticker if (ticker.isdigit() and len(ticker) == 6) else ticker
    try:
        w = ds.prices([mt], start=start, end=end, field="adj_close")
    except Exception:
        return None
    if w.empty or mt not in w.columns:
        return None
    s = w[[mt]].dropna()
    if s.empty:
        return None
    s.columns = ["Close"]
    return s


def _fetch_price_series(ticker: str, start: str, end: str):
    # 1) 중앙 datastore 우선 (KR 종목 과거 수정종가)
    s = _fetch_from_master(ticker, start, end)
    if s is not None:
        return s
    # 2) fallback: FinanceDataReader (벤치마크 지수·master 미수록·최신 구간)
    try:
        import FinanceDataReader as fdr
        df = fdr.DataReader(ticker, start, end)
        if df is None or df.empty:
            return None
        return df[["Close"]].copy()
    except Exception as e:
        logger.debug(f"가격 수집 실패 ({ticker}): {e}")
        return None


def collect_forward_returns(
    signals_df: pd.DataFrame,
    horizons: list = [5, 10, 20],
    benchmark: str = "KS11",
    use_cache: bool = True,
) -> pd.DataFrame:
    """각 (scan_date, ticker)에 대해 N영업일 후 forward return 수집"""
    cache = _load_returns_cache() if use_cache else {}

    tickers     = sorted(signals_df["ticker"].unique())
    min_date    = pd.to_datetime(signals_df["scan_date"].min())
    max_date    = pd.to_datetime(signals_df["scan_date"].max())
    fetch_start = (min_date - timedelta(days=10)).strftime("%Y-%m-%d")
    fetch_end   = (max_date + timedelta(days=max(horizons) + 10)).strftime("%Y-%m-%d")

    logger.info(
        f"가격 수집: {len(tickers)}종목 × {fetch_start} ~ {fetch_end} "
        f"(캐시 {len(cache)}건)"
    )

    bench_df = _fetch_price_series(benchmark, fetch_start, fetch_end) if benchmark else None

    price_cache = {}
    for ticker in tqdm(tickers, desc="가격 수집"):
        if ticker in cache:
            price_cache[ticker] = cache[ticker]
            continue
        df = _fetch_price_series(ticker, fetch_start, fetch_end)
        if df is not None:
            price_cache[ticker] = df
            cache[ticker] = df

    if use_cache:
        _save_returns_cache(cache)

    rows = []
    for _, sig in tqdm(signals_df.iterrows(), total=len(signals_df), desc="수익률 계산"):
        ticker    = sig["ticker"]
        scan_date = sig["scan_date"]
        if ticker not in price_cache:
            continue

        price_df  = price_cache[ticker]
        scan_dt   = pd.to_datetime(scan_date)
        entry_idx = price_df.index.searchsorted(scan_dt, side="right")
        if entry_idx >= len(price_df):
            continue
        entry_price = float(price_df["Close"].iloc[entry_idx])

        row = {"scan_date": scan_date, "ticker": ticker}
        for h in horizons:
            exit_idx = entry_idx + h
            if exit_idx >= len(price_df):
                row[f"ret_{h}d"] = np.nan
                continue
            exit_price = float(price_df["Close"].iloc[exit_idx])
            row[f"ret_{h}d"] = (exit_price / entry_price - 1) * 100

            if bench_df is not None:
                b_entry = bench_df.index.searchsorted(scan_dt, side="right")
                b_exit  = b_entry + h
                if b_exit < len(bench_df) and b_entry < len(bench_df):
                    b_ret = (
                        float(bench_df["Close"].iloc[b_exit])
                        / float(bench_df["Close"].iloc[b_entry]) - 1
                    ) * 100
                    row[f"ret_{h}d_excess"] = row[f"ret_{h}d"] - b_ret
        rows.append(row)

    returns_df = pd.DataFrame(rows)
    logger.info(f"수익률 매칭: {len(returns_df)}건")
    return returns_df


# ══════════════════════════════════════════════════════════════════════════════
# 메트릭
# ══════════════════════════════════════════════════════════════════════════════
def ic_analysis(scores: pd.Series, returns: pd.Series) -> dict:
    """IC (Information Coefficient) — Spearman 순위 상관"""
    from scipy.stats import spearmanr
    mask = scores.notna() & returns.notna()
    if mask.sum() < 10:
        return {"ic": np.nan, "n": int(mask.sum())}
    ic, p = spearmanr(scores[mask], returns[mask])
    return {"ic": round(float(ic), 4), "p_value": round(float(p), 4), "n": int(mask.sum())}


def component_ic(merged: pd.DataFrame, ret_col: str = "ret_10d") -> pd.DataFrame:
    """
    컴포넌트별 IC + 정형만(T+D) 분리.
    '정형만'은 뉴스 lookahead bias 없어서 백필링 결과 해석에 신뢰도 높음.
    """
    from scipy.stats import spearmanr
    rows = []
    structural = merged["score_t"].fillna(50) * 0.5 + merged["score_d"].fillna(50) * 0.5

    targets = [
        ("score_t",    "T  (기술)"),
        ("s_text",     "S_text (감성)"),
        ("score_d",    "D  (수급)"),
        ("score_total","S_total"),
        (None,         "정형만 (T+D)  ← lookahead bias 없음"),
    ]
    for col, label in targets:
        series = merged[col] if col else structural
        mask   = series.notna() & merged[ret_col].notna()
        if mask.sum() < 10:
            continue
        ic, p = spearmanr(series[mask], merged[ret_col][mask])
        rows.append({"컴포넌트": label, "IC": round(float(ic), 4),
                     "p값": round(float(p), 4), "N": int(mask.sum())})
    return pd.DataFrame(rows)


def ablation_ic(merged: pd.DataFrame, ret_col: str = "ret_10d") -> pd.DataFrame:
    """
    정형 vs 전체 IC 비교 — 비정형(s_text)의 실제 기여도 측정.

    발표 활용:
      - 정형(T+D) IC: lookahead bias 없음, 신뢰도 높음
      - 전체(T+S+D) IC: 비정형 포함
      - 차이 = s_text가 IC를 얼마나 높이는지 (또는 낮추는지)
      - 뉴스 API 없어도 '정형 단독 IC'로 발표 가능
    """
    from scipy.stats import spearmanr

    sub = merged[
        merged["score_t"].notna()
        & merged["s_text"].notna()
        & merged["score_d"].notna()
        & merged[ret_col].notna()
    ].copy()
    if len(sub) < 10:
        return pd.DataFrame()

    rows = []

    # 단계 1: 정형만 (T+D, 동일 가중)
    s_structural = sub["score_t"] * 0.50 + sub["score_d"] * 0.50
    ic1, p1 = spearmanr(s_structural, sub[ret_col])
    rows.append({
        "단계": "① 정형만 (T+D)",
        "구성":  "T×0.50 + D×0.50",
        "IC":   round(float(ic1), 4),
        "p값":  round(float(p1),  4),
        "N":    len(sub),
        "비고": "★ lookahead bias 없음",
    })

    # 단계 2: 현재 설정 (T+S+D, 0.30/0.40/0.30)
    s_full = sub["score_t"] * 0.30 + sub["s_text"] * 0.40 + sub["score_d"] * 0.30
    ic2, p2 = spearmanr(s_full, sub[ret_col])
    rows.append({
        "단계": "② 전체 (T+S_text+D)",
        "구성":  "T×0.30 + S×0.40 + D×0.30",
        "IC":   round(float(ic2), 4),
        "p값":  round(float(p2),  4),
        "N":    len(sub),
        "비고": "현재 설정",
    })

    df = pd.DataFrame(rows)

    # 비정형 기여도
    delta = ic2 - ic1
    pct   = (delta / abs(ic1) * 100) if ic1 != 0 else 0
    df.attrs["delta"] = delta
    df.attrs["pct"]   = pct
    return df


def grade_performance(merged: pd.DataFrame, ret_col: str = "ret_10d") -> pd.DataFrame:
    """등급별 평균 수익률·승률 (+ 벤치마크 대비 초과수익)"""
    ex_col = f"{ret_col}_excess"
    has_ex = ex_col in merged.columns
    rows = []
    for grade in ["집중", "주시", "참고"]:
        g   = merged[merged["grade"] == grade]
        sub = g[ret_col].dropna()
        if len(sub) == 0:
            continue
        row = {
            "등급":      grade,
            "N":         len(sub),
            "평균(%)":   round(sub.mean(), 2),
            "중간값(%)": round(sub.median(), 2),
            "승률(%)":   round((sub > 0).mean() * 100, 1),
            "Sharpe":    round(sub.mean() / sub.std(), 3) if sub.std() > 0 else 0,
        }
        if has_ex:
            ex = g[ex_col].dropna()
            row["평균초과(%)"] = round(ex.mean(), 2) if len(ex) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def engine_performance(merged: pd.DataFrame, ret_col: str = "ret_10d") -> pd.DataFrame:
    """엔진별 수익률 — engine_a / engine_b / both"""
    if "source" not in merged.columns:
        return pd.DataFrame()
    ex_col = f"{ret_col}_excess"
    has_ex = ex_col in merged.columns
    rows = []
    for src in ["engine_a", "engine_b", "both"]:
        g   = merged[merged["source"] == src]
        sub = g[ret_col].dropna()
        if len(sub) == 0:
            continue
        row = {
            "엔진":    src,
            "N":       len(sub),
            "평균(%)": round(sub.mean(), 2),
            "승률(%)": round((sub > 0).mean() * 100, 1),
            "Sharpe":  round(sub.mean() / sub.std(), 3) if sub.std() > 0 else 0,
        }
        if has_ex:
            ex = g[ex_col].dropna()
            row["평균초과(%)"] = round(ex.mean(), 2) if len(ex) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def decile_analysis(merged: pd.DataFrame, ret_col: str = "ret_10d") -> pd.DataFrame:
    """점수 10분위 분석 — 단조성 확인"""
    ex_col = f"{ret_col}_excess"
    cols   = ["score_total", ret_col] + ([ex_col] if ex_col in merged.columns else [])
    sub = merged[cols].dropna(subset=["score_total", ret_col])
    if len(sub) < 50:
        return pd.DataFrame()
    sub = sub.copy()
    sub["decile"] = pd.qcut(sub["score_total"], 10, labels=False, duplicates="drop") + 1
    base = sub.groupby("decile")[ret_col].agg(["count", "mean", "median"]).round(2)
    if ex_col in sub.columns:
        base["excess_mean"] = sub.groupby("decile")[ex_col].mean().round(2)
    return base.reset_index()


def threshold_search(merged: pd.DataFrame, ret_col: str = "ret_10d") -> dict:
    """임계값 추천 — 데이터 기반 vs 현재 설정(75/65) 비교"""
    sub = merged[["score_total", ret_col]].dropna()
    if len(sub) < 50:
        return {}
    rec = {}
    for pct, label in [(0.10, "데이터기반 집중(상위10%)"), (0.30, "데이터기반 주시(상위30%)")]:
        cutoff = float(sub["score_total"].quantile(1 - pct))
        top    = sub[sub["score_total"] >= cutoff]
        rec[label] = {
            "추천_컷오프":    round(cutoff, 1),
            "N":              len(top),
            "평균_수익률(%)": round(top[ret_col].mean(), 2),
            "승률(%)":        round((top[ret_col] > 0).mean() * 100, 1),
        }
    for cutoff, label in [(75, "현재 집중(≥75)"), (65, "현재 주시(≥65)")]:
        top = sub[sub["score_total"] >= cutoff]
        if len(top) > 0:
            rec[label] = {
                "추천_컷오프":    cutoff,
                "N":              len(top),
                "평균_수익률(%)": round(top[ret_col].mean(), 2),
                "승률(%)":        round((top[ret_col] > 0).mean() * 100, 1),
            }
    return rec


# ══════════════════════════════════════════════════════════════════════════════
# 그리드 서치
# ══════════════════════════════════════════════════════════════════════════════
def grid_search(
    merged: pd.DataFrame,
    ret_col: str = "ret_10d",
    step: float = 0.05,
    objective: str = "ic",
) -> pd.DataFrame:
    from scipy.stats import spearmanr
    weights = np.round(np.arange(0, 1 + step, step), 2)
    valid   = merged[
        merged["score_t"].notna() & merged["s_text"].notna()
        & merged["score_d"].notna() & merged[ret_col].notna()
    ].copy()

    if len(valid) < 30:
        logger.warning(f"표본 부족 ({len(valid)}건) — 신뢰도 낮음")
    logger.info(f"그리드 서치: {len(valid)}건, step={step}, 목적={objective}")

    results = []
    for wt, wx, wc in product(weights, repeat=3):
        if abs(wt + wx + wc - 1.0) > 1e-6:
            continue
        scores = valid["score_t"] * wt + valid["s_text"] * wx + valid["score_d"] * wc
        rets   = valid[ret_col]
        try:
            if objective == "ic":
                ic, _ = spearmanr(scores, rets)
                metric = float(ic) if not np.isnan(ic) else -999
            elif objective == "top_decile":
                metric = float(rets[scores >= scores.quantile(0.9)].mean())
            elif objective == "long_short":
                metric = float(
                    rets[scores >= scores.quantile(0.9)].mean()
                    - rets[scores <= scores.quantile(0.1)].mean()
                )
            else:
                raise ValueError(f"unknown objective: {objective}")
        except Exception:
            metric = -999
        results.append({"w_tech": wt, "w_text": wx, "w_cross": wc, objective: round(metric, 4)})

    return pd.DataFrame(results).sort_values(objective, ascending=False)


# ══════════════════════════════════════════════════════════════════════════════
# 리포트
# ══════════════════════════════════════════════════════════════════════════════
def _merge_signals_returns(signals_df: pd.DataFrame, returns_df: pd.DataFrame):
    """
    P0-3: (scan_date, ticker) 중복으로 인한 cartesian join·과대계상 방지.

    반환:
      merged_full   — 엔진(source)별 행 보존 (엔진별 성과용)
      merged_unique — 종목·일자당 1건(최고 score_total)으로 유일화 (포트폴리오 지표용)
    """
    # returns_df는 (scan_date,ticker)당 값이 동일 → 중복 제거해 join 폭증 차단
    returns_u   = returns_df.drop_duplicates(subset=["scan_date", "ticker"])
    merged_full = signals_df.merge(returns_u, on=["scan_date", "ticker"], how="inner")
    merged_unique = (
        merged_full.sort_values("score_total", ascending=False)
                   .drop_duplicates(subset=["scan_date", "ticker"])
                   .reset_index(drop=True)
    )
    return merged_full, merged_unique


def print_report(signals_df: pd.DataFrame, returns_df: pd.DataFrame, horizons: list):
    merged_full, merged = _merge_signals_returns(signals_df, returns_df)
    logger.info(f"매칭: full={len(merged_full)}건 → 유니크(종목·일자)={len(merged)}건")
    has_ex = any(f"ret_{h}d_excess" in merged.columns for h in horizons)

    print("\n" + "═" * 70)
    print("AlphaRadar Backtest Report")
    print("═" * 70)
    print(f"기간   : {signals_df['scan_date'].min()} ~ {signals_df['scan_date'].max()}")
    print(f"시그널 : {len(signals_df)}건  |  유니크 매칭: {len(merged)}건  |  종목: {merged['ticker'].nunique()}개")
    print(f"주의   : 수익률은 원수익률(raw). 하락장 편향을 피하려면 '초과(excess, vs 벤치마크)'로 판단." if has_ex
          else "주의   : 벤치마크 미설정 — 초과수익 미계산 (--benchmark 지정 권장)")
    if "source" in merged_full.columns:
        src_cnt = merged_full["source"].value_counts().to_dict()
        print(f"엔진(중복포함) : " + " / ".join(f"{k}={v}" for k, v in src_cnt.items()))

    for h in horizons:
        ret_col = f"ret_{h}d"
        if ret_col not in merged.columns:
            continue

        print(f"\n{'─'*70}\nHolding {h}일\n{'─'*70}")

        # 총점 IC
        ic_res  = ic_analysis(merged["score_total"], merged[ret_col])
        verdict = (
            "★★★ 강한 신호"  if abs(ic_res.get("ic") or 0) >= 0.10
            else "★★ 유의미" if abs(ic_res.get("ic") or 0) >= 0.05
            else "★ 약함"    if abs(ic_res.get("ic") or 0) >= 0.02
            else "신호 없음"
        )
        print(f"\n[총점 IC]  IC={ic_res['ic']}  p={ic_res.get('p_value')}  N={ic_res['n']}  → {verdict}")

        # P0-1: 초과수익(벤치마크 대비) IC — 하락장 편향 제거한 진짜 종목변별력
        ex_col = f"ret_{h}d_excess"
        if ex_col in merged.columns:
            ic_ex = ic_analysis(merged["score_total"], merged[ex_col])
            ver_ex = (
                "★★★ 강한 신호"  if abs(ic_ex.get("ic") or 0) >= 0.10
                else "★★ 유의미" if abs(ic_ex.get("ic") or 0) >= 0.05
                else "★ 약함"    if abs(ic_ex.get("ic") or 0) >= 0.02
                else "신호 없음"
            )
            print(f"[초과 IC]  IC={ic_ex['ic']}  p={ic_ex.get('p_value')}  N={ic_ex['n']}  → {ver_ex}  ◀ 벤치마크 대비(권장 지표)")

        # 컴포넌트별 IC (정형만 분리 포함)
        comp = component_ic(merged, ret_col)
        if not comp.empty:
            print("\n[컴포넌트별 IC]")
            print(comp.to_string(index=False))

        # ── 정형 vs 전체 IC — 비정형 기여도 (발표 핵심 슬라이드) ──
        abl = ablation_ic(merged, ret_col)
        if not abl.empty:
            print("\n[정형 vs 전체 IC — 비정형 기여도]")
            print(abl[["단계","구성","IC","p값","N","비고"]].to_string(index=False))
            delta = abl.attrs.get("delta", 0)
            pct   = abl.attrs.get("pct",   0)
            sign  = "+" if delta >= 0 else ""
            print(f"\n  비정형(s_text) 기여: {sign}{delta:.4f}  ({sign}{pct:.1f}%)")
            if abs(delta) < 0.005:
                print("  → 기여 미미 — 정형 신호 단독 IC로 발표 권장")
            elif delta > 0:
                print("  → 비정형이 IC를 높임 — 정형+비정형 통합 모델 유효")
            else:
                print("  → 비정형이 IC를 낮춤 — s_text 가중치 축소 검토 필요")

        # 엔진별 성과 (source 보존 위해 중복포함 merged_full 사용)
        eng = engine_performance(merged_full, ret_col)
        if not eng.empty:
            print("\n[엔진별 성과]")
            print(eng.to_string(index=False))

        # 등급별 성과
        grade_df = grade_performance(merged, ret_col)
        if not grade_df.empty:
            print("\n[등급별 성과]")
            print(grade_df.to_string(index=False))

        # Decile 분석
        dec = decile_analysis(merged, ret_col)
        if not dec.empty:
            print("\n[Decile 분석]  (1=하위 10%, 10=상위 10%)")
            print(dec.to_string(index=False))

        # 임계값 추천
        thr = threshold_search(merged, ret_col)
        if thr:
            print("\n[임계값 — 데이터기반 추천 vs 현재 설정]")
            for k, v in thr.items():
                print(f"  {k}: {v}")


def print_grid_search(
    signals_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    horizon: int = 10,
    objective: str = "ic",
    top_n: int = 10,
):
    # P0-3: 유니크(종목·일자)로 유일화해 중복 종목 과대계상 방지
    _, merged = _merge_signals_returns(signals_df, returns_df)
    ret_col = f"ret_{horizon}d"
    if ret_col not in merged.columns:
        logger.error(f"{ret_col} 컬럼 없음 — --horizons에 {horizon} 포함 확인")
        return

    print("\n" + "═" * 70)
    print(f"가중치 그리드 서치  (목적: {objective}, holding: {horizon}일)")
    print("═" * 70)

    grid = grid_search(merged, ret_col=ret_col, objective=objective)
    if grid.empty:
        print("결과 없음"); return

    print(f"\nTop {top_n} 조합:")
    print(grid.head(top_n).to_string(index=False))

    best = grid.iloc[0]
    print(f"\n── 최적 가중치 ──")
    print(f"  w_tech  = {best['w_tech']}")
    print(f"  w_text  = {best['w_text']}")
    print(f"  w_cross = {best['w_cross']}")
    print(f"  {objective} = {best[objective]}")

    current = grid[
        (grid["w_tech"] == 0.30) & (grid["w_text"] == 0.40) & (grid["w_cross"] == 0.30)
    ]
    if not current.empty:
        cur_val = current.iloc[0][objective]
        print(f"\n현재 설정(0.30/0.40/0.30) {objective} = {cur_val}")
        print(f"개선폭: {best[objective] - cur_val:+.4f}")

    print(f"\n── config.yaml 패치 제안 ──")
    print("scoring:")
    print(f"  w_tech:  {best['w_tech']}")
    print(f"  w_text:  {best['w_text']}")
    print(f"  w_cross: {best['w_cross']}")


# ══════════════════════════════════════════════════════════════════════════════
# Walk-forward 검증 (P0-2) — 그리드서치 과최적화(in-sample) 여부 확인
# ══════════════════════════════════════════════════════════════════════════════
def _weighted_ic(df: pd.DataFrame, w: tuple, ret_col: str) -> float:
    """가중치 w=(w_tech,w_text,w_cross)로 합성점수 만들어 IC 산출"""
    from scipy.stats import spearmanr
    s = df["score_t"] * w[0] + df["s_text"] * w[1] + df["score_d"] * w[2]
    m = s.notna() & df[ret_col].notna()
    if m.sum() < 10:
        return np.nan
    return float(spearmanr(s[m], df[ret_col][m])[0])


def walk_forward(
    signals_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    horizon: int = 10,
    objective: str = "ic",
    n_splits: int = 3,
    step: float = 0.05,
) -> pd.DataFrame:
    """
    시간순 확장창(expanding window) walk-forward.
    각 폴드: 과거구간(train)에서 최적 가중치 탐색 → 미래구간(test)에서 검증.
    IS(train) 최적 가중치가 OOS(test)에서도 유지되는지가 핵심.
    """
    ret_col = f"ret_{horizon}d"
    ex_col  = f"{ret_col}_excess"
    _, merged = _merge_signals_returns(signals_df, returns_df)
    if ret_col not in merged.columns:
        logger.error(f"{ret_col} 컬럼 없음 — --horizons에 {horizon} 포함 확인")
        return pd.DataFrame()

    merged = merged[
        merged["score_t"].notna() & merged["s_text"].notna()
        & merged["score_d"].notna() & merged[ret_col].notna()
    ].copy()

    dates = sorted(merged["scan_date"].unique())
    if len(dates) < (n_splits + 1) * 2:
        logger.warning(f"스캔일 부족({len(dates)}일, 폴드 {n_splits}) — 폴드 신뢰도 낮음")

    # (n_splits+1) 등분: 폴드 i의 train=[0..bound_i), test=[bound_i..bound_{i+1})
    bounds   = [int(len(dates) * k / (n_splits + 1)) for k in range(n_splits + 2)]
    baseline = (0.30, 0.40, 0.30)
    has_ex   = ex_col in merged.columns

    rows = []
    for i in range(1, n_splits + 1):
        tr_dates = set(dates[:bounds[i]])
        te_dates = set(dates[bounds[i]:bounds[i + 1]])
        if not tr_dates or not te_dates:
            continue
        tr = merged[merged["scan_date"].isin(tr_dates)]
        te = merged[merged["scan_date"].isin(te_dates)]
        if len(tr) < 30 or len(te) < 10:
            logger.warning(f"폴드{i} 표본부족(train={len(tr)}, test={len(te)}) — 건너뜀")
            continue

        grid = grid_search(tr, ret_col=ret_col, objective=objective, step=step)
        if grid.empty:
            continue
        best = grid.iloc[0]
        w    = (float(best["w_tech"]), float(best["w_text"]), float(best["w_cross"]))

        row = {
            "fold":         i,
            "train_N":      len(tr),
            "test_N":       len(te),
            "test_기간":    f"{min(te_dates)}~{max(te_dates)}",
            "최적W(IS)":    f"{w[0]:.2f}/{w[1]:.2f}/{w[2]:.2f}",
            "IS_IC":        round(float(best[objective]), 4),
            "OOS_IC(최적)": round(_weighted_ic(te, w, ret_col), 4),
            "OOS_IC(기존)": round(_weighted_ic(te, baseline, ret_col), 4),
        }
        if has_ex:
            row["OOS초과(최적)"] = round(_weighted_ic(te, w, ex_col), 4)
            row["OOS초과(기존)"] = round(_weighted_ic(te, baseline, ex_col), 4)
        rows.append(row)

    return pd.DataFrame(rows)


def print_walk_forward(
    signals_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    horizon: int = 10,
    objective: str = "ic",
    n_splits: int = 3,
):
    print("\n" + "═" * 70)
    print(f"Walk-forward 검증  (holding: {horizon}일, 목적: {objective}, 폴드: {n_splits})")
    print("═" * 70)
    print("각 폴드: 과거(train)에서 최적 가중치 탐색 → 미래(test) OOS 성능 측정")
    print("핵심: IS 최적 가중치가 OOS에서 '기존 설정'보다 나은가? (아니면 과최적화)\n")

    wf = walk_forward(signals_df, returns_df, horizon=horizon,
                      objective=objective, n_splits=n_splits)
    if wf.empty:
        print("결과 없음 — 표본/스캔일 부족"); return

    print(wf.to_string(index=False))

    opt  = wf["OOS_IC(최적)"].dropna()
    base = wf["OOS_IC(기존)"].dropna()
    is_ic = wf["IS_IC"].dropna()
    m_opt, m_base, m_is = opt.mean(), base.mean(), is_ic.mean()

    print(f"\n── 요약 (평균) ──")
    print(f"  IS 최적 IC (in-sample)       : {m_is:+.4f}")
    print(f"  OOS 최적W IC (out-of-sample) : {m_opt:+.4f}")
    print(f"  OOS 기존설정 IC              : {m_base:+.4f}")
    print(f"  IS→OOS 낙폭(최적W)           : {m_opt - m_is:+.4f}")
    print(f"  최적W가 기존 대비 OOS 우위    : {m_opt - m_base:+.4f}")

    # 판정
    wins = int((wf["OOS_IC(최적)"] > wf["OOS_IC(기존)"]).sum())
    print(f"\n── 판정 ──")
    if m_opt <= 0:
        print("  ❌ 과최적화 — IS 최적 가중치의 OOS IC가 0 이하. 일반화 실패.")
    elif m_opt <= m_base + 0.005:
        print(f"  ⚠️ 과최적화 의심 — OOS에서 최적W가 기존 대비 우위 없음({wins}/{len(wf)} 폴드만 우세).")
    else:
        print(f"  ✅ 일부 일반화 — OOS에서도 최적W가 기존 대비 우세({wins}/{len(wf)} 폴드).")
    print("  ※ 단일 국면·소표본이면 결과는 잠정적. 국면 다양화 후 재검증 권장.")


# ══════════════════════════════════════════════════════════════════════════════
# 진입점
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="AlphaRadar 백테스트")
    parser.add_argument("--start",        type=str,            default=None)
    parser.add_argument("--end",          type=str,            default=None)
    parser.add_argument("--horizons",     type=int, nargs="+", default=[5, 10, 20])
    parser.add_argument("--report",       action="store_true")
    parser.add_argument("--grid-search",  action="store_true")
    parser.add_argument("--walk-forward", action="store_true",
                        help="P0-2: 시간순 train/test 분할로 그리드서치 과최적화 검증")
    parser.add_argument("--wf-splits",    type=int,            default=3,
                        help="walk-forward 폴드 수 (기본 3)")
    parser.add_argument("--objective",    type=str,            default="ic",
                        choices=["ic", "top_decile", "long_short"])
    parser.add_argument("--grid-horizon", type=int,            default=10)
    parser.add_argument("--no-cache",     action="store_true")
    parser.add_argument("--benchmark",    type=str,            default="KS11")
    args = parser.parse_args()

    if not args.report and not args.grid_search and not args.walk_forward:
        args.report = True

    try:
        signals_df = load_signals(start=args.start, end=args.end)
    except (FileNotFoundError, ValueError) as e:
        logger.error(str(e)); sys.exit(1)

    returns_df = collect_forward_returns(
        signals_df,
        horizons=args.horizons,
        benchmark=args.benchmark or None,
        use_cache=not args.no_cache,
    )

    if args.report:
        print_report(signals_df, returns_df, args.horizons)
    if args.grid_search:
        print_grid_search(signals_df, returns_df,
                          horizon=args.grid_horizon, objective=args.objective)
    if args.walk_forward:
        print_walk_forward(signals_df, returns_df,
                           horizon=args.grid_horizon, objective=args.objective,
                           n_splits=args.wf_splits)

    print("\n" + "═" * 70)


if __name__ == "__main__":
    main()
