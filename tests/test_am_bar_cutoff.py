"""아침 회차 시세 컷 — 회차 날짜 '미만'의 바만 쓴다(실행 시각과 무관).

2026-10-08 아침 런이 10:07(장중)에 돌아 10/8 미완성 바로 등락률·RSI·BB·이격도를 계산했다.
"""

import numpy as np
import pandas as pd
import pytest

import alpharadar as ar

UCFG = {"min_price": 500, "min_market_cap": 200_000_000_000,
        "cap_tier": {"large_threshold": 5_000_000_000_000, "mid_threshold": 500_000_000_000},
        "investor_exclude_today": True}
INFO = {"name": "테스트", "market": "KOSPI", "sector": "기타", "market_cap": 600_000_000_000}
HOLIDAYS = {pd.Timestamp("2026-10-03"), pd.Timestamp("2026-10-05"), pd.Timestamp("2026-10-09")}


def bars_until(last):
    """2026-04-01 부터 last 까지 영업일(휴장 제외) 바. 종가는 날마다 다르게, 마지막 바는 '장중'."""
    idx = [d for d in pd.bdate_range("2026-04-01", last) if d not in HOLIDAYS]
    close = np.arange(len(idx), dtype=float) * 10 + 10_000
    df = pd.DataFrame({"Open": close, "High": close, "Low": close, "Close": close,
                       "Volume": np.full(len(idx), 1_000_000.0)}, index=pd.DatetimeIndex(idx))
    df["Change"] = df["Close"].pct_change().fillna(0)
    return df


@pytest.fixture
def fdr_ignoring_end(monkeypatch):
    """종료일을 무시하고 '지금'까지의 바(당일 장중 바 포함)를 주는 소스. 받은 종료일은 기록한다."""
    state = {"last": None, "ends": []}
    import FinanceDataReader as fdr
    def reader(ticker, start, end=None):
        state["ends"].append(end)
        return bars_until(state["last"]).copy()
    monkeypatch.setattr(fdr, "DataReader", reader)
    monkeypatch.setattr(ar, "_get_investor_data", lambda *a, **k: (3, 0, 0, 0, 0))
    ar._PRECOMP_STATS.clear(); ar._PRECOMP_ERR_SAMPLE.clear()
    return state


def expected(last_bar):
    df = bars_until(last_bar)
    return float(df["Close"].iloc[-1]), float(df["Change"].iloc[-1] * 100)


def test_1008_morning_run_at_1007_uses_1007_bar(fdr_ignoring_end):
    """10/8 10:07 실행 — 소스가 10/8 장중 바를 붙여 줘도 10/7 바까지만 쓴다."""
    fdr_ignoring_end["last"] = "2026-10-08"
    r = ar._precompute_ticker("041190", "20260401", "20261008", INFO, UCFG, bar_before="20261008")
    close, chg = expected("2026-10-07")
    assert r["current_price"] == close
    assert r["change_pct"] == pytest.approx(chg)
    assert fdr_ignoring_end["ends"][-1] == "20261007"


def test_1012_morning_uses_1008_bar_across_holiday(fdr_ignoring_end):
    """10/12(월) 아침 — 10/9 휴장·주말을 건너 10/8 바가 마지막이다."""
    fdr_ignoring_end["last"] = "2026-10-12"
    r = ar._precompute_ticker("041190", "20260401", "20261012", INFO, UCFG, bar_before="20261012")
    close, chg = expected("2026-10-08")
    assert r["current_price"] == close
    assert r["change_pct"] == pytest.approx(chg)


def test_evening_keeps_same_day_bar(fdr_ignoring_end):
    fdr_ignoring_end["last"] = "2026-10-08"
    r = ar._precompute_ticker("041190", "20260401", "20261008", INFO, UCFG, bar_before=None)
    assert r["current_price"] == expected("2026-10-08")[0]
    assert fdr_ignoring_end["ends"][-1] == "20261008"


def test_cutoff_by_slot_not_clock(monkeypatch):
    assert ar.run_bar_cutoff("20261008", "am") == "20261008"
    assert ar.run_bar_cutoff("20261008", "pm") is None
    # main() 이 --run-type 을 ALPHARADAR_RUN_TYPE 으로 고정한다 — 실행 시각이 오후여도 am 이면 자른다
    monkeypatch.setenv("ALPHARADAR_RUN_TYPE", "am")
    assert ar.run_bar_cutoff("20261008") == "20261008"
    monkeypatch.setenv("ALPHARADAR_RUN_TYPE", "pm")
    assert ar.run_bar_cutoff("20261008") is None
