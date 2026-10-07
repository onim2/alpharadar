"""시총 결측 대응 — 2026-10-06 아침 런 유니버스 0 사고.

fdr.StockListing 이 장중에 Marcap·Close 를 전부 비워 준 날, 시총 게이트가 int(nan) 예외로
전 종목을 로그 없이 탈락시켰고 깨진 목록이 DB 스냅샷까지 덮어썼다.
"""

import numpy as np
import pandas as pd
import pytest

import alpharadar as ar

UCFG = {"min_price": 500, "min_market_cap": 200_000_000_000,
        "cap_tier": {"large_threshold": 5_000_000_000_000, "mid_threshold": 500_000_000_000},
        "investor_exclude_today": True}


@pytest.fixture
def fake_prices(monkeypatch):
    """FDR·KIS 를 막고 종가 10,000원짜리 150거래일 시계열을 돌려준다."""
    idx = pd.bdate_range("2026-04-01", periods=150)
    close = np.full(150, 10_000.0)
    df = pd.DataFrame({"Open": close, "High": close, "Low": close, "Close": close,
                       "Volume": np.full(150, 1_000_000.0), "Change": np.zeros(150)}, index=idx)
    import FinanceDataReader as fdr
    monkeypatch.setattr(fdr, "DataReader", lambda *a, **k: df.copy())
    monkeypatch.setattr(ar, "_get_investor_data", lambda *a, **k: (3, 0, 0, 0, 0))
    ar._PRECOMP_STATS.clear(); ar._PRECOMP_ERR_SAMPLE.clear()
    return df


def _info(**kw):
    base = {"name": "테스트", "market": "KOSPI", "sector": "기타"}
    base.update(kw)
    return base


def test_listing_cap_used_as_is(fake_prices):
    """정상 시총은 그대로 쓴다 — 계산 경로를 타지 않는다."""
    r = ar._precompute_ticker("000001", "20260401", "20261006",
                              _info(market_cap=600_000_000_000, Stocks=1), UCFG, cap_fallback=True)
    assert r["market_cap"] == 600_000_000_000
    assert r["market_cap_src"] == "listing"
    assert r["cap_tier"] == "mid"
    assert ar._PRECOMP_STATS["cap_computed"] == 0


def test_nan_cap_computed_from_stocks_when_fallback(fake_prices):
    """시총 결측 런: 상장주식수 × 최근 바 종가 (10,000원 × 3천만주 = 3,000억)."""
    r = ar._precompute_ticker("000002", "20260401", "20261006",
                              _info(market_cap=float("nan"), Stocks=30_000_000), UCFG, cap_fallback=True)
    assert r is not None
    assert r["market_cap"] == 300_000_000_000
    assert r["market_cap_src"] == "stocks_x_close"
    assert r["cap_tier"] == "small"
    assert ar._PRECOMP_STATS["cap_computed"] == 1


def test_computed_cap_still_gated(fake_prices):
    """계산한 시총도 게이트를 그대로 받는다 (10,000원 × 1천만주 = 1,000억 < 2,000억)."""
    r = ar._precompute_ticker("000003", "20260401", "20261006",
                              _info(market_cap=None, Stocks=10_000_000), UCFG, cap_fallback=True)
    assert r is None
    assert ar._PRECOMP_STATS["cap_below"] == 1


@pytest.mark.parametrize("stocks", [None, float("nan"), "-", 0])
def test_no_stocks_rejected(fake_prices, stocks):
    """시총도 상장주식수도 없으면 통과시키지 않는다 (기존 원칙 유지)."""
    r = ar._precompute_ticker("000004", "20260401", "20261006",
                              _info(market_cap=float("nan"), Stocks=stocks), UCFG, cap_fallback=True)
    assert r is None
    assert ar._PRECOMP_STATS["cap_unknown"] == 1


def test_nan_cap_without_fallback_rejected_without_exception(fake_prices):
    """정상 런(cap_fallback=False)에서 시총이 빈 종목은 예전처럼 탈락 — 다만 예외가 아니라
    '시총 결측' 으로 집계된다. 정상인 날의 유니버스는 수리 전과 같아야 한다."""
    r = ar._precompute_ticker("000005", "20260401", "20261006",
                              _info(market_cap=float("nan"), Stocks=30_000_000), UCFG, cap_fallback=False)
    assert r is None
    assert ar._PRECOMP_STATS["cap_missing"] == 1
    assert ar._PRECOMP_STATS["exception"] == 0


class _FakeFdr:
    def __init__(self, df):
        self.df = df

    def StockListing(self, mkt):
        return self.df.copy()


def _listing(marcap):
    return pd.DataFrame({"Code": ["000001", "000002", "000003", "000004"], "Name": list("ABCD"),
                         "Marcap": marcap, "Stocks": [1e7] * 4})


@pytest.mark.parametrize("marcap,saved", [
    ([np.nan] * 4, False),                 # 10/6 아침 — 전부 결측
    ([1e12, np.nan, np.nan, np.nan], False),  # 25% < 50%
    ([1e12, 1e12, np.nan, np.nan], True),   # 50% — 경계는 저장
    ([1e12] * 4, True),                     # 정상
])
def test_listing_snapshot_skipped_when_cap_missing(monkeypatch, marcap, saved):
    """시총이 절반도 안 찬 목록은 쓰되 스냅샷(마지막 폴백)은 덮지 않는다."""
    calls = []
    monkeypatch.setattr(ar, "_save_listing_snapshot", lambda *a, **k: calls.append(a))
    df = ar._stock_listing("KOSPI", "20261006", _FakeFdr(_listing(marcap)))
    assert len(df) == 4                    # 목록은 그대로 돌려준다
    assert bool(calls) is saved


def test_marcap_valid_ratio():
    assert ar._marcap_valid_ratio(_listing([1e12, np.nan, 0, "-"])) == 0.25
    assert ar._marcap_valid_ratio(pd.DataFrame({"Code": ["1"]})) == 0.0
    assert ar._marcap_valid_ratio(None) == 0.0
