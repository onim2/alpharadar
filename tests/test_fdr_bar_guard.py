"""저녁 회차 FDR 당일 봉 가드 — 봉이 없으면 10분×3 기다리고, 끝내 없으면 False(스캔 중단)."""
import sys, types

import pandas as pd

import alpharadar as ar


def _fake_fdr(monkeypatch, plan):
    """plan: 호출마다 돌려줄 마지막 봉 날짜(YYYYMMDD) 또는 예외."""
    calls = []

    def reader(ticker, start, end):
        x = plan[min(len(calls), len(plan) - 1)]
        calls.append(x)
        if isinstance(x, Exception):
            raise x
        return pd.DataFrame({"Close": [1, 2]}, index=pd.to_datetime(["20261023", x]))
    monkeypatch.setitem(sys.modules, "FinanceDataReader", types.SimpleNamespace(DataReader=reader))
    return calls


def test_bar_present_no_wait(monkeypatch):
    calls = _fake_fdr(monkeypatch, ["20261026"])
    slept = []
    assert ar.wait_fdr_today_bar("20261026", _sleep=slept.append) is True
    assert calls == ["20261026"] and slept == []


def test_bar_appears_after_two_waits(monkeypatch):
    _fake_fdr(monkeypatch, ["20261023", RuntimeError("x"), "20261026"])
    slept = []
    assert ar.wait_fdr_today_bar("20261026", _sleep=slept.append) is True
    assert slept == [600, 600]


def test_bar_never_appears_gives_up_after_three_waits(monkeypatch):
    calls = _fake_fdr(monkeypatch, ["20261023"])
    slept = []
    assert ar.wait_fdr_today_bar("20261026", _sleep=slept.append) is False
    assert slept == [600, 600, 600] and len(calls) == 4
