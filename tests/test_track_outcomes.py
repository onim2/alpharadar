"""track_outcomes 당일 바 차단 테스트 (2026-09-18 핫픽스).

가짜 FinanceDataReader 가 요청 종료일을 무시하고 '오늘 장중 바'까지 돌려준다 —
종료일 절단과 응답 필터 두 겹이 모두 작동해야 통과한다. 실제 DB·로그를 건드리지
않도록 임시 디렉터리에서 import 하고 --db 로 임시 DB 만 쓴다.
"""
import importlib
import os
import sqlite3
import sys
import types
from datetime import datetime

import pandas as pd
import pytest


@pytest.fixture(scope="module")
def T(tmp_path_factory):
    # import 시점에 data/logs/scanner_*.log 를 연다 — 저장소 로그를 건드리지 않게.
    d = tmp_path_factory.mktemp("to_import")
    old = os.getcwd()
    os.chdir(d)
    try:
        sys.path.insert(0, old)
        mod = importlib.import_module("track_outcomes")
    finally:
        os.chdir(old)
    return mod


KST_NOW = lambda s: datetime.fromisoformat(s).replace(tzinfo=None)


def fake_fdr(monkeypatch, bars, calls):
    """bars: {date: close}. 종료일을 무시하고 전부 돌려준다(최악의 소스)."""
    def reader(tk, start, end):
        calls.append((tk, start, end))
        idx = pd.to_datetime(sorted(bars))
        c = [bars[k] for k in sorted(bars)]
        return pd.DataFrame({"Open": c, "High": c, "Low": c, "Close": c, "Volume": 1}, index=idx)
    mod = types.ModuleType("FinanceDataReader")
    mod.DataReader = reader
    monkeypatch.setitem(sys.modules, "FinanceDataReader", mod)


def set_now(T, monkeypatch, s):
    fixed = datetime.fromisoformat(s).replace(tzinfo=T.KST)
    monkeypatch.setattr(T, "_now_kst", lambda: fixed, raising=False)   # 원본 코드 비교용


def make_db(tmp_path, scan_date="20260914"):
    p = tmp_path / "t.db"
    with sqlite3.connect(p) as c:
        c.execute("CREATE TABLE scan_results (scan_date TEXT, ticker TEXT)")
        c.execute("INSERT INTO scan_results VALUES (?, '000001')", (scan_date,))
    return p


def run_main(T, monkeypatch, db):
    monkeypatch.setattr(T, "_track_control", lambda: False)
    monkeypatch.setattr(sys, "argv", ["track_outcomes.py", "--db", str(db)])
    T.main()
    with sqlite3.connect(db) as c:
        return c.execute("SELECT fwd1, fwd5 FROM outcomes WHERE ticker='000001'").fetchone()


# 9/11(금) 100 · 9/14(월) 100 · 9/15(화) 최종 110. 장중(09:30)엔 999 로 보인다.
FINAL = {"2026-09-11": 100.0, "2026-09-14": 100.0, "2026-09-15": 110.0}
INTRADAY = {**FINAL, "2026-09-15": 999.0}


def test_cutoff_is_previous_kst_day(T):
    assert T._price_cutoff(datetime(2026, 9, 15, 9, 30, tzinfo=T.KST)) == pd.Timestamp("2026-09-14")
    assert T._price_cutoff(datetime(2026, 9, 15, 23, 30, tzinfo=T.KST)) == pd.Timestamp("2026-09-14")
    # 월요일 → 일요일(= 직전 거래일 금요일까지)
    assert T._price_cutoff(datetime(2026, 9, 21, 6, 10, tzinfo=T.KST)) == pd.Timestamp("2026-09-20")


def test_kst_not_utc(T, monkeypatch):
    """러너는 UTC. UTC 9/15 00:30 = KST 9/15 09:30 → 절단은 9/14 (UTC 로 보면 9/14 가 아니라 9/13)."""
    from datetime import timezone
    utc = datetime(2026, 9, 15, 0, 30, tzinfo=timezone.utc)
    assert T._price_cutoff(utc.astimezone(T.KST)) == pd.Timestamp("2026-09-14")


def test_fetch_drops_today_bar_and_caps_end(T, monkeypatch):
    calls = []
    fake_fdr(monkeypatch, INTRADAY, calls)
    set_now(T, monkeypatch, "2026-09-15T09:30")
    got = T._fetch_prices(["000001"], "2026-09-07", "2026-10-29")
    assert calls[0][2] == "2026-09-14", "종료일이 전날로 잘리지 않았다"
    assert pd.Timestamp("2026-09-15") not in got["000001"].index, "당일 장중 바가 남았다"


def test_intraday_run_does_not_freeze_today_exit(T, monkeypatch, tmp_path):
    """사고 재현: 09:30 아침 런이 fwd1 을 장중 999 로 채우고 영구 고정하던 경로."""
    db = make_db(tmp_path)
    fake_fdr(monkeypatch, INTRADAY, [])
    set_now(T, monkeypatch, "2026-09-15T09:30")
    fwd1, _ = run_main(T, monkeypatch, db)
    assert fwd1 is None, f"당일 장중 바로 fwd1 을 채웠다: {fwd1}"

    # 다음 날 아침 런: 최종 종가로 채워진다
    fake_fdr(monkeypatch, FINAL, [])
    set_now(T, monkeypatch, "2026-09-16T08:10")
    fwd1, _ = run_main(T, monkeypatch, db)
    assert fwd1 == pytest.approx(10.0)


def test_evening_run_also_skips_today(T, monkeypatch, tmp_path):
    """저녁 런(23:30)도 당일 바를 쓰지 않는다 — 당일 값의 계열이 아직 확정되지 않았다."""
    db = make_db(tmp_path)
    fake_fdr(monkeypatch, FINAL, [])
    set_now(T, monkeypatch, "2026-09-15T23:30")
    fwd1, _ = run_main(T, monkeypatch, db)
    assert fwd1 is None


def test_regression_past_bars_unchanged(T, monkeypatch, tmp_path):
    """당일 바가 끼지 않는 평소 경우 결과는 기존과 같다 + 기존 non-null 보존."""
    db = make_db(tmp_path)
    fake_fdr(monkeypatch, FINAL, [])
    set_now(T, monkeypatch, "2026-09-16T08:10")
    fwd1, fwd5 = run_main(T, monkeypatch, db)
    assert fwd1 == pytest.approx(10.0) and fwd5 is None      # fwd5 미도래

    # 이미 채운 값은 소스가 달라져도 덮지 않는다(기존 동작)
    fake_fdr(monkeypatch, {**FINAL, "2026-09-15": 120.0}, [])
    set_now(T, monkeypatch, "2026-09-17T08:10")
    fwd1, _ = run_main(T, monkeypatch, db)
    assert fwd1 == pytest.approx(10.0)
