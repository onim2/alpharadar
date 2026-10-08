"""런 가드 — 휴장일 스킵과 같은 회차 키 중단.

2026-10-05(개천절 대체공휴일): 저녁 런이 10/6 03:47 에 도착해 10/2 바로 재스캔·발송했다.
2026-10-08: 10/7 저녁 런과 10/8 아침 런이 둘 다 '20261008 am' 에 써서 기록이 접혔다.
"""

import logging

import pytest

import alpharadar as ar


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    monkeypatch.setattr(ar, "DB_PATH", tmp_path / "t.db")
    ar.init_db()
    return tmp_path / "t.db"


def kis_bars(*dates):
    """KIS 일봉 응답 모양 — output2 는 최신순, 빈 dict 가 섞여 올 수 있다."""
    rows = [{"stck_bsop_date": d, "stck_clpr": "10000"} for d in sorted(dates, reverse=True)]
    return {"rt_cd": "0", "output2": rows + [{}]}


# 10/5 시점에 KIS 가 돌려줄 수 있는 최근 바: 10/2(금)가 마지막 (10/3 토, 10/4 일, 10/5 휴장)
BARS_AT_1005 = ("20260928", "20260929", "20260930", "20261001", "20261002")


def test_1005_pm_holiday_skips(tmp_db, monkeypatch, caplog):
    monkeypatch.setattr(ar, "_kis_get", lambda *a, **k: kis_bars(*BARS_AT_1005))
    with caplog.at_level(logging.INFO, logger=ar.logger.name):
        assert ar.run_guard("20261005", "pm") == (False, 0)
    assert "휴장일 스킵: 오늘 20261005, 최근 거래일 20261002" in caplog.text


def test_trading_day_pm_proceeds(tmp_db, monkeypatch):
    monkeypatch.setattr(ar, "_kis_get", lambda *a, **k: kis_bars(*BARS_AT_1005, "20261006"))
    assert ar.run_guard("20261006", "pm") == (True, 0)


def test_am_is_not_judged_by_data(tmp_db, monkeypatch):
    """아침은 장 시작 전이라 거래일에도 당일 바가 없다 — 데이터로 거르지 않는다(한계)."""
    called = []
    monkeypatch.setattr(ar, "_kis_get", lambda *a, **k: called.append(1) or kis_bars(*BARS_AT_1005))
    assert ar.run_guard("20261005", "am") == (True, 0)
    assert not called


def test_unknown_latest_does_not_skip(tmp_db, monkeypatch, caplog):
    monkeypatch.setattr(ar, "_kis_get", lambda *a, **k: {})
    import FinanceDataReader as fdr
    def boom(*a, **k):
        raise ConnectionError("down")
    monkeypatch.setattr(fdr, "DataReader", boom)
    with caplog.at_level(logging.WARNING, logger=ar.logger.name):
        assert ar.run_guard("20261005", "pm") == (True, 0)
    assert "휴장일 판정 불가" in caplog.text


def test_latest_ignores_bars_after_asof(monkeypatch):
    monkeypatch.setattr(ar, "_kis_get", lambda *a, **k: kis_bars(*BARS_AT_1005, "20261006"))
    assert ar.latest_trading_day("20261005") == "20261002"


@pytest.mark.parametrize("table", ["scan_results", "sent_history"])
def test_existing_slot_key_aborts(tmp_db, monkeypatch, caplog, table):
    monkeypatch.setattr(ar, "_kis_get", lambda *a, **k: kis_bars("20261007", "20261008"))
    if table == "scan_results":
        ar.save_scan_results([{"ticker": "053300", "name": "x", "score": 43.37}], "20261008", "am")
    else:
        ar.mark_sent("053300", "20261008", "참고", 43.37, "am")
    assert sum(ar.slot_rows("20261008", "am")) > 0, "준비한 행이 안 들어갔다"
    with caplog.at_level(logging.WARNING, logger=ar.logger.name):
        assert ar.run_guard("20261008", "am") == (False, 1)
    assert "같은 회차 키가 이미 있다: 20261008 am" in caplog.text
    # 다른 회차는 막지 않는다
    assert ar.run_guard("20261008", "pm") == (True, 0)
