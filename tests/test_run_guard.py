"""런 가드 — 휴장일 스킵과 같은 회차 키 처리.

2026-10-05(개천절 대체공휴일): 아침 런(08:55)과 저녁 런(10/6 03:47 도착)이 10/2 바로 재스캔·발송했다.
2026-10-08: 10/7 저녁 런과 10/8 아침 런이 둘 다 '20261008 am' 에 써서 기록이 접혔다.
"""

import json
import logging

import pytest

import alpharadar as ar


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    monkeypatch.setattr(ar, "DB_PATH", tmp_path / "t.db")
    monkeypatch.setattr(ar, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(ar, "_HOLIDAY_CACHE", tmp_path / "cache" / "kis_holiday.json")
    monkeypatch.setattr(ar, "_SCAN_RESULTS_READONLY", False)
    monkeypatch.setattr(ar, "today_kst", lambda: "20261008")
    ar.init_db()
    return tmp_path


# 10/5 시점 KIS 일봉: 10/2(금)가 마지막 (10/3 토, 10/4 일, 10/5 휴장)
BARS_AT_1005 = ("20260928", "20260929", "20260930", "20261001", "20261002")
# KIS chk-holiday 실측 응답(2026-10-08, BASS_DT=20261002)의 일부
HOLIDAYS = {"20261002": "Y", "20261003": "N", "20261004": "N", "20261005": "N",
            "20261006": "Y", "20261007": "Y", "20261008": "Y", "20261009": "N",
            "20261010": "N", "20261011": "N", "20261012": "Y"}


def fake_kis(bars=BARS_AT_1005, holidays=HOLIDAYS, calls=None):
    """경로로 갈라 KIS 응답 모양을 흉내 낸다. 일봉 output2 는 최신순, 빈 dict 가 섞여 온다."""
    def _get(path, params, tr_id, *a, **k):
        if calls is not None:
            calls.append(tr_id)
        if tr_id == ar._KIS_HOLIDAY_TR:
            if holidays is None:
                return {}
            base = params["BASS_DT"]
            rows = [{"bass_dt": d, "opnd_yn": v, "bzdy_yn": v} for d, v in sorted(holidays.items())
                    if d >= base]
            return {"rt_cd": "0", "output": rows}
        rows = [{"stck_bsop_date": d, "stck_clpr": "10000"} for d in sorted(bars, reverse=True)]
        return {"rt_cd": "0", "output2": rows + [{}]}
    return _get


def scan_row(ticker="053300", score=43.37):
    return {"ticker": ticker, "name": "x", "score": score}


# ── 휴장일 ────────────────────────────────────────────────────────────────────

def test_1005_pm_holiday_skips(tmp_db, monkeypatch, caplog):
    monkeypatch.setattr(ar, "_kis_get", fake_kis())
    with caplog.at_level(logging.INFO, logger=ar.logger.name):
        assert ar.run_guard("20261005", "pm") == (False, 0)
    assert "휴장일 스킵: 오늘 20261005, 최근 거래일 20261002" in caplog.text


def test_trading_day_pm_proceeds(tmp_db, monkeypatch):
    monkeypatch.setattr(ar, "_kis_get", fake_kis(bars=BARS_AT_1005 + ("20261006",)))
    assert ar.run_guard("20261006", "pm") == (True, 0)


def test_1005_am_holiday_skips(tmp_db, monkeypatch, caplog):
    monkeypatch.setattr(ar, "_kis_get", fake_kis())
    with caplog.at_level(logging.INFO, logger=ar.logger.name):
        assert ar.run_guard("20261005", "am") == (False, 0)
    assert "휴장일 스킵: 오늘 20261005, KIS 개장일 아님(opnd_yn=N)" in caplog.text


def test_trading_day_am_proceeds(tmp_db, monkeypatch):
    monkeypatch.setattr(ar, "_kis_get", fake_kis())
    assert ar.run_guard("20261008", "am") == (True, 0)


def test_am_holiday_api_failure_runs_with_warning(tmp_db, monkeypatch, caplog):
    monkeypatch.setattr(ar, "_kis_get", fake_kis(holidays=None))
    with caplog.at_level(logging.WARNING, logger=ar.logger.name):
        assert ar.run_guard("20261009", "am") == (True, 0)
    assert "휴장일 판정 불가(KIS 휴장일조회 실패)" in caplog.text


def test_am_holiday_api_exception_runs(tmp_db, monkeypatch):
    def boom(*a, **k):
        raise ConnectionError("down")
    monkeypatch.setattr(ar, "_kis_get", boom)
    assert ar.run_guard("20261009", "am") == (True, 0)


def test_holiday_cache_once_per_day(tmp_db, monkeypatch):
    calls = []
    monkeypatch.setattr(ar, "_kis_get", fake_kis(calls=calls))
    assert ar.market_open_kis("20261009") is False
    assert ar.market_open_kis("20261012") is True
    assert ar.market_open_kis("20261009") is False
    assert calls == [ar._KIS_HOLIDAY_TR], "같은 날 두 번째부터는 캐시를 써야 한다"
    cached = json.loads(ar._HOLIDAY_CACHE.read_text(encoding="utf-8"))
    assert cached["fetched"] == "20261008"


def test_holiday_cache_from_other_day_is_refetched(tmp_db, monkeypatch):
    ar.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    ar._HOLIDAY_CACHE.write_text(json.dumps({"fetched": "20261007", "days": {"20261009": "Y"}}))
    calls = []
    monkeypatch.setattr(ar, "_kis_get", fake_kis(calls=calls))
    assert ar.market_open_kis("20261009") is False
    assert calls == [ar._KIS_HOLIDAY_TR]


def test_pm_unknown_latest_does_not_skip(tmp_db, monkeypatch, caplog):
    monkeypatch.setattr(ar, "_kis_get", lambda *a, **k: {})
    import FinanceDataReader as fdr
    def boom(*a, **k):
        raise ConnectionError("down")
    monkeypatch.setattr(fdr, "DataReader", boom)
    with caplog.at_level(logging.WARNING, logger=ar.logger.name):
        assert ar.run_guard("20261005", "pm") == (True, 0)
    assert "휴장일 판정 불가" in caplog.text


def test_latest_ignores_bars_after_asof(monkeypatch):
    monkeypatch.setattr(ar, "_kis_get", fake_kis(bars=BARS_AT_1005 + ("20261006",)))
    assert ar.latest_trading_day("20261005") == "20261002"


# ── 같은 회차 키 ──────────────────────────────────────────────────────────────

def test_rerun_after_send_failure_proceeds(tmp_db, monkeypatch, caplog):
    """스캔은 기록됐지만 발송이 실패한 회차 — 스캔 행을 지우고 다시 돈다."""
    monkeypatch.setattr(ar, "_kis_get", fake_kis())
    ar.save_scan_results([scan_row(), scan_row("317400", 60.0)], "20261008", "am")
    assert ar.slot_rows("20261008", "am") == (2, 0)
    with caplog.at_level(logging.WARNING, logger=ar.logger.name):
        assert ar.run_guard("20261008", "am") == (True, 0)
    assert ar.slot_rows("20261008", "am") == (0, 0)
    assert "발송 기록 없는 회차 재실행: 20261008 am scan_results 2행" in caplog.text


def test_dry_run_then_real_run_proceeds(tmp_db, monkeypatch):
    monkeypatch.setattr(ar, "_kis_get", fake_kis())
    # dry-run: 가드 통과, scan_results 미기록
    assert ar.run_guard("20261008", "am", dry_run=True) == (True, 0)
    monkeypatch.setattr(ar, "_SCAN_RESULTS_READONLY", True)
    ar.save_scan_results([scan_row()], "20261008", "am")
    assert ar.slot_rows("20261008", "am") == (0, 0)
    # 본 런
    monkeypatch.setattr(ar, "_SCAN_RESULTS_READONLY", False)
    assert ar.run_guard("20261008", "am") == (True, 0)


def test_dry_run_on_sent_slot_is_not_blocked(tmp_db, monkeypatch):
    monkeypatch.setattr(ar, "_kis_get", fake_kis())
    ar.mark_sent("053300", "20261008", "참고", 50.0, "am")
    assert ar.run_guard("20261008", "am", dry_run=True) == (True, 0)
    assert ar.slot_rows("20261008", "am") == (0, 1)


def test_rerun_of_sent_slot_aborts(tmp_db, monkeypatch, caplog):
    monkeypatch.setattr(ar, "_kis_get", fake_kis())
    ar.save_scan_results([scan_row()], "20261008", "am")
    ar.mark_sent("053300", "20261008", "참고", 50.0, "am")
    with caplog.at_level(logging.WARNING, logger=ar.logger.name):
        assert ar.run_guard("20261008", "am") == (False, 1)
    assert "같은 회차에 발송 기록이 이미 있다: 20261008 am" in caplog.text
    assert ar.slot_rows("20261008", "am") == (1, 1), "중단할 때는 아무것도 지우지 않는다"


def test_other_slot_not_affected(tmp_db, monkeypatch):
    monkeypatch.setattr(ar, "_kis_get", fake_kis(bars=BARS_AT_1005 + ("20261006", "20261007", "20261008")))
    ar.mark_sent("053300", "20261008", "참고", 50.0, "am")
    assert ar.run_guard("20261008", "pm") == (True, 0)
