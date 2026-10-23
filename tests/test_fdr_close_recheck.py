"""저녁 FDR 종가 스냅숏 → 다음 날 아침 같은 날짜 봉 대조."""
import logging

import alpharadar as ar


def _setup(monkeypatch, tmp_path):
    monkeypatch.setattr(ar, "DB_PATH", tmp_path / "t.db")
    monkeypatch.setattr(ar, "_SCAN_RESULTS_READONLY", False)
    ar.init_db()


def _u(**closes):
    return {t: {"current_price": c, "bar_date": d} for t, (c, d) in closes.items()}


def test_pm_snapshot_then_am_recheck(monkeypatch, tmp_path, caplog):
    _setup(monkeypatch, tmp_path)
    ar.fdr_close_recheck(_u(**{"005930": (100.0, "20261026"), "000660": (200.0, "20261026"),
                               "035420": (50.0, "20261023")}),        # 전일 봉 — 저장 안 함
                         "20261026", "pm")
    caplog.set_level(logging.INFO, logger=ar.logger.name)
    ar.fdr_close_recheck(_u(**{"005930": (100.0, "20261026"), "000660": (198.0, "20261026")}),
                         "20261027", "am")
    line = [r.message for r in caplog.records if "FDR 종가 재조회" in r.message][0]
    assert "20261026 pm 저장 2 · 대조 2 · 불일치 1 (50.0%)" in line and "000660 -1.00%" in line


def test_dry_run_pm_does_not_write(monkeypatch, tmp_path, caplog):
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(ar, "_SCAN_RESULTS_READONLY", True)
    ar.fdr_close_recheck(_u(**{"005930": (100.0, "20261026")}), "20261026", "pm")
    caplog.set_level(logging.INFO, logger=ar.logger.name)
    ar.fdr_close_recheck(_u(**{"005930": (100.0, "20261026")}), "20261027", "am")
    assert any("대조할 저녁 스냅숏 없음" in r.message for r in caplog.records)
