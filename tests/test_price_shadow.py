"""price_shadow — 출처 기록·폴백·예외 알림·실전 DB 보호·장중 행 제외.

KIS·FDR·텔레그램은 전부 가짜다. DB 는 임시 파일, 로그는 임시 디렉터리.
"""
import sqlite3
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

import alpharadar as ar
import price_shadow as ps

SD = "20260918"
DAYS = pd.bdate_range(end="2026-09-18", periods=130)


def output2(seed=0):
    rng = np.random.default_rng(seed)
    close = np.round(10000 * np.exp(np.cumsum(rng.normal(0, 0.02, len(DAYS)))), -1)
    return [{"stck_bsop_date": d.strftime("%Y%m%d"), "stck_clpr": str(c), "stck_hgpr": str(c * 1.02),
             "acml_vol": str(1000 + i)} for i, (d, c) in enumerate(zip(DAYS, close))][::-1]


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PRICE_SHADOW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(ps, "_LOG", None)
    db = tmp_path / "t.db"
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE scan_results (scan_date, run_type, ticker, cap_tier, score_total, "
                  "score_t, change_pct, created_at)")
        c.execute("INSERT INTO scan_results VALUES (?,?,?,?,?,?,?,?)",
                  (SD, "pm", "000001", "mid", 55.0, 60.0, 1.0, "2026-09-18 14:30:00"))
        c.execute("CREATE TABLE pool_history (scan_date, ticker, stage, reason, cap_tier)")
        c.executemany("INSERT INTO pool_history VALUES (?,?,?,?,?)",
                      [(SD, "000002", "filtered", "rsi", "small"), (SD, "000003", "no_signal", None, "large")])
        c.execute("CREATE TABLE gated_tickers (scan_date, ticker, reason)")
        c.execute("INSERT INTO gated_tickers VALUES (?,?,?)", (SD, "000004", "overheat:ret5d"))
    kis_ok = {"000001": 1, "000003": 3, "000004": 4}           # 000002 은 KIS 실패
    monkeypatch.setattr(ar, "_kis_get", lambda path, params, tr: (
        {"rt_cd": "0", "output2": output2(kis_ok[params["FID_INPUT_ISCD"]])}
        if params["FID_INPUT_ISCD"] in kis_ok else {}))
    fdr_ok = {"000002"}                                         # 000002 은 FDR 로 폴백 성공
    def fdr(tk, d1, d2):
        if tk not in fdr_ok:
            return None
        o = output2(2)[::-1]
        return pd.DataFrame({"Close": [float(r["stck_clpr"]) for r in o], "High": 1.0, "Volume": 1.0},
                            index=DAYS)
    monkeypatch.setattr(ps, "_fdr_daily", fdr)
    sent = []
    monkeypatch.setattr(ps, "notify", lambda text: sent.append(text))
    return db, sent


NIGHT = datetime(2026, 9, 18, 23, 30, tzinfo=ar.KST)


def rows(db):
    with sqlite3.connect(db) as c:
        c.row_factory = sqlite3.Row
        return {r["ticker"]: dict(r) for r in c.execute("SELECT * FROM price_shadow_j")}


def test_sources_fallback_and_alert(env):
    db, sent = env
    ps.run(ar, db, now=NIGHT)
    r = rows(db)
    assert r["000001"]["price_src"] == "kis_j" and r["000002"]["price_src"] == "fdr"
    assert r["000001"]["origin"] == "scan" and r["000002"]["origin"] == "f:rsi"
    assert r["000003"]["origin"] == "ns" and r["000004"]["origin"] == "gated"
    assert r["000001"]["score_total_j_est"] is not None and r["000002"]["score_total_j_est"] is None
    assert sent and "fdr폴백 1" in sent[0], "폴백은 반드시 알린다"


def test_values_come_from_production_functions(env):
    db, _ = env
    ps.run(ar, db, now=NIGHT)
    df, _ = ps.load_series(ar, "000001", SD, now=NIGHT)
    pf = ar._price_features(df)
    got = rows(db)["000001"]
    for k in ("current_price", "change_pct", "disparity", "rsi", "bb_pos", "ma20", "res_top"):
        assert got[k] == pytest.approx(pf[k]), k
    assert got["score_t_j"] == ar._calc_t(pf)


def test_lookup_failure_is_recorded_and_alerted(env, monkeypatch):
    db, sent = env
    monkeypatch.setattr(ps, "_fdr_daily", lambda *a: None)      # 000002 은 KIS·FDR 둘 다 실패
    ps.run(ar, db, now=NIGHT)
    assert rows(db)["000002"]["price_src"] is None
    assert sent and "조회실패 1" in sent[0]


def test_per_ticker_exception_is_counted_and_alerted(env, monkeypatch):
    db, sent = env
    real = ar._price_features
    boom = {"n": 0}
    def flaky(df):
        boom["n"] += 1
        if boom["n"] == 1:
            raise ValueError("boom")
        return real(df)
    monkeypatch.setattr(ar, "_price_features", flaky)
    ps.run(ar, db, now=NIGHT)
    assert "error" in {v["price_src"] for v in rows(db).values()}
    assert sent and "계산예외 1" in sent[0] and "ValueError" in sent[0]


def test_whole_failure_alerts_and_exits_zero(env, monkeypatch):
    db, sent = env
    monkeypatch.setattr(ps, "run", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db locked")))
    assert ps.main(["--db", str(db)]) == 0
    assert sent and "실패" in sent[0] and "db locked" in sent[0]


def test_import_failure_still_alerts(monkeypatch):
    """alpharadar 를 못 불러와도 알림은 텔레그램 API 로 직접 간다."""
    monkeypatch.setitem(sys.modules, "alpharadar", None)      # import alpharadar → ImportError
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "c")
    posted = []
    import requests
    monkeypatch.setattr(requests, "post", lambda url, json=None, timeout=None: posted.append(json))
    assert ps.main(["--db", "/nonexistent.db"]) == 0
    assert posted and "price shadow(J) 실패" in posted[0]["text"]


def test_refuses_live_db_without_flag(env, monkeypatch):
    monkeypatch.delenv("PRICE_SHADOW_ALLOW_LIVE_DB", raising=False)
    called = []
    monkeypatch.setattr(ps, "run", lambda *a, **k: called.append(1))
    assert ps.main([]) == 0 and called == []


def test_last_bar_follows_run_label_not_clock(env):
    """am 런은 scan_date 전 바까지, pm 런은 scan_date 바까지 — 실행 시각과 무관."""
    df, src = ps.load_series(ar, "000001", SD, "am", now=NIGHT)      # 밤에 돌려도 am 이면 전일 바
    assert src == "kis_j" and df.index[-1] == pd.Timestamp("2026-09-17")
    df2, _ = ps.load_series(ar, "000001", SD, "pm", now=NIGHT)
    assert df2.index[-1] == pd.Timestamp("2026-09-18")
    # 자정 넘긴 저녁 런(9/14분이 20260915 am 으로 찍힘)은 9/14 바를 본다
    df3, _ = ps.load_series(ar, "000001", "20260915", "am", now=NIGHT)
    assert df3.index[-1] == pd.Timestamp("2026-09-14")


def test_intraday_guard_drops_today_even_for_pm(env):
    morning = datetime(2026, 9, 18, 10, 0, tzinfo=ar.KST)
    df, _ = ps.load_series(ar, "000001", SD, "pm", now=morning)
    assert df.index[-1] == pd.Timestamp("2026-09-17")
