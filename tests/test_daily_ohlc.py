"""daily_ohlc — 확정 규칙·멱등·장중 행 제외·null 보존·대상 합집합·폴백.

KIS·FDR·텔레그램은 전부 가짜다. DB 는 임시 파일, 로그는 임시 디렉터리.
"""
import sqlite3
from datetime import datetime

import pytest

import alpharadar as ar
import daily_ohlc as do
import price_shadow as ps

KST = do.KST
NIGHT = datetime(2026, 11, 13, 23, 30, tzinfo=KST)       # 봉 당일 밤 — 미확정
EARLY = datetime(2026, 11, 14, 8, 59, tzinfo=KST)        # 다음 날 09:00 직전 — 미확정
MORNING = datetime(2026, 11, 14, 9, 0, tzinfo=KST)       # 다음 날 09:00 — 확정


def bar(c, o=None, h=None, low=None, v=1000.0, val=None):
    return {"open": o or c, "high": h or c, "low": low or c, "close": c, "volume": v, "value": val}


def rows(db, tk=None):
    with sqlite3.connect(db) as c:
        c.row_factory = sqlite3.Row
        q = f"SELECT * FROM {do.TABLE}" + (" WHERE ticker=?" if tk else "")
        return {(r["ticker"], r["date"]): dict(r) for r in c.execute(q, (tk,) if tk else ())}


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DAILY_OHLC_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(do, "_LOG", None)
    p = tmp_path / "t.db"
    with sqlite3.connect(p) as c:
        do.ensure_schema(c)
    return p


def put(db, tk, bars, src, now):
    st = do.new_stats()
    with sqlite3.connect(db) as c:
        do.store(c, tk, bars, src, now, st)
    return st


def test_final_boundary():
    assert do.is_final("20261113", NIGHT) == 0
    assert do.is_final("20261113", EARLY) == 0
    assert do.is_final("20261113", MORNING) == 1


def test_idempotent(db):
    put(db, "328130", {"20261112": bar(15000.0)}, "kis_j", MORNING)
    put(db, "328130", {"20261112": bar(15000.0)}, "kis_j", MORNING)
    assert len(rows(db)) == 1


def test_night_then_morning_finalizes_and_keeps_first_close(db):
    put(db, "328130", {"20261113": bar(15250.0, h=15400.0)}, "kis_j", NIGHT)
    r = rows(db)[("328130", "20261113")]
    assert r["is_final"] == 0 and r["first_close"] == 15250.0 and r["close"] == 15250.0
    st = put(db, "328130", {"20261113": bar(15020.0, h=15400.0)}, "kis_j", EARLY)
    assert rows(db)[("328130", "20261113")]["is_final"] == 0, "09:00 전 재조회는 아직 미확정"
    st = put(db, "328130", {"20261113": bar(15020.0, h=15400.0)}, "kis_j", MORNING)
    r = rows(db)[("328130", "20261113")]
    assert r["is_final"] == 1 and r["close"] == 15020.0
    assert r["first_close"] == 15250.0 and r["first_seen"] == "2026-11-13 23:30:00"
    assert st["finalized"] == [("328130", "20261113", 15250.0, 15020.0)]
    line, _ = do.stats_lines(st, "OHLC")
    assert "불일치 1" in line and "328130 20261113" in line


def test_final_row_changed_value_updates_and_warns(db):
    put(db, "005930", {"20261110": bar(100000.0)}, "kis_j", MORNING)
    st = put(db, "005930", {"20261110": bar(100000.0)}, "kis_j", MORNING)
    assert st["unchanged"] == 1 and not st["changed_final"]
    st = put(db, "005930", {"20261110": bar(50000.0)}, "kis_j", MORNING)
    r = rows(db)[("005930", "20261110")]
    assert r["close"] == 50000.0 and r["is_final"] == 1 and r["first_close"] == 100000.0
    _, warns = do.stats_lines(st, "OHLC")
    assert st["changed_final"] and "005930 20261110" in warns[0]


def test_intraday_today_dropped():
    lunch = datetime(2026, 11, 17, 12, 50, tzinfo=KST)
    after = datetime(2026, 11, 17, 15, 40, tzinfo=KST)
    bars = {"20261116": bar(1.0), "20261117": bar(2.0)}
    assert set(do.drop_intraday(bars, lunch)) == {"20261116"}
    assert set(do.drop_intraday(bars, after)) == {"20261116", "20261117"}


def test_empty_fields_are_null_not_zero(db):
    d, b = do.parse_stock_row({"stck_bsop_date": "20261113", "stck_clpr": "15020", "stck_oprc": "",
                               "stck_hgpr": "0", "stck_lwpr": None, "acml_vol": "0", "acml_tr_pbmn": ""})
    assert b["open"] is None and b["high"] is None and b["low"] is None
    assert b["volume"] == 0.0, "거래량 0 은 사실이다"
    assert b["value"] is None
    assert do.parse_stock_row({"stck_bsop_date": "20261113", "stck_clpr": ""}) is None
    put(db, "328130", {d: b}, "kis_j", MORNING)
    r = rows(db)[("328130", "20261113")]
    assert r["open"] is None and r["value"] is None


def test_index_units():
    d, b = do.parse_index_row({"stck_bsop_date": "20261116", "bstp_nmix_prpr": "5100.50",
                               "bstp_nmix_oprc": "5305.40", "bstp_nmix_hgpr": "5327.99",
                               "bstp_nmix_lwpr": "5069.83", "acml_vol": "221296", "acml_tr_pbmn": "16458294"})
    assert (b["close"], b["open"], b["low"]) == (5100.50, 5305.40, 5069.83)
    assert b["volume"] == 221296e3 and b["value"] == 16458294e6


def test_fdr_never_overwrites_kis_and_stock_fdr_stays_unfinal(db):
    put(db, "000660", {"20261113": bar(1216000.0)}, "kis_j", MORNING)
    st = put(db, "000660", {"20261113": bar(1220000.0)}, "fdr", MORNING)
    assert st["skipped_fdr"] == 1 and rows(db)[("000660", "20261113")]["close"] == 1216000.0
    put(db, "000001", {"20261112": bar(10.0)}, "fdr", MORNING)
    assert rows(db)[("000001", "20261112")]["is_final"] == 0
    put(db, "000001", {"20261112": bar(11.0)}, "kis_j", MORNING)
    r = rows(db)[("000001", "20261112")]
    assert r["is_final"] == 1 and r["price_src"] == "kis_j" and r["close"] == 11.0


# ── run(): 대상 합집합·폴백·실패 계속 ──────────────────────────────────────

def kis_row(d, c):
    return {"stck_bsop_date": d, "stck_clpr": str(c), "stck_oprc": str(c), "stck_hgpr": str(c + 10),
            "stck_lwpr": str(c - 10), "acml_vol": "100", "acml_tr_pbmn": "1000"}


def idx_row(d, c):
    return {"stck_bsop_date": d, "bstp_nmix_prpr": str(c), "bstp_nmix_oprc": str(c),
            "bstp_nmix_hgpr": str(c + 1), "bstp_nmix_lwpr": str(c - 1), "acml_vol": "1", "acml_tr_pbmn": "1"}


@pytest.fixture
def world(db, tmp_path, monkeypatch):
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE scan_results (scan_date, run_type, ticker, cap_tier, score_total, "
                  "score_t, change_pct, created_at)")
        c.execute("INSERT INTO scan_results VALUES ('20261113','pm','000001','mid',50,50,0,'2026-11-13 16:00')")
        c.execute("CREATE TABLE sent_history (ticker, send_date, grade, score, run_type)")
        c.executemany("INSERT INTO sent_history VALUES (?,?,NULL,NULL,'pm')",
                      [("5", "20261110"), ("000009", "20260901")])      # 오래된 발송은 빠진다
    (tmp_path / "w.txt").write_text("000002  # 관심\nbad\n", encoding="utf-8")
    (tmp_path / "b.txt").write_text("# 주석만\n000003\n", encoding="utf-8")
    cfg = {"stockcard": {"watchlist_path": str(tmp_path / "w.txt")},
           "daily_ohlc": {"briefing_watchlist_path": str(tmp_path / "b.txt"), "sent_lookback_days": 30}}
    monkeypatch.setattr(ar, "load_config", lambda: cfg)
    kis_ok = {"000001", "000002", "000005"}                    # 000003 KIS 실패 → FDR, 지수 KOSDAQ 실패 → FDR
    def kis(path, params, tr):
        tk = params["FID_INPUT_ISCD"]
        if tr == do.INDEX_TR:
            return {"rt_cd": "0", "output2": [idx_row("20261113", 5290.53)]} if tk == "0001" else {}
        return {"rt_cd": "0", "output2": [kis_row("20261113", 1000)]} if tk in kis_ok else {}
    monkeypatch.setattr(ar, "_kis_get", kis)
    def fdr(sym, d1, d2):
        if sym in ("000003", "KQ11"):
            return {"20261113": bar(700.0)}
        return None
    monkeypatch.setattr(do, "_fdr_bars", fdr)
    sent = []
    monkeypatch.setattr(do, "notify", lambda t: sent.append(t))
    return cfg, sent


def test_targets_union(db, world):
    cfg, _ = world
    with sqlite3.connect(db) as c:
        s = do.target_sets(c, cfg, MORNING)
    assert s["shadow"] == {"000001"} and s["watchlist"] == {"000002"} and s["briefing"] == {"000003"}
    assert s["sent"] == {"000005"}, "6자리 zfill, 30일 밖 발송 제외"


def test_targets_missing_files_and_tables(tmp_path, monkeypatch):
    monkeypatch.setattr(do, "_LOG", None)
    p = tmp_path / "e.db"
    cfg = {"stockcard": {"watchlist_path": str(tmp_path / "none.txt")},
           "daily_ohlc": {"briefing_watchlist_path": str(tmp_path / "none2.txt")}}
    (tmp_path / "empty.txt").write_text("", encoding="utf-8")
    with sqlite3.connect(p) as c:
        do.ensure_schema(c)
        s = do.target_sets(c, cfg, MORNING)
        assert all(v == set() for v in s.values())
        cfg["stockcard"]["watchlist_path"] = str(tmp_path / "empty.txt")
        assert do.target_sets(c, cfg, MORNING)["watchlist"] == set()


def test_run_sources_fallback_and_continue(db, world):
    _, sent = world
    out = do.run(ar, db, now=MORNING)
    r = rows(db)
    assert r[("000001", "20261113")]["price_src"] == "kis_j" and r[("000001", "20261113")]["is_final"] == 1
    assert r[("000003", "20261113")]["price_src"] == "fdr" and r[("000003", "20261113")]["is_final"] == 0
    assert r[("KOSPI", "20261113")]["price_src"] == "kis_j" and r[("KOSPI", "20261113")]["is_final"] == 1
    assert r[("KOSDAQ", "20261113")]["price_src"] == "fdr" and r[("KOSDAQ", "20261113")]["is_final"] == 1
    assert out["src"] == {"kis_j": 4, "fdr": 2} and not out["failed"]
    assert sent and "fdr폴백 2" in sent[0], "폴백은 알린다"


def test_run_both_fail_makes_no_row(db, world, monkeypatch):
    monkeypatch.setattr(do, "_fdr_bars", lambda *a: None)
    out = do.run(ar, db, now=MORNING)
    assert "000003" in out["failed"] and not rows(db, "000003")
    assert rows(db, "000001"), "한 종목 실패가 런을 멈추지 않는다"


def test_run_skips_tickers_price_shadow_just_stored(db, world, monkeypatch):
    calls = []
    orig = ar._kis_get
    monkeypatch.setattr(ar, "_kis_get", lambda p, prm, tr: calls.append(prm["FID_INPUT_ISCD"]) or orig(p, prm, tr))
    do.store_sink(db, {"000001": {"20261113": bar(1000.0)}}, MORNING)
    do.run(ar, db, now=MORNING)
    assert "000001" not in calls and "000002" in calls


# ── price_shadow 연동: 같은 응답으로 저장, 기존 반환값 유지 ────────────────

def test_price_shadow_kis_daily_fills_sink_and_keeps_return(monkeypatch):
    monkeypatch.setattr(ar, "_kis_get", lambda p, prm, tr: {"rt_cd": "0", "output2": [kis_row("20261113", 1000)]})
    monkeypatch.setattr(ps, "_OHLC_SINK", {})
    df = ps._kis_daily(ar, "000001", "20261101", "20261113")
    assert list(df.columns) == ["Close", "High", "Volume"]
    assert ps._OHLC_SINK["000001"]["20261113"]["low"] == 990.0


def test_run_warns_on_stale_unfinal(db, world):
    _, sent = world
    put(db, "000007", {"20261105": bar(1.0)}, "kis_j", datetime(2026, 11, 5, 23, 0, tzinfo=KST))
    do.run(ar, db, now=MORNING)
    assert any("4일 넘게 미확정 1건/1종목" in t for t in sent)
