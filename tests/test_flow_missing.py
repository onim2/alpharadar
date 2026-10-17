"""수급 결측 처리와 카드 정렬 (2026-10-17 수리).

- KIS 수급 조회가 실패하면 0 이 아니라 None 이어야 한다. 0 은 "순매수 0일"이라는
  사실이고, 조회 실패는 사실을 모른다는 뜻이다(CLAUDE.md 데이터 원칙).
- 카드 정렬은 수급 점수(D) → 총점 → 관찰 횟수. 9/11 진입 후보 규칙은 8/13~9/29
  일자내 재검정에서 fwd10 −11.0%p(승 2/17)라 뺐다.
"""

import pytest

import alpharadar as ar


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    monkeypatch.setattr(ar, "DB_PATH", tmp_path / "t.db")
    ar.init_db()
    return tmp_path / "t.db"


# ── _get_investor_data 실패 경로 4종 ─────────────────────────────────────────

def test_missing_app_key_returns_none(monkeypatch):
    monkeypatch.delenv("KIS_APP_KEY", raising=False)
    monkeypatch.setattr(ar, "_KIS_WARNED", False)
    assert ar._get_investor_data("005930", "20260901", "20260930") == ar._FLOW_MISSING


def test_abnormal_output_returns_none(monkeypatch):
    monkeypatch.setenv("KIS_APP_KEY", "k")
    monkeypatch.setattr(ar, "_kis_get", lambda *a, **k: {"rt_cd": "0", "output": None})
    assert ar._get_investor_data("005930", "20260901", "20260930") == ar._FLOW_MISSING


def test_no_settled_rows_returns_none(monkeypatch):
    monkeypatch.setenv("KIS_APP_KEY", "k")
    rows = [{"stck_bsop_date": "20260930", "orgn_ntby_qty": "", "frgn_ntby_qty": "-"}]
    monkeypatch.setattr(ar, "_kis_get", lambda *a, **k: {"rt_cd": "0", "output": rows})
    assert ar._get_investor_data("005930", "20260901", "20260930") == ar._FLOW_MISSING


def test_exception_returns_none(monkeypatch):
    monkeypatch.setenv("KIS_APP_KEY", "k")
    def boom(*a, **k): raise RuntimeError("timeout")
    monkeypatch.setattr(ar, "_kis_get", boom)
    monkeypatch.setattr(ar, "_KIS_TB_LOGGED", 0)
    assert ar._get_investor_data("005930", "20260901", "20260930") == ar._FLOW_MISSING


def test_normal_rows_still_counted(monkeypatch):
    """회귀: 정상 응답은 수리 전과 같은 값을 낸다 (5일 중 순매수 3일, 합계 양수)."""
    monkeypatch.setenv("KIS_APP_KEY", "k")
    monkeypatch.setattr(ar, "today_kst", lambda: "20261017")
    def row(d, o, f, p):
        return {"stck_bsop_date": d, "orgn_ntby_qty": str(o), "frgn_ntby_qty": str(f),
                "prsn_ntby_qty": str(p)}
    rows = [row("20261016", 100, 50, -150), row("20261015", -30, 10, 20),
            row("20261014", 20, 20, -40), row("20261013", -5, -5, 10),
            row("20261010", 40, 0, -40)]
    monkeypatch.setattr(ar, "_kis_get", lambda *a, **k: {"rt_cd": "0", "output": rows})
    net_days, net_total, retail_days, inst, frgn = ar._get_investor_data("005930", "a", "b")
    assert (net_days, net_total, retail_days, inst, frgn) == (3, 200, 2, 125, 75)


# ── 카드: 정렬과 결측 표기 ────────────────────────────────────────────────────

def _res(ticker, score, d, **kw):
    base = {
        "ticker": ticker, "name": "N" + ticker, "sector": "기타", "cap_tier": "mid",
        "score": score, "t": 50.0, "d": d, "s_text": 50.0, "news_score": 50.0,
        "dart_score": 50.0, "rsi": 55.0, "bb_pos": 50.0, "disparity": 101.0,
        "change_pct": 0.5, "current_price": 10000, "vol_slope": 0.0,
        "net_buy_days": 3, "net_buy_total": 100, "retail_buy_days": 1,
        "inst_net": 60, "foreign_net": 40, "flow_missing": False,
        "hype_slope": 0.0, "hype_rank": 9999, "n_accel": False, "v_surge": False,
        "overheat_pen": 0.0, "source": "engine_a", "grade": "참고",
        "news_count": 0, "dart_count": 0, "finbert_mode": "none",
        "best_dart_title": "", "rating_bond": None, "rating_cp": None,
        "fg_industry": None, "market_cap": 1e12,
    }
    base.update(kw)
    return base


def _run(results, capsys, monkeypatch, tmp_db):
    monkeypatch.setattr(ar, "get_watch_history", lambda tickers, d: {})
    monkeypatch.setattr(ar, "dart_best_titles", {}, raising=False)
    cfg = {"grade": {"min_display_score": 45}, "scoring": {}}
    ar.run_step4(results, cfg, date="20261017", dry_run=True, run_type="am")
    return capsys.readouterr().out


def test_sort_is_d_then_score(tmp_db, capsys, monkeypatch):
    out = _run([_res("111111", 70, 20), _res("222222", 55, 80), _res("333333", 60, 80)],
               capsys, monkeypatch, tmp_db)
    i1, i2, i3 = out.index("(111111)"), out.index("(222222)"), out.index("(333333)")
    assert i3 < i2 < i1           # D 80 둘이 먼저(총점 60 > 55), D 20 은 마지막
    assert "수급 점수순" in out
    assert "진입 후보" not in out


def test_missing_flow_is_labelled_not_zero(tmp_db, capsys, monkeypatch):
    missing = _res("444444", 60, 0, net_buy_days=None, net_buy_total=None,
                   inst_net=None, foreign_net=None, retail_buy_days=None, flow_missing=True)
    zero = _res("555555", 60, 30, net_buy_days=0, inst_net=0, foreign_net=0)
    out = _run([missing, zero], capsys, monkeypatch, tmp_db)
    assert "조회 실패" in out and "(결측)" in out
    # 진짜 0 은 결측으로 표기되지 않는다 — 두 사실을 구분한다
    assert out.count("조회 실패") == 2 and "(0일)" in out
