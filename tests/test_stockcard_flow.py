"""T0-a 단위 테스트 — 언피벗 무손실 · 결측 null · upsert 멱등 · as_of 갱신.

표본은 2026-09-16 우리넷(115440) 실제 KIS 응답에서 가져왔다.
"""

import pytest

import alpharadar as ar
import stockcard_common as sc
import stockcard_flow as flow


# 실제 응답 1행 — 22컬럼 전부.
# stck_clpr 9,670 은 FDR 정규장 종가 9,660 과 다르다. 9/16 밤에는 KIS 도 9,660 을
# 주다가 9/17 에 9,670 으로 바뀌었다 — 시간외를 반영한 '익일 기준가'이기 때문이고,
# 값이 사후에 움직인다는 사실 자체가 as_of·upsert 설계의 근거다.
RAW_115440 = {
    "stck_bsop_date": "20260916", "stck_clpr": "9670",
    "prdy_vrss": "960", "prdy_vrss_sign": "2",
    "prsn_ntby_qty": "-2610", "frgn_ntby_qty": "3760", "orgn_ntby_qty": "-14",
    "prsn_ntby_tr_pbmn": "109", "frgn_ntby_tr_pbmn": "-98", "orgn_ntby_tr_pbmn": "0",
    "prsn_shnu_vol": "7255060", "frgn_shnu_vol": "1265978", "orgn_shnu_vol": "1417",
    "prsn_shnu_tr_pbmn": "72120", "frgn_shnu_tr_pbmn": "12404", "orgn_shnu_tr_pbmn": "14",
    "prsn_seln_vol": "7257670", "frgn_seln_vol": "1262218", "orgn_seln_vol": "1431",
    "prsn_seln_tr_pbmn": "72011", "frgn_seln_tr_pbmn": "12502", "orgn_seln_tr_pbmn": "14",
}


@pytest.fixture
def db(tmp_path, monkeypatch):
    """격리된 DB. 저장소의 scores_history.db 를 건드리지 않는다."""
    p = tmp_path / "test.db"
    monkeypatch.setattr(ar, "DB_PATH", p)
    sc.ensure_schema()
    return p


def count(table="investor_flow"):
    with ar._conn() as con:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ── 언피벗 ───────────────────────────────────────────────────────────────────

def test_언피벗은_투자자_3행과_날짜행을_만든다():
    rows, daily = flow.unpivot("115440", RAW_115440, "2026-09-16 21:00:00", "pm",
                               close_krx=9660)
    assert len(rows) == 3
    assert {r[2] for r in rows} == {"prsn", "frgn", "orgn"}
    #                 ticker      date       close_krx base_price base_chg base_sign
    assert daily[:6] == ("115440", "20260916", 9660,    9670,      960,     "2")


def test_정규장_종가와_익일_기준가를_따로_싣는다():
    """2026-09-14부터 KIS stck_clpr 은 시간외를 반영한 익일 기준가다.
    우리넷 9/16 은 FDR 9,660 · KIS 9,670 으로 실제로 다르다."""
    _, daily = flow.unpivot("115440", RAW_115440, "t", None, close_krx=9660)
    close_krx, base_price = daily[2], daily[3]
    assert close_krx == 9660, "정본은 FDR 정규장 종가"
    assert base_price == 9670, "기준가는 KIS stck_clpr 그대로"
    assert base_price - close_krx == 10, "차이가 곧 시간외에서 움직인 폭"


def test_FDR_조회가_실패해도_수급은_적재된다():
    """close_krx 만 null 이 되고 멈추지 않는다."""
    _, daily = flow.unpivot("115440", RAW_115440, "t", None, close_krx=None)
    assert daily[2] is None and daily[3] == 9670


def test_언피벗은_무손실이다():
    """22컬럼 = 날짜단위 4 + 투자자 3 × 6. 원본 값이 전부 복원돼야 한다."""
    rows, daily = flow.unpivot("115440", RAW_115440, "t", None)
    restored = {"stck_bsop_date": daily[1], "stck_clpr": str(daily[3]),
                "prdy_vrss": str(daily[4]), "prdy_vrss_sign": daily[5]}
    for r in rows:
        pre = r[2]
        for i, f in enumerate(flow.FIELDS):
            restored[f"{pre}_{f}"] = str(r[3 + i])
    assert restored == RAW_115440


def test_종목코드는_6자리로_채워진다():
    rows, daily = flow.unpivot("5930", dict(RAW_115440), "t", None)
    assert all(r[0] == "005930" for r in rows)
    assert daily[0] == "005930"


# ── 결측 처리 ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad", ["", "-", None, "  "])
def test_결측은_0이_아니라_null이다(bad):
    raw = dict(RAW_115440, frgn_ntby_qty=bad)
    rows, _ = flow.unpivot("115440", raw, "t", None)
    frgn = next(r for r in rows if r[2] == "frgn")
    assert frgn[3] is None, "결측을 0으로 적으면 '순매수 0'과 구분할 수 없다"


def test_여섯_값이_모두_결측이면_행을_만들지_않는다():
    raw = dict(RAW_115440)
    for f in flow.FIELDS:
        raw[f"orgn_{f}"] = "-"
    rows, _ = flow.unpivot("115440", raw, "t", None)
    assert {r[2] for r in rows} == {"prsn", "frgn"}


def test_날짜가_없으면_아무것도_만들지_않는다():
    rows, daily = flow.unpivot("115440", {"stck_bsop_date": ""}, "t", None)
    assert rows == [] and daily is None


# ── upsert ───────────────────────────────────────────────────────────────────

def _run(monkeypatch, raws, as_of, run_type="pm", closes=None):
    """네트워크를 타지 않는다 — KIS 와 FDR 을 둘 다 갈아끼운다."""
    monkeypatch.setattr(flow, "fetch_investor_rows", lambda t: raws)
    monkeypatch.setattr(flow, "fdr_closes", lambda t, d: closes or {})
    return flow.collect(["115440"], run_type=run_type, as_of=as_of)


def test_재실행해도_행수가_늘지_않는다(db, monkeypatch):
    """T0 완료 기준 — 하루 두 런과 수동 재실행이 같은 날짜를 중복 적재하면 안 된다."""
    s1 = _run(monkeypatch, [RAW_115440], "2026-09-16 18:40:00")
    assert count() == 3 and s1["inserted"] == 3

    s2 = _run(monkeypatch, [RAW_115440], "2026-09-16 18:40:00")
    assert count() == 3, "행이 늘었다 — append 가 되고 있다"
    assert s2["skipped"] == 3 and s2["inserted"] == 0


def test_늦은_as_of가_이전_값을_덮어쓴다(db, monkeypatch):
    """장중 잠정치 → 마감 후 확정치. 애프터마켓 런이 갱신하는 경로다."""
    _run(monkeypatch, [dict(RAW_115440, frgn_ntby_qty="8000")], "2026-09-16 13:00:00")
    with ar._conn() as con:
        assert con.execute(
            "SELECT ntby_qty FROM investor_flow WHERE investor='frgn'").fetchone()[0] == 8000

    s = _run(monkeypatch, [RAW_115440], "2026-09-16 20:30:00")
    assert s["updated"] == 3 and count() == 3
    with ar._conn() as con:
        qty, as_of = con.execute(
            "SELECT ntby_qty, as_of FROM investor_flow WHERE investor='frgn'").fetchone()
    assert qty == 3760 and as_of == "2026-09-16 20:30:00"


def test_이른_as_of는_무시된다(db, monkeypatch):
    """소급 재실행이 확정치를 잠정치로 되돌리면 안 된다."""
    _run(monkeypatch, [RAW_115440], "2026-09-16 20:30:00")
    s = _run(monkeypatch, [dict(RAW_115440, frgn_ntby_qty="999")], "2026-09-16 13:00:00")
    assert s["skipped"] == 3
    with ar._conn() as con:
        assert con.execute(
            "SELECT ntby_qty FROM investor_flow WHERE investor='frgn'").fetchone()[0] == 3760


def test_조회_실패는_적재하지_않고_계속한다(db, monkeypatch):
    monkeypatch.setattr(flow, "fetch_investor_rows", lambda t: [])
    s = flow.collect(["115440", "000660"], run_type="pm", as_of="t")
    assert s["failed"] == 2 and s["tickers"] == 0 and count() == 0


def test_날짜행도_함께_적재된다(db, monkeypatch):
    _run(monkeypatch, [RAW_115440], "2026-09-16 18:40:00",
         closes={"20260916": 9660})
    assert count("investor_flow_daily") == 1
    with ar._conn() as con:
        row = con.execute(
            "SELECT close_krx, base_price FROM investor_flow_daily").fetchone()
    assert tuple(row) == (9660, 9670), "정규장 종가와 익일 기준가가 따로 실려야 한다"


# ── watchlist ────────────────────────────────────────────────────────────────

def test_watchlist_파서(tmp_path):
    p = tmp_path / "w.txt"
    p.write_text(
        "# 주석만 있는 줄\n"
        "\n"
        "115440    # 우리넷\n"
        "  005930\n"
        "5930\n"            # 6자리가 아니면 버린다
        "abcdef\n"
        "115440\n",         # 중복
        encoding="utf-8")
    assert sc.load_watchlist(p) == ["115440", "005930"]


def test_watchlist_파일이_없으면_빈_목록(tmp_path):
    assert sc.load_watchlist(tmp_path / "없음.txt") == []


def test_db경로를_지정하지_않으면_실전DB에_쓰지_않는다(monkeypatch):
    """2026-09-16 에 로컬 CLI 가 추적 중인 DB에 빈 테이블을 만들었다.
    브랜치 diff 로 새어 나가는 경로라 기본값을 '거부'로 둔다."""
    monkeypatch.delenv(sc.ENV_ALLOW_LIVE, raising=False)
    before = ar.DB_PATH
    with pytest.raises(SystemExit) as e:
        sc.resolve_db(None)
    assert str(sc.LIVE_DB) in str(e.value)
    assert ar.DB_PATH == before, "거부했는데 경로가 바뀌었다"


def test_db경로를_명시하면_그곳에_쓴다(tmp_path, monkeypatch):
    monkeypatch.delenv(sc.ENV_ALLOW_LIVE, raising=False)
    p = tmp_path / "copy.db"
    assert sc.resolve_db(p) == p and ar.DB_PATH == p


def test_환경변수로만_실전DB가_열린다(monkeypatch):
    """Actions 스텝에서만 설정한다."""
    monkeypatch.setenv(sc.ENV_ALLOW_LIVE, "1")
    assert sc.resolve_db(None) == sc.LIVE_DB


def test_대상은_런을_가리지_않는다(db, tmp_path):
    """자정을 넘긴 저녁 런은 'am'으로 찍힌다(실측 9/7 00:23 도착).
    거기서 런을 좁혀 잡으면 대상이 0종목이 되고 카드가 조용히 사라진다."""
    with ar._conn() as con:
        con.execute("CREATE TABLE scan_results "
                    "(scan_date TEXT, ticker TEXT, run_type TEXT)")
        con.executemany("INSERT INTO scan_results VALUES (?,?,?)", [
            ("20260916", "000660", "am"),
            ("20260916", "005930", "pm"),
            ("20260915", "035720", "am"),   # 다른 날 — 들어오면 안 된다
        ])

    w = tmp_path / "w.txt"
    w.write_text("115440\n", encoding="utf-8")

    tickers, origin = sc.resolve_targets("20260916", None, w)
    assert tickers == ["000660", "005930", "115440"]
    assert origin["scan"] == 2 and origin["watchlist_only"] == 1

    # 런을 지정하면 좁아진다는 것도 확인해 둔다(호출부가 쓰지 않을 뿐 기능은 남긴다)
    only_am, _ = sc.resolve_targets("20260916", "am", w)
    assert only_am == ["000660", "115440"]


def test_저장소_watchlist에_우리넷이_있다():
    """첫 검증 표본이 풀 밖 종목이라 watchlist 없이는 카드가 나오지 않는다."""
    assert "115440" in sc.load_watchlist("data/watchlist.txt")


# ── 값 변환 ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,want", [
    ("1,234", 1234), ("-56", -56), ("0", 0),
    ("", None), ("-", None), (None, None), ("  ", None), ("abc", None),
])
def test_to_int_or_none(raw, want):
    assert sc.to_int_or_none(raw) is want or sc.to_int_or_none(raw) == want
