"""outcomes_clean 뷰 규칙 — 장중 표시·UN 날짜 규칙·해제가 열 단위로 맞게 동작하는지."""
import sqlite3

import flag_outcomes as F

COLS = ["fwd1", "fwd5"]


def make(tmp_path):
    con = sqlite3.connect(tmp_path / "t.db")
    con.execute("CREATE TABLE outcomes (scan_date TEXT, ticker TEXT, origin TEXT, fwd1 REAL, fwd5 REAL)")
    con.executemany("INSERT INTO outcomes VALUES (?,?,?,?,?)", [
        ("20260901", "A", "scan", 1.0, 5.0),   # fwd1 끝 9/2(장중 confirmed) · fwd5 끝 9/8(깨끗)
        ("20260908", "B", "scan", 2.0, 6.0),   # fwd1 끝 9/9(깨끗) · fwd5 끝 9/15(UN)
        ("20260914", "C", "scan", 3.0, None),  # 진입부터 UN
        ("20260910", "D", "scan", 4.0, None),  # 끝 바 기록 없음 → UN 규칙상 NULL
    ])
    F.ensure_schema(con)
    con.executemany("INSERT INTO outcomes_window_end VALUES (?,?,?,?,?)", [
        ("20260901", "A", "scan", "fwd1", "2026-09-02"), ("20260901", "A", "scan", "fwd5", "2026-09-08"),
        ("20260908", "B", "scan", "fwd1", "2026-09-09"), ("20260908", "B", "scan", "fwd5", "2026-09-15"),
        ("20260914", "C", "scan", "fwd1", "2026-09-15"),
    ])
    con.execute("INSERT INTO outcomes_flags VALUES ('20260901','A','scan','fwd1','intraday_bar',"
                "'2026-09-02','confirmed',1.0,0.5,1,'t')")
    return con


def view(con):
    return {r[0]: r[1:] for r in con.execute("SELECT ticker, fwd1, fwd5 FROM outcomes_clean ORDER BY ticker")}


def test_rules_per_column(tmp_path):
    con = make(tmp_path)
    F.create_view(con, COLS, "2026-09-14")
    v = view(con)
    assert v["A"] == (None, 5.0)      # 장중 제외는 그 열만, 다른 지평은 산다
    assert v["B"] == (2.0, None)      # 창이 9/14 를 넘은 열만 NULL
    assert v["C"] == (None, None)     # 진입이 9/14 이후
    assert v["D"] == (None, None)     # 끝 바 기록 없음 → 보수적 NULL
    assert con.execute("SELECT COUNT(*) FROM outcomes_clean").fetchone()[0] == 4


def test_lift_un_rule_keeps_intraday_flag(tmp_path):
    con = make(tmp_path)
    F.create_view(con, COLS, None)
    v = view(con)
    assert v["A"] == (None, 5.0)      # 장중 표시는 해제와 무관하게 유지
    assert v["B"] == (2.0, 6.0) and v["C"] == (3.0, None) and v["D"] == (4.0, None)
