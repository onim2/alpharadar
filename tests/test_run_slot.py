"""런 회차 판정 — 크론 지연으로 저녁 런이 자정을 넘겨 다음 날 am 으로 기록되던 문제."""

from datetime import datetime

import pytest

from run_slot import KST, resolve


def kst(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=KST)


@pytest.mark.parametrize("cron,now,expect", [
    # 실측 발사 시각들
    ("40 9 * * 1-5", "2026-10-08 01:51", ("pm", "20261007")),  # 10/7 저녁 → 이전엔 20261008 am
    ("40 9 * * 1-5", "2026-10-03 00:56", ("pm", "20261002")),  # 금요일 저녁 → 토요일 새벽
    ("40 9 * * 1-5", "2026-09-29 02:47", ("pm", "20260928")),
    ("40 9 * * 1-5", "2026-08-19 19:00", ("pm", "20260819")),  # 정상 지연
    # 2026-10-26~ : 저녁 백업 크론 17:27 KST, 아침 크론 07:53 KST
    ("27 8 * * 1-5", "2026-10-26 17:30", ("pm", "20261026")),
    ("27 8 * * 1-5", "2026-10-27 02:10", ("pm", "20261026")),   # 자정 넘긴 도착
    ("27 8 * * 1-5", "2026-10-31 01:00", ("pm", "20261030")),   # 금요일 저녁 → 토요일 새벽
    ("53 22 * * 0-4", "2026-10-26 07:55", ("am", "20261026")),
    ("53 22 * * 0-4", "2026-10-26 11:40", ("am", "20261026")),
    ("10 21 * * 0-4", "2026-10-08 09:56", ("am", "20261008")),
    ("10 21 * * 0-4", "2026-10-06 10:43", ("am", "20261006")),
    ("10 21 * * 0-4", "2026-10-05 08:55", ("am", "20261005")),  # 월요일 아침
])
def test_schedule(cron, now, expect):
    assert resolve(cron=cron, now=kst(now)) == expect


def test_dispatch_on_time_and_early():
    assert resolve("am", now=kst("2026-10-08 06:10")) == ("am", "20261008")
    assert resolve("pm", now=kst("2026-10-08 18:39")) == ("pm", "20261008")
    assert resolve("pm", now=kst("2026-10-26 16:30")) == ("pm", "20261026")  # cron-job.org dispatch
    assert resolve("pm", now=kst("2026-10-26 16:01")) == ("pm", "20261026")  # 경계 16:30 의 30분 전까지
    # 30분 넘게 이르면 직전 회차로 본다
    assert resolve("pm", now=kst("2026-10-26 15:59")) == ("pm", "20261023")


def test_check_sent_gate(tmp_path, monkeypatch, capsys):
    """gate 잡 — 같은 회차 발송 기록이 있으면 done=1, 없거나 테이블이 없으면 done=0."""
    import sqlite3
    import run_slot
    db = tmp_path / "live.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE sent_history (ticker TEXT, send_date TEXT, grade TEXT, score REAL, run_type TEXT)")
    con.execute("INSERT INTO sent_history VALUES ('005930','20261026','','50','pm')")
    con.commit(); con.close()
    out = tmp_path / "out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    for slot, date, want in [("pm", "20261026", "done=1"), ("am", "20261026", "done=0"),
                             ("pm", "20261027", "done=0")]:
        monkeypatch.setenv("SLOT_TYPE", slot); monkeypatch.setenv("SLOT_DATE", date)
        out.write_text("")
        run_slot.check_sent(str(db))
        assert out.read_text().strip() == want
    empty = tmp_path / "empty.db"; sqlite3.connect(empty).close()
    assert run_slot.slot_sent(str(empty), "20261026", "pm") == 0


def test_weekend_falls_back_to_friday():
    assert resolve("am", now=kst("2026-10-10 09:00")) == ("am", "20261009")  # 토요일


def test_explicit_date_wins():
    assert resolve("pm", date_input="2026-10-07", now=kst("2026-10-08 12:00")) == ("pm", "20261007")


def test_unknown_keeps_legacy():
    assert resolve("", cron="", now=kst("2026-10-08 12:00")) == ("", "")
    assert resolve("auto", now=kst("2026-10-08 12:00")) == ("", "")
