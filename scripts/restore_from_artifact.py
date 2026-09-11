#!/usr/bin/env python3
"""유실된 런을 Actions 아티팩트 DB에서 현재 DB로 되살린다.

왜 필요한가 — daily.yml 의 커밋 스텝은 DB(바이너리)를 rebase 로 병합할 수 없어
원격이 움직였으면 exit 1 로 끝난다. 그때 스캔 결과는 `scores-db-<run_id>`
아티팩트에만 남는다. 2026-09-09·09-10 저녁 런이 그렇게 유실됐고(발송은 정상),
원인인 concurrency 경합은 afterhours 를 같은 큐에 세워 막았다. 이 스크립트는
이미 유실된 분을 건져 올리는 쪽이다.

원칙
  * 현재 DB에 없는 행만 넣는다. 기존 행은 절대 덮어쓰지 않는다 — 아티팩트는
    자기 런 시작 시점의 체크아웃이라 그 뒤에 들어온 커밋(afterhours, 다음 아침
    런)보다 오래된 상태다. REPLACE 로 밀면 최신 행을 과거로 되돌린다.
  * 실행 전 현재 DB를 data/backup/ 으로 복사한다.
  * 기본은 드라이런이다. 실제로 쓰려면 --apply.

scan_results 는 PK가 surrogate id(AUTOINCREMENT)라 중복 판정에 쓸 수 없다.
(scan_date, ticker, created_at) 조합으로 본다 — 같은 날 두 런은 created_at 이
갈리므로 런 단위로 정확히 구분된다. id 는 복사하지 않는다(현재 DB 기준 재발급).

sent_history 는 특별하다. PK가 (ticker, send_date)라 하루 두 런이 한 행으로
접히고, 아티팩트에 남은 것도 '접힌 결과'뿐이다 — 어느 런이 보냈는지 아티팩트
자체로는 복원되지 않는다. 그래서 발송 사실을 scan_results 에서 다시 세운다:
발송 대상은 `score_total >= min_display_score` 와 정확히 일치한다(실측 3개 런에서
19→13 · 26→20 · 17→11 전부 일치). run_type 컬럼이 생긴 뒤에만 이 경로를 쓴다.
컬럼이 없으면 건너뛴다 — 지금 'am'으로 넣어두면 마이그레이션 뒤에 같은 발송이
'pm'으로 한 번 더 들어와 두 배로 세어진다.
"""
from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

DEFAULT_DB = Path("data/scores_history.db")
BACKUP_DIR = Path("data/backup")

# scan_date 로 잘라 INSERT OR IGNORE 하면 되는 테이블 (PK가 스스로 중복을 막는다)
PK_TABLES = (
    "pool_history",
    "gated_tickers",
    "engine_b_history",
    "news_articles",
    "dart_filings",
)

_GRADE_RANK = {"참고": 0, "주시": 1, "집중": 2}


def _cols(con: sqlite3.Connection, table: str, schema: str = "main") -> list[str]:
    return [r[1] for r in con.execute(f"PRAGMA {schema}.table_info({table})")]


def _shared_cols(con: sqlite3.Connection, table: str, drop=()) -> list[str]:
    """양쪽에 다 있는 컬럼만 쓴다. 아티팩트가 구 스키마여도 깨지지 않게."""
    main = _cols(con, table, "main")
    src = set(_cols(con, table, "src"))
    return [c for c in main if c in src and c not in drop]


def restore_scan_results(con, scan_date, dry):
    cols = _shared_cols(con, "scan_results", drop=("id",))
    cl = ", ".join(f'"{c}"' for c in cols)
    sel = ", ".join(f's."{c}"' for c in cols)
    where = """
        FROM src.scan_results s
        WHERE s.scan_date = ?
          AND NOT EXISTS (
              SELECT 1 FROM main.scan_results m
              WHERE m.scan_date = s.scan_date
                AND m.ticker    IS s.ticker
                AND m.created_at IS s.created_at
          )
    """
    n = con.execute(f"SELECT COUNT(*) {where}", (scan_date,)).fetchone()[0]
    if n and not dry:
        con.execute(f"INSERT INTO main.scan_results ({cl}) SELECT {sel} {where}", (scan_date,))
    return n


def restore_pk_table(con, table, scan_date, dry):
    cols = _shared_cols(con, table)
    cl = ", ".join(f'"{c}"' for c in cols)
    where = f"FROM src.{table} s WHERE s.scan_date = ?"
    before = con.execute(f"SELECT COUNT(*) FROM main.{table} WHERE scan_date = ?",
                         (scan_date,)).fetchone()[0]
    if dry:
        # IGNORE 가 몇 건을 떨굴지 미리 세려면 PK를 알아야 한다. 대신 실제
        # INSERT 를 롤백 없는 savepoint 안에서 돌려 차이를 재는 쪽이 단순하다.
        con.execute("SAVEPOINT probe")
        con.execute(f"INSERT OR IGNORE INTO main.{table} ({cl}) "
                    f"SELECT {', '.join(f'''s.\"{c}\"''' for c in cols)} {where}", (scan_date,))
        after = con.execute(f"SELECT COUNT(*) FROM main.{table} WHERE scan_date = ?",
                            (scan_date,)).fetchone()[0]
        con.execute("ROLLBACK TO probe")
        con.execute("RELEASE probe")
        return after - before
    con.execute(f"INSERT OR IGNORE INTO main.{table} ({cl}) "
                f"SELECT {', '.join(f'''s.\"{c}\"''' for c in cols)} {where}", (scan_date,))
    after = con.execute(f"SELECT COUNT(*) FROM main.{table} WHERE scan_date = ?",
                        (scan_date,)).fetchone()[0]
    return after - before


def restore_sent_history(con, scan_date, run_type, floor, dry):
    """발송 기록을 아티팩트의 scan_results 에서 다시 세워 넣는다.

    run_type 컬럼이 없으면 (ticker, send_date) PK가 두 런을 접어버리므로
    아무것도 하지 않고 건너뛴다 — 이유는 모듈 docstring 참고.
    """
    if "run_type" not in _cols(con, "sent_history"):
        return None, "run_type 컬럼 없음 — 건너뜀(작업 3 마이그레이션 후 재실행)"
    if run_type is None:
        return None, "--run-type 미지정 — 건너뜀"

    # 이 아티팩트가 담고 있는 '그 런' = 해당 scan_date 의 마지막 created_at.
    row = con.execute("SELECT MAX(created_at) FROM src.scan_results WHERE scan_date=?",
                      (scan_date,)).fetchone()
    if not row or row[0] is None:
        return 0, "아티팩트에 해당 scan_date 행이 없음"
    created_at = row[0]

    sent = con.execute(
        "SELECT ticker, grade, score_total FROM src.scan_results "
        "WHERE scan_date=? AND created_at IS ? AND score_total >= ?",
        (scan_date, created_at, floor)).fetchall()

    added = 0
    for ticker, grade, score in sent:
        cur = con.execute(
            "SELECT grade, score FROM main.sent_history "
            "WHERE ticker=? AND send_date=? AND run_type=?",
            (ticker, scan_date, run_type)).fetchone()
        if cur is not None:
            # mark_sent 과 같은 우선순위: 강등 금지, 같은 등급이면 높은 점수 유지
            if _GRADE_RANK.get(grade, -1) < _GRADE_RANK.get(cur[0], -1):
                continue
            if (_GRADE_RANK.get(grade, -1) == _GRADE_RANK.get(cur[0], -1)
                    and (cur[1] or 0) >= (score or 0)):
                continue
        else:
            added += 1
        if not dry:
            con.execute(
                "INSERT OR REPLACE INTO main.sent_history "
                "(ticker, send_date, grade, score, run_type) VALUES (?,?,?,?,?)",
                (ticker, scan_date, grade, score, run_type))
    return added, f"{created_at} 런 · score>={floor} {len(sent)}건 기준"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("artifact_db", type=Path, help="아티팩트에서 받은 scores_history.db")
    ap.add_argument("scan_date", help="복구할 scan_date (YYYYMMDD)")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB, help="대상 DB (기본 data/scores_history.db)")
    ap.add_argument("--run-type", choices=("am", "pm"),
                    help="이 아티팩트가 담은 런의 구분. sent_history 복구에 필요")
    ap.add_argument("--send-floor", type=float, default=45.0,
                    help="발송 하한 (config.yaml display.min_display_score, 기본 45)")
    ap.add_argument("--apply", action="store_true", help="실제로 쓴다 (기본은 드라이런)")
    a = ap.parse_args(argv)

    if not a.artifact_db.exists():
        sys.exit(f"아티팩트 DB가 없다: {a.artifact_db}")
    if not a.db.exists():
        sys.exit(f"대상 DB가 없다: {a.db}")

    dry = not a.apply
    print(f"{'[드라이런]' if dry else '[적용]'} {a.artifact_db} → {a.db} · scan_date={a.scan_date}"
          f" · run_type={a.run_type or '미지정'}")

    if not dry:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        bak = BACKUP_DIR / f"scores_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2(a.db, bak)
        print(f"백업: {bak}")

    con = sqlite3.connect(a.db)
    con.execute("ATTACH ? AS src", (str(a.artifact_db),))
    try:
        n = restore_scan_results(con, a.scan_date, dry)
        print(f"  scan_results   +{n}")
        for t in PK_TABLES:
            print(f"  {t:<14} +{restore_pk_table(con, t, a.scan_date, dry)}")
        n, note = restore_sent_history(con, a.scan_date, a.run_type, a.send_floor, dry)
        print(f"  sent_history   {'생략' if n is None else f'+{n}'}  ({note})")
        if not dry:
            con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.execute("DETACH src")

    print("\nscan_results 일자별 현황:")
    for r in con.execute(
            "SELECT scan_date, COUNT(*), COUNT(DISTINCT created_at) FROM scan_results "
            "WHERE scan_date >= ? GROUP BY scan_date ORDER BY scan_date",
            (str(int(a.scan_date) - 4),)):
        print(f"  {r[0]}  {r[1]:>3}행  런 {r[2]}개")
    con.close()
    if dry:
        print("\n드라이런이었다. 실제 반영은 --apply.")


if __name__ == "__main__":
    main()
