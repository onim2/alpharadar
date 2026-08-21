"""master.db → data/cache/sector_map_v1.pkl 로 섹터 메타를 떠낸다.

스캔은 GitHub Actions에서 도는데 master.db는 로컬 경로
(/Users/summer123/Project_2/data_store/master.db)에만 있다. 그래서 러너에서는
FnGuide 섹터·KSIC·신용등급 보강이 통째로 건너뛰어지고, scores_history.db의
fg_sector/fg_industry/ksic 컬럼이 100% 비어 있었다(2026-08-21 확인).

리포에 커밋되는 pkl로 떠내면 러너도 같은 메타를 쓴다. master.db가 갱신되면
(연 1회 universe 연도 추가) 이 스크립트를 다시 돌려 커밋한다.

    python export_sector_map.py
"""
import os
import pickle
import sqlite3
import sys
from collections import Counter
from pathlib import Path

MASTER = "/Users/summer123/Project_2/data_store/master.db"
OUT = Path("data/cache/sector_map_v1.pkl")


def _s(x):
    return x.strip() if isinstance(x, str) else x


def main():
    if not os.path.exists(MASTER):
        print(f"master.db 없음: {MASTER}", file=sys.stderr)
        return 1
    with sqlite3.connect(f"file:{MASTER}?mode=ro", uri=True) as c:
        yr = c.execute("SELECT MAX(year) FROM universe").fetchone()[0]
        rows = c.execute(
            "SELECT ticker, fg_sector, fg_industry, ksic, rating_bond, rating_cp, "
            "largest_holder FROM universe WHERE year=?", (yr,)).fetchall()

    meta = {}
    for tk, fgs, fgi, ksic, rb, rcp, lh in rows:
        code = tk[1:] if isinstance(tk, str) and tk.startswith("A") and tk[1:].isdigit() else tk
        meta[str(code)] = {"fg_sector": _s(fgs), "fg_industry": _s(fgi), "ksic": _s(ksic),
                           "rating_bond": _s(rb), "rating_cp": _s(rcp), "largest_holder": _s(lh)}

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "wb") as f:
        pickle.dump({"year": yr, "meta": meta}, f)

    ind = Counter(v["fg_industry"] for v in meta.values() if v.get("fg_industry"))
    print(f"{OUT} 저장 — year={yr} | 종목 {len(meta)}개")
    print(f"  fg_industry 채움 {sum(ind.values())}개 / 고유 {len(ind)}종")
    print(f"  fg_sector   채움 {sum(1 for v in meta.values() if v.get('fg_sector'))}개")
    return 0


if __name__ == "__main__":
    sys.exit(main())
