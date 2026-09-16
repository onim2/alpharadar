"""stockcard 공통 — 대상 종목 해석 · 스키마 · 지연 로거.

기존 스캐너는 건드리지 않는다. alpharadar.py 에서 헬퍼만 가져다 쓰고
(_kis_get / _conn / today_kst / run_type_kst), 테이블 생성도 init_db() 가 아니라
여기 ensure_schema() 에서 한다. 점수·발송 경로에 회귀 위험을 0으로 두기 위해서다.

경로 상수가 전부 상대경로이므로 저장소 루트에서 실행해야 한다(CLAUDE.md).
"""

import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import yaml

import alpharadar as ar  # noqa: E402  — load_dotenv() 와 KIS/DB 헬퍼를 함께 얻는다

KST = ar.KST
LOG_DIR = Path("data/logs")

_LOGGER = None


def logger() -> logging.Logger:
    """지연 초기화 로거.

    모듈 import 시점에 핸들러를 붙이면 LOG_DIR 이 그때 고정돼, 실행 디렉터리나
    날짜가 바뀌어도 첫 경로를 계속 쓴다(8/24 이월 과제). 처음 쓰이는 순간에
    만들고, 파일 핸들러는 디렉터리가 있을 때만 붙인다.
    """
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    lg = logging.getLogger("stockcard")
    if not lg.handlers:
        lg.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] stockcard — %(message)s")
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        lg.addHandler(sh)
        try:
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(
                LOG_DIR / f"stockcard_{ar.today_kst()}.log", encoding="utf-8"
            )
            fh.setFormatter(fmt)
            lg.addHandler(fh)
        except OSError:
            # 로그 파일을 못 만드는 것이 수집을 막을 이유는 없다. 콘솔로 계속 간다.
            pass
        lg.propagate = False
    _LOGGER = lg
    return _LOGGER


def now_kst() -> str:
    """수집 시각(as_of). 초 단위 ISO, KST 고정."""
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def load_config() -> dict:
    if not Path("config.yaml").exists():
        return {}
    with open("config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def sc_config(cfg: dict | None = None) -> dict:
    """config.yaml 의 stockcard 절. 없으면 빈 dict — 호출부가 기본값을 정한다."""
    return (cfg if cfg is not None else load_config()).get("stockcard", {}) or {}


# ── 대상 종목 ────────────────────────────────────────────────────────────────

_TICKER_RE = re.compile(r"^\d{6}$")


def load_watchlist(path=None) -> list[str]:
    """수동 관심종목. 한 줄에 6자리 코드 하나, '#' 뒤는 주석.

    스캐너 유니버스는 시총 2,000억 하한이라 그 아래 종목은 어디에도 잡히지 않는다
    (우리넷 1,044억). 카드로 보고 싶은 풀 밖 종목이 여기 들어온다.
    형식이 어긋난 줄은 조용히 버리지 않고 경고로 남긴다 — 오타 한 글자로 종목이
    사라지는 것이 가장 알아채기 어렵다.
    """
    if path is None:
        path = sc_config().get("watchlist_path", "data/watchlist.txt")
    p = Path(path)
    if not p.exists():
        logger().warning(f"watchlist 없음: {p}")
        return []

    out, bad = [], []
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if _TICKER_RE.match(line):
            out.append(line)
        else:
            bad.append(raw.strip())
    if bad:
        logger().warning(f"watchlist 형식 오류 {len(bad)}줄 무시: {bad[:3]}")
    # 순서를 지키며 중복만 제거한다
    return list(dict.fromkeys(out))


def scan_targets(scan_date: str, run_type: str | None = None) -> list[str]:
    """그 날 스코어링까지 간 종목(= POOL_B). 발송분은 이 안에 포함된다.

    scan_results 는 run_step3 를 통과한 종목만 담으므로 POOL_B 와 같고,
    sent_history(하한 45 통과)는 그 부분집합이다. 둘을 따로 합칠 필요가 없다.
    """
    sql = "SELECT DISTINCT ticker FROM scan_results WHERE scan_date = ?"
    args = [scan_date]
    if run_type:
        sql += " AND run_type = ?"
        args.append(run_type)
    try:
        with ar._conn() as con:
            rows = con.execute(sql, args).fetchall()
    except Exception as e:  # DB가 없거나 스키마가 다를 때
        logger().warning(f"scan_results 조회 실패: {type(e).__name__}: {e}")
        return []
    return [str(r[0]).zfill(6) for r in rows]


def resolve_targets(scan_date: str, run_type: str | None = None,
                    watchlist_path=None) -> tuple[list[str], dict]:
    """카드 대상 = 스캔 통과 종목 ∪ watchlist.

    반환: (정렬된 종목 목록, 출처 요약). 출처는 로그·보고용이다.
    """
    scanned = scan_targets(scan_date, run_type)
    watch = load_watchlist(watchlist_path)
    merged = list(dict.fromkeys([*scanned, *watch]))
    origin = {
        "scan": len(scanned),
        "watchlist": len(watch),
        "watchlist_only": len([t for t in watch if t not in set(scanned)]),
        "total": len(merged),
    }
    return sorted(merged), origin


# ── 스키마 ───────────────────────────────────────────────────────────────────

_SCHEMA = """
-- 투자자별 일별 수급. KIS inquire-investor 응답(날짜당 1행 wide)을 언피벗한 정본이다.
-- 22컬럼 = 날짜단위 4 + 투자자 3 × 6 으로 정확히 나뉘므로 long 변환에 손실이 없다.
--
-- investor 는 prsn(개인) / frgn(외국인) / orgn(기관) 3종뿐이다. KIS가 기타법인·
-- 투신·연기금을 세분화하지 않으므로 자사주(기타법인) 매입은 이 테이블로 판별할 수
-- 없다 — 카드에는 '미확인(KIS 미제공)'으로 두고 채우지 않는다.
--
-- append 가 아니라 upsert 다. 응답이 매번 최근 30영업일을 통째로 돌려주므로
-- 아침·저녁·수동 재실행이 같은 날짜를 중복 적재한다. 같은 PK에 더 늦은 as_of 가
-- 오면 덮어쓴다 — 장중 잠정치가 마감 후 확정치로 갱신되는 과정이 그렇게 남는다.
CREATE TABLE IF NOT EXISTS investor_flow (
    ticker       TEXT NOT NULL,
    date         TEXT NOT NULL,   -- stck_bsop_date, YYYYMMDD (매매일)
    investor     TEXT NOT NULL,   -- prsn | frgn | orgn
    ntby_qty     INTEGER,         -- 순매수 수량
    ntby_tr_pbmn INTEGER,         -- 순매수 거래대금
    shnu_vol     INTEGER,         -- 매수 수량
    shnu_tr_pbmn INTEGER,         -- 매수 거래대금
    seln_vol     INTEGER,         -- 매도 수량
    seln_tr_pbmn INTEGER,         -- 매도 거래대금
    as_of        TEXT NOT NULL,   -- 수집 시각 KST
    run_type     TEXT,
    PRIMARY KEY (ticker, date, investor)
);

CREATE INDEX IF NOT EXISTS idx_investor_flow_ticker_date
    ON investor_flow (ticker, date);

-- 날짜 단위 값(투자자와 무관한 4컬럼). investor_flow 세 행에 같은 값을 세 번
-- 싣지 않으려고 분리했다.
CREATE TABLE IF NOT EXISTS investor_flow_daily (
    ticker   TEXT NOT NULL,
    date     TEXT NOT NULL,
    close    INTEGER,             -- stck_clpr
    chg      INTEGER,             -- prdy_vrss
    sign     TEXT,                -- prdy_vrss_sign
    as_of    TEXT NOT NULL,
    run_type TEXT,
    PRIMARY KEY (ticker, date)
);
"""


def ensure_schema() -> None:
    """stockcard 테이블 생성. 멱등이고, 기존 테이블은 건드리지 않는다."""
    ar.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ar._conn() as con:
        con.executescript(_SCHEMA)


# ── 값 변환 ──────────────────────────────────────────────────────────────────

def to_int_or_none(v):
    """KIS 문자열 → int 또는 None.

    alpharadar._safe_int 는 실패를 0으로 채운다. 여기서는 그러면 안 된다 —
    '조회 실패'와 '순매수 0'은 다른 사실이고, 0으로 적으면 되돌릴 수 없다(9/15 이슈 ③).
    """
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s in ("", "-"):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None
