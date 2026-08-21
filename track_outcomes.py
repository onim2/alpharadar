#!/usr/bin/env python3
# ══════════════════════════════════════════════════════════════════════════════
# track_outcomes.py — 스캔·gated 종목의 forward return 추적 (로컬+Actions 겸용, 멱등)
#
# scan_results(발송 후보)와 gated_tickers(과열 배제)의 과거 행에 대해
# FinanceDataReader로 T+1/T+5/T+10/T+20 '거래일' 종가 수익률을 계산해
# outcomes 테이블에 저장한다. 진입가 = scan_date 당일(또는 그 이후 첫 거래일) 종가.
#
# 멱등성: 이미 4개 지평이 모두 채워진 (scan_date,ticker,origin)은 스킵.
#         미도래(미래) 지평은 NULL로 두고 다음 실행에서 채움. 신규 PK 삽입 0이면 멱등.
#
# ── 사전 등록(pre-registered) 주 지표 ──────────────────────────────────────────
#   [과열필터 판정] median fwd10(origin='gated', reason LIKE 'overheat%')
#                   < median fwd10(origin='scan', grade in ('집중','주시'))
#       → 참이면 "배제한 종목이 실제로 덜 올랐다" = 과열 필터가 옳게 작동.
#   [Shadow 판정]  Spearman IC(score_presurge, fwd10)
#                   > Spearman IC(score_legacy=score_total, fwd10)   (고유 종목 n>=30)
#       → 참이면 물밑 스코어가 legacy보다 예측력 우위 → pre_surge_mode 승격 검토.
#   ※ 판정은 별도 분석에서 수행. 본 스크립트는 outcomes 적재만 담당.
# ══════════════════════════════════════════════════════════════════════════════
import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

import pandas as pd

KST = timezone(timedelta(hours=9))
LOG_DIR = Path("data/logs")


def _setup_logging():
    """스캐너가 쓰는 그 날짜 로그 파일에 이어 쓴다.

    stdout만 쓰면 적재 결과가 Actions 콘솔에만 남고 90일 뒤 사라진다. 대조군
    추적이 언제부터 몇 건씩 밀렸는지 사후에 물을 수 없다. 워크플로가 커밋하는
    data/logs/scanner_*.log에 붙여 스캔 로그와 같은 자리에서 보이게 한다.
    날짜 라벨은 스캐너와 같은 KST 기준 — 러너는 UTC로 돈다.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(KST).strftime("%Y%m%d")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] track_outcomes — %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_DIR / f"scanner_{date_str}.log", encoding="utf-8"),
        ])


_setup_logging()
logger = logging.getLogger(__name__)

DB_PATH = Path("data/scores_history.db")
HORIZONS = [1, 5, 10, 20]          # 거래일
COL = {1: "fwd1", 5: "fwd5", 10: "fwd10", 20: "fwd20"}


@lru_cache(maxsize=1)
def _track_control() -> bool:
    """대조군(pool_history)까지 추적할지 — config의 control_sample.track_outcomes.

    끄고 싶은 이유가 실재한다. 대조군은 하루 ~160종목이 늘고 종목마다 가격 조회가
    붙는데, 이 스크립트는 스캔(32~44분) 뒤 같은 60분 워크플로 안에서 돈다.
    시간이 모자라면 스캔 결과 자체를 잃는 것보다 대조군을 포기하는 편이 낫다.
    설정이 없으면 켠 것으로 본다 — 대조군 없이는 성과 해석이 성립하지 않는다.
    """
    try:
        import yaml
        cfg = yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8"))
        return bool((cfg.get("control_sample") or {}).get("track_outcomes", True))
    except Exception as e:
        logger.warning(f"config 읽기 실패 → 대조군 추적 켬 ({type(e).__name__}: {e})")
        return True


def _init_outcomes(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS outcomes (
            scan_date TEXT, ticker TEXT, origin TEXT,
            fwd1 REAL, fwd5 REAL, fwd10 REAL, fwd20 REAL,
            PRIMARY KEY (scan_date, ticker, origin)
        )
    """)


def _load_targets(con):
    """(scan_date, ticker, origin) 목록 — scan_results + gated_tickers + pool_history.

    pool_history는 대조군 표본이다. 이게 추적되지 않으면 "스캔을 통과한 종목이
    정말 나은가"를 물을 상대가 없다. 실제로 그 부재 때문에 비교 대상이 무작위
    KRX 표본밖에 없었고, 거기엔 유니버스 필터(주가·시총·거래대금)를 통과하지
    못할 종목이 섞여 결론이 뒤집혔다 — 무작위 대비로는 +30% 급등 2.3배였는데
    유니버스를 맞추자 오히려 열세로 나왔다.

    origin 값:
      scan       스캔 통과 (스코어링·발송 대상)
      gated      과열 배제 (전수)
      ns         유니버스는 통과했으나 신호 없음 (POOL_A 미진입) — 표본
      f:<사유>   신호는 있었으나 하드필터 탈락 — 사유별 표본
                 (ma_trend / turnover / disp_upper / disp_lower / rsi / overheat)

    사유를 origin에 접어 넣는 이유: outcomes 스키마를 바꾸지 않고도 사유별로
    성과를 가를 수 있어야 한다. "MA추세로 자른 종목이 실제로 못 갔는가" 같은
    질문이 여기서 답해진다 — 지금은 근거 없이 하루 600~1,000개를 자르고 있다.
    """
    targets = []
    for row in con.execute("SELECT DISTINCT scan_date, ticker FROM scan_results"):
        targets.append((row[0], row[1], "scan"))
    # gated_tickers는 Task3 이후에만 존재 → 없으면 무시
    try:
        for row in con.execute("SELECT DISTINCT scan_date, ticker FROM gated_tickers"):
            targets.append((row[0], row[1], "gated"))
    except sqlite3.OperationalError:
        logger.info("gated_tickers 테이블 없음 → scan만 추적")
    # pool_history도 마찬가지로 나중에 생긴 테이블이라 없을 수 있다
    if not _track_control():
        logger.info("control_sample.track_outcomes=false → 대조군 추적 생략")
        return targets
    try:
        n = 0
        for row in con.execute(
                "SELECT DISTINCT scan_date, ticker, stage, reason FROM pool_history"):
            origin = "ns" if row[2] == "no_signal" else f"f:{row[3]}"
            targets.append((row[0], row[1], origin))
            n += 1
        if n:
            logger.info(f"pool_history 대조군 {n}건 추적 대상 포함")
    except sqlite3.OperationalError:
        logger.info("pool_history 테이블 없음 → 대조군 추적 생략")
    return targets


def _load_existing(con):
    """{(scan_date,ticker,origin): {fwd1..fwd20}} — 기존 outcomes."""
    out = {}
    for r in con.execute("SELECT scan_date,ticker,origin,fwd1,fwd5,fwd10,fwd20 FROM outcomes"):
        out[(r[0], r[1], r[2])] = {"fwd1": r[3], "fwd5": r[4], "fwd10": r[5], "fwd20": r[6]}
    return out


def _fetch_prices(tickers, start, end):
    import FinanceDataReader as fdr
    cache = {}
    uniq = sorted(set(tickers))
    failed = []
    for tk in uniq:
        try:
            df = fdr.DataReader(tk, start, end)
            if df is not None and not df.empty and "Close" in df.columns:
                cache[tk] = df["Close"].astype(float)
            else:
                failed.append((tk, "빈 응답"))
        except Exception as e:
            failed.append((tk, f"{type(e).__name__}: {e}"))
    # debug로 묻어두면 추적이 조용히 비는 것과 구별되지 않는다. 종목마다 한 줄씩
    # 쌓으면 수백 줄이 되므로 한 줄로 접어 warning으로 올린다.
    if failed:
        head = ", ".join(f"{tk}({why})" for tk, why in failed[:5])
        logger.warning(f"가격 조회 실패 {len(failed)}/{len(uniq)}종목 — {head}"
                       + (" …" if len(failed) > 5 else ""))
    return cache


def _forward_returns(close_s, scan_date):
    """진입가=scan_date 당일 이후 첫 거래일 종가. 각 지평 h거래일 후 수익률(%). 미도래→None."""
    scan_dt = pd.to_datetime(scan_date, format="%Y%m%d")
    idx0 = close_s.index.searchsorted(scan_dt, side="left")  # date >= scan_date 첫 바
    if idx0 >= len(close_s):
        return {COL[h]: None for h in HORIZONS}
    entry = float(close_s.iloc[idx0])
    res = {}
    for h in HORIZONS:
        j = idx0 + h
        if entry > 0 and j < len(close_s):
            res[COL[h]] = round((float(close_s.iloc[j]) / entry - 1) * 100, 4)
        else:
            res[COL[h]] = None          # 미도래
    return res


def main():
    ap = argparse.ArgumentParser(description="forward return 추적 (멱등)")
    ap.add_argument("--db", default=str(DB_PATH))
    args = ap.parse_args()
    db = Path(args.db)
    if not db.exists():
        logger.error(f"DB 없음: {db}")
        return

    con = sqlite3.connect(db)
    _init_outcomes(con)
    con.commit()

    targets  = _load_targets(con)
    existing = _load_existing(con)

    def _incomplete(vals) -> bool:
        return any(vals.get(COL[h]) is None for h in HORIZONS)

    # 이미 4개 지평 모두 채워진 키는 스킵 (재조회 불필요)
    pending = [t for t in targets if _incomplete(existing.get(t, {}))]

    # targets에서 사라진 미완 키(고아)도 계속 추적 대상에 넣는다.
    #
    # pool_history의 PK는 (scan_date, ticker, stage)라 reason이 빠져 있다.
    # 같은 scan_date에 아침·저녁 두 런이 도는데(둘 다 KST 같은 날짜 라벨),
    # 두 번째 런이 같은 종목의 reason을 덮어쓰면 첫 런이 만들어둔 outcomes의
    # origin='f:<옛 reason>' 행이 targets에서 통째로 사라진다. 그러면 그 행은
    # 4지평 전부 NULL인 채 영원히 고정된다 — 2026-08-20 실측 44건(대조군 2.2%),
    # 8/13·8/18·8/19에 발생했다. 가격을 못 구해서가 아니다: 같은 날짜·같은
    # 종목의 다른 origin 행은 값이 멀쩡히 채워져 있다.
    #
    # 대조군 추적이 꺼져 있으면(런타임 예산) 대조군 고아는 되살리지 않는다 —
    # 끈 이유가 조회 종목 수를 줄이는 것이었으므로.
    seen = set(targets)
    orphans = [k for k, v in existing.items()
               if k not in seen and _incomplete(v)
               and (_track_control() or k[2] in ("scan", "gated"))]
    if orphans:
        logger.info(f"고아 행 {len(orphans)}건 복원 (targets에서 탈락한 미완 키)")
        pending += orphans

    logger.info(f"대상 {len(targets)}건 | 갱신 필요 {len(pending)}건 | 완료 스킵 {len(targets)-len(pending)+len(orphans)}건")
    if not pending:
        logger.info("갱신할 outcomes 없음 (전부 완료) — 멱등 종료")
        con.close()
        return

    # 가격 조회는 고유 종목 수에 비례하고, 이 스크립트는 스캔 뒤 같은 60분 워크플로
    # 안에서 돈다. 대조군이 얼마나 부담을 더하는지 로그로 드러내야 timeout이
    # 닥쳤을 때 무엇을 끌지 판단할 수 있다.
    _ctrl = {t for t in pending if t[2] == "ns" or t[2].startswith("f:")}
    _base_tk = {t[1] for t in pending if t not in _ctrl}
    _ctrl_tk = {t[1] for t in _ctrl} - _base_tk
    logger.info(f"  가격 조회 고유 종목 {len(_base_tk) + len(_ctrl_tk)}개 "
                f"(기존 {len(_base_tk)} + 대조군 추가 {len(_ctrl_tk)})")

    dates = [t[0] for t in pending]
    lo = pd.to_datetime(min(dates), format="%Y%m%d") - timedelta(days=7)
    hi = pd.to_datetime(max(dates), format="%Y%m%d") + timedelta(days=45)
    prices = _fetch_prices([t[1] for t in pending],
                           lo.strftime("%Y-%m-%d"), hi.strftime("%Y-%m-%d"))

    new_pk = 0      # 신규 PK(멱등 측정용)
    updated = 0
    for (scan_date, ticker, origin) in pending:
        if ticker not in prices:
            continue
        vals = _forward_returns(prices[ticker], scan_date)
        prev = existing.get((scan_date, ticker, origin))
        # 기존 non-null 값은 보존, 새로 도래한 지평만 채움
        if prev:
            merged = {c: (prev.get(c) if prev.get(c) is not None else vals.get(c)) for c in COL.values()}
            if merged == prev:            # 변화 없음(전부 미도래 유지 등) → 재기록 생략
                continue
        else:
            merged = vals
            new_pk += 1
        con.execute(
            "INSERT OR REPLACE INTO outcomes (scan_date,ticker,origin,fwd1,fwd5,fwd10,fwd20) "
            "VALUES (?,?,?,?,?,?,?)",
            (scan_date, ticker, origin, merged["fwd1"], merged["fwd5"], merged["fwd10"], merged["fwd20"]))
        updated += 1
    con.commit()
    con.close()
    logger.info(f"outcomes 적재: 신규 {new_pk}건 | 갱신(신규+보완) {updated}건")


if __name__ == "__main__":
    main()
