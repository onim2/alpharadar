"""T0-a — 투자자별 수급 원시행 적재 (investor_flow).

KIS inquire-investor 응답을 언피벗해 DB에 upsert 한다. 스캐너의 _get_investor_data
와는 별도 경로다 — 그쪽은 유니버스 909종목을 돌면서 5일 합계만 내고 버린다.
여기 후킹하지 않는 이유:

  909종목 × 30일 × 3투자자 = 8.2만 행이 첫 런에 들어오는데, 이 DB는 매 런 git에
  바이너리로 커밋된다(하루 3회). 대상 종목(발송·POOL_B·watchlist, 약 30개)만
  담아도 응답이 매번 최근 30영업일을 통째로 주므로, 새로 편입된 종목이 첫 카드에서
  30일 이력을 자동으로 갖는다 — 소급 채움이 필요 없다.

스캐너와 다른 점이 하나 더 있다. _get_investor_data 는 당일 행을 뺀다(두 런이 서로
다른 5일 창을 보면 net_buy_days 가 뒤집히기 때문). 여기서는 당일 행도 적재한다 —
이 테이블은 점수가 아니라 기록이고, 장중 잠정치가 마감 후 확정치로 갱신되는 과정을
as_of 로 남기는 것이 목적이기 때문이다.
"""

import argparse

import alpharadar as ar
import stockcard_common as sc

# KIS 국내주식 투자자 매매동향 (일별)
TR_ID = "FHKST01010900"
PATH = "/uapi/domestic-stock/v1/quotations/inquire-investor"

# 응답의 투자자 접두어. KIS는 이 3종만 준다 — 기타법인·투신·연기금 세분화가 없다.
INVESTORS = {"prsn": "개인", "frgn": "외국인", "orgn": "기관"}

# 투자자당 6개 수치. 22컬럼 = 날짜단위 4 + 3 × 6 이라 언피벗에 손실이 없다.
FIELDS = ("ntby_qty", "ntby_tr_pbmn", "shnu_vol", "shnu_tr_pbmn",
          "seln_vol", "seln_tr_pbmn")

_UPSERT_FLOW = f"""
INSERT INTO investor_flow
    (ticker, date, investor, {', '.join(FIELDS)}, as_of, run_type)
VALUES ({', '.join('?' * (3 + len(FIELDS) + 2))})
ON CONFLICT(ticker, date, investor) DO UPDATE SET
    {', '.join(f'{f} = excluded.{f}' for f in FIELDS)},
    as_of = excluded.as_of,
    run_type = excluded.run_type
WHERE excluded.as_of > investor_flow.as_of
"""

_UPSERT_DAILY = """
INSERT INTO investor_flow_daily
    (ticker, date, close_krx, base_price, base_chg, base_sign, as_of, run_type)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(ticker, date) DO UPDATE SET
    close_krx = excluded.close_krx, base_price = excluded.base_price,
    base_chg = excluded.base_chg, base_sign = excluded.base_sign,
    as_of = excluded.as_of, run_type = excluded.run_type
WHERE excluded.as_of > investor_flow_daily.as_of
"""


def fetch_investor_rows(ticker: str) -> list[dict]:
    """KIS 원시 응답 행. 실패하면 빈 리스트 — 0으로 채우지 않는다.

    _kis_get 은 예외를 던지지 않고 실패 시 {} 를 준다. 그 경우와 정상 빈 응답을
    구분하지 않는 이유는 어느 쪽이든 '적재할 사실이 없다'로 같기 때문이다.
    """
    data = ar._kis_get(PATH, params={
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": str(ticker).zfill(6),
    }, tr_id=TR_ID)
    rows = data.get("output", data.get("output2", []))
    return rows if isinstance(rows, list) else []


def fdr_closes(ticker: str, dates) -> dict:
    """정규장 종가(FDR) — 정본.

    KIS stck_clpr 은 2026-09-14부터 시간외를 반영한 '익일 기준가'라 값이 다르다.
    파이프라인의 등락률·RSI·이격도·MA 가 전부 FDR 계열에서 나오므로, 카드에서
    지표와 나란히 놓을 수 있는 가격은 이쪽뿐이다.

    실패하면 빈 dict — close_krx 가 null 로 남을 뿐 수급 적재는 계속된다.
    """
    if not dates:
        return {}
    try:
        import FinanceDataReader as fdr

        lo, hi = min(dates), max(dates)
        df = fdr.DataReader(str(ticker).zfill(6),
                            f"{lo[:4]}-{lo[4:6]}-{lo[6:]}",
                            f"{hi[:4]}-{hi[4:6]}-{hi[6:]}")
        if df is None or not len(df) or "Close" not in df.columns:
            return {}
        return {d.strftime("%Y%m%d"): int(v) for d, v in df["Close"].items()}
    except Exception as e:
        sc.logger().warning(f"FDR 종가 조회 실패 ({ticker}): {type(e).__name__}: {e}")
        return {}


def unpivot(ticker: str, raw: dict, as_of: str, run_type: str | None,
            close_krx=None):
    """원시 1행(wide) → 투자자 3행(long) + 날짜행 1건.

    6개 값이 전부 결측인 투자자는 행을 만들지 않는다. 미집계 구간을 null 행으로
    채우면 '조회했으나 값이 없음'과 '그날 그 투자자가 없음'이 구분되지 않는다.
    """
    date = str(raw.get("stck_bsop_date", "")).strip()
    if not date:
        return [], None

    ticker = str(ticker).zfill(6)
    flow = []
    for pre in INVESTORS:
        vals = [sc.to_int_or_none(raw.get(f"{pre}_{f}")) for f in FIELDS]
        if all(v is None for v in vals):
            continue
        flow.append((ticker, date, pre, *vals, as_of, run_type))

    sign = str(raw.get("prdy_vrss_sign") or "").strip() or None
    daily = (ticker, date,
             close_krx,                                  # 정규장 종가 (FDR, 정본)
             sc.to_int_or_none(raw.get("stck_clpr")),    # 익일 기준가 (KIS)
             sc.to_int_or_none(raw.get("prdy_vrss")),    # 기준가 대비
             sign, as_of, run_type)
    return flow, daily


def _classify(existing: dict, key, as_of: str) -> str:
    """이 행이 신규인지, 갱신인지, 무시인지. upsert 의 WHERE 절과 같은 규칙."""
    prev = existing.get(key)
    if prev is None:
        return "inserted"
    return "updated" if as_of > prev else "skipped"


def collect(tickers, run_type=None, as_of=None, max_rows=30) -> dict:
    """대상 종목의 수급을 적재하고 통계를 돌려준다.

    한 종목이 실패해도 나머지는 계속한다 — 이 레이어는 발송에 영향을 주면 안 된다.
    """
    log = sc.logger()
    sc.ensure_schema()
    as_of = as_of or sc.now_kst()
    run_type = run_type or ar.run_type_kst()

    stats = {"tickers": 0, "failed": 0, "rows": 0,
             "inserted": 0, "updated": 0, "skipped": 0}

    with ar._conn() as con:
        for ticker in tickers:
            ticker = str(ticker).zfill(6)
            try:
                raws = fetch_investor_rows(ticker)
            except Exception as e:
                log.warning(f"수급 조회 실패 ({ticker}): {type(e).__name__}: {e}")
                stats["failed"] += 1
                continue
            if not raws:
                log.warning(f"수급 응답 없음 ({ticker}) — 적재 생략")
                stats["failed"] += 1
                continue

            existing = {
                (d, i): a for d, i, a in con.execute(
                    "SELECT date, investor, as_of FROM investor_flow WHERE ticker = ?",
                    (ticker,))
            }
            window = raws[:max_rows]
            # 정규장 종가는 FDR 에서 따로 받는다. KIS 계열과 다른 숫자이고,
            # 지표와 비교 가능한 쪽은 FDR 이다(2026-09-14 이후 두 계열이 갈린다).
            closes = fdr_closes(
                ticker, [str(r.get("stck_bsop_date", "")).strip()
                         for r in window if r.get("stck_bsop_date")])

            for raw in window:
                flow, daily = unpivot(
                    ticker, raw, as_of, run_type,
                    closes.get(str(raw.get("stck_bsop_date", "")).strip()))
                for row in flow:
                    # 통계는 투자자 행 기준으로만 센다. 날짜행은 같은 규칙으로
                    # 움직이므로 따로 세면 숫자가 두 배로 보인다.
                    stats[_classify(existing, (row[1], row[2]), as_of)] += 1
                    con.execute(_UPSERT_FLOW, row)
                    stats["rows"] += 1
                if daily:
                    con.execute(_UPSERT_DAILY, daily)

            stats["tickers"] += 1

    log.info(
        f"수급 적재: {stats['tickers']}종목 (실패 {stats['failed']}) | "
        f"신규 {stats['inserted']} · 갱신 {stats['updated']} · 무시 {stats['skipped']}"
    )
    return stats


def main(argv=None):
    p = argparse.ArgumentParser(description="stockcard T0-a — 투자자 수급 적재")
    p.add_argument("--ticker", action="append", help="특정 종목만 (반복 지정 가능)")
    p.add_argument("--date", default=None, help="대상 판정 기준일 YYYYMMDD (기본: 오늘)")
    p.add_argument("--run-type", choices=["am", "pm"], default=None)
    p.add_argument("--db", default=None,
                   help="쓸 DB 경로. 없으면 실전 DB에 쓰지 않는다")
    args = p.parse_args(argv)

    sc.logger().info(f"DB: {sc.resolve_db(args.db)}")
    cfg = sc.load_config()
    scfg = sc.sc_config(cfg)
    scan_date = (args.date or ar.today_kst()).replace("-", "")
    run_type = args.run_type or ar.run_type_kst()

    if args.ticker:
        tickers = [str(t).zfill(6) for t in args.ticker]
        sc.logger().info(f"대상: 지정 {len(tickers)}종목")
    else:
        # run_type 으로 좁히지 않는 이유는 resolve_targets docstring 참고.
        tickers, origin = sc.resolve_targets(
            scan_date, None, scfg.get("watchlist_path"))
        sc.logger().info(
            f"대상 {origin['total']}종목 "
            f"(스캔 {origin['scan']} + watchlist 전용 {origin['watchlist_only']}) "
            f"| {scan_date} 라벨={run_type}")

    if not tickers:
        sc.logger().warning("대상 종목이 없다 — 적재 생략")
        return 0

    collect(tickers, run_type=run_type,
            max_rows=(scfg.get("flow", {}) or {}).get("max_rows", 30))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
