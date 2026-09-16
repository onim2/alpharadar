"""stockcard — 종목 카드 레이어 진입점.

스캐너가 고른 종목의 사실·지표를 모아 카드를 만든다. 수집·계산은 기계가 하고
판단은 사람이 한다. 주문 실행은 이 레이어의 범위 밖이다.

    python stockcard.py                          # 오늘 대상 전체, flow + chart
    python stockcard.py --step flow              # 수급만
    python stockcard.py --ticker 115440 --date 2026-09-16

기존 스캐너·점수·발송 경로는 건드리지 않는다. alpharadar.py 에서 헬퍼만 가져다 쓰고
테이블도 따로 만든다. 저장소 루트에서 실행해야 한다(경로 상수가 상대경로).

작업 순서: T0(수급·차트) → T1(DART 물량구조) → T2(신용·대차) → T3(지표)
         → T5(동종 그룹) → T4(베이스레이트) → T6(카드 생성) → T7(유형 태그)
지금 구현된 것은 T0 까지다.
"""

import argparse
import sys

import alpharadar as ar
import stockcard_common as sc

STEPS = ("flow", "chart", "card", "all")


def _targets(args, scfg, scan_date, run_type):
    if args.ticker:
        tickers = [str(t).zfill(6) for t in args.ticker]
        sc.logger().info(f"대상: 지정 {len(tickers)}종목 — {', '.join(tickers)}")
        return tickers

    # run_type 으로 좁히지 않는다 — 자정을 넘긴 저녁 런은 'am'으로 찍혀
    # 대상이 0종목이 될 수 있다(resolve_targets docstring 참고).
    tickers, origin = sc.resolve_targets(
        scan_date, None, scfg.get("watchlist_path"))
    sc.logger().info(
        f"대상 {origin['total']}종목 "
        f"(스캔 {origin['scan']} + watchlist 전용 {origin['watchlist_only']}) "
        f"| {scan_date} 라벨={run_type}")
    return tickers


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="stockcard — 종목 카드 레이어")
    p.add_argument("--step", choices=STEPS, default="all",
                   help="flow=수급 적재 · chart=차트 PNG · card=카드 생성(T6) · all")
    p.add_argument("--ticker", action="append",
                   help="특정 종목만 (반복 지정 가능). 없으면 스캔 대상 ∪ watchlist")
    p.add_argument("--date", default=None, help="기준일 YYYYMMDD 또는 YYYY-MM-DD")
    p.add_argument("--run-type", choices=["am", "pm"], default=None,
                   help="런 구분 강제 (기본: KST 정오 기준 자동)")
    args = p.parse_args(argv)

    cfg = sc.load_config()
    scfg = sc.sc_config(cfg)
    if not scfg.get("enabled", True):
        sc.logger().info("stockcard.enabled=false — 아무것도 하지 않는다")
        return 0

    scan_date = (args.date or ar.today_kst()).replace("-", "")
    run_type = args.run_type or ar.run_type_kst()
    sc.ensure_schema()

    tickers = _targets(args, scfg, scan_date, run_type)
    if not tickers:
        sc.logger().warning("대상 종목이 없다 — 종료")
        return 0

    want = {args.step} if args.step != "all" else {"flow", "chart", "card"}
    failed = []

    # 단계 하나가 넘어져도 나머지는 간다. 이 레이어는 발송에 영향을 주면 안 된다.
    if "flow" in want and (scfg.get("flow", {}) or {}).get("enabled", True):
        import stockcard_flow as flow
        try:
            flow.collect(tickers, run_type=run_type,
                         max_rows=(scfg.get("flow", {}) or {}).get("max_rows", 30))
        except Exception as e:
            sc.logger().error(f"수급 적재 실패: {type(e).__name__}: {e}")
            failed.append("flow")

    if "chart" in want and (scfg.get("chart", {}) or {}).get("enabled", True):
        import stockcard_chart as chart
        ccfg = scfg.get("chart", {}) or {}
        try:
            chart.render_all(tickers, scan_date, run_type,
                             chart.ticker_names(tickers, scan_date),
                             scfg.get("charts_dir", "data/charts"),
                             ccfg.get("bars", 60), ccfg.get("ma", 20))
        except Exception as e:
            sc.logger().error(f"차트 실패: {type(e).__name__}: {e}")
            failed.append("chart")

    if "card" in want:
        if args.step == "card":
            sc.logger().error(
                "카드 생성은 아직 없다 — T6 작업이다. "
                "지금 쓸 수 있는 단계: --step flow | chart")
            return 1
        sc.logger().info("카드 생성은 T6 — 이번 런에서는 건너뛴다")

    if failed:
        sc.logger().warning(f"실패한 단계: {', '.join(failed)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
