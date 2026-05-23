#!/usr/bin/env python3
"""
종목 점수 분해 — 총점이 어떻게 구성됐는지 분석

사용:
  python3 inspect_ticker.py 코리아써키트         # 종목명으로
  python3 inspect_ticker.py 007810              # 티커로
  python3 inspect_ticker.py 코리아써키트 20260523  # 날짜 지정
  python3 inspect_ticker.py --top 10            # dart_score 상위 10종목 모두
"""
import argparse
import pickle
import sys
from pathlib import Path

CACHE_DIR = Path("data/cache")


def load_results(date=None):
    if date:
        path = CACHE_DIR / f"results_{date}.pkl"
        if not path.exists():
            sys.exit(f"❌ 파일 없음: {path}")
        return pickle.load(open(path, "rb")), date
    files = sorted(CACHE_DIR.glob("results_*.pkl"))
    if not files:
        sys.exit(f"❌ {CACHE_DIR}에 results_*.pkl 없음.")
    latest = files[-1]
    return pickle.load(open(latest, "rb")), latest.stem.replace("results_", "")


def find_ticker(results, query):
    """이름 또는 티커로 검색"""
    q = query.strip()
    # 정확한 티커 매칭 우선
    for r in results:
        if r["ticker"] == q:
            return r
    # 이름 부분 매칭
    matches = [r for r in results if q in r["name"]]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        print(f"⚠️ '{q}' 일치 종목 여러 개:")
        for r in matches:
            print(f"   - {r['name']} ({r['ticker']})")
        sys.exit(1)
    return None


def inspect(r, cfg=None):
    """한 종목 상세 분해"""
    print(f"\n{'='*68}")
    print(f" 🎯 {r['name']} ({r['ticker']})")
    print(f"{'='*68}")
    print(f"\n  📌 기본 정보")
    print(f"     섹터:       {r.get('sector','?')}")
    print(f"     시총 등급:  {r.get('cap_tier','?')}")
    print(f"     소스:       {r.get('source','?')}")
    print(f"     현재가:     {r.get('current_price',0):,.0f}원 ({r.get('change_pct',0):+.2f}%)")

    score = r["score"]
    t = r.get("t", 0)
    s_text = r.get("s_text", 0)
    d = r.get("d", 0)
    # 가중치 (기본값)
    w_tech = 0.30
    w_text = 0.40
    w_cross = 0.30

    print(f"\n  🏆 총점: {score:.2f}  ({r.get('grade','-')})")
    print(f"     ┌─────────────────────────────────────────────────────────┐")
    print(f"     │  T (기술/수급)  × {w_tech:.2f} = {t:>5.1f} × 0.30 = {t*w_tech:>5.1f}   │")
    print(f"     │  S_text (뉴스+공시) × {w_text:.2f} = {s_text:>5.1f} × 0.40 = {s_text*w_text:>5.1f}   │")
    print(f"     │  D (크로스 신호) × {w_cross:.2f} = {d:>5.1f} × 0.30 = {d*w_cross:>5.1f}   │")
    print(f"     └─────────────────────────────────────────────────────────┘")
    print(f"     합계: {t*w_tech + s_text*w_text + d*w_cross:.2f}")

    # T 점수의 내부 구성
    print(f"\n  📊 T (기술적/수급) = {t}")
    print(f"     RSI:        {r.get('rsi',50):.1f}")
    print(f"     BB 위치:    {r.get('bb_pos',50):.1f}%")
    print(f"     이격도:     {r.get('disparity',100):.1f}%")
    print(f"     기관 순매수: {r.get('inst_net',0):+,}백만")
    print(f"     외인 순매수: {r.get('foreign_net',0):+,}백만")
    print(f"     기관 매수일수: {r.get('net_buy_days',0)}일")
    print(f"     거래량 기울기: {r.get('vol_slope',0):+.3f}")

    # S_text = 뉴스 × 0.6 + 공시 × 0.4 (run_step3 기본)
    news = r.get("news_score", 50)
    dart = r.get("dart_score", 50)
    print(f"\n  📝 S_text (뉴스+공시) = {s_text}")
    print(f"     news_score: {news:.1f}  (뉴스 {r.get('news_count',0)}건)")
    print(f"     dart_score: {dart:.1f}  (공시 {r.get('dart_count',0)}건)")
    if news != 50 or dart != 50:
        # 가중평균 역산 — 어떤 비율로 합쳐졌는지
        # s_text = news*nw + dart*dw, nw+dw=1
        # 보통 nw=0.6, dw=0.4
        nw = 0.6
        dw = 0.4
        print(f"     공식: news × {nw} + dart × {dw} = {news*nw:.1f} + {dart*dw:.1f} = {news*nw + dart*dw:.1f}")
    if r.get("best_headline"):
        print(f"     최고 호재 헤드라인: \"{r['best_headline'][:60]}...\" ({r.get('best_headline_pct',0):.0f}%)")
    if r.get("best_dart_title"):
        print(f"     최고 임팩트 공시: {r['best_dart_title']}")

    # D 점수 (크로스 신호)
    print(f"\n  ⚡ D (크로스 신호) = {d}")
    print(f"     N-Accel (수급+검색량 동조): {'✅' if r.get('n_accel') else '❌'}")
    print(f"     V-Surge (검색량 급증): {'✅' if r.get('v_surge') else '❌'}")
    print(f"     검색량 기울기: {r.get('hype_slope',0):+.3f}")
    print(f"     검색량 순위: {r.get('hype_rank',9999)}위")

    # 결정적 요인 진단
    print(f"\n  🔍 진단")
    parts = [
        ("T", t * w_tech, t),
        ("S_text", s_text * w_text, s_text),
        ("D", d * w_cross, d),
    ]
    parts.sort(key=lambda x: x[1], reverse=True)
    top = parts[0]
    print(f"     가장 큰 기여:  {top[0]} ({top[2]:.1f}점 → 가중 후 {top[1]:.1f})")

    # 약점
    if t < 40:
        print(f"     ⚠️ T 점수가 약함 — 수급/기술 신호 부족")
    if s_text < 50:
        print(f"     ⚠️ S_text가 약함 — 뉴스/공시 부정적이거나 부족")
    if d < 50:
        print(f"     ⚠️ D 점수가 약함 — 크로스 신호 부재")
    if d >= 70:
        print(f"     ✨ D 점수 강함 — N-Accel/V-Surge 잡힘")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="?", help="종목명 또는 티커")
    parser.add_argument("date", nargs="?", default=None)
    parser.add_argument("--top", type=int, default=0, help="dart_score 상위 N개")
    args = parser.parse_args()

    results, date = load_results(args.date)
    print(f"\n📅 분석 기준일: {date}  |  전체 {len(results)}개")

    if args.top > 0:
        # dart_score 상위 N개 비교
        has_dart = [r for r in results if r.get("dart_count", 0) > 0]
        top_n = sorted(has_dart, key=lambda x: x["dart_score"], reverse=True)[:args.top]
        for r in top_n:
            inspect(r)
        return

    if not args.query:
        parser.print_help()
        sys.exit(1)

    r = find_ticker(results, args.query)
    if not r:
        sys.exit(f"❌ '{args.query}' 종목 없음")
    inspect(r)


if __name__ == "__main__":
    main()
