#!/usr/bin/env python3
"""
DART 공시 점수 분포 분석 — Phase 2 효과 검증

사용:
  python3 analyze_dart.py                  # 최신 실행 결과
  python3 analyze_dart.py 20260523         # 특정 날짜
  python3 analyze_dart.py --csv            # CSV로도 저장

출력:
  - 종목별 dart_score 분포 (히스토그램)
  - 강호재/중립/부정 추정 분포
  - dart_score가 등급에 미친 영향 (집중/주시/참고)
  - 상위/하위 10종목 상세 (왜 그 점수인지 추정)
"""
import argparse
import pickle
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

CACHE_DIR = Path("data/cache")

def load_results(date=None):
    """results_YYYYMMDD.pkl 로드"""
    if date:
        path = CACHE_DIR / f"results_{date}.pkl"
        if not path.exists():
            sys.exit(f"❌ 파일 없음: {path}")
        return pickle.load(open(path, "rb")), date
    # 가장 최신 파일
    files = sorted(CACHE_DIR.glob("results_*.pkl"))
    if not files:
        sys.exit(f"❌ {CACHE_DIR}에 results_*.pkl 없음. alpharadar.py 먼저 실행하세요.")
    latest = files[-1]
    date = latest.stem.replace("results_", "")
    return pickle.load(open(latest, "rb")), date

def histogram(values, bins=10, width=40):
    """간단한 ASCII 히스토그램"""
    if not values: return
    lo, hi = min(values), max(values)
    if lo == hi:
        print(f"  모든 값이 {lo} (분포 없음)")
        return
    bin_size = (hi - lo) / bins
    buckets = [0] * bins
    for v in values:
        idx = min(int((v - lo) / bin_size), bins - 1)
        buckets[idx] += 1
    max_count = max(buckets)
    for i, cnt in enumerate(buckets):
        lo_b = lo + i * bin_size
        hi_b = lo + (i+1) * bin_size
        bar = "█" * int(cnt / max_count * width) if max_count > 0 else ""
        print(f"  [{lo_b:5.1f}~{hi_b:5.1f}] {bar} {cnt}")

def categorize_dart_score(score):
    """dart_score를 카테고리로 추정 — 키워드+FinBERT 50:50 가중평균 가정"""
    # 키워드 점수: 25(부정) / 50(중립/없음) / 60(중립호재) / 75(강호재)
    # 결합: (FinBERT + 키워드) / 2, FinBERT는 보통 45~55 분포
    if score <= 38:    return "강부정"
    if score <= 47:    return "부정"
    if score <= 53:    return "중립"
    if score <= 60:    return "중립호재"
    return "강호재"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", nargs="?", default=None,
                       help="YYYYMMDD (생략 시 최신)")
    parser.add_argument("--csv", action="store_true", help="CSV 저장")
    args = parser.parse_args()

    results, date = load_results(args.date)
    print(f"\n{'='*72}")
    print(f" DART 점수 분포 분석 — {date}")
    print(f"{'='*72}")

    if not results:
        sys.exit("결과 없음")

    print(f"\n📊 전체 종목: {len(results)}개")

    # 1. 공시 데이터가 있는 종목 vs 없는 종목
    has_dart = [r for r in results if r.get("dart_count", 0) > 0]
    no_dart  = [r for r in results if r.get("dart_count", 0) == 0]
    print(f"  - 공시 있음: {len(has_dart)}개 ({len(has_dart)*100//max(len(results),1)}%)")
    print(f"  - 공시 없음: {len(no_dart)}개 (dart_score=50 기본값)")

    if not has_dart:
        print("\n⚠️ 공시 데이터가 있는 종목이 없습니다. corp_map 매핑 확인 필요.")
        return

    # 2. dart_score 분포 히스토그램
    print(f"\n📈 dart_score 분포 (공시 있는 {len(has_dart)}개)")
    scores = [r["dart_score"] for r in has_dart]
    histogram(scores)

    # 3. 카테고리 추정 분포
    print(f"\n🎯 추정 카테고리 분포")
    cats = Counter(categorize_dart_score(s) for s in scores)
    for cat in ["강부정", "부정", "중립", "중립호재", "강호재"]:
        cnt = cats[cat]
        pct = cnt * 100 // len(scores) if scores else 0
        icon = {"강부정":"⚫", "부정":"🔴", "중립":"⚪", "중립호재":"🟡", "강호재":"🟢"}[cat]
        print(f"  {icon} {cat:<6} {cnt:>4}개 ({pct:>2}%)")

    # 4. 등급별 dart_score 평균
    print(f"\n🏆 등급별 평균 dart_score (등급 관계 확인)")
    for grade in ["집중", "주시", "참고"]:
        rs = [r for r in results if r.get("grade") == grade and r.get("dart_count", 0) > 0]
        if rs:
            avg = sum(r["dart_score"] for r in rs) / len(rs)
            print(f"  {grade}: {len(rs)}개, 평균 dart_score = {avg:.1f}")
        else:
            print(f"  {grade}: 공시 있는 종목 없음")

    # 5. 상위 10 (강호재 추정)
    print(f"\n🟢 dart_score 상위 10 (강호재 추정)")
    top = sorted(has_dart, key=lambda r: r["dart_score"], reverse=True)[:10]
    print(f"  {'종목':<20} {'섹터':<15} {'dart':<6} {'공시수':<5} {'총점':<6} 등급")
    for r in top:
        print(f"  {r['name']:<20} {r['sector']:<15} {r['dart_score']:<6.1f} "
              f"{r['dart_count']:<5} {r['score']:<6.1f} {r.get('grade','-')}")

    # 6. 하위 10 (강부정 추정)
    print(f"\n⚫ dart_score 하위 10 (강부정 추정)")
    bot = sorted(has_dart, key=lambda r: r["dart_score"])[:10]
    print(f"  {'종목':<20} {'섹터':<15} {'dart':<6} {'공시수':<5} {'총점':<6} 등급")
    for r in bot:
        print(f"  {r['name']:<20} {r['sector']:<15} {r['dart_score']:<6.1f} "
              f"{r['dart_count']:<5} {r['score']:<6.1f} {r.get('grade','-')}")

    # 7. dart_score가 등급에 영향 준 종목
    print(f"\n💡 dart_score가 결정적 영향을 준 종목 (강호재로 등급↑, 부정으로 등급↓ 추정)")
    boost = [r for r in has_dart if r["dart_score"] >= 65 and r.get("grade") in ("집중","주시")]
    drag = [r for r in has_dart if r["dart_score"] <= 35]
    print(f"  - 강호재 공시 + 발송됨: {len(boost)}개")
    print(f"  - 강부정 공시 받음: {len(drag)}개 (이 종목들이 발송 제외됐으면 Phase 2 효과)")

    if drag:
        print(f"\n  ⚠️ 강부정 공시 받은 종목 — 회피 대상")
        for r in drag[:5]:
            print(f"    {r['name']} ({r['ticker']}) — dart {r['dart_score']:.1f}, 총점 {r['score']:.1f}, 등급 {r.get('grade','-')}")

    # CSV 저장
    if args.csv:
        import csv
        out = Path(f"dart_analysis_{date}.csv")
        with open(out, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["ticker","name","sector","grade","score","dart_score",
                       "news_score","s_text","dart_count","news_count"])
            for r in sorted(results, key=lambda x:x.get("dart_score",50), reverse=True):
                w.writerow([
                    r["ticker"], r["name"], r.get("sector",""),
                    r.get("grade",""), r["score"],
                    r.get("dart_score",50), r.get("news_score",50),
                    r.get("s_text",50), r.get("dart_count",0), r.get("news_count",0),
                ])
        print(f"\n💾 CSV 저장: {out}")

if __name__ == "__main__":
    main()
