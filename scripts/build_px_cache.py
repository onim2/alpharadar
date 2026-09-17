"""종목별 일간 등락률 캐시 빌더 — analyze_prev_spike.py 의 --px 입력.

왜 캐시가 필요한가. D-5 검정은 DB 의 change_pct 를 일자축으로 이어붙이지 않는다.
change_pct 의 기준 바가 런마다 다르기 때문이다 — 저녁 런은 당일 종가 등락률이지만
아침 런은 장 시작 전이라 직전 거래일 등락률이다. 두 런이 같은 scan_date 를
공유하므로 DB 값을 그대로 쓰면 하루가 어긋난다. 그래서 각 행의 change_pct 와
일치하는 시세 바를 역추적해 '관측 바'를 찾고, 그 바의 직전 거래일을 prev 로 삼는다.
이 캐시가 그 역추적의 대조표다.

8/26 판정에 쓴 캐시는 즉석에서 만들어 저장소에 남지 않았다. 그래서 그 뒤로
판정을 재현할 수 없었다. 이 스크립트가 그 자리를 메운다.

형식
    {
      "__meta__": {생성일·조회범위·소스·건수},
      "005930": {"20260523": -1.23, "20260526": 0.44, ...},   # 값은 퍼센트
      ...
    }
  analyze_prev_spike.py 는 px.get(ticker) 로 읽으므로 __meta__ 키가 섞여 있어도
  영향이 없다. 종목코드는 6자리 숫자라 이 이름과 충돌할 수 없다.

멱등. 다시 돌리면 이미 확보한 구간은 건너뛰고 모자란 종목만 채운다.
시세는 FinanceDataReader 로 받는다 — pykrx 는 KRX 응답 포맷 변경으로 죽어 있다.

    python scripts/build_px_cache.py                     # 기본 경로·전 구간
    python scripts/build_px_cache.py --to 20260826       # 8/26 판정 재현용
    python scripts/build_px_cache.py --refresh           # 전량 재수집
"""

import argparse
import pickle
import socket
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

KST = timezone(timedelta(hours=9))
META = "__meta__"

# FinanceDataReader 는 요청 타임아웃을 노출하지 않는다. 그물이 없으면 응답이
# 오지 않는 연결에 영원히 매달린다 — 실제로 8스레드가 42분간 0% CPU 로 멈춰 있었다.
# 소켓 기본 타임아웃이 그 아래 requests 까지 걸린다.
socket.setdefaulttimeout(20)


def ymd(d):
    return d.strftime("%Y%m%d")


def dash(s):
    return f"{s[:4]}-{s[4:6]}-{s[6:]}"


def fetch(ticker: str, start: str, end: str):
    """일간 등락률(%) — {YYYYMMDD: float}. 실패하면 None 을 돌려 구분한다."""
    import FinanceDataReader as fdr

    try:
        df = fdr.DataReader(ticker, dash(start), dash(end))
    except Exception:
        return None
    if df is None or not len(df) or "Change" not in df.columns:
        return None
    out = {}
    for d, v in df["Change"].items():
        if v is None:
            continue
        try:
            out[d.strftime("%Y%m%d")] = round(float(v) * 100, 4)
        except (TypeError, ValueError):
            continue
    return out or None


def main(argv=None):
    p = argparse.ArgumentParser(description="등락률 캐시 빌더 (analyze_prev_spike --px)")
    p.add_argument("--db", default="data/scores_history.db")
    p.add_argument("--out", default="data/cache/px_change_pct.pkl")
    p.add_argument("--from-date", default=None,
                   help="기본: scan_results 최소 scan_date - 15일(직전 바 확보용)")
    p.add_argument("--to", default=None, help="기본: scan_results 최대 scan_date")
    p.add_argument("--refresh", action="store_true", help="기존 캐시를 무시하고 전량 재수집")
    a = p.parse_args(argv)

    con = sqlite3.connect(a.db)
    lo, hi = con.execute(
        "SELECT MIN(scan_date), MAX(scan_date) FROM scan_results").fetchone()
    if not lo:
        print("scan_results 가 비어 있다.", file=sys.stderr)
        return 1

    start = a.from_date or ymd(datetime.strptime(lo, "%Y%m%d") - timedelta(days=15))
    end = a.to or hi
    tickers = sorted(str(t).zfill(6) for (t,) in con.execute(
        "SELECT DISTINCT ticker FROM scan_results WHERE scan_date <= ?", (end,)))

    out = Path(a.out)
    cache = {}
    if out.exists() and not a.refresh:
        try:
            cache = pickle.loads(out.read_bytes())
        except Exception as e:
            print(f"기존 캐시를 읽지 못했다 ({type(e).__name__}) — 새로 만든다")
            cache = {}
    prev_meta = cache.pop(META, None)

    # 구간을 이미 덮고 있으면 건너뛴다(멱등). 거래 정지·상장 전 구간이 있으므로
    # 완전 일치가 아니라 '양 끝 근처에 바가 있는가'로 느슨하게 본다.
    def covered(ser):
        if not ser:
            return False
        ks = sorted(ser)
        return ks[0] <= ymd(datetime.strptime(start, "%Y%m%d") + timedelta(days=20)) \
            and ks[-1] >= ymd(datetime.strptime(end, "%Y%m%d") - timedelta(days=20))

    todo = [t for t in tickers if a.refresh or not covered(cache.get(t))]
    print(f"종목 {len(tickers)}개 | 수집 대상 {len(todo)}개 "
          f"(캐시 적중 {len(tickers) - len(todo)}) | {start}~{end}", flush=True)

    def save():
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(out.suffix + ".tmp")
        tmp.write_bytes(pickle.dumps(cache))
        tmp.replace(out)

    # 직렬로 돈다. FDR 을 8스레드로 부르면 42분간 0% CPU 로 멈춰 서는 것을 겪었다
    # (세션이 스레드 안전하지 않은 것으로 보인다). 단건은 0.1초라 직렬로도
    # 490종목이 1분이면 끝난다 — 병렬로 얻을 것이 없다.
    ok = fail = 0
    failed_tickers = []
    for i, t in enumerate(todo, 1):
        try:
            ser = fetch(t, start, end)
        except Exception:
            ser = None
        if ser:
            cache[t] = {**(cache.get(t) or {}), **ser}
            ok += 1
        else:
            fail += 1
            failed_tickers.append(t)
        # flush 가 필요하다. 출력이 파이프로 가면 블록 버퍼링이라 진행이 안 보인다.
        if i % 25 == 0 or i == len(todo):
            print(f"  {i}/{len(todo)}  성공 {ok} 실패 {fail}", flush=True)
        # 중간 저장 — 도중에 끊겨도 다음 실행이 이어받는다(멱등).
        if i % 100 == 0:
            cache[META] = {"built_at": "(중간 저장)", "partial": True}
            save()
            cache.pop(META, None)

    bars = sum(len(v) for k, v in cache.items() if k != META)
    cache[META] = {
        "built_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
        "range": {"start": start, "end": end},
        "source": "FinanceDataReader DataReader Change (pykrx 는 고장)",
        "unit": "percent",
        "db": str(a.db),
        "tickers": len([k for k in cache if k != META]),
        "bars": bars,
        "fetched_now": {"ok": ok, "failed": fail, "attempted": len(todo),
                        "failed_tickers": failed_tickers[:50]},
        "prev_built_at": (prev_meta or {}).get("built_at"),
        "builder": "scripts/build_px_cache.py",
    }

    save()
    m = cache[META]
    print(f"\n저장: {out}  ({out.stat().st_size / 1024:.0f} KB)")
    print(f"  생성 {m['built_at']} | 범위 {m['range']['start']}~{m['range']['end']}")
    print(f"  종목 {m['tickers']}개 | 바 {m['bars']:,}개 | 이번 수집 {ok}성공 {fail}실패")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
