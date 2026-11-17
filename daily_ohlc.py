#!/usr/bin/env python3
"""daily_ohlc.py — KRX 정규장 일봉(시가·고가·저가·종가·거래량)을 daily_ohlc_j 에 확정값으로 쌓는다.

정본은 KIS 일봉(FHKST03010100) 시장구분 J 다(CLAUDE.md KIS 함정 5). 밤에 받은 당일 봉은
다음 날 다시 받은 값과 다를 수 있어(11/13 루닛 15,250 → 15,020) 확정 여부(is_final)를 같이 적는다.

    확정 규칙: 받은 시각이 봉 날짜의 다음 달력일 08:00 KST 이후면 is_final=1.
              08:00 은 가설이다 — first_close(처음 받은 종가)와 확정 종가의 차이를 런마다 로그로 남겨 검증한다.
    is_final=0 행은 다시 받으면 값을 갱신하고 규칙에 따라 1 로 올린다. first_close·first_seen 은 그대로 둔다.
    is_final=1 행은 값이 같으면 as_of 만 갱신, 다르면 갱신하고 경고를 남긴다(수정주가 등).
    장 마감(15:40) 전에 받은 당일 행은 버린다(장중 현재가, KIS 함정 6).

대상(합집합):
    (가) price_shadow.targets 종목 — price_shadow 가 같은 응답에서 저장한다(store_sink, 추가 호출 없음).
         이 스크립트는 그 런에서 저장되지 않은 종목만 다시 받는다(KIS 실패 등).
    (나) stockcard.watchlist_path  (다) daily_ohlc.briefing_watchlist_path
    (라) sent_history 최근 sent_lookback_days 달력일 발송 종목
    (마) 최근 refetch_unfinal_days 일 안의 미확정(is_final=0) 행이 남은 종목 — 저녁에만 잡히고
         아침 스캔에서 빠진 종목도 확정되게 한다.
    지수 KOSPI(업종 0001)·KOSDAQ(1001) — 국내주식업종기간별시세 FHKUP03500100, 시장구분 U.

가격 출처(price_src): 'kis_j' / 'fdr'(명시적 폴백). 둘 다 실패면 행을 만들지 않는다.
종목의 fdr 행은 is_final=0 으로만 둔다 — 9/14 이후 FDR Close 는 통합가(UN) 계열이라 정규장 종가가 아니다.
kis_j 행을 fdr 값으로 덮어쓰지 않는다. 지수는 애프터마켓이 없어 fdr 행도 시각 규칙을 따른다
(11/12~16 KIS·FDR 지수 OHLC 전부 일치 확인, 2026-11-17).

단위: 종목 volume=주, value=원(acml_tr_pbmn). 지수 응답은 acml_vol=천주, acml_tr_pbmn=백만원이라
주·원으로 바꿔 넣는다(FDR KS11 Volume·Amount 와 대조 확인, 2026-11-17).

    python daily_ohlc.py --db /tmp/t.db                 # 로컬은 사본으로(필수)
    python daily_ohlc.py --db /tmp/t.db --backfill 60   # 과거 60영업일 1회 채움
    python daily_ohlc.py                                 # Actions: DAILY_OHLC_ALLOW_LIVE_DB=1

실전 스캔·점수·발송 경로와 분리돼 있다. 실패해도 종료 코드 0, 문제는 텔레그램으로 알린다.
"""
import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

TABLE = "daily_ohlc_j"
KST = timezone(timedelta(hours=9))
STOCK_PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
STOCK_TR = "FHKST03010100"
INDEX_PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice"
INDEX_TR = "FHKUP03500100"
INDICES = {"KOSPI": ("0001", "KS11"), "KOSDAQ": ("1001", "KQ11")}   # ticker: (KIS 업종코드, FDR 심볼)
FINAL_HHMM = (8, 0)              # 가설 — 다음 달력일 이 시각 이후에 받은 봉을 확정으로 본다
MARKET_CLOSE_HHMM = (15, 40)     # 이 전이면 오늘 행은 장중 현재가 — 버린다(KIS 함정 6)
VALUE_COLS = ["open", "high", "low", "close", "volume", "value"]
STALE_DAYS = 4                   # 봉 날짜가 이보다 오래됐는데 kis_j 행이 미확정이면 경고(주말 포함 여유)
COVERED_MIN = 30                 # 이만큼 안에 kis_j 로 저장된 종목은 price_shadow 가 이번 런에 받은 것으로 본다

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    ticker TEXT NOT NULL,            -- 6자리 문자열, 지수는 'KOSPI'·'KOSDAQ'
    date TEXT NOT NULL,              -- 봉 날짜 YYYYMMDD
    open REAL, high REAL, low REAL,
    close REAL,                      -- is_final=1 이면 정규장 종가
    volume REAL,                     -- 주
    value REAL,                      -- 거래대금(원), 응답에 없으면 NULL
    price_src TEXT,                  -- kis_j / fdr(명시적 폴백)
    is_final INTEGER NOT NULL DEFAULT 0,
    first_close REAL,                -- 처음 받은 close(측정용, 덮어쓰지 않음)
    first_seen TEXT,                 -- 처음 받은 시각 KST
    as_of TEXT NOT NULL,             -- 마지막으로 받은 시각 KST
    PRIMARY KEY (ticker, date)
);
"""

_LOG = None


def log() -> logging.Logger:
    """지연 초기화 로거(LOG_DIR import 시점 고정 문제 회피)."""
    global _LOG
    if _LOG is None:
        lg = logging.getLogger("daily_ohlc")
        if not lg.handlers:
            lg.setLevel(logging.INFO)
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] daily_ohlc — %(message)s")
            sh = logging.StreamHandler(sys.stdout)
            sh.setFormatter(fmt)
            lg.addHandler(sh)
            try:
                d = Path(os.getenv("DAILY_OHLC_LOG_DIR", "data/logs"))
                d.mkdir(parents=True, exist_ok=True)
                fh = logging.FileHandler(d / f"daily_ohlc_{datetime.now(KST):%Y%m%d}.log", encoding="utf-8")
                fh.setFormatter(fmt)
                lg.addHandler(fh)
            except OSError:
                pass
            lg.propagate = False
        _LOG = lg
    return _LOG


def notify(text: str):
    import price_shadow
    price_shadow.notify(text)


def ensure_schema(con):
    con.executescript(SCHEMA)


# ── 확정 규칙 ────────────────────────────────────────────────────────────────

def is_final(bar_date: str, now: datetime) -> int:
    d = datetime.strptime(bar_date, "%Y%m%d").replace(tzinfo=KST)
    return int(now >= d + timedelta(days=1, hours=FINAL_HHMM[0], minutes=FINAL_HHMM[1]))


def drop_intraday(bars: dict, now: datetime) -> dict:
    """장 마감 전이면 오늘 봉을 버린다. 미래 날짜도 버린다."""
    today = now.strftime("%Y%m%d")
    closed = (now.hour, now.minute) >= MARKET_CLOSE_HHMM
    return {d: b for d, b in bars.items() if d < today or (d == today and closed)}


# ── 응답 파싱 ────────────────────────────────────────────────────────────────

def _num(v, scale=1.0):
    """빈 값은 None. 0 은 0 (거래정지일 거래량 0 은 사실이다)."""
    if v in (None, ""):
        return None
    try:
        return float(v) * scale
    except (TypeError, ValueError):
        return None


def _px(v):
    """가격 0 은 값이 아니라 공란(거래 없는 날의 시·고·저 등) — None."""
    x = _num(v)
    return x if x else None


def parse_stock_row(r: dict):
    """FHKST03010100 output2 한 행 → (date, bar) . 종가가 없으면 None."""
    d, c = r.get("stck_bsop_date"), _px(r.get("stck_clpr"))
    if not d or c is None:
        return None
    return d, {"open": _px(r.get("stck_oprc")), "high": _px(r.get("stck_hgpr")),
               "low": _px(r.get("stck_lwpr")), "close": c,
               "volume": _num(r.get("acml_vol")), "value": _num(r.get("acml_tr_pbmn"))}


def parse_index_row(r: dict):
    """FHKUP03500100 output2 한 행. acml_vol 천주→주, acml_tr_pbmn 백만원→원."""
    d, c = r.get("stck_bsop_date"), _px(r.get("bstp_nmix_prpr"))
    if not d or c is None:
        return None
    return d, {"open": _px(r.get("bstp_nmix_oprc")), "high": _px(r.get("bstp_nmix_hgpr")),
               "low": _px(r.get("bstp_nmix_lwpr")), "close": c,
               "volume": _num(r.get("acml_vol"), 1e3), "value": _num(r.get("acml_tr_pbmn"), 1e6)}


# ── 조회 ─────────────────────────────────────────────────────────────────────

def _kis_bars(ar, path, tr, params, parse, d1, d2, page=100, max_pages=5):
    """KIS 일봉을 d2 부터 거슬러 받는다. 실패 None."""
    bars, end = {}, d2
    for _ in range(max_pages):
        d = ar._kis_get(path, {**params, "FID_INPUT_DATE_1": d1, "FID_INPUT_DATE_2": end,
                               "FID_PERIOD_DIV_CODE": "D"}, tr)
        if not d or d.get("rt_cd") != "0":
            return None
        out = [r for r in (d.get("output2") or []) if r.get("stck_bsop_date")]
        for r in out:
            p = parse(r)
            if p:
                bars[p[0]] = p[1]
        if len(out) < page:
            break
        first = min(r["stck_bsop_date"] for r in out)
        if first <= d1:
            break
        end = (datetime.strptime(first, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
    return bars or None


def _fdr_bars(sym, d1, d2):
    try:
        import FinanceDataReader as fdr
        df = fdr.DataReader(sym, f"{d1[:4]}-{d1[4:6]}-{d1[6:]}", f"{d2[:4]}-{d2[4:6]}-{d2[6:]}")
    except Exception:
        return None
    if df is None or not len(df):
        return None
    out = {}
    for ts, r in df.iterrows():
        g = lambda k: (None if k not in r or r[k] != r[k] else float(r[k]))   # NaN → None
        if not g("Close"):
            continue
        out[ts.strftime("%Y%m%d")] = {"open": g("Open") or None, "high": g("High") or None,
                                      "low": g("Low") or None, "close": g("Close"),
                                      "volume": g("Volume"), "value": g("Amount")}
    return out or None


def fetch_stock(ar, tk, d1, d2):
    bars = _kis_bars(ar, STOCK_PATH, STOCK_TR, {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": tk,
                                                "FID_ORG_ADJ_PRC": "0"}, parse_stock_row, d1, d2)
    if bars:
        return bars, "kis_j"
    bars = _fdr_bars(tk, d1, d2)
    return (bars, "fdr") if bars else (None, None)


def fetch_index(ar, name, d1, d2):
    code, sym = INDICES[name]
    bars = _kis_bars(ar, INDEX_PATH, INDEX_TR, {"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": code},
                     parse_index_row, d1, d2, page=50)
    if bars:
        return bars, "kis_j"
    bars = _fdr_bars(sym, d1, d2)
    return (bars, "fdr") if bars else (None, None)


# ── 저장 ─────────────────────────────────────────────────────────────────────

def _same(a, b):
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= 1e-6 * max(1.0, abs(a))


def new_stats():
    return {"inserted": 0, "finalized": [], "changed_final": [], "unchanged": 0, "skipped_fdr": 0}


def store(con, tk, bars, src, now, stats):
    """한 종목의 봉들을 확정 규칙대로 넣는다. stats 를 갱신한다."""
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    index = tk in INDICES
    for d, b in sorted(drop_intraday(bars, now).items()):
        fin = is_final(d, now) if (src == "kis_j" or index) else 0
        old = con.execute(f"SELECT {', '.join(VALUE_COLS)}, price_src, is_final, first_close "
                          f"FROM {TABLE} WHERE ticker=? AND date=?", (tk, d)).fetchone()
        vals = [b.get(c) for c in VALUE_COLS]
        if old is None:
            con.execute(f"INSERT INTO {TABLE} (ticker, date, {', '.join(VALUE_COLS)}, price_src, is_final, "
                        f"first_close, first_seen, as_of) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (tk, d, *vals, src, fin, b.get("close"), ts, ts))
            stats["inserted"] += 1
            continue
        old_vals, old_src, old_fin, first_close = old[:6], old[6], old[7], old[8]
        if old_src == "kis_j" and src != "kis_j":
            stats["skipped_fdr"] += 1          # 정본을 폴백 값으로 덮지 않는다
            continue
        same = all(_same(a, v) for a, v in zip(old_vals, vals))
        if old_fin and same and old_src == src:
            con.execute(f"UPDATE {TABLE} SET as_of=? WHERE ticker=? AND date=?", (ts, tk, d))
            stats["unchanged"] += 1
            continue
        if old_fin and not same:
            stats["changed_final"].append((tk, d, old_vals[3], b.get("close")))
        fin = max(fin, old_fin) if src == old_src else fin
        con.execute(f"UPDATE {TABLE} SET {', '.join(c + '=?' for c in VALUE_COLS)}, price_src=?, is_final=?, "
                    f"as_of=? WHERE ticker=? AND date=?", (*vals, src, fin, ts, tk, d))
        if fin and not old_fin:
            stats["finalized"].append((tk, d, first_close, b.get("close")))


def stats_lines(stats, label):
    """확정 전환 행의 first_close 대 확정 close 차이 — 08:00 경계 가설 검증용 한 줄 + 경고들."""
    fz = [(t, d, (c / f0 - 1) * 100) for t, d, f0, c in stats["finalized"] if f0 and c]
    bad = [x for x in fz if abs(x[2]) > 1e-9]
    line = (f"{label} 확정 전환 {len(fz)}행 · first_close 불일치 {len(bad)}"
            + (f" ({len(bad) / len(fz):.1%})" if fz else ""))
    if bad:
        ds = sorted(x[2] for x in bad)
        w = max(bad, key=lambda x: abs(x[2]))
        line += f" · 차 중앙 {ds[len(ds) // 2]:+.2f}% · 최대 {w[0]} {w[1]} {w[2]:+.2f}%"
    line += f" · 신규 {stats['inserted']} · 확정값 변경 {len(stats['changed_final'])}"
    warns = [f"확정 봉 값 변경: {t} {d} close {a} → {b}" for t, d, a, b in stats["changed_final"][:20]]
    return line, warns


def store_sink(db, sink, now, store_days=14):
    """price_shadow 가 이미 받은 KIS J 응답을 저장한다(추가 호출 없음). 최근 store_days 달력일만."""
    lo = (now - timedelta(days=store_days)).strftime("%Y%m%d")
    stats = new_stats()
    con = sqlite3.connect(db)
    try:
        ensure_schema(con)
        for tk, bars in sink.items():
            store(con, tk, {d: b for d, b in bars.items() if d >= lo}, "kis_j", now, stats)
        con.commit()
    finally:
        con.close()
    return stats


# ── 대상 ─────────────────────────────────────────────────────────────────────

def _cfg(cfg):
    return cfg.get("daily_ohlc", {}) or {}


def target_sets(con, cfg, now):
    """{집합 이름: set(ticker)}. 파일·테이블이 없어도 실패하지 않는다."""
    import price_shadow
    from stockcard_common import load_watchlist
    dc = _cfg(cfg)
    out = {}
    try:
        out["shadow"] = {t[0] for t in price_shadow.targets(con)[2]}
    except sqlite3.OperationalError:
        out["shadow"] = set()
    for name, path in [("watchlist", (cfg.get("stockcard", {}) or {}).get("watchlist_path")),
                       ("briefing", dc.get("briefing_watchlist_path"))]:
        try:
            out[name] = set(load_watchlist(path)) if path else set()
        except Exception as e:
            log().warning(f"{name} 읽기 실패 {path}: {e}")
            out[name] = set()
    since = (now - timedelta(days=int(dc.get("sent_lookback_days", 30)))).strftime("%Y%m%d")
    try:
        out["sent"] = {str(r[0]).zfill(6) for r in
                       con.execute("SELECT DISTINCT ticker FROM sent_history WHERE send_date>=?", (since,))}
    except sqlite3.OperationalError:
        out["sent"] = set()
    lo = (now - timedelta(days=int(dc.get("refetch_unfinal_days", 7)))).strftime("%Y%m%d")
    out["unfinal"] = {r[0] for r in con.execute(f"SELECT DISTINCT ticker FROM {TABLE} WHERE is_final=0 "
                                                f"AND date>=?", (lo,)) if r[0] not in INDICES}
    return out


def run(ar, db, now=None, backfill=None):
    cfg = ar.load_config()
    dc = _cfg(cfg)
    now = now or datetime.now(KST)
    con = sqlite3.connect(db)
    ensure_schema(con)
    sets = target_sets(con, cfg, now)
    union = sorted(set().union(*sets.values()))
    d2 = now.strftime("%Y%m%d")
    if backfill:
        d1 = ar._workdays_before(d2, int(backfill))
        todo = union
    else:
        d1 = (now - timedelta(days=int(dc.get("fetch_days", 14)))).strftime("%Y%m%d")
        cut = (now - timedelta(minutes=COVERED_MIN)).strftime("%Y-%m-%d %H:%M:%S")
        covered = {r[0] for r in con.execute(f"SELECT DISTINCT ticker FROM {TABLE} "
                                             f"WHERE price_src='kis_j' AND as_of>=?", (cut,))}
        todo = [t for t in union if t not in covered]
    log().info(f"시작 {d1}~{d2}{' 백필' if backfill else ''} · "
               + " · ".join(f"{k} {len(v)}" for k, v in sets.items())
               + f" · 합집합 {len(union)} · 조회 {len(todo)}")
    t0, calls0 = datetime.now(), ar._KIS_STATS["calls"]
    stats, n_src, failed = new_stats(), {"kis_j": 0, "fdr": 0}, []
    for i, tk in enumerate(todo + list(INDICES), 1):
        try:
            bars, src = (fetch_index if tk in INDICES else fetch_stock)(ar, tk, d1, d2)
        except Exception as e:                     # 한 종목의 예외가 전체를 멈추지 않게
            log().warning(f"{tk} 조회 예외 {type(e).__name__}: {e}")
            bars, src = None, None
        if not bars:
            failed.append(tk)
            continue
        n_src[src] += 1
        store(con, tk, bars, src, now, stats)
        if i % 50 == 0:
            con.commit()
            log().info(f"진행 {i}/{len(todo) + len(INDICES)} · {(datetime.now() - t0).seconds}초 · "
                       f"KIS 호출 {ar._KIS_STATS['calls'] - calls0}")
    con.commit()
    stale_lo = (now - timedelta(days=STALE_DAYS)).strftime("%Y%m%d")
    stale = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT ticker) FROM {TABLE} WHERE is_final=0 "
                        f"AND price_src='kis_j' AND date<?", (stale_lo,)).fetchone()
    con.close()
    secs = (datetime.now() - t0).total_seconds()
    calls = ar._KIS_STATS["calls"] - calls0
    msg = (f"조회 {len(todo) + len(INDICES)} · kis_j {n_src['kis_j']} · fdr폴백 {n_src['fdr']} · "
           f"실패 {len(failed)} · KIS 호출 {calls} · {secs:.0f}초")
    log().info(msg)
    line, warns = stats_lines(stats, "OHLC")
    log().info(line)
    for w in warns:
        log().warning(w)
    if stale[0]:                                   # 확정이 며칠째 밀렸다 — 아침 런이 계속 못 돌았거나 조회 실패
        log().warning(f"{STALE_DAYS}일 넘게 미확정인 kis_j 행 {stale[0]}건/{stale[1]}종목")
    if n_src["fdr"] or failed or stats["changed_final"] or stale[0]:
        detail = f"\n실패: {', '.join(failed[:10])}" if failed else ""
        detail += f"\n확정값 변경 {len(stats['changed_final'])}건(로그 참조)" if stats["changed_final"] else ""
        detail += f"\n{STALE_DAYS}일 넘게 미확정 {stale[0]}건/{stale[1]}종목" if stale[0] else ""
        notify(f"ℹ️ daily OHLC(J) — 스캔·발송 영향 없음\n{msg}{detail}")
    return {"sets": {k: len(v) for k, v in sets.items()}, "union": len(union), "fetched": len(todo),
            "src": n_src, "failed": failed, "calls": calls, "secs": secs, "stats": stats}


def main(argv=None):
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--db", default=None, help="쓸 DB. 로컬은 사본 필수.")
    ap.add_argument("--backfill", type=int, default=None, help="과거 N 영업일을 한 번 채운다")
    a = ap.parse_args(argv)
    try:
        import alpharadar as ar
        if a.db:
            db = Path(a.db)
        elif os.getenv("DAILY_OHLC_ALLOW_LIVE_DB") == "1":
            db = ar.DB_PATH
        else:
            print("실전 DB 에 쓰지 않는다 — --db <사본> 을 주거나 DAILY_OHLC_ALLOW_LIVE_DB=1(Actions)")
            return 0
        run(ar, db, backfill=a.backfill)
    except Exception as e:                   # 실패는 스캔·발송과 무관 — 알리고 0 으로 끝낸다
        try:
            log().exception(f"daily OHLC 실패: {e}")
        except Exception:
            pass
        notify(f"⚠️ daily OHLC(J) 실패 — 스캔·발송 영향 없음\n{type(e).__name__}: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
