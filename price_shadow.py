#!/usr/bin/env python3
"""price_shadow.py — KIS J(KRX 정규장 종가) 기준 가격 지표를 shadow 로 병행 기록한다.

정본은 KIS J 로 결정됐지만(2026-09-18) 스캐너는 아직 FDR 종가를 쓴다. FDR 은 9/14 부터
과거 일자를 통합가(UN) 계열로 준다. 전환 전에 '같은 런을 J 로 계산했다면'을 나란히
쌓아 둔다. 실전 스캔·점수·발송 경로와 분리돼 있다 — 별도 프로세스·별도 테이블이고,
워크플로에서도 발송·DB 커밋이 끝난 뒤에 돈다.

    python price_shadow.py --db /tmp/t.db          # 로컬은 사본으로(필수)
    python price_shadow.py                          # Actions: PRICE_SHADOW_ALLOW_LIVE_DB=1

계산은 실전 함수를 그대로 부른다 — 재구현하지 않는다.
    가격 지표   alpharadar._price_features   (_precompute_ticker 가 쓰는 것과 같은 함수)
    필터 판정   alpharadar._step2_decision   (run_step2 가 쓰는 것과 같은 함수)
    T 점수      alpharadar._calc_t
    과열 C항    alpharadar._overheat_penalty (등락률 외 항을 0 으로 두고 호출)
점수 추정 score_total_j_est = score_total + w_tech·(T_j − T) − w_cross·(C_j − C)
    뉴스·수급·검색량 항은 가격과 무관해 그대로 둔다. 가중치는 config 에서 읽는다.
가격 출처(price_src): 'kis_j' / 'fdr'(명시적 폴백) / NULL(둘 다 실패) / 'error'(계산 예외).
알림: 폴백·실패·계산 예외·스크립트 전체 예외는 전부 텔레그램으로 간다. 종료 코드는 항상 0.
"""
import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

KIS_PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
KIS_TR = "FHKST03010100"
MARKET_CLOSE_HHMM = (15, 40)     # 이 전이면 오늘 행은 장중 현재가 — 버린다(KIS 함정 6)
FALLBACK_ALERT_RATE = 0.05

_LOG = None


def log() -> logging.Logger:
    """지연 초기화 로거(LOG_DIR import 시점 고정 문제 회피)."""
    global _LOG
    if _LOG is None:
        lg = logging.getLogger("price_shadow")
        if not lg.handlers:
            lg.setLevel(logging.INFO)
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] price_shadow — %(message)s")
            sh = logging.StreamHandler(sys.stdout)
            sh.setFormatter(fmt)
            lg.addHandler(sh)
            try:
                d = Path(os.getenv("PRICE_SHADOW_LOG_DIR", "data/logs"))
                d.mkdir(parents=True, exist_ok=True)
                day = datetime.now(timezone(timedelta(hours=9)))
                fh = logging.FileHandler(d / f"price_shadow_{day:%Y%m%d}.log", encoding="utf-8")
                fh.setFormatter(fmt)
                lg.addHandler(fh)
            except OSError:
                pass
            lg.propagate = False
        _LOG = lg
    return _LOG


def notify(text: str):
    """텔레그램 알림. alpharadar 를 못 불러와도 가도록 API 를 직접 부르는 길을 둔다."""
    try:
        import alpharadar as ar
        ar.TelegramClient().send(text)
        return
    except Exception:
        pass
    try:
        import requests
        tok, chat = os.getenv("TELEGRAM_BOT_TOKEN", ""), os.getenv("TELEGRAM_CHAT_ID", "")
        if tok and chat:
            requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          json={"chat_id": chat, "text": text}, timeout=10)
        else:
            print(text)
    except Exception:
        print(text)


SCHEMA = """
CREATE TABLE IF NOT EXISTS price_shadow_j (
    scan_date TEXT, run_type TEXT, ticker TEXT, origin TEXT,
    price_src TEXT,                 -- kis_j / fdr(명시적 폴백) / NULL(실패) / error(계산 예외)
    bar_date TEXT,                  -- 계산에 쓴 마지막 바
    current_price REAL, change_pct REAL, prev_change_pct REAL, disparity REAL, rsi REAL,
    bb_pos REAL, ma20 REAL, ma60 REAL, ma120 REAL, w52_high REAL, res_top REAL,
    ret_5d REAL, ret_20d REAL, w52_proximity REAL, vol_slope REAL,
    vol_5d_avg REAL, vol_20ma REAL, vol_60ma REAL,
    score_t_j REAL, chg_pen_j REAL,
    filter_reason_j TEXT,           -- _step2_decision 결과('' = 통과, 시총 미상이라 거래대금 제외)
    score_total REAL, score_t REAL, score_total_j_est REAL,   -- scan 행만
    as_of TEXT,
    PRIMARY KEY (scan_date, run_type, ticker, origin)
);
"""
FEATURE_COLS = ["current_price", "change_pct", "prev_change_pct", "disparity", "rsi", "bb_pos",
                "ma20", "ma60", "ma120", "w52_high", "res_top", "ret_5d", "ret_20d",
                "w52_proximity", "vol_slope", "vol_5d_avg", "vol_20ma", "vol_60ma"]


def _kis_daily(ar, tk, d1, d2):
    """KIS J 일봉(수정주가). 100행 단위로 거슬러 받는다. 실패 None."""
    import pandas as pd
    rows, end = {}, d2
    for _ in range(3):
        d = ar._kis_get(KIS_PATH, {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": tk,
                                   "FID_INPUT_DATE_1": d1, "FID_INPUT_DATE_2": end,
                                   "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"}, KIS_TR)
        if not d or d.get("rt_cd") != "0":
            return None
        out = [r for r in (d.get("output2") or []) if r.get("stck_bsop_date")]
        for r in out:
            if r.get("stck_clpr") in (None, "", "0"):
                continue
            rows[r["stck_bsop_date"]] = (float(r["stck_clpr"]), float(r["stck_hgpr"]),
                                         float(r["acml_vol"]))
        if len(out) < 100:
            break
        first = min(r["stck_bsop_date"] for r in out)
        if first <= d1:
            break
        end = (datetime.strptime(first, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
    if not rows:
        return None
    df = pd.DataFrame.from_dict(rows, orient="index", columns=["Close", "High", "Volume"])
    df.index = pd.to_datetime(df.index, format="%Y%m%d")
    return df.sort_index()


def _fdr_daily(tk, d1, d2):
    try:
        import FinanceDataReader as fdr
        df = fdr.DataReader(tk, f"{d1[:4]}-{d1[4:6]}-{d1[6:]}", f"{d2[:4]}-{d2[4:6]}-{d2[6:]}")
        return df[["Close", "High", "Volume"]].astype(float) if df is not None and len(df) else None
    except Exception:
        return None


def load_series(ar, tk, scan_date, run_type="pm", now=None):
    """(df, src). KIS J 우선, 실패 시 FDR 명시적 폴백.

    마지막 바는 런의 라벨로 정한다(시각 추론 금지): am 런은 scan_date 이전 바까지,
    pm 런은 scan_date 바까지 — 아침 런은 장 시작 전에 돌아 그날 바를 보지 않았다.
    자정을 넘겨 'am'으로 찍힌 저녁 런(9/14분 = 20260915 am)도 이 규칙이면 9/14 바를 본다.
    추가 안전장치로 장 마감(15:40) 전이면 오늘 행은 무조건 버린다(장중 현재가, KIS 함정 6).

    창은 _precompute_ticker 와 같다(_get_last_weekday → _workdays_before 125).
    Change 는 FDR 과 같은 정의(Close.pct_change)로 붙인다 — FDR Change = Close.pct_change()
    임을 2026-09-18 전수 확인했다. _price_features 가 이 칼럼으로 change_pct 를 낸다.
    """
    import pandas as pd
    end = ar._get_last_weekday(scan_date)
    start = ar._workdays_before(end, 125)
    now = now or datetime.now(ar.KST)
    df, src = _kis_daily(ar, tk, start, end), "kis_j"
    if df is None:
        df, src = _fdr_daily(tk, start, end), "fdr"
    if df is None:
        return None, None
    sd = pd.Timestamp(datetime.strptime(scan_date, "%Y%m%d").date())
    df = df[df.index < sd] if run_type == "am" else df[df.index <= sd]
    if (now.hour, now.minute) < MARKET_CLOSE_HHMM:
        df = df[df.index < pd.Timestamp(now.date())]
    df = df.copy()
    df["Change"] = df["Close"].pct_change()
    return df, src


def targets(con):
    """방금 끝난 런의 (scan_date, run_type)과 대상 [(ticker, origin, cap_tier, scan_row)]."""
    last = con.execute("SELECT scan_date, run_type FROM scan_results "
                       "ORDER BY created_at DESC LIMIT 1").fetchone()
    if not last:
        return None, None, []
    sd, rt = last
    out = []
    for r in con.execute("""SELECT ticker, cap_tier, score_total, score_t, change_pct
                            FROM scan_results WHERE scan_date=? AND run_type=?""", (sd, rt)):
        out.append((str(r[0]).zfill(6), "scan", r[1], {"score_total": r[2], "score_t": r[3],
                                                        "change_pct": r[4]}))
    for q, kind in [("SELECT ticker, cap_tier, stage, reason FROM pool_history WHERE scan_date=?", "pool"),
                    ("SELECT ticker, NULL, 'gated', reason FROM gated_tickers WHERE scan_date=?", "gated")]:
        try:
            for r in con.execute(q, (sd,)):
                o = ("ns" if r[2] == "no_signal" else f"f:{r[3]}") if kind == "pool" else "gated"
                out.append((str(r[0]).zfill(6), o, r[1], None))
        except sqlite3.OperationalError:
            pass
    return sd, rt, out


def shadow_row(ar, df, tier, srow, fcfg, ocfg, w_tech, w_cross):
    """실전 함수로 J 기준 값을 계산한다."""
    pf = ar._price_features(df)
    reason, _ = ar._step2_decision({**pf, "cap_tier": tier or "large", "market_cap": None}, fcfg)
    t_j = ar._calc_t(pf)
    c_j = ar._overheat_penalty({"change_pct": pf["change_pct"]}, [], [], ocfg)
    row = {**{k: pf[k] for k in FEATURE_COLS}, "bar_date": df.index[-1].strftime("%Y-%m-%d"),
           "score_t_j": t_j, "chg_pen_j": c_j, "filter_reason_j": reason or ""}
    if srow is not None:
        c_old = ar._overheat_penalty({"change_pct": srow["change_pct"] or 0.0}, [], [], ocfg)
        row.update(score_total=srow["score_total"], score_t=srow["score_t"],
                   score_total_j_est=round(srow["score_total"] + w_tech * (t_j - (srow["score_t"] or 0))
                                           - w_cross * (c_j - c_old), 2))
    return row


def run(ar, db, now=None):
    import pandas as pd
    cfg = ar.load_config()
    fcfg = cfg["filter"]
    scfg = cfg.get("scoring", {}) or {}
    ocfg = scfg.get("overheat_penalty", {}) or {}
    w_tech = scfg.get("w_tech", scfg.get("w1", 0.35))      # run_step3 와 같은 읽기
    w_cross = scfg.get("w_cross", scfg.get("w3", 0.35))
    con = sqlite3.connect(db)
    con.executescript(SCHEMA)
    sd, rt, tg = targets(con)
    if not sd:
        log().info("scan_results 가 비었다 — 할 일 없음")
        return {}
    cache, n_src, errors = {}, {"kis_j": 0, "fdr": 0, None: 0}, []
    as_of = datetime.now(ar.KST).strftime("%Y-%m-%d %H:%M:%S")
    n_uniq = len({t[0] for t in tg})
    log().info(f"시작 {sd} {rt} · 대상 {len(tg)}행/{n_uniq}종목")
    t0 = datetime.now()
    rows = []
    for tk, origin, tier, srow in tg:
        if tk not in cache:
            cache[tk] = load_series(ar, tk, sd, rt, now)
            n_src[cache[tk][1]] += 1
            if len(cache) % 50 == 0:            # 제한시간에 걸렸을 때 어디까지 갔는지 보이게
                log().info(f"진행 {len(cache)}/{n_uniq}종목 · {(datetime.now() - t0).seconds}초 · "
                           f"KIS 호출 {ar._KIS_STATS['calls']} · fdr폴백 {n_src['fdr']} · 실패 {n_src[None]}")
        df, src = cache[tk]
        base = {"scan_date": sd, "run_type": rt, "ticker": tk, "origin": origin,
                "price_src": src, "as_of": as_of}
        if df is None or len(df) < 20:
            rows.append(base)
            continue
        try:
            rows.append({**base, **shadow_row(ar, df, tier, srow, fcfg, ocfg, w_tech, w_cross)})
        except Exception as e:                       # 한 종목의 예외가 전체를 멈추지 않게 — 대신 센다
            errors.append(f"{tk}:{type(e).__name__}")
            rows.append({**base, "price_src": "error"})
    out = pd.DataFrame(rows)
    con.execute("DELETE FROM price_shadow_j WHERE scan_date=? AND run_type=?", (sd, rt))
    have = [r[1] for r in con.execute("PRAGMA table_info(price_shadow_j)")]
    out[[c for c in have if c in out.columns]].to_sql("price_shadow_j", con, if_exists="append", index=False)
    con.commit()
    con.close()

    n_tk = len(cache)
    fb = n_src["fdr"] + n_src[None]
    msg = (f"{sd} {rt} · {len(rows)}행/{n_tk}종목 · kis_j {n_src['kis_j']} · fdr폴백 {n_src['fdr']} · "
           f"조회실패 {n_src[None]} · 계산예외 {len(errors)}")
    log().info(msg)
    if fb or errors:
        lvl = "⚠️" if (fb / max(n_tk, 1) > FALLBACK_ALERT_RATE or errors) else "ℹ️"
        detail = f"\n예외: {', '.join(errors[:10])}" if errors else ""
        notify(f"{lvl} price shadow(J) — 스캔·발송 영향 없음\n{msg}{detail}")
    return {"scan_date": sd, "run_type": rt, "rows": len(rows), "src": n_src, "errors": errors}


def main(argv=None):
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--db", default=None, help="쓸 DB. 로컬은 사본 필수.")
    a = ap.parse_args(argv)
    try:
        import alpharadar as ar               # import 실패도 알림 대상이라 try 안에서 부른다
        if a.db:
            db = Path(a.db)
        elif os.getenv("PRICE_SHADOW_ALLOW_LIVE_DB") == "1":
            db = ar.DB_PATH
        else:
            print("실전 DB 에 쓰지 않는다 — --db <사본> 을 주거나 PRICE_SHADOW_ALLOW_LIVE_DB=1(Actions)")
            return 0
        run(ar, db)
    except Exception as e:                   # shadow 실패는 스캔·발송과 무관 — 알리고 0 으로 끝낸다
        try:
            log().exception(f"shadow 실패: {e}")
        except Exception:
            pass
        notify(f"⚠️ price shadow(J) 실패 — 스캔·발송 영향 없음\n{type(e).__name__}: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
