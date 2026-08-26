#!/usr/bin/env python3
# afterhours_scanner.py — NXT 애프터마켓(15:40~20:00 KST) 상승 종목 관찰 로깅
#
# 애프터마켓 수량·대금은 NXT 분봉(FHKST03010200, 시장구분 NX)을 15:40~20:00 구간만
# 합산해서 만든다. 현재가 조회의 acml_vol/acml_tr_pbmn 을 쓰면 안 된다 — 넥스트레이드는
# 자체 정규장도 돌리므로 그건 NXT '하루 전체' 누적이다(2026-08-24 실측: 두산에너빌리티
# 하루 1,674억 vs 애프터마켓 구간 972억).
#
# 관찰 전용 모듈이다. 알파래더 점수 파이프라인에 들어가지 않고 텔레그램도 쏘지
# 않는다. 검증 질문은 "애프터 섹터 강도 상위 → 익일 시초→종가"의 IC다. 갭 자체가
# 아니라 갭 이후가 먹을 자리인지를 묻는 것이라, 판정은 track_outcomes의 익일
# 가격에 조인해서 한다.
#
# 동기(2026-08-24 실사례): KOSPI -3.12% 폭락일 저녁 NXT에서 한국전력·두산에너빌리티·
# 한전기술이 동반 급등했다. 공시·기사는 없었고 다른 대형주·금융주도 소폭 올라
# "미 선물 상승 기반 익일 반등 베팅 + 섹터 서열"로 읽혔다. 그 수동 판별을 자동화한다.
# 판별 순서 교훈이 설계에 들어가 있다 — 개별 재료보다 "시장 전체가 올랐나"가 먼저다.
# 그래서 지수 등락률을 행마다 같이 적재한다.
#
# 사용법:
#   uv run afterhours_scanner.py                     # K200+KQ150, DB 적재
#   uv run afterhours_scanner.py --tickers uni.txt   # 한 줄에 한 종목코드
#   uv run afterhours_scanner.py --no-db --csv       # 적재 없이 CSV만
#   uv run afterhours_scanner.py --replay rows.json  # 저장된 행을 DB에 재삽입
#
# 환경변수: KIS_APP_KEY, KIS_APP_SECRET, KIS_IS_REAL (알파래더와 동일)

import argparse
import csv
import json
import logging
import os
import pickle
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

load_dotenv()          # 로컬 실행 시 .env 의 KIS_* 를 읽는다(Actions는 secrets)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("afterhours")

DB_PATH     = Path("data/scores_history.db")
TOKEN_FILE  = Path(".kis_token")          # 알파래더와 같은 파일·같은 포맷을 쓴다
SECTOR_CACHE = Path("data/cache/sector_map_v1.pkl")
UNIV_CACHE  = Path("data/cache/universe_k200_kq150.json")   # 유니버스 폴백(커밋 대상)
ROWS_JSON   = Path("afterhours_rows.json")   # 푸시 충돌 시 재삽입용(리포 밖 취급)
KST         = timezone(timedelta(hours=9))
SESSION     = (15 * 60 + 40, 20 * 60)        # NXT 애프터마켓 15:40~20:00 KST
AFTER_START = "154000"                       # 분봉 필터용 HHMMSS
AFTER_END   = "200000"


def _kst_now() -> datetime:
    return datetime.now(KST)


def session_minutes(now: datetime | None = None) -> int:
    """애프터마켓 개시(15:40 KST) 이후 경과 분. 개시 전이면 음수."""
    now = now or _kst_now()
    return now.hour * 60 + now.minute - SESSION[0]


# ── KIS ───────────────────────────────────────────────────────────────────────
def _is_real() -> bool:
    return os.getenv("KIS_IS_REAL", "0").strip().lower() in ("1", "true", "yes", "y", "real")


def _base_url() -> str:
    return ("https://openapi.koreainvestment.com:9443" if _is_real()
            else "https://openapivts.koreainvestment.com:29443")


def get_token() -> str:
    """알파래더와 같은 .kis_token 파일을 공유한다.

    KIS는 토큰 발급을 분당 1회로 제한한다. 캐시 파일을 따로 두면 같은 앱키로
    두 프로세스가 각자 발급을 시도해 EGW00133으로 서로를 막는다. 파일과 포맷을
    맞춰 두면 같은 머신에서는 한쪽이 받은 토큰을 다른 쪽이 그대로 쓴다.
    (Actions 러너에서는 .kis_token이 gitignore라 매 런 새로 받는다 — 그때를
    대비해 발급 실패는 짧게 재시도한다.)
    """
    now = datetime.now()
    if TOKEN_FILE.exists():
        try:
            d = yaml.safe_load(TOKEN_FILE.read_text())
            if now < datetime.fromisoformat(d["expires"]) - timedelta(minutes=5):
                return d["token"]
        except Exception:
            pass

    key, secret = os.getenv("KIS_APP_KEY", ""), os.getenv("KIS_APP_SECRET", "")
    if not key or not secret:
        logger.error("KIS_APP_KEY / KIS_APP_SECRET 미설정 — 조회 불가")
        return ""

    for attempt in range(3):
        try:
            r = requests.post(f"{_base_url()}/oauth2/tokenP",
                              json={"grant_type": "client_credentials",
                                    "appkey": key, "appsecret": secret}, timeout=10)
            r.raise_for_status()
            d = r.json()
            exp = now + timedelta(seconds=int(d.get("expires_in", 86400) or 86400))
            TOKEN_FILE.write_text(yaml.dump({"token": d["access_token"],
                                             "expires": exp.isoformat()}))
            logger.info("KIS 토큰 발급 완료")
            return d["access_token"]
        except Exception as e:
            wait = 20 * (attempt + 1)      # 분당 1회 제한을 넘기려면 이 정도는 기다려야 한다
            logger.warning(f"KIS 토큰 발급 실패({attempt+1}/3): {e} — {wait}초 후 재시도")
            if attempt < 2:
                time.sleep(wait)
    return ""


class Quote:
    """현재가 조회. NXT 시장구분이 안 먹으면 통합(UN)으로 한 번만 내려간다.

    NX 응답 스키마가 J와 같은지는 문서로 확정하지 못했다. 그래서 첫 성공 응답의
    키를 한 번 로그로 남긴다 — 다음 런부터는 그 로그를 보고 필드를 확정하면 된다.
    """
    def __init__(self, token: str, rate: float):
        self.token = token
        self.interval = 1.0 / rate
        self.nx_market = "NX"
        self.nx_fail = 0
        self._schema_logged = set()

    def _get(self, code: str, market: str) -> dict | None:
        try:
            r = requests.get(
                f"{_base_url()}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers={"authorization": f"Bearer {self.token}",
                         "appkey": os.getenv("KIS_APP_KEY", ""),
                         "appsecret": os.getenv("KIS_APP_SECRET", ""),
                         "tr_id": "FHKST01010100"},
                params={"FID_COND_MRKT_DIV_CODE": market, "FID_INPUT_ISCD": code},
                timeout=5)
        except requests.RequestException:
            return None
        finally:
            time.sleep(self.interval)
        if r.status_code != 200:
            return None
        d = r.json()
        if d.get("rt_cd") != "0":
            return None
        out = d.get("output")
        if out and market not in self._schema_logged:
            self._schema_logged.add(market)
            logger.info(f"[{market}] 응답 필드 {len(out)}개: {','.join(sorted(out)[:18])}…")
        return out

    def _bars(self, code: str, hour: str) -> list:
        """NXT 당일 분봉 30건(hour 기준 역순)."""
        try:
            r = requests.get(
                f"{_base_url()}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
                headers={"authorization": f"Bearer {self.token}",
                         "appkey": os.getenv("KIS_APP_KEY", ""),
                         "appsecret": os.getenv("KIS_APP_SECRET", ""),
                         "tr_id": "FHKST03010200"},
                params={"FID_ETC_CLS_CODE": "", "FID_COND_MRKT_DIV_CODE": self.nx_market,
                        "FID_INPUT_ISCD": code, "FID_INPUT_HOUR_1": hour,
                        "FID_PW_DATA_INCU_YN": "Y"},
                timeout=8)
        except requests.RequestException:
            return []
        finally:
            time.sleep(self.interval)
        if r.status_code != 200:
            return []
        d = r.json()
        return (d.get("output2") or []) if d.get("rt_cd") == "0" else []

    def after_hours(self, code: str, until: str) -> dict:
        """15:40 이후 NXT 체결만 집계한다.

        현재가 조회의 acml_vol·acml_tr_pbmn 을 그대로 쓰면 안 된다. 넥스트레이드는
        자체 정규장도 돌리므로 그 값은 NXT '하루 전체' 누적이다 — 실측으로
        두산에너빌리티가 하루 1,674억인데 애프터마켓 구간만 보면 972억이었다.
        저녁에 새로 들어온 수급만 떼어 보는 게 이 모듈의 목적이라 분봉을 합산한다.
        30봉씩 역순으로 내려가며 15:40에 닿으면 멈춘다(최대 12쪽 = 360분).
        """
        seen, hour = {}, until
        for _ in range(12):
            rows = self._bars(code, hour)
            if not rows:
                break
            for b in rows:
                seen[b.get("stck_cntg_hour", "")] = b
            lo = min(b.get("stck_cntg_hour", "999999") for b in rows)
            if lo <= AFTER_START:
                break
            t = int(lo[:2]) * 60 + int(lo[2:4]) - 1
            nxt_hour = f"{t // 60:02d}{t % 60:02d}00"
            if t < 0 or nxt_hour == hour:
                break
            hour = nxt_hour
        bars = [b for t, b in seen.items() if AFTER_START <= t <= AFTER_END]
        if not bars:
            return {"vol": 0, "value": 0.0, "high": None, "low": None, "bars": 0}
        vol = sum(int(_f(b, "cntg_vol")) for b in bars)
        val = sum(int(_f(b, "cntg_vol")) * _f(b, "stck_prpr") for b in bars)
        traded = [b for b in bars if _f(b, "cntg_vol") > 0]
        return {"vol": vol, "value": val / 1e8, "bars": len(bars),
                "high": max((_f(b, "stck_hgpr") for b in traded), default=None),
                "low":  min((_f(b, "stck_lwpr") for b in traded), default=None)}

    def krx(self, code: str) -> dict | None:
        return self._get(code, "J")

    def nxt(self, code: str) -> dict | None:
        out = self._get(code, self.nx_market)
        if out is None and self.nx_market == "NX":
            self.nx_fail += 1
            # 연속으로 안 되면 시장구분 자체가 안 먹는 것으로 보고 통합으로 내린다.
            if self.nx_fail >= 20:
                self.nx_market = "UN"
                logger.warning("NX 시장구분 연속 실패 20회 → UN(통합)으로 전환")
        elif out is not None:
            self.nx_fail = 0
        return out


def _f(d: dict, *keys, default=0.0) -> float:
    for k in keys:
        v = d.get(k)
        if v not in (None, "", "-"):
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return default


# ── 유니버스·섹터 ─────────────────────────────────────────────────────────────
def load_sectors() -> dict:
    """fg_industry 체계를 그대로 쓴다(2026-08-21에 소속부 → fg_industry로 바로잡은 것)."""
    if not SECTOR_CACHE.exists():
        logger.warning(f"{SECTOR_CACHE} 없음 — 섹터 없이 진행")
        return {}
    with SECTOR_CACHE.open("rb") as f:
        blob = pickle.load(f)
    meta = blob.get("meta", {})
    return {c: (m.get("fg_industry") or m.get("fg_sector") or "기타")
            for c, m in meta.items()}


def load_names() -> dict:
    """종목코드 → 종목명.

    KIS 현재가 응답에는 종목명이 없다(hts_kor_isnm 부재, bstp_kor_isnm은 업종명이다).
    FDR 상장목록이 한 번의 호출로 전체를 주므로 그걸 쓰고, 실패하면 DB에 이미
    쌓인 이름으로 메운다. 이름이 없어도 코드로 식별되니 치명적이진 않다.
    """
    try:
        import FinanceDataReader as fdr
        df = fdr.StockListing("KRX")
        return dict(zip(df["Code"].astype(str), df["Name"].astype(str)))
    except Exception as e:
        logger.warning(f"FDR 상장목록 조회 실패({e}) — DB 기록으로 대체")
    names = {}
    if DB_PATH.exists():
        con = sqlite3.connect(DB_PATH)
        try:
            names = dict(con.execute(
                "SELECT ticker, name FROM scan_results WHERE name IS NOT NULL"))
        except sqlite3.Error:
            pass
        finally:
            con.close()
    return names


def _write_univ_cache(codes: list[str], source: str) -> None:
    """마지막으로 성공한 유니버스를 파일로 남긴다. 다음 런이 조회에 실패해도
    같은 구성으로 돌 수 있어야 시계열 비교가 깨지지 않는다."""
    try:
        UNIV_CACHE.parent.mkdir(parents=True, exist_ok=True)
        UNIV_CACHE.write_text(json.dumps({
            "generated": datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
            "source": source, "count": len(codes), "codes": codes,
        }, ensure_ascii=False, indent=1))
    except Exception as e:
        logger.warning(f"유니버스 캐시 기록 실패({e}) — 무시하고 진행")


def _read_univ_cache() -> list[str]:
    if not UNIV_CACHE.exists():
        return []
    try:
        d = json.loads(UNIV_CACHE.read_text())
        codes = [str(c).zfill(6) for c in d.get("codes", [])]
        if codes:
            logger.info(f"유니버스 캐시 {len(codes)}종목 사용 "
                        f"(생성 {d.get('generated','?')}, 출처 {d.get('source','?')})")
        return codes
    except Exception as e:
        logger.warning(f"유니버스 캐시 읽기 실패({e})")
        return []


def rebuild_univ_cache() -> list[str]:
    """FDR 상장목록에서 코스피 시총 상위 200 + 코스닥 상위 150으로 캐시를 만든다.

    지수 구성종목과 완전히 같지는 않다 — K200/KQ150은 시총 외에 유동비율·
    업종 배분·정기변경 주기가 걸린다. 애프터마켓 상승 종목 관찰이 목적이라
    이 정도 근사로 충분하고, 워크플로가 죽는 것을 막는 게 우선이다.
    정식 소스 교체(KIS 지수구성종목 조회 또는 KRX 정보데이터시스템 CSV)는
    이월 과제다.
    """
    import FinanceDataReader as fdr
    df = fdr.StockListing("KRX")
    # 보통주만 남긴다. 우선주는 코드 끝자리가 0이 아니고, 스팩은 애프터마켓
    # 관찰 대상이 아니다.
    df = df[df["Code"].astype(str).str.endswith("0")]
    df = df[~df["Name"].astype(str).str.contains("스팩", na=False)]
    df = df[df["Marcap"].notna()]
    kp = df[df["Market"] == "KOSPI"].nlargest(200, "Marcap")
    kq = df[df["Market"].isin(["KOSDAQ", "KOSDAQ GLOBAL"])].nlargest(150, "Marcap")
    codes = sorted({str(c).zfill(6) for c in list(kp["Code"]) + list(kq["Code"])})
    logger.info(f"FDR 시총 근사 유니버스 생성: 코스피 {len(kp)} + 코스닥 {len(kq)} "
                f"= {len(codes)}종목")
    _write_univ_cache(codes, "fdr_marcap_top(kospi200+kosdaq150)")
    return codes


def load_universe(path: str | None) -> list[str]:
    if path:
        return [l.strip().zfill(6) for l in Path(path).read_text().splitlines() if l.strip()]
    try:
        from pykrx import stock
        today = datetime.now().strftime("%Y%m%d")
        codes = (stock.get_index_portfolio_deposit_file("1028", today)      # KOSPI200
                 + stock.get_index_portfolio_deposit_file("2203", today))   # KOSDAQ150
        if codes:
            codes = sorted({c.zfill(6) for c in codes})
            _write_univ_cache(codes, "pykrx_index_portfolio")
            return codes
        raise RuntimeError("빈 목록")
    except Exception as e:
        # 유니버스 구성이 날마다 달라지면 시계열 비교가 깨진다. pykrx가 흔들리면
        # 마지막 성공분 → 직전 런 순으로 같은 구성을 재사용한다.
        # 2026-08-26 현재 pykrx는 KRX 응답 스키마 변경으로 죽어 있어 이 경로가
        # 상시 경로다.
        logger.warning(f"pykrx 유니버스 조회 실패({e}) — 캐시 폴백")
        cached = _read_univ_cache()
        if cached:
            return cached
        logger.warning("유니버스 캐시 없음 — 직전 런 유니버스 재사용 시도")
        if DB_PATH.exists():
            con = sqlite3.connect(DB_PATH)
            try:
                last = con.execute("SELECT MAX(scan_date) FROM afterhours_universe").fetchone()[0]
                if last:
                    rows = [r[0] for r in con.execute(
                        "SELECT code FROM afterhours_universe WHERE scan_date=?", (last,))]
                    logger.info(f"직전 유니버스 {len(rows)}종목 재사용 (기준 {last})")
                    return rows
            except sqlite3.Error:
                pass
            finally:
                con.close()
        logger.error("유니버스를 구성하지 못했다 — 중단. "
                     "`--rebuild-universe` 로 캐시를 만들어 두면 이 경로가 열린다")
        return []


def index_moves() -> dict:
    """당일 KOSPI·KOSDAQ 등락률. '전일 지수 -2% 이하인 날' 분리 집계의 근거 컬럼."""
    try:
        import FinanceDataReader as fdr
        out = {}
        for key, sym in (("kospi_pct", "KS11"), ("kosdaq_pct", "KQ11")):
            df = fdr.DataReader(sym, (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"))
            out[key] = round(float(df["Close"].pct_change().iloc[-1]) * 100, 2) if len(df) > 1 else None
        return out
    except Exception as e:
        logger.warning(f"지수 등락률 조회 실패: {e}")
        return {"kospi_pct": None, "kosdaq_pct": None}


# ── DB ────────────────────────────────────────────────────────────────────────
def init_db(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS afterhours_history (
            scan_date TEXT, code TEXT, name TEXT, sector TEXT,
            close_krx REAL, price_nxt REAL, pct REAL,
            -- vol/value 는 '애프터마켓 구간'(15:40~20:00) 분봉 합산이다.
            -- vol_nxt_day/value_nxt_day 는 현재가 조회의 NXT 하루 누적으로,
            -- 둘이 크게 어긋나면 분봉 수집이 샌 것이라 대조용으로 같이 남긴다.
            vol INTEGER, value REAL,
            high_after REAL, low_after REAL, bars INTEGER,
            vol_nxt_day INTEGER, value_nxt_day REAL,
            sector_peers INTEGER,
            kospi_pct REAL, kosdaq_pct REAL,
            -- 러너 localtime은 UTC라 default에 맡기면 뜻이 흐려진다. KST로 명시한다.
            -- session_min(개시 후 경과 분)은 큐 지연 때문에 날마다 다르다.
            -- 스냅샷 시각이 결과에 얼마나 먹히는지 나중에 통제하려면 이 값이 필요하다.
            captured_kst TEXT, session_min INTEGER,
            PRIMARY KEY (scan_date, code)
        );
        CREATE INDEX IF NOT EXISTS idx_ah_date   ON afterhours_history(scan_date);
        CREATE INDEX IF NOT EXISTS idx_ah_sector ON afterhours_history(scan_date, sector);
        CREATE TABLE IF NOT EXISTS afterhours_universe (
            scan_date TEXT, code TEXT,
            PRIMARY KEY (scan_date, code)
        );
    """)


def save(rows: list[dict], universe: list[str], scan_date: str) -> int:
    con = sqlite3.connect(DB_PATH)
    try:
        init_db(con)
        for r in rows:
            con.execute("""INSERT OR REPLACE INTO afterhours_history
                (scan_date,code,name,sector,close_krx,price_nxt,pct,vol,value,
                 high_after,low_after,bars,vol_nxt_day,value_nxt_day,
                 sector_peers,kospi_pct,kosdaq_pct,captured_kst,session_min)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (scan_date, r["code"], r["name"], r["sector"], r["close_krx"],
                 r["price_nxt"], r["pct"], r["vol"], r["value"],
                 r.get("high_after"), r.get("low_after"), r.get("bars"),
                 r.get("vol_nxt_day"), r.get("value_nxt_day"),
                 r["sector_peers"], r.get("kospi_pct"), r.get("kosdaq_pct"),
                 r.get("captured_kst"), r.get("session_min")))
        for c in universe:
            con.execute("INSERT OR REPLACE INTO afterhours_universe VALUES (?,?)", (scan_date, c))
        con.commit()
    finally:
        con.close()
    return len(rows)


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="NXT 애프터마켓 상승 종목 관찰 로깅")
    ap.add_argument("--tickers", help="종목코드 파일(한 줄 1종목). 없으면 K200+KQ150")
    ap.add_argument("--min-pct",   type=float, default=1.0,  help="최소 등락률 %%")
    ap.add_argument("--min-value", type=float, default=0.5,
                    help="최소 시간외 거래대금(억). 얇은 호가의 한 틱 왜곡을 막는다")
    ap.add_argument("--rate",  type=float, default=15.0, help="초당 요청 수(실전 한도 20)")
    ap.add_argument("--date",  help="scan_date 지정(YYYYMMDD). 없으면 오늘")
    ap.add_argument("--no-db", action="store_true", help="DB에 적재하지 않는다")
    ap.add_argument("--csv",   action="store_true", help="CSV로도 저장")
    ap.add_argument("--replay", help="저장된 rows.json을 DB에 재삽입하고 종료")
    ap.add_argument("--force", action="store_true", help="애프터마켓 창 밖에서도 실행")
    ap.add_argument("--rebuild-universe", action="store_true",
                    help="FDR 시총 상위(코스피 200 + 코스닥 150)로 유니버스 캐시를 "
                         "다시 만들고 종료. pykrx가 죽어 있을 때의 부트스트랩이다")
    args = ap.parse_args()

    scan_date = args.date or datetime.now().strftime("%Y%m%d")

    if args.rebuild_universe:
        codes = rebuild_univ_cache()
        logger.info(f"유니버스 캐시 기록 완료: {UNIV_CACHE} ({len(codes)}종목)")
        return

    if args.replay:
        blob = json.loads(Path(args.replay).read_text())
        n = save(blob["rows"], blob.get("universe", []), blob["scan_date"])
        logger.info(f"재삽입 완료: {n}건 (기준일 {blob['scan_date']})")
        return

    # 애프터마켓 수량·대금은 분봉(과거 데이터)에서 뽑으므로 장중에 붙어 있을
    # 필요가 없다. 오히려 20:00 마감 뒤에 돌리면 구간이 완결된다. 그래서 Actions
    # 큐 지연(daily.yml 실측 중앙 56~117분, 최대 204분)이 문제가 되지 않는다 —
    # 늦게 도착할수록 완전해진다. 막아야 하는 건 개시 전(15:40 이전) 실행뿐이다.
    # session_min 이 260 미만이면 구간이 덜 찬 스냅샷이라는 뜻이다.
    smin = session_minutes()
    if not args.force and not (0 <= smin <= 24 * 60 - SESSION[0]):
        logger.warning(f"애프터마켓 개시 전 (KST {_kst_now():%H:%M}, 개시까지 {-smin}분) — "
                       f"적재 없이 종료. --force 로 무시할 수 있다")
        return

    token = get_token()
    if not token:
        sys.exit(1)
    codes = load_universe(args.tickers)
    if not codes:
        sys.exit(1)
    sectors = load_sectors()
    names = load_names()
    idx = index_moves()
    logger.info(f"KIS 도메인: {'실전' if _is_real() else 'VTS'} ({_base_url()})")
    logger.info(f"기준일 {scan_date} | KST {_kst_now():%H:%M} (개시 후 {smin}분"
                f"{', 구간 완결' if smin >= SESSION[1] - SESSION[0] else ', 구간 미완'}) | "
                f"유니버스 {len(codes)}종목 | "
                f"KOSPI {idx.get('kospi_pct')}% KOSDAQ {idx.get('kosdaq_pct')}% | "
                f"예상 {len(codes) * 2 / args.rate / 60:.1f}분")

    q = Quote(token, args.rate)
    until = min(_kst_now().strftime("%H%M%S"), AFTER_END)

    # 1단계 — 유니버스 전 종목의 현재가 2콜(J·NX)로 등락률만 먼저 거른다.
    # 분봉은 종목당 9콜이라 전 종목에 돌리면 예산이 터진다. 후보만 2단계로 넘긴다.
    cand, seen = [], 0
    for i, code in enumerate(codes):
        krx, nxt = q.krx(code), q.nxt(code)
        if not krx or not nxt:
            continue
        seen += 1
        close_krx = _f(krx, "stck_prpr")
        price_nxt = _f(nxt, "stck_prpr")
        if close_krx <= 0 or price_nxt <= 0:
            continue
        # 등락률 기준은 '당일 정규장 종가 대비'다. HTS 표기(전일 종가 대비)와 다른데,
        # 저녁에 새로 들어온 수급만 떼어 보려면 이쪽이어야 한다. 장 마감 후 J의
        # stck_prpr 이 정규장 종가와 같은 것은 실측으로 확인했다(034020, 15:30 봉 73,000).
        pct = (price_nxt / close_krx - 1) * 100
        if pct >= args.min_pct:
            cand.append({"code": code,
                         "name": names.get(code, ""),
                         "sector": sectors.get(code, "기타"),
                         "close_krx": close_krx, "price_nxt": price_nxt,
                         "pct": round(pct, 2),
                         "vol_nxt_day": int(_f(nxt, "acml_vol")),
                         "value_nxt_day": round(_f(nxt, "acml_tr_pbmn") / 1e8, 2)})
        if (i + 1) % 100 == 0:
            logger.info(f"  1단계 …{i+1}/{len(codes)} (후보 {len(cand)}건)")
    logger.info(f"1단계 완료: 조회 성공 {seen}/{len(codes)} | 등락 ≥{args.min_pct}% 후보 {len(cand)}건")

    # 2단계 — 후보만 분봉으로 애프터마켓 구간 수량·대금을 실측하고 대금 하한을 건다.
    rows = []
    for c in cand:
        a = q.after_hours(c["code"], until)
        c.update({"vol": a["vol"], "value": round(a["value"], 2), "bars": a["bars"],
                  "high_after": a["high"], "low_after": a["low"],
                  "captured_kst": _kst_now().strftime("%Y-%m-%d %H:%M:%S"),
                  "session_min": session_minutes(), **idx})
        if c["value"] < args.min_value:
            continue
        rows.append(c)
    logger.info(f"2단계 완료: 대금 ≥{args.min_value}억 통과 {len(rows)}/{len(cand)}건")

    # 섹터 내 동반 종목 수. 1종목 급등은 무시하라는 규칙이 있지만 여기서는 세기만
    # 하고 버리지 않는다 — 저녁 시세는 소급 수집이 불가능해서, 기준(>=3)을 나중에
    # 못 바꾸게 되는 쪽이 더 위험하다. 필터는 분석 시점의 WHERE 절로 건다.
    peers = {}
    for r in rows:
        peers[r["sector"]] = peers.get(r["sector"], 0) + 1
    for r in rows:
        r["sector_peers"] = peers[r["sector"]]

    rows.sort(key=lambda x: -x["pct"])
    logger.info(f"최종 포착 {len(rows)}건")
    for r in rows[:20]:
        logger.info(f"  {r['code']} {r['name'][:10]:<10s} {r['sector'][:8]:<8s} "
                    f"{r['pct']:+6.2f}%  애프터 {r['value']:8.1f}억 / 하루 "
                    f"{r['value_nxt_day']:8.1f}억  동반 {r['sector_peers']}")
    top = sorted(peers.items(), key=lambda x: -x[1])[:5]
    logger.info("섹터 동반: " + ", ".join(f"{s}({n})" for s, n in top) if top else "섹터 동반: 없음")

    ROWS_JSON.write_text(json.dumps(
        {"scan_date": scan_date, "rows": rows, "universe": codes}, ensure_ascii=False))

    if not args.no_db:
        logger.info(f"DB 적재 {save(rows, codes, scan_date)}건 → afterhours_history")
    if args.csv and rows:
        out = Path(f"afterhours_{scan_date}.csv")
        with out.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
        logger.info(f"CSV 저장: {out}")


if __name__ == "__main__":
    main()
