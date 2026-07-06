"""
AlphaRadar v3.3.1 — 단일 파일 안정화 버전
정형:비정형 = 6:4 (S_text = 뉴스+공시 FinBERT)

변경사항:
  v3.3.1 (2026-05-26): Phase A.1 — 매핑 신뢰도 게이트
  [외부 보고서 6건 매핑 오염 사례 대응]
  - B7: _is_subject() — 종목명이 텍스트 주체인지 검증 (위치+횟수)
  - R5: score_with_confidence() — 감성 점수에 confidence 동시 산출
  - C1: run_step3 신뢰도 게이트 — confidence < 0.5 면 감성 폐기
  - C2: fmt() 경고 표시 — ⚠️ 매핑 신뢰도 낮음
  - hotfix v3: news_texts 캐시 어제 종목 → pool_b 가드 추가

  v3.3 (2026-05-25): Phase A — Tier 1·2 통합 패치
  [TelegramClient 강화]
  - R1: timeout=(3, 10) 분리 — DNS 막힘 빠른 컷
  - R2: 429 Retry-After 헤더 우선 처리 (상한 30초)
  - R3: token AND chat_id 둘 다 있어야 enabled, 하나만 있으면 콘솔 폴백
  - R4: 분할 발송 시 [k/N] 청크 헤더 — 사용자 끊김 인지 가능
  [뉴스-종목 매핑 정밀화]
  - B6: 네이버 search query 자동 따옴표 (3자↑)
  - B1: 짧은 종목명(<3자) 컨텍스트 토큰 동반 필수
  - B2: 종목명 직후 자회사/계열사 접미사 검사 (서비스/증권/홀딩스 등)
  - B4: 종목명 직후 우선주 접미사 검사 ('우', '우B' 등)
  - WB: 한국어 조사 화이트리스트 기반 단어 경계 (오리온자리 차단)
  - B3: 시황 토큰이 종목명보다 앞에 등장하면 reject
  - B5: _is_duplicate 사전 필터(길이비율·head/tail) — O(n²) 비용 감소
  [캐시 정책]
  - A2: step3 텍스트(뉴스·공시) 일자별 incremental 캐시
  - A3: DART 섹터 v1→v2 변환 시 mtime 승계
  - A4: DART corp_codes 30일 만료 + last-known-good 폴백

  v3.2 (2026-05-21):
  - 수급: pykrx → KIS API 교체
  - KIS: 멀티스레드 토큰 경합 방지 (threading.Lock 이중 체크)
  - 섹터: DART API로 KOSPI 업종명 보완 (30일 캐시)
  - config: 이격도 상한 하향(115/120/130), cap_tier 하드코딩 제거
  - 가중치: w_tech/w_text/w_cross 명시 (w1/w2/w3 하위호환 유지)

실행:
  python3 alpharadar.py                  # 전체 실행
  python3 alpharadar.py --dry-run        # 실제 데이터, 텔레그램 미발송
  python3 alpharadar.py --mock --dry-run # 목 데이터 테스트
  python3 alpharadar.py --limit 300      # 종목 수 제한
  python3 alpharadar.py --setup-dart     # 최초 1회 DART 법인코드 초기화

스코어 공식:
  S = T×0.30 + S_text×0.40 + D×0.30
"""

# ── 표준 라이브러리 ────────────────────────────────────────────────────────────
import argparse, io, logging, os, pickle, random, re, sqlite3, sys, time, threading, zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path

# ── 외부 라이브러리 ────────────────────────────────────────────────────────────
import numpy as np
import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# ── FinBERT 선택적 로드 ────────────────────────────────────────────────────────
try:
    from transformers import pipeline as hf_pipeline
    _FINBERT_AVAILABLE = True
except ImportError:
    _FINBERT_AVAILABLE = False

# ══════════════════════════════════════════════════════════════════════════════
# 설정
# ══════════════════════════════════════════════════════════════════════════════
CACHE_DIR = Path("data/cache")
DB_PATH   = Path("data/scores_history.db")
LOG_DIR   = Path("data/logs")

DEFAULT_CONFIG = {
    "universe": {
        "min_price": 500, "min_market_cap": 30_000_000_000,
        "min_listed_days": 126, "lookback_days": 125,
    },
    "engine_a": {
        "vol_base_days": 60, "vol_recent_days": 5,
        "vol_surge_pct": 0.50,
        "net_buy_recent_days": 5, "net_buy_min_days": 3,
    },
    "engine_b": {"hype_trend_days": 7, "max_negative_sentiment": 0.30,
                  "hype_top_n": 500},
    "cap_tier": {
        "large_threshold": 5_000_000_000_000,
        "mid_threshold":     500_000_000_000,
    },
    "filter": {
        "max_disparity_large": 115,
        "max_disparity_mid":   120,
        "max_disparity_small": 130,
        "min_disparity": 93,
        "require_ma_trend": True,
        "max_rsi": 80,
        "min_sector_peers": 2,
        "min_turnover_ratio":  0.01,
        "min_turnover_amount": 2_000_000_000,
    },
    "scoring": {
        "w_tech": 0.30, "w_text": 0.40, "w_cross": 0.30,
        "w1": 0.30, "w2": 0.40, "w3": 0.30,
        "finbert_model": "snunlp/KR-FinBert-SC",
        "news_count":   10,
        "news_weight":  0.60,
        "dart_weight":  0.40,
        "n_accel_window": 3, "v_surge_rank": 20,
        "strength_top_pct": 0.20,
        "dart_days": 90,
        "dart_positive": {},
    },
    "grade": {"high_interest": 75, "interest": 65, "min_display_score": 55},
    "negative_keywords": [
        "배임","횡령","유상증자","하한가","상장폐지","불성실공시",
        "감사의견거절","자본잠식","회계감리","검찰","구속",
    ],
}

def load_config() -> dict:
    if Path("config.yaml").exists():
        with open("config.yaml", encoding="utf-8") as f:
            return yaml.safe_load(f)
    return DEFAULT_CONFIG

def validate_config(cfg: dict):
    s = cfg["scoring"]
    w = round(s.get("w_tech", s.get("w1",0)) +
              s.get("w_text", s.get("w2",0)) +
              s.get("w_cross", s.get("w3",0)), 10)
    assert w == 1.0, f"가중치 합계 오류: {w}"

# ══════════════════════════════════════════════════════════════════════════════
# KSIC (한국표준산업분류) 코드 → 한글 업종명 매핑
# ══════════════════════════════════════════════════════════════════════════════
KSIC_DIVISION_MAP = {
    "01": "농업",        "02": "임업",        "03": "어업",
    "05": "광업",        "06": "광업",        "07": "광업",
    "10": "식료품",      "11": "음료",        "12": "담배",
    "13": "섬유",        "14": "의복",        "15": "가죽·신발",
    "16": "목재",        "17": "제지",        "18": "인쇄",
    "19": "석유정제",    "20": "화학",        "21": "제약·바이오",
    "22": "고무·플라스틱","23": "비금속광물제품",
    "24": "철강",        "25": "금속가공",
    "26": "전자·통신장비","27": "정밀·광학기기",
    "28": "전기장비",    "29": "기계장비",
    "30": "자동차",      "31": "기타 운송장비",
    "32": "가구",        "33": "기타 제조",
    "35": "전기·가스",   "36": "수도",
    "37": "하수처리",    "38": "폐기물·재활용", "39": "환경복원",
    "41": "건설",        "42": "전문건설",
    "45": "자동차 판매", "46": "도매·중개",   "47": "소매",
    "49": "육상운송",    "50": "해상운송",    "51": "항공운송",
    "52": "물류·창고",
    "55": "숙박",        "56": "음식·주점",
    "58": "출판",        "59": "영상·음반",   "60": "방송",
    "61": "통신서비스",  "62": "소프트웨어",  "63": "정보서비스",
    "64": "금융",        "65": "보험",        "66": "금융보조서비스",
    "68": "부동산",      "70": "연구개발",
    "71": "전문서비스",  "72": "엔지니어링",  "73": "기타 전문서비스",
    "74": "사업지원",    "75": "임대업",      "76": "임대업",
    "84": "공공행정",    "85": "교육",        "86": "보건",
    "87": "사회복지",    "90": "창작·예술",   "91": "스포츠·오락",
    "94": "협회·단체",   "95": "수리업",      "96": "개인서비스",
}

def _ksic_to_sector(code) -> str:
    if code is None: return ""
    s = str(code).strip()
    if not s or not s[0].isdigit(): return s
    prefix = s[:2]
    return KSIC_DIVISION_MAP.get(prefix, f"기타({s})")

# ══════════════════════════════════════════════════════════════════════════════
# 로깅
# ══════════════════════════════════════════════════════════════════════════════
def setup_logging(date_str: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"data/logs/scanner_{date_str}.log", encoding="utf-8"),
    ])
    return logging.getLogger("main")

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# 유틸리티 — 기술적 지표
# ══════════════════════════════════════════════════════════════════════════════
def moving_average(series, days):
    if len(series) < days: return float("nan")
    return float(series.iloc[-days:].mean())

def linear_slope(values):
    arr = np.array(values, dtype=float)
    if len(arr) < 2 or np.nanmean(arr) == 0: return 0.0
    norm = arr / np.nanmean(arr)
    try: return float(np.polyfit(np.arange(len(norm)), norm, 1)[0])
    except (ValueError, np.linalg.LinAlgError): return 0.0

def disparity(price, ma20):
    if not ma20 or ma20 == 0: return 0.0
    return (price / ma20) * 100

def resistance_top(df):
    if df is None or len(df) < 10: return float("inf")
    threshold = df["거래량"].quantile(0.90)
    price_col = "고가" if "고가" in df.columns else "종가"
    high_vol = df[df["거래량"] >= threshold][price_col]
    return float(high_vol.max()) if not high_vol.empty else float("inf")

def is_top_percentile(value, all_values, pct=0.20):
    if not all_values or value is None: return False
    arr = sorted([v for v in all_values if v is not None], reverse=True)
    if not arr: return False
    cutoff = arr[max(1, int(len(arr)*pct)) - 1]
    return value >= cutoff

def calc_rsi(close_series, period=14) -> float:
    if len(close_series) < period+1: return 50.0
    delta = close_series.diff()
    gain  = delta.where(delta>0, 0.0)
    loss  = -delta.where(delta<0, 0.0)
    avg_g = gain.rolling(period).mean().iloc[-1]
    avg_l = loss.rolling(period).mean().iloc[-1]
    if avg_l == 0: return 100.0
    rs = avg_g / avg_l
    return round(float(100 - 100/(1+rs)), 1)

def calc_bb_position(close_series, period=20, std_mult=2) -> float:
    if len(close_series) < period: return 50.0
    ma    = close_series.rolling(period).mean().iloc[-1]
    std   = close_series.rolling(period).std().iloc[-1]
    upper = ma + std_mult*std
    lower = ma - std_mult*std
    width = upper - lower
    if width == 0: return 50.0
    pos = (float(close_series.iloc[-1]) - lower) / width * 100
    return round(max(0.0, min(100.0, pos)), 1)

# ══════════════════════════════════════════════════════════════════════════════
# FinBERT 감성 분석
# ══════════════════════════════════════════════════════════════════════════════
_POSITIVE_KW = ["신고가","급등","수주","호실적","흑자전환","매수추천","자사주",
                "배당","목표가상향","신규계약","FDA승인","실적호조","성장"]
_NEGATIVE_KW = ["급락","실적쇼크","소송","횡령","배임","상장폐지","영업손실",
                "적자전환","검찰","과징금","리콜","부도","구조조정"]

class FinBertClient:
    def __init__(self, model_name="snunlp/KR-FinBert-SC"):
        self.model_name = model_name
        self._pipe = None
        self.mode = "finbert" if _FINBERT_AVAILABLE else "keyword"
        if not _FINBERT_AVAILABLE:
            logger.warning("transformers 미설치 → 키워드 폴백 모드\n  설치: pip install transformers torch")

    def _load(self):
        if self._pipe is not None or not _FINBERT_AVAILABLE: return
        try:
            try:
                import torch
                device = 0 if torch.cuda.is_available() else -1
                device_label = f"GPU (cuda:{device})" if device==0 else "CPU"
            except ImportError:
                device, device_label = -1, "CPU"
            batch = 32 if device==0 else 8
            logger.info(f"FinBERT 로딩: {self.model_name} ({device_label}, batch={batch})")
            self._pipe = hf_pipeline(
                "text-classification", model=self.model_name,
                device=device, truncation=True, max_length=512,
                batch_size=batch,
                top_k=None,
            )
            self.device_label = device_label
            logger.info(f"FinBERT 로드 완료 ({device_label})")
        except Exception as e:
            logger.warning(f"FinBERT 로드 실패 → 키워드 폴백: {e}")
            self.mode = "keyword"

    def analyze(self, texts):
        cleaned = [t.strip() for t in texts if t and len(t.strip()) > 3]
        if not cleaned: return []
        if self.mode == "finbert" and self._pipe:
            try:
                raw = self._pipe(cleaned)
                results = []
                for result in raw:
                    prob = {}
                    for item in result:
                        lbl = item["label"].lower()
                        if lbl in ("positive", "pos", "label_2"):   lbl = "positive"
                        elif lbl in ("negative", "neg", "label_0"): lbl = "negative"
                        else:                                         lbl = "neutral"
                        prob[lbl] = round(item["score"], 4)
                    best_lbl = max(prob, key=prob.get)
                    results.append({
                        "label":    best_lbl,
                        "score":    prob[best_lbl],
                        "pos_prob": prob.get("positive", 0.0),
                        "neg_prob": prob.get("negative", 0.0),
                    })
                return results
            except Exception as e:
                logger.warning(f"FinBERT 분석 실패 → 폴백: {e}")
        results = []
        for text in cleaned:
            pos = sum(1 for kw in _POSITIVE_KW if kw in text)
            neg = sum(1 for kw in _NEGATIVE_KW if kw in text)
            if pos > neg:
                lbl = "positive"
                pos_prob = round(min(0.5 + pos * 0.1, 0.95), 4)
                neg_prob = 0.0
            elif neg > pos:
                lbl = "negative"
                pos_prob = 0.0
                neg_prob = round(min(0.5 + neg * 0.1, 0.95), 4)
            else:
                lbl = "neutral"
                pos_prob = neg_prob = 0.0
            results.append({
                "label":    lbl,
                "score":    pos_prob if lbl == "positive" else neg_prob if lbl == "negative" else 0.5,
                "pos_prob": pos_prob,
                "neg_prob": neg_prob,
            })
        return results

    def score(self, texts):
        s, _, _ = self.score_with_best(texts)
        return s

    def score_with_best(self, texts):
        results = self.analyze(texts)
        if not results: return 50.0, "", 0.0
        diffs = [r["pos_prob"] - r["neg_prob"] for r in results]
        overall = round(max(0.0, min(100.0, 50.0 + np.mean(diffs) * 50.0)), 1)
        cleaned_texts = [t.strip() for t in texts if t and len(t.strip()) > 3]
        best_text, best_pct = "", 0.0
        for text, result in zip(cleaned_texts, results):
            if result["pos_prob"] > best_pct:
                best_pct = result["pos_prob"]
                best_text = text
        return overall, best_text, round(best_pct * 100, 1)

    def score_with_confidence(self, texts, name=None, position_threshold=0.3):
        """
        Phase A.1 — 매핑 신뢰도(confidence) 동시 반환.

        반환: (score, best_headline, headline_pct, confidence)

        name 이 주어지면 각 텍스트가 _is_subject 통과 비율로 confidence 계산.
        confidence < 0.5 → score=50 (중립), 헤드라인 빈 문자열.
        """
        if not texts:
            return 50.0, "", 0.0, 0.0
        if name:
            valid = []
            for t in texts:
                if not t:
                    continue
                idx = t.find(name)
                if idx < 0:
                    continue
                if idx / max(len(t), 1) >= position_threshold:
                    continue
                if t.count(name) < 2 and idx > 15:
                    continue
                valid.append(t)
            confidence = round(len(valid) / len(texts), 3) if texts else 0.0
            if confidence < 0.5 or not valid:
                return 50.0, "", 0.0, confidence
            score, headline, pct = self.score_with_best(valid)
            return score, headline, pct, confidence
        score, headline, pct = self.score_with_best(texts)
        return score, headline, pct, 1.0

# ══════════════════════════════════════════════════════════════════════════════
# DB
# ══════════════════════════════════════════════════════════════════════════════
def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _conn() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS scan_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_date TEXT, ticker TEXT, name TEXT, sector TEXT,
                cap_tier TEXT,
                score_total REAL, score_t REAL, score_m REAL, score_d REAL,
                s_text REAL, news_score REAL, dart_score REAL,
                grade TEXT, source TEXT, n_accel INTEGER DEFAULT 0,
                v_surge INTEGER DEFAULT 0, finbert_mode TEXT,
                news_count INTEGER, dart_count INTEGER,
                inst_net INTEGER, foreign_net INTEGER,
                rsi REAL, bb_pos REAL, change_pct REAL,
                vol_slope REAL, net_buy_days INTEGER,
                hype_slope REAL, hype_rank INTEGER, disparity REAL,
                rating_bond TEXT, rating_cp TEXT, fg_sector TEXT,
                fg_industry TEXT, ksic TEXT, largest_holder TEXT,
                created_at TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS engine_b_history (
                scan_date TEXT, ticker TEXT, hype_slope REAL,
                PRIMARY KEY (scan_date, ticker)
            );
            CREATE TABLE IF NOT EXISTS sent_history (
                ticker TEXT, send_date TEXT, grade TEXT, score REAL,
                PRIMARY KEY (ticker, send_date)
            );
            CREATE INDEX IF NOT EXISTS idx_scan_date_ticker
                ON scan_results(scan_date, ticker);
            CREATE INDEX IF NOT EXISTS idx_scan_ticker
                ON scan_results(ticker, scan_date DESC);
            CREATE INDEX IF NOT EXISTS idx_engine_b_ticker
                ON engine_b_history(ticker, scan_date DESC);
            CREATE INDEX IF NOT EXISTS idx_sent_ticker_date
                ON sent_history(ticker, send_date);
        """)
        cols = [r[1] for r in con.execute("PRAGMA table_info(scan_results)").fetchall()]
        if "cap_tier" not in cols:
            con.execute("ALTER TABLE scan_results ADD COLUMN cap_tier TEXT")
            logger.info("DB 마이그레이션: cap_tier 컬럼 추가 완료")
        # master universe 보강 메타 컬럼 (없으면 추가)
        for _c in ("rating_bond","rating_cp","fg_sector","fg_industry","ksic","largest_holder"):
            if _c not in cols:
                con.execute(f"ALTER TABLE scan_results ADD COLUMN {_c} TEXT")
                logger.info(f"DB 마이그레이션: {_c} 컬럼 추가 완료")

@contextmanager
def _conn():
    con = sqlite3.connect(DB_PATH); con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

def save_scan_results(results, scan_date):
    with _conn() as con:
        for r in results:
            con.execute("""INSERT OR REPLACE INTO scan_results
                (scan_date,ticker,name,sector,cap_tier,score_total,score_t,score_m,score_d,
                 s_text,news_score,dart_score,grade,source,n_accel,v_surge,
                 finbert_mode,news_count,dart_count,inst_net,foreign_net,
                 rsi,bb_pos,change_pct,vol_slope,net_buy_days,
                 hype_slope,hype_rank,disparity,
                 rating_bond,rating_cp,fg_sector,fg_industry,ksic,largest_holder)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (scan_date, r["ticker"], r.get("name"), r.get("sector"),
                 r.get("cap_tier"),
                 r.get("score"), r.get("t"), None, r.get("d"),
                 r.get("s_text"), r.get("news_score"), r.get("dart_score"),
                 r.get("grade"), r.get("source"),
                 int(r.get("n_accel",False)), int(r.get("v_surge",False)),
                 r.get("finbert_mode"), r.get("news_count",0), r.get("dart_count",0),
                 r.get("inst_net",0), r.get("foreign_net",0),
                 r.get("rsi",50), r.get("bb_pos",50), r.get("change_pct",0),
                 r.get("vol_slope"), r.get("net_buy_days"),
                 r.get("hype_slope"), r.get("hype_rank"), r.get("disparity"),
                 r.get("rating_bond"), r.get("rating_cp"), r.get("fg_sector"),
                 r.get("fg_industry"), r.get("ksic"), r.get("largest_holder")))

def save_engine_b_history(tickers, precomputed, scan_date):
    with _conn() as con:
        for t in tickers:
            slope = precomputed.get(t,{}).get("hype_slope",0)
            con.execute("INSERT OR REPLACE INTO engine_b_history VALUES (?,?,?)", (scan_date,t,slope))

def get_engine_b_history(ticker, window_days=3):
    with _conn() as con:
        rows = con.execute(
            "SELECT scan_date FROM engine_b_history WHERE ticker=? ORDER BY scan_date DESC LIMIT ?",
            (ticker, window_days*2)).fetchall()
    return [r["scan_date"] for r in rows]

def was_sent_today(ticker, scan_date):
    with _conn() as con:
        row = con.execute("SELECT grade FROM sent_history WHERE ticker=? AND send_date=?",
                          (ticker,scan_date)).fetchone()
    return row["grade"] if row else None

def mark_sent(ticker, scan_date, grade, score):
    with _conn() as con:
        con.execute("INSERT OR REPLACE INTO sent_history VALUES (?,?,?,?)",
                    (ticker,scan_date,grade,score))

# ══════════════════════════════════════════════════════════════════════════════
# API 클라이언트
# ══════════════════════════════════════════════════════════════════════════════
class DartClient:
    def __init__(self):
        self.api_key = os.getenv("DART_API_KEY","")
        if not self.api_key: logger.warning("DART_API_KEY 없음")

    def _get(self, endpoint, params, retries=3):
        params["crtfc_key"] = self.api_key
        for attempt in range(retries):
            try:
                r = requests.get(f"https://opendart.fss.or.kr/api/{endpoint}",
                                 params=params, timeout=10)
                if r.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"DART rate limit → {wait}초 대기")
                    time.sleep(wait)
                    continue
                if r.status_code == 403: return {}
                r.raise_for_status()
                data = r.json()
                if data.get("status") not in ("000", None): return {}
                return data
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.debug(f"DART API 최종 실패 ({endpoint}): {e}")
                    return {}
        return {}

    _DART_PUBLICATION_TYPES = ["A", "B", "C", "I"]

    def get_report_titles(self, corp_code, days=90):
        if not self.api_key or not corp_code: return []
        bgn = (datetime.now()-timedelta(days=days)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")
        titles = []
        for pty in self._DART_PUBLICATION_TYPES:
            data = self._get("list.json", {
                "corp_code":corp_code, "bgn_de":bgn, "end_de":end,
                "pblntf_ty":pty, "page_count":"20",
            })
            for r in data.get("list", []):
                nm = r.get("report_nm")
                if nm: titles.append(nm)
            time.sleep(0.12)
        seen, unique = set(), []
        for t in titles:
            if t not in seen:
                seen.add(t); unique.append(t)
        return unique


# ══════════════════════════════════════════════════════════════════════════════
# 공시 임팩트 분류 (Phase 2)
# ══════════════════════════════════════════════════════════════════════════════
DART_IMPACT_CATEGORIES = {
    "strong_positive": {
        "score": 75,
        "label": "🟢 강호재",
        "keywords": [
            "단일판매ㆍ공급계약", "단일판매·공급계약", "단일판매.공급계약",
            "공급계약체결",
            "영업양수", "영업양도",
            "자기주식취득결정", "자기주식 취득 결정",
            "자기주식취득 결과", "자기주식 취득 결과",
            "자기주식취득신탁계약체결", "자기주식취득 신탁계약",
            "무상증자", "주식분할",
            "현금배당", "현금ㆍ현물배당", "현금·현물배당", "특별배당",
            "타법인 주식 및 출자증권 취득", "타법인주식및출자증권취득",
            "흡수합병", "합병결정",
            "임상시험계획승인", "임상2상승인", "임상3상승인",
            "품목허가", "신약허가", "특허취득",
            "조건부지정승인",
            "주식소각결정", "주식소각",
            "이익소각", "자기주식소각",
            "이익소각결정", "자기주식소각결정",
        ],
    },
    "mild_positive": {
        "score": 60,
        "label": "🟡 중립",
        "keywords": [
            "사업보고서", "분기보고서", "반기보고서",
            "주식배당", "정관변경", "주주총회",
            "임원ㆍ주요주주특정증권등소유상황보고",
        ],
    },
    "negative": {
        "score": 25,
        "label": "⚫ 부정",
        "keywords": [
            "유상증자",
            "전환사채권발행결정", "신주인수권부사채권발행결정",
            "교환사채권발행결정",
            "주요사항보고서(전환사채권발행결정)",
            "주요사항보고서(신주인수권부사채권발행결정)",
            "주요사항보고서(유상증자결정)",
            "자기주식처분결정", "자기주식 처분 결정",
            "자기주식처분 결과", "자기주식 처분 결과",
            "불성실공시법인지정", "불성실공시",
            "관리종목지정",
            "투자주의환기종목지정", "투자위험종목지정", "투자경고종목지정",
            "회계감리", "감사의견거절", "감사범위제한", "감사의견한정",
            "자본잠식", "유보율감소",
            "횡령", "배임",
            "감자결정", "무상감자",
            "회생절차", "파산", "부도",
            "주식매매거래정지", "거래정지",
            "상장적격성실질심사", "상장폐지",
            "임상시험중단", "임상실패", "품목허가취소",
        ],
    },
}

def classify_dart_title(title: str) -> tuple[str, int]:
    if not title: return ("neutral", 50)
    t = title.replace(" ", "")
    for cat in ("negative", "strong_positive", "mild_positive"):
        for kw in DART_IMPACT_CATEGORIES[cat]["keywords"]:
            if kw.replace(" ", "") in t:
                return (cat, DART_IMPACT_CATEGORIES[cat]["score"])
    return ("neutral", 50)

def calc_dart_keyword_score(titles: list[str]) -> tuple[float, str]:
    if not titles: return (50.0, "")
    classified = [(t, *classify_dart_title(t)) for t in titles]
    weighted_sum, weight_total = 0.0, 0.0
    for _, cat, sc in classified:
        w = 2.0 if cat == "negative" else 1.0
        weighted_sum += sc * w
        weight_total += w
    avg_score = weighted_sum / weight_total if weight_total > 0 else 50.0

    non_neutral = [(t, cat, sc) for t, cat, sc in classified if cat != "neutral"]
    if non_neutral:
        best = min(non_neutral, key=lambda x: (-abs(x[2] - 50), x[2]))
        max_impact = best[2]
        best_title = best[0]
    else:
        max_impact = 50
        best_title = ""

    score = max_impact * 0.7 + avg_score * 0.3
    return (round(score, 1), best_title)


# ══════════════════════════════════════════════════════════════════════════════
# 뉴스-종목 매핑 정밀화 — Phase A 모듈 상수 + 단어 경계 헬퍼
# ══════════════════════════════════════════════════════════════════════════════
# 자회사·계열사 접미사: 종목명 직후 이게 붙으면 별도 회사로 판단 (B2)
_SUBSIDIARY_SUFFIXES: tuple[str, ...] = (
    "서비스", "판매", "유통", "물류", "증권", "생명", "화재", "카드",
    "캐피탈", "건설", "에너지", "솔루션", "메디신", "헬스케어", "케미칼",
    "산업", "엔지니어링", "투자", "자산운용", "홀딩스", "지주",
)
# 우선주 접미사: 긴 패턴 우선 매칭(우B → 우) (B4)
_PREFERRED_SUFFIXES: tuple[str, ...] = ("우B", "우C", "우", "1우", "2우B", "3우C")
# 짧은 종목명에 강제할 컨텍스트 토큰 (B1)
_CTX_TOKENS: tuple[str, ...] = (
    "주식", "주가", "상장", "시총", "증권", "투자", "거래량", "공시",
    "실적", "매수", "매도", "수주", "공급계약", "배당", "신고가",
)
# 시황 기사 표지 토큰 (B3)
_MARKET_TOKENS: tuple[str, ...] = (
    "코스피", "코스닥", "KOSPI", "KOSDAQ", "지수", "시황", "마감", "개장",
)
# 한국어 조사 — 긴 것 우선 (greedy match) (WB)
_KO_PARTICLES: tuple[str, ...] = (
    "으로서", "으로써", "에서", "에게", "한테", "께서", "부터", "까지", "마저",
    "조차", "이라", "으로", "라고", "이다",
    "이", "가", "은", "는", "을", "를", "도", "만", "의", "에", "와", "과",
    "로", "께", "뿐", "야", "랑",
)

def _hangul(c: str) -> bool:
    return bool(c) and "\uac00" <= c <= "\ud7a3"

def _has_word_boundary(text: str, idx: int, name_len: int) -> bool:
    """
    종목명 직후가 단어 경계인지 검사 (WB).
      - 텍스트 끝             → True
      - 비한글(공백/구두점/숫자/영문) → True
      - 한글이지만 한국어 조사로 시작하고 그 조사 다음이 비한글 → True
      - 그 외(한글 합성어 가능성) → False (예: 오리온자리)
    """
    tail = text[idx + name_len:]
    if not tail:
        return True
    if not _hangul(tail[0]):
        return True
    for p in _KO_PARTICLES:
        if tail.startswith(p):
            rest = tail[len(p):]
            if not rest or not _hangul(rest[0]):
                return True
    return False


class NaverClient:
    """
    네이버 Open API 클라이언트 — N개 키 지원, API별 독립 회전.
    환경변수 (최대 9개 자동 감지):
      NAVER_CLIENT_ID  + NAVER_CLIENT_SECRET            (키 1)
      NAVER_CLIENT_ID_2 + NAVER_CLIENT_SECRET_2         (키 2)
      ... up to _9
    """
    def __init__(self):
        self.keys: list[tuple[str, str]] = []
        cid  = os.getenv("NAVER_CLIENT_ID", "").strip()
        csec = os.getenv("NAVER_CLIENT_SECRET", "").strip()
        if cid and csec:
            self.keys.append((cid, csec))
        for i in range(2, 10):
            cid  = os.getenv(f"NAVER_CLIENT_ID_{i}", "").strip()
            csec = os.getenv(f"NAVER_CLIENT_SECRET_{i}", "").strip()
            if cid and csec:
                self.keys.append((cid, csec))

        self.client_id     = self.keys[0][0] if self.keys else ""
        self.client_secret = self.keys[0][1] if self.keys else ""

        self._key_idx: dict[str, int] = {"datalab": 0, "search": 0}
        self._key_lock = threading.Lock()

        if not self.keys:
            logger.warning("NAVER_CLIENT_ID 없음 — mock 관심 지수 사용")
        elif len(self.keys) > 1:
            logger.info(f"NaverClient: {len(self.keys)}개 키 로드 (DataLab·검색 독립 회전)")

    def _current_headers(self, kind: str) -> dict:
        if not self.keys:
            return {}
        idx = self._key_idx.get(kind, 0)
        if idx >= len(self.keys):
            idx = len(self.keys) - 1
        cid, csec = self.keys[idx]
        return {"X-Naver-Client-Id": cid,
                "X-Naver-Client-Secret": csec,
                "Content-Type": "application/json"}

    def _rotate_key(self, kind: str, attempted_idx: int) -> bool:
        with self._key_lock:
            current = self._key_idx.get(kind, 0)
            if current > attempted_idx:
                return True
            if current + 1 >= len(self.keys):
                return False
            self._key_idx[kind] = current + 1
            logger.info(f"Naver {kind} 키{current+1} 한도/인증 오류 → 키{current+2} 전환")
            return True

    def _mock(self, days):
        base = random.uniform(30,70)
        return [max(0,base+i*random.uniform(0,3)+random.uniform(-5,5)) for i in range(days)]

    def get_trend(self, keyword, days=7):
        if not self.keys: return self._mock(days)
        body = {"startDate":(datetime.now()-timedelta(days=days+5)).strftime("%Y-%m-%d"),
                "endDate":datetime.now().strftime("%Y-%m-%d"),
                "timeUnit":"date",
                "keywordGroups":[{"groupName":keyword,"keywords":[keyword]}]}
        MAX_NET = 2
        MAX_KEY_ROTATIONS = len(self.keys)
        MAX_BURST_RETRY = 3
        BURST_BACKOFF = [0.5, 1.0, 2.0]
        net_attempts = 0
        rotations = 0
        while True:
            attempted_idx = self._key_idx.get("datalab", 0)
            try:
                r = None
                for burst_try in range(MAX_BURST_RETRY):
                    r = requests.post("https://openapi.naver.com/v1/datalab/search",
                                      json=body, headers=self._current_headers("datalab"),
                                      timeout=(4, 8))
                    if r.status_code == 429 and burst_try < MAX_BURST_RETRY - 1:
                        time.sleep(BURST_BACKOFF[burst_try])
                        continue
                    break
                if r.status_code in (429, 401):
                    if rotations < MAX_KEY_ROTATIONS and self._rotate_key("datalab", attempted_idx):
                        rotations += 1
                        continue
                    logger.warning(f"DataLab 한도/인증 오류({r.status_code}) ({keyword}) — 모든 키 소진")
                    return []
                r.raise_for_status()
                results = r.json().get("results",[])
                if not results: return []
                return [d["ratio"] for d in results[0].get("data",[])[-days:]]
            except (requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError) as e:
                net_attempts += 1
                if net_attempts < MAX_NET:
                    time.sleep(0.5 * net_attempts)
                    continue
                logger.warning(f"DataLab 타임아웃 ({keyword}): {e}")
                return []
            except requests.exceptions.HTTPError as e:
                logger.warning(f"DataLab 실패 ({keyword}): {e}")
                return []
            except Exception as e:
                logger.warning(f"DataLab 실패 ({keyword}): {e}")
                return []

    _AD_MARKERS = [
        "[광고]","[PR]","[협찬]","[이벤트]","보도자료","제공","후원",
        "무료체험","할인쿠폰","신청하세요","이벤트 참여","~하는 방법",
    ]

    def _is_ad(self, text: str) -> bool:
        return any(m in text for m in self._AD_MARKERS)

    def _is_subject(self, text: str, name: str, position_threshold: float = 0.3) -> bool:
        """
        Phase A.1 (B7) — 종목명이 텍스트의 '주체'인지 검증.

        외부 보고서 6건 매핑 오염 사례 대응:
          한솔홀딩스 ← 서한 / 키스트론 ← 산일전기 /
          예스티 ← 주성엔지니어링 / 삼영전자 ← 에스피지 /
          디케이티·성우 ← 성우전자

        규칙:
          - 위치 검증: 종목명이 첫 N% 안에 등장 (기본 30%)
          - 횟수 검증: 1회만 등장 시 매우 앞쪽(15자 이내)이어야 통과
        """
        if not text or not name:
            return False
        idx = text.find(name)
        if idx < 0:
            return False
        text_len = len(text)
        if idx / max(text_len, 1) >= position_threshold:
            return False
        count = text.count(name)
        if count < 2 and idx > 15:
            return False
        return True

    def _is_relevant(self, text: str, company: str) -> bool:
        """
        v3.3 — 단어 경계·자회사·우선주·시황 5중 가드.
          B1: 종목명 < 3자면 컨텍스트 토큰 동반 필수
          B2: 종목명 직후 자회사 접미사 → reject
          B4: 종목명 직후 '우' 접미사 → reject
          WB: 한글이 이어지면 조사일 때만 통과 (오리온자리 차단)
          B3: 시황 토큰이 종목명보다 앞에 등장하면 reject
        """
        if not company or not text:
            return False

        # B1: 2자 이하 — 컨텍스트 토큰 강제
        if len(company) < 3:
            if company not in text:
                return False
            return any(c in text for c in _CTX_TOKENS)

        idx = text.find(company)
        if idx < 0:
            return False

        after = text[idx + len(company): idx + len(company) + 8]

        # B2: 자회사/계열사 접미사
        for sfx in _SUBSIDIARY_SUFFIXES:
            if after.startswith(sfx):
                return False
        # B4: 우선주 접미사 — 긴 패턴부터
        for sfx in _PREFERRED_SUFFIXES:
            if after.startswith(sfx):
                return False

        # WB: 단어 경계 — '오리온자리', '삼성전자세무사' 같은 미등록 합성어 차단
        if not _has_word_boundary(text, idx, len(company)):
            return False

        # B3: 시황 기사 — 지수 토큰이 종목명보다 앞에 등장 시 reject
        if text.count(company) == 1:
            for m in _MARKET_TOKENS:
                m_idx = text.find(m)
                if 0 <= m_idx < idx:
                    return False

        # B7 (Phase A.1): 주체 검증 — 본문 앞쪽 30% 안에 + 2회 이상
        if not self._is_subject(text, company):
            return False

        return True

    def _is_duplicate(self, text: str, seen: list[str], threshold: float = 0.7) -> bool:
        """
        B5 — SequenceMatcher 호출 전 사전 필터로 99% 후보를 즉시 컷.
          가드 1: 길이 비율 < 0.5 면 명백히 다른 기사 → 통과
          가드 2: 첫 5자도 마지막 5자도 다르면 → 통과
          그 외만 SequenceMatcher ratio 계산.
        """
        text_len = len(text)
        if text_len == 0:
            return False
        text_head = text[:5]
        text_tail = text[-5:]
        for s in seen:
            s_len = len(s)
            if s_len == 0:
                continue
            if min(text_len, s_len) / max(text_len, s_len) < 0.5:
                continue
            if text_head != s[:5] and text_tail != s[-5:]:
                continue
            if SequenceMatcher(None, text, s).ratio() > threshold:
                return True
        return False

    def get_news_headlines(self, query, target=10, fetch=30):
        """
        뉴스 헤드라인 수집 — FinBERT 입력용.
        B6: 3자 이상 query 는 자동 따옴표(정확 매칭) — 자회사 흡수 1차 차단.
        """
        if not self.keys: return []

        # B6: 정확 매칭 query 변환
        api_query = f'"{query}"' if query and len(query) >= 3 else query

        MAX_KEY_ROTATIONS = len(self.keys)
        MAX_BURST_RETRY = 3
        BURST_BACKOFF = [0.5, 1.0, 2.0]
        rotations = 0
        raw_items = []
        while True:
            attempted_idx = self._key_idx.get("search", 0)
            got_response = False
            for burst_try in range(MAX_BURST_RETRY):
                try:
                    r = requests.get(
                        "https://openapi.naver.com/v1/search/news.json",
                        headers=self._current_headers("search"),
                        params={"query": api_query, "display": min(fetch,100), "sort":"date"},
                        timeout=10,
                    )
                except requests.exceptions.RequestException as e:
                    logger.warning(f"뉴스 검색 실패 ({query}): {e}")
                    return []

                if r.status_code == 429 and burst_try < MAX_BURST_RETRY - 1:
                    time.sleep(BURST_BACKOFF[burst_try])
                    continue
                got_response = True
                break

            if not got_response:
                return []

            if r.status_code in (429, 401):
                if rotations < MAX_KEY_ROTATIONS and self._rotate_key("search", attempted_idx):
                    rotations += 1
                    continue
                logger.warning(f"뉴스 검색 한도/인증 오류({r.status_code}) ({query}) — 모든 키 소진")
                return []
            try:
                r.raise_for_status()
                raw_items = r.json().get("items", [])
                break
            except requests.exceptions.HTTPError as e:
                logger.warning(f"뉴스 검색 실패 ({query}): {e}")
                return []
            except Exception as e:
                logger.warning(f"뉴스 검색 실패 ({query}): {e}")
                return []

        clean, seen = [], []
        stats = {"ad":0, "irrelevant":0, "duplicate":0, "pass":0}

        for item in raw_items:
            title = re.sub(r"<[^>]+>", "", item.get("title", "")).strip()
            desc  = re.sub(r"<[^>]+>", "", item.get("description", "")).strip()

            if not title:
                continue

            if self._is_ad(title):
                stats["ad"] += 1
                continue

            if not self._is_relevant(title, query) and not self._is_relevant(desc, query):
                stats["irrelevant"] += 1
                continue

            if self._is_duplicate(title, seen):
                stats["duplicate"] += 1
                continue

            text = f"{title}. {desc[:100]}" if desc else title

            clean.append(text)
            seen.append(title)
            stats["pass"] += 1

            if len(clean) >= target:
                break

        logger.debug(
            f"뉴스 필터 [{query}]: 수집 {len(raw_items)}건 → "
            f"광고 -{stats['ad']} 무관 -{stats['irrelevant']} "
            f"중복 -{stats['duplicate']} → 최종 {len(clean)}건"
        )
        time.sleep(0.10)
        return clean


# ══════════════════════════════════════════════════════════════════════════════
# TelegramClient — Phase A 강화 (R1·R2·R3·R4)
# ══════════════════════════════════════════════════════════════════════════════
class TelegramClient:
    """
    v3.3 텔레그램 발송 — 4중 강화.
      R1: timeout=(3, 10) 분리 — connect/read 분리, DNS 막힘 빠른 컷
      R2: 429 Retry-After 헤더 우선 처리 (상한 30초)
      R3: token AND chat_id 둘 다 있어야 enabled, 하나만 있으면 콘솔 폴백
      R4: 분할 발송 시 [k/N] 청크 헤더
    """
    MAX_LEN = 4096
    CIRCUIT_THRESHOLD = 3

    def __init__(self):
        self.token   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        # R3: 둘 다 필수
        self.enabled = bool(self.token and self.chat_id)
        if not self.enabled:
            missing = []
            if not self.token:   missing.append("TELEGRAM_BOT_TOKEN")
            if not self.chat_id: missing.append("TELEGRAM_CHAT_ID")
            logger.warning(f"텔레그램 미설정 ({', '.join(missing)}) — 콘솔 출력 모드")

    def _send_raw(self, text, retries=3):
        if not self.enabled:
            print(text); return True
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text,
                   "parse_mode": "HTML", "disable_web_page_preview": True}
        backoff = [1, 3, 5]
        for i in range(retries):
            try:
                # R1: (connect=3s, read=10s)
                r = requests.post(url, json=payload, timeout=(3, 10))
                # R2: 429 → Retry-After 우선
                if r.status_code == 429:
                    try:
                        wait = int(r.json().get("parameters", {}).get("retry_after", backoff[i]))
                    except Exception:
                        wait = backoff[i]
                    wait = min(max(wait, 1), 30)
                    logger.warning(f"텔레그램 429 → {wait}초 대기 (Retry-After)")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return True
            except Exception as e:
                if i < retries - 1:
                    time.sleep(backoff[i] if i < len(backoff) else 5)
                else:
                    logger.error(f"텔레그램 발송 실패: {e}")
        return False

    def send(self, text):
        """R4: 분할 발송 시 [k/N] 청크 헤더 부착."""
        if len(text) <= self.MAX_LEN:
            return self._send_raw(text)

        lines, chunks, cur, length = text.split("\n"), [], [], 0
        header_reserve = 24
        for line in lines:
            if length + len(line) + 1 > self.MAX_LEN - header_reserve:
                chunks.append("\n".join(cur))
                cur, length = [], 0
            cur.append(line); length += len(line) + 1
        if cur:
            chunks.append("\n".join(cur))

        all_ok, n = True, len(chunks)
        for k, chunk in enumerate(chunks, 1):
            hdr = f"<b>[{k}/{n}]</b>\n" if n > 1 else ""
            if not self._send_raw(hdr + chunk):
                all_ok = False
            time.sleep(0.5)
        return all_ok

    def send_many(self, messages, delay=0.5):
        results, consecutive_fail, circuit_open = [], 0, False
        for idx, msg in enumerate(messages):
            if circuit_open:
                results.append(False); continue
            ok = self.send(msg)
            results.append(ok)
            if ok:
                consecutive_fail = 0
            else:
                consecutive_fail += 1
                if consecutive_fail >= self.CIRCUIT_THRESHOLD:
                    remaining = len(messages) - idx - 1
                    logger.error(
                        f"텔레그램 연속 {self.CIRCUIT_THRESHOLD}개 실패 → 중단 "
                        f"(남은 {remaining}개 skip)"
                    )
                    circuit_open = True
            time.sleep(delay)
        return results


# ══════════════════════════════════════════════════════════════════════════════
# DART 캐시 정책 — Phase A (A4)
# ══════════════════════════════════════════════════════════════════════════════
def load_dart_corp_codes(api_key: str, max_age_days: int = 30) -> dict:
    """
    A4 — corp_codes 30일 만료 정책 + last-known-good 폴백.
    """
    cache_path = CACHE_DIR / "dart_corp_codes.pkl"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    need_refresh = not cache_path.exists()
    if not need_refresh:
        age_days = (datetime.now().timestamp() - cache_path.stat().st_mtime) / 86400
        if age_days > max_age_days:
            need_refresh = True
            logger.info(f"DART corp_code 캐시 {age_days:.0f}일 경과 → 재다운로드")

    if not need_refresh:
        with open(cache_path, "rb") as f:
            return pickle.load(f)

    if not api_key:
        logger.warning("DART_API_KEY 없음 — corp_code 매핑 비활성화")
        return {}

    try:
        r = requests.get(
            "https://opendart.fss.or.kr/api/corpCode.xml",
            params={"crtfc_key": api_key},
            timeout=30,
        )
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        root = ET.fromstring(z.read("CORPCODE.xml").decode("utf-8"))
        corp_map = {
            item.findtext("stock_code", "").strip(): item.findtext("corp_code", "").strip()
            for item in root.findall("list")
            if len(item.findtext("stock_code", "").strip()) == 6
        }
        tmp = cache_path.with_suffix(".pkl.tmp")
        with open(tmp, "wb") as f: pickle.dump(corp_map, f)
        tmp.replace(cache_path)
        logger.info(f"DART corp_code 캐시 갱신 완료: {len(corp_map)}개")
        return corp_map
    except Exception as e:
        logger.warning(f"DART corp_code 다운로드 실패: {e}")
        # fail-soft: 옛 캐시라도 있으면 사용
        if cache_path.exists():
            with open(cache_path, "rb") as f:
                logger.warning("옛 corp_code 캐시 사용 (last-known-good)")
                return pickle.load(f)
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# Step3 텍스트 수집 캐시 — Phase A (A2)
# ══════════════════════════════════════════════════════════════════════════════
def collect_texts_with_cache(pool_b, naver, dart, news_cnt, dart_days,
                              date: str, workers: int = 6):
    """
    A2 — 뉴스·공시 텍스트 일자별 incremental 캐시.
    같은 날 가중치 튜닝 재실행 시 외부 API 0건.
    """
    news_cache = CACHE_DIR / f"news_titles_{date}.pkl"
    dart_cache = CACHE_DIR / f"dart_titles_{date}.pkl"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    news_texts: dict = {}
    dart_texts: dict = {}
    if news_cache.exists():
        try:
            with open(news_cache, "rb") as f: news_texts = pickle.load(f)
        except Exception: news_texts = {}
    if dart_cache.exists():
        try:
            with open(dart_cache, "rb") as f: dart_texts = pickle.load(f)
        except Exception: dart_texts = {}

    missing = [(t, m) for t, m in pool_b.items()
               if t not in news_texts or t not in dart_texts]

    if not missing:
        logger.info(f"텍스트 캐시 전체 적중: {len(pool_b)}종목 (외부 API 0건)")
        return news_texts, dart_texts

    logger.info(
        f"텍스트 신규 수집: {len(missing)}종목 "
        f"(캐시 적중 {len(pool_b) - len(missing)}종목)"
    )

    def _collect(item):
        ticker, meta = item
        nm = meta.get("name", ticker)
        news = naver.get_news_headlines(nm, target=news_cnt, fetch=news_cnt * 3)
        dart_t = (dart.get_report_titles(meta.get("corp_code", ""), dart_days)
                  if meta.get("corp_code") else [])
        return ticker, news, dart_t

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for ticker, news, dart_t in ex.map(_collect, missing):
            news_texts[ticker] = news
            dart_texts[ticker] = dart_t

    # 원자적 저장 — 도중 실패해도 옛 캐시 보존
    tmp_n = news_cache.with_suffix(".pkl.tmp")
    tmp_d = dart_cache.with_suffix(".pkl.tmp")
    with open(tmp_n, "wb") as f: pickle.dump(news_texts, f)
    with open(tmp_d, "wb") as f: pickle.dump(dart_texts, f)
    tmp_n.replace(news_cache)
    tmp_d.replace(dart_cache)

    logger.info(
        f"수집 완료: 뉴스 {sum(len(v) for v in news_texts.values())}건 | "
        f"공시 {sum(len(v) for v in dart_texts.values())}건"
    )
    return news_texts, dart_texts


# ══════════════════════════════════════════════════════════════════════════════
# Mock 데이터
# ══════════════════════════════════════════════════════════════════════════════
MOCK_TICKERS = {
    "005930":("삼성전자","반도체",10_000_000_000_000),
    "000660":("SK하이닉스","반도체",8_000_000_000_000),
    "035420":("NAVER","IT서비스",4_000_000_000_000),
    "035720":("카카오","IT서비스",3_500_000_000_000),
    "051910":("LG화학","2차전지",3_000_000_000_000),
    "006400":("삼성SDI","2차전지",2_800_000_000_000),
    "373220":("LG에너지솔루션","2차전지",2_500_000_000_000),
    "207940":("삼성바이오로직스","바이오",2_000_000_000_000),
    "005380":("현대차","자동차",1_800_000_000_000),
    "000270":("기아","자동차",1_500_000_000_000),
    "247540":("에코프로비엠","2차전지",800_000_000_000),
    "003670":("포스코퓨처엠","2차전지",700_000_000_000),
    "068270":("셀트리온","바이오",600_000_000_000),
    "066570":("LG전자","가전",600_000_000_000),
    "086790":("하나금융지주","금융",550_000_000_000),
    "105560":("KB금융","금융",520_000_000_000),
    "017670":("SK텔레콤","통신",500_000_000_000),
    "030200":("KT","통신",480_000_000_000),
    "086520":("에코프로","2차전지",300_000_000_000),
    "091990":("셀트리온헬스케어","바이오",280_000_000_000),
    "293490":("카카오게임즈","게임",250_000_000_000),
    "112040":("위메이드","게임",200_000_000_000),
    "145020":("휴젤","바이오",180_000_000_000),
    "214150":("클래시스","의료기기",150_000_000_000),
    "950130":("엑세스바이오","바이오",120_000_000_000),
    "039030":("이오테크닉스","반도체장비",100_000_000_000),
    "095340":("ISC","반도체장비",90_000_000_000),
    "328130":("루닛","AI/의료",80_000_000_000),
    "348210":("넥스트칩","AI/반도체",70_000_000_000),
    "396270":("넥스틴","반도체장비",60_000_000_000),
}
MOCK_CORP = {
    "005930":"00126380","000660":"00164779","035420":"00266961","035720":"00918444",
    "051910":"00806577","006400":"00126659","247540":"01166931","373220":"01426674",
    "207940":"00877059","068270":"00104628","003670":"00104636","066570":"00401731",
    "005380":"00164742","000270":"00106641","086790":"00699638","105560":"00382199",
}

def generate_mock_precomputed():
    random.seed(42)
    tickers = list(MOCK_TICKERS.keys())
    scenarios = {
        "strong_both":   tickers[:4],    "engine_a_only": tickers[4:10],
        "engine_b_only": tickers[10:14], "retail_signal": tickers[14:20],
        "weak_signal":   tickers[20:26], "high_disparity":tickers[26:29],
        "no_signal":     tickers[29:],
    }
    precomputed = {}
    for scenario, tlist in scenarios.items():
        for ticker in tlist:
            if ticker not in MOCK_TICKERS: continue
            name, sector, market_cap = MOCK_TICKERS[ticker]
            cap_tier = "large" if market_cap>=5_000_000_000_000 else "mid" if market_cap>=500_000_000_000 else "small"
            base   = random.uniform(3000,80000)
            prices = [base]
            for _ in range(124):
                t = 0.003 if "strong" in scenario else 0.0
                prices.append(max(100, prices[-1]*(1+t+random.gauss(0,0.012))))
            vols = [int(random.randint(100000,5000000)*random.uniform(0.6,1.4)) for _ in range(125)]
            if "a_only" in scenario or "both" in scenario:
                for i in range(120,125): vols[i]=int(vols[i]*random.uniform(1.3,2.0))
            close_s=pd.Series(prices); vol_s=pd.Series(vols)
            ma20=float(close_s.iloc[-20:].mean()); ma60=float(close_s.iloc[-60:].mean())
            ma120=float(close_s.iloc[-120:].mean())
            vol_60ma=float(vol_s.iloc[-60:].mean()); vol_20ma=float(vol_s.iloc[-20:].mean())
            vol_5d=float(vol_s.iloc[-5:].mean())
            current=float(close_s.iloc[-1])
            if "high_disparity" in scenario: current=ma20*1.35
            net_pos = 4 if ("both" in scenario or "a_only" in scenario) else 2 if cap_tier=="small" else 1
            net_list=[random.randint(5_000_000_000,50_000_000_000) if i>=5-net_pos else random.randint(-20_000_000_000,20_000_000_000) for i in range(5)]
            retail_pos = 4 if "retail" in scenario else 2 if cap_tier=="small" else 1
            retail_list=[random.randint(1_000_000_000,5_000_000_000) if i>=5-retail_pos else random.randint(-2_000_000_000,2_000_000_000) for i in range(5)]
            rising="b_only" in scenario or "both" in scenario or "retail" in scenario
            base_h=random.uniform(5,30) if not rising else random.uniform(20,60)
            hype=[max(0,min(100,base_h+(i*random.uniform(0,3) if rising else -i*random.uniform(0,2))+random.gauss(0,2))) for i in range(7)]
            precomputed[ticker]={
                "ticker":ticker,"name":name,"sector":sector,
                "market":"KOSPI" if market_cap>500_000_000_000 else "KOSDAQ",
                "market_cap":market_cap,"cap_tier":cap_tier,"current_price":current,
                "ma20":ma20,"ma60":ma60,"ma120":ma120,"disparity":(current/ma20)*100,
                "vol_60ma":vol_60ma,"vol_20ma":vol_20ma,"vol_5d_avg":vol_5d,
                "vol_slope":float(np.polyfit(range(5),vol_s.iloc[-5:].tolist(),1)[0]/vol_20ma) if vol_20ma else 0,
                "net_buy_days":sum(1 for v in net_list if v>0),
                "net_buy_total":sum(v for v in net_list),"net_buy_list":net_list,
                "retail_buy_days":sum(1 for v in retail_list if v>0),"retail_buy_total":0,
                "inst_net":int(sum(v for v in net_list)*0.6),
                "foreign_net":int(sum(v for v in net_list)*0.4),
                "rsi":round(random.uniform(35,75),1),
                "bb_pos":round(random.uniform(20,90),1),
                "change_pct":round(random.uniform(-3,5),2),
                "w52_high":float(close_s.max()),
                "res_top":float(close_s.iloc[-60:].max()*random.uniform(0.95,1.05)),
                "hype_latest":hype[-1],"hype_7d_ago":hype[0],
                "hype_slope":float(np.polyfit(range(7),hype,1)[0]),
                "hype_rank":0,"neg_ratio":0.05 if "strong" in scenario else random.uniform(0,0.15),
                "corp_code":MOCK_CORP.get(ticker,""),
            }
    items=sorted(precomputed.items(),key=lambda x:x[1]["hype_latest"],reverse=True)
    for rank,(t,_) in enumerate(items,1): precomputed[t]["hype_rank"]=rank
    return precomputed

# ══════════════════════════════════════════════════════════════════════════════
# Step 0 — 유니버스 필터링
# ══════════════════════════════════════════════════════════════════════════════
def _get_last_weekday(date):
    dt=datetime.strptime(date,"%Y%m%d")
    while dt.weekday()>=5: dt-=timedelta(days=1)
    return dt.strftime("%Y%m%d")

def _workdays_before(date,n):
    return (datetime.strptime(date,"%Y%m%d")-timedelta(days=int(n*1.5))).strftime("%Y%m%d")

def _precompute_ticker(ticker, start_date, end_date, info, ucfg):
    import FinanceDataReader as fdr
    try:
        df=fdr.DataReader(ticker,start_date,end_date)
        if df is None or len(df)<20: return None
        close_s=df["Close"].astype(float); volume_s=df["Volume"].astype(float)
        current=float(close_s.iloc[-1])
        if current<ucfg["min_price"]: return None
        ma20=moving_average(close_s,20)
        if np.isnan(ma20) or ma20==0: return None
        ma60=moving_average(close_s,60); ma120=moving_average(close_s,120)
        vol_60ma=moving_average(volume_s,60); vol_20ma=moving_average(volume_s,20)
        vol_5d=moving_average(volume_s,5)
        mkt=info.get("market","KOSPI")

        marcap_real = int(info.get("market_cap", 0) or 0)
        cap = marcap_real if marcap_real > 0 else int(current * float(vol_20ma) * 50)

        if marcap_real > 0 and marcap_real < ucfg.get("min_market_cap", 30_000_000_000):
            return None

        _ct       = ucfg.get("cap_tier", {})
        _large_th = _ct.get("large_threshold", 5_000_000_000_000)
        _mid_th   = _ct.get("mid_threshold",     500_000_000_000)
        cap_tier  = ("large" if cap >= _large_th
                     else "mid" if cap >= _mid_th
                     else "small")
        net_days,net_total,retail_days,inst_net,foreign_net=_get_investor_data(ticker,start_date,end_date)
        rsi    = calc_rsi(close_s)
        bb_pos = calc_bb_position(close_s)
        change_pct = float(df["Change"].iloc[-1]*100) if "Change" in df.columns else 0.0
        sector=str(info.get("sector") or "기타").strip()
        if sector in ("nan","None",""): sector="기타"
        high_s = df["High"].astype(float) if "High" in df.columns else close_s
        hist_df = pd.DataFrame({"종가": close_s, "고가": high_s, "거래량": volume_s})
        return {
            "ticker":ticker,"name":info.get("name") or ticker,"sector":sector,
            # master universe 보강 메타(보조필드) — DB 영속·표시용, 스코어링 미사용
            "rating_bond":info.get("rating_bond"),"rating_cp":info.get("rating_cp"),
            "fg_sector":info.get("fg_sector"),"fg_industry":info.get("fg_industry"),
            "ksic":info.get("ksic"),"largest_holder":info.get("largest_holder"),
            "market":mkt,"market_cap":cap,"cap_tier":cap_tier,"current_price":current,
            "ma20":ma20,"ma60":ma60,"ma120":ma120,"disparity":disparity(current,ma20),
            "vol_60ma":vol_60ma,"vol_20ma":vol_20ma,"vol_5d_avg":vol_5d,
            "vol_slope":linear_slope(volume_s.iloc[-5:].tolist()),
            "net_buy_days":net_days,"net_buy_total":net_total,"net_buy_list":[],
            "retail_buy_days":retail_days,"retail_buy_total":0,
            "inst_net":inst_net,"foreign_net":foreign_net,
            "rsi":rsi,"bb_pos":bb_pos,"change_pct":change_pct,
            "w52_high":float(close_s.max()),"res_top":resistance_top(hist_df.iloc[-60:]),
            "corp_code":"","hype_latest":0.0,"hype_7d_ago":0.0,
            "hype_slope":0.0,"hype_rank":9999,"neg_ratio":0.0,
        }
    except Exception as e:
        logger.debug(f"_precompute_ticker 실패 ({ticker}): {e}")
        return None

# ── KIS API 수급 클라이언트 ───────────────────────────────────────────────────
_KIS_TOKEN_CACHE: dict = {}
_KIS_LAST_CALL   = 0.0
_KIS_INTERVAL    = 0.06
_KIS_WARNED      = False
_KIS_TB_LOGGED   = 0
_KIS_TOKEN_LOCK  = threading.Lock()
_KIS_RATE_LOCK   = threading.Lock()

def _kis_base_url() -> str:
    raw = os.getenv("KIS_IS_REAL", "0").strip().lower()
    is_real = raw in ("1", "true", "yes", "y", "real")
    return ("https://openapi.koreainvestment.com:9443" if is_real
            else "https://openapivts.koreainvestment.com:29443")

def _kis_token() -> str:
    global _KIS_TOKEN_CACHE
    now = datetime.now()

    if _KIS_TOKEN_CACHE.get("token") and _KIS_TOKEN_CACHE.get("expires"):
        if now < _KIS_TOKEN_CACHE["expires"] - timedelta(minutes=5):
            return _KIS_TOKEN_CACHE["token"]

    with _KIS_TOKEN_LOCK:
        now = datetime.now()

        if _KIS_TOKEN_CACHE.get("token") and _KIS_TOKEN_CACHE.get("expires"):
            if now < _KIS_TOKEN_CACHE["expires"] - timedelta(minutes=5):
                return _KIS_TOKEN_CACHE["token"]

        token_file = Path(".kis_token")
        if token_file.exists():
            try:
                data = yaml.safe_load(token_file.read_text())
                exp  = datetime.fromisoformat(data["expires"])
                if now < exp - timedelta(minutes=5):
                    _KIS_TOKEN_CACHE = {"token": data["token"], "expires": exp}
                    return data["token"]
            except Exception:
                pass

        app_key    = os.getenv("KIS_APP_KEY", "")
        app_secret = os.getenv("KIS_APP_SECRET", "")
        if not app_key or app_key == "your_kis_app_key":
            return ""

        try:
            r = requests.post(
                f"{_kis_base_url()}/oauth2/tokenP",
                json={"grant_type": "client_credentials",
                      "appkey": app_key, "appsecret": app_secret},
                timeout=10,
            )
            r.raise_for_status()
            data    = r.json()
            token   = data["access_token"]
            exp_sec = _safe_int(data.get("expires_in", 86400), default=86400)
            if exp_sec <= 0:
                exp_sec = 86400
            expires = datetime.now() + timedelta(seconds=exp_sec)
            _KIS_TOKEN_CACHE = {"token": token, "expires": expires}
            token_file.write_text(yaml.dump({"token": token, "expires": expires.isoformat()}))
            logger.info("KIS 토큰 발급 완료")
            return token
        except Exception as e:
            logger.warning(f"KIS 토큰 발급 실패: {e}")
            return ""

def _kis_get(path: str, params: dict, tr_id: str, retries: int = 3) -> dict:
    global _KIS_LAST_CALL
    token = _kis_token()
    if not token:
        return {}

    app_key    = os.getenv("KIS_APP_KEY", "")
    app_secret = os.getenv("KIS_APP_SECRET", "")

    with _KIS_RATE_LOCK:
        elapsed = time.time() - _KIS_LAST_CALL
        if elapsed < _KIS_INTERVAL:
            time.sleep(_KIS_INTERVAL - elapsed)
        _KIS_LAST_CALL = time.time()

    headers = {
        "authorization": f"Bearer {token}",
        "appkey":        app_key,
        "appsecret":     app_secret,
        "tr_id":         tr_id,
        "custtype":      "P",
    }

    for attempt in range(retries):
        try:
            r = requests.get(
                f"{_kis_base_url()}{path}",
                headers=headers, params=params, timeout=10,
            )
            r.raise_for_status()
            data = r.json()
            if data.get("rt_cd") == "1" and "EGW00201" in data.get("msg_cd", ""):
                wait = 2 ** (attempt + 1)
                logger.warning(f"KIS 속도 제한 → {wait}초 대기")
                time.sleep(wait)
                continue
            return data
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                logger.debug(f"KIS API 최종 실패 ({path}): {e}")
    return {}

def _safe_int(v, default=0):
    if v is None:
        return default
    s = str(v).strip().replace(",", "")
    if s == "" or s == "-":
        return default
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return default

def _get_investor_data(ticker, start_date, end_date):
    global _KIS_WARNED
    app_key = os.getenv("KIS_APP_KEY", "")
    if not app_key or app_key == "your_kis_app_key":
        if not _KIS_WARNED:
            logger.warning(
                "KIS_APP_KEY 미설정 → 수급 데이터를 가져올 수 없습니다.\n"
                "  .env에 KIS_APP_KEY / KIS_APP_SECRET / KIS_IS_REAL 설정 필요"
            )
            _KIS_WARNED = True
        return 0, 0, 0, 0, 0

    try:
        tr_id = "FHKST01010900"
        data  = _kis_get(
            "/uapi/domestic-stock/v1/quotations/inquire-investor",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD":         ticker,
            },
            tr_id=tr_id,
        )

        if os.getenv("KIS_DEBUG_TICKER") == ticker:
            import json
            _out = data.get("output", data.get("output1", data.get("output2", [])))
            logger.info(
                f"[KIS RAW {ticker}] rt_cd={data.get('rt_cd')} "
                f"msg_cd={data.get('msg_cd')} msg1={data.get('msg1')} "
                f"top_keys={list(data.keys())} "
                f"output_type={type(_out).__name__} "
                f"row_count={len(_out) if isinstance(_out, list) else 'N/A'}"
            )
            if isinstance(_out, list) and _out:
                logger.info(
                    f"[KIS ROW0 {ticker}]\n"
                    + json.dumps(_out[0], ensure_ascii=False, indent=2)
                )
            else:
                logger.info(
                    f"[KIS FULL {ticker}]\n"
                    + json.dumps(data, ensure_ascii=False, indent=2)
                )

        output2 = data.get("output", data.get("output2", []))
        if not isinstance(output2, list) or not output2:
            logger.debug(f"KIS 수급 빈/비정상 응답 ({ticker}): type={type(output2).__name__}")
            return 0, 0, 0, 0, 0

        _FIELDS = ("orgn_ntby_tr_pbmn", "frgn_ntby_tr_pbmn", "prsn_ntby_tr_pbmn",
                   "orgn_ntby_qty",     "frgn_ntby_qty",     "prsn_ntby_qty")
        def _settled(r):
            if not isinstance(r, dict):
                return False
            return any(str(r.get(k, "")).strip() not in ("", "-") for k in _FIELDS)

        valid = [r for r in output2 if _settled(r)]
        rows  = valid[:5]
        if not rows:
            logger.debug(f"KIS 수급 미집계 ({ticker})")
            return 0, 0, 0, 0, 0

        use_pbmn = any(
            str(r.get("orgn_ntby_tr_pbmn", "")).strip() not in ("", "-") for r in rows
        )
        if use_pbmn:
            f_inst, f_for, f_ret = "orgn_ntby_tr_pbmn", "frgn_ntby_tr_pbmn", "prsn_ntby_tr_pbmn"
        else:
            f_inst, f_for, f_ret = "orgn_ntby_qty", "frgn_ntby_qty", "prsn_ntby_qty"

        inst_list    = [_safe_int(r.get(f_inst)) for r in rows]
        foreign_list = [_safe_int(r.get(f_for))  for r in rows]
        retail_list  = [_safe_int(r.get(f_ret))  for r in rows]

        combined = [i + f for i, f in zip(inst_list, foreign_list)]

        return (
            sum(1 for v in combined  if v > 0),
            sum(combined),
            sum(1 for v in retail_list if v > 0),
            sum(inst_list),
            sum(foreign_list),
        )

    except Exception as e:
        global _KIS_TB_LOGGED
        if _KIS_TB_LOGGED < 3:
            import traceback
            logger.warning(
                f"_get_investor_data 실패 ({ticker}): {type(e).__name__}: {e}\n"
                f"{traceback.format_exc()}"
            )
            _KIS_TB_LOGGED += 1
        else:
            logger.warning(f"_get_investor_data 실패 ({ticker}): {type(e).__name__}: {e}")
        return 0, 0, 0, 0, 0

def _load_dart_sector_map() -> dict:
    """
    DART API로 KOSPI 종목 업종명 조회 — 캐시 파일 사용 (v2: KSIC 한글명 변환 적용)
    v3.3 (A3): v1→v2 변환 시 mtime 승계 — 30일 만료 정책 정합성 보장.
    """
    cache_path    = CACHE_DIR / "dart_sector_map_v2.pkl"
    cache_path_v1 = CACHE_DIR / "dart_sector_map.pkl"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if cache_path.exists():
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        if datetime.now() - mtime < timedelta(days=30):
            try:
                with open(cache_path, "rb") as f:
                    return pickle.load(f)
            except Exception:
                pass

    if cache_path_v1.exists():
        mtime = datetime.fromtimestamp(cache_path_v1.stat().st_mtime)
        if datetime.now() - mtime < timedelta(days=30):
            try:
                with open(cache_path_v1, "rb") as f:
                    v1_map = pickle.load(f)
                v2_map = {tk: _ksic_to_sector(code) for tk, code in v1_map.items()}
                with open(cache_path, "wb") as f:
                    pickle.dump(v2_map, f)
                # A3: v1 mtime 을 v2 에 승계 — 30일 정책 정합성
                try:
                    v1_mtime = cache_path_v1.stat().st_mtime
                    os.utime(cache_path, (v1_mtime, v1_mtime))
                except Exception as e:
                    logger.debug(f"sector mtime 승계 실패: {e}")
                logger.info(f"DART 섹터맵 v1→v2 변환 완료: {len(v2_map)}개 (KSIC 코드 → 한글 업종명)")
                return v2_map
            except Exception as e:
                logger.warning(f"v1 → v2 변환 실패: {e}")

    api_key = os.getenv("DART_API_KEY", "")
    if not api_key:
        return {}

    logger.info("DART API로 KOSPI 업종 정보 수집 중...")
    sector_map = {}
    try:
        r = requests.get(
            "https://opendart.fss.or.kr/api/corpCode.xml",
            params={"crtfc_key": api_key},
            timeout=30,
        )
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        root = ET.fromstring(z.read("CORPCODE.xml").decode("utf-8"))

        try:
            import FinanceDataReader as fdr
            kospi_tickers = set(fdr.StockListing("KOSPI")["Code"].astype(str).tolist())
        except Exception:
            kospi_tickers = set()

        corp_list = [
            (item.findtext("corp_code","").strip(),
             item.findtext("stock_code","").strip())
            for item in root.findall("list")
            if len(item.findtext("stock_code","").strip()) == 6
            and (not kospi_tickers or item.findtext("stock_code","").strip() in kospi_tickers)
        ]
        logger.info(f"DART 업종 조회 대상: {len(corp_list)}개 KOSPI 종목 (약 {len(corp_list)//20}초 소요)")

        for corp_code, stock_code in tqdm(corp_list, desc="DART 업종", unit="종목"):
            try:
                res = requests.get(
                    "https://opendart.fss.or.kr/api/company.json",
                    params={"crtfc_key": api_key, "corp_code": corp_code},
                    timeout=5,
                )
                data = res.json()
                if data.get("status") == "000":
                    induty_code = str(data.get("induty_code", "")).strip()
                    induty = _ksic_to_sector(induty_code) if induty_code else ""
                    if not induty:
                        cls_map = {"Y": "유가증권", "K": "코스닥", "N": "코넥스", "E": "기타"}
                        induty = cls_map.get(data.get("corp_cls",""), "기타")
                    if induty:
                        sector_map[stock_code] = induty
                time.sleep(0.05)
            except Exception:
                continue

        logger.info(f"DART 업종 수집 완료: {len(sector_map)}개")
        with open(cache_path, "wb") as f:
            pickle.dump(sector_map, f)

    except Exception as e:
        logger.warning(f"DART 업종 수집 실패: {e}")

    return sector_map

def _load_master_universe_meta() -> dict:
    """중앙 datastore(master.db) universe 최신 연도(PIT)에서 종목 메타 로드.
    반환: {'005930': {fg_sector, fg_industry, ksic, rating_bond, rating_cp, largest_holder}}.
    티커는 master 'A005930' → '005930' 매핑. master 없으면 {} (보강 생략)."""
    try:
        import sqlite3
        db = "/Users/summer123/Project_2/data_store/master.db"
        if not os.path.exists(db):
            return {}
        with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as c:
            yr = c.execute("SELECT MAX(year) FROM universe").fetchone()[0]
            rows = c.execute(
                "SELECT ticker, fg_sector, fg_industry, ksic, rating_bond, rating_cp, "
                "largest_holder FROM universe WHERE year=?", (yr,)).fetchall()
        def _s(x):  # xlsx 패딩 제거
            return x.strip() if isinstance(x, str) else x
        meta = {}
        for tk, fgs, fgi, ksic, rb, rcp, lh in rows:
            code = tk[1:] if isinstance(tk, str) and tk.startswith("A") and tk[1:].isdigit() else tk
            meta[code] = {"fg_sector": _s(fgs), "fg_industry": _s(fgi), "ksic": _s(ksic),
                          "rating_bond": _s(rb), "rating_cp": _s(rcp), "largest_holder": _s(lh)}
        logger.info(f"master universe 메타 로드: {len(meta)}개 (year={yr})")
        return meta
    except Exception as e:
        logger.info(f"master universe 메타 미사용: {e}")
        return {}


def run_step0(date,cfg,market="ALL",limit=None):
    dart_sector_map = _load_dart_sector_map()
    import FinanceDataReader as fdr
    ucfg=cfg["universe"]
    ucfg["cap_tier"]=cfg.get("cap_tier",{})
    end_date=_get_last_weekday(date)
    start_date=_workdays_before(end_date,ucfg["lookback_days"])
    if end_date!=date: logger.info(f"날짜 조정: {date} → {end_date}")
    logger.info(f"FDR 데이터 수집: {start_date} ~ {end_date}")

    markets=["KOSPI","KOSDAQ"] if market=="ALL" else [market]
    frames=[]
    for mkt in markets:
        try:
            df=fdr.StockListing(mkt)
            logger.debug(f"{mkt} StockListing 컬럼: {list(df.columns)}")
            cols={}
            for c in df.columns:
                cl=c.lower()
                if "symbol" in cl or cl in ("code","티커"):        cols[c]="ticker"
                elif cl in ("name","종목명","회사명"):              cols[c]="name"
                elif any(x in cl for x in ("sector","industry","업종","dept")):
                                                                    cols[c]="sector"
                elif "listing" in cl or ("date" in cl and "update" not in cl):
                                                                    cols[c]="listing_date"
                elif any(x in cl for x in ("marcap","시가총액","mktcap","market_cap")):
                                                                    cols[c]="market_cap"
            df=df.rename(columns=cols); df["market"]=mkt; frames.append(df)
        except Exception as e: logger.warning(f"목록 조회 실패 ({mkt}): {e}")

    if not frames: return {}
    stock_df=pd.concat(frames,ignore_index=True)
    if "listing_date" in stock_df.columns:
        min_l=datetime.strptime(end_date,"%Y%m%d")-timedelta(days=ucfg["min_listed_days"])
        stock_df["listing_date"]=pd.to_datetime(stock_df["listing_date"],errors="coerce")
        stock_df=stock_df[stock_df["listing_date"].isna()|(stock_df["listing_date"]<=min_l)]
    if "ticker" not in stock_df.columns: return {}
    stock_df=stock_df.dropna(subset=["ticker"])
    all_tickers=stock_df["ticker"].astype(str).tolist()
    ticker_info=stock_df.set_index("ticker").to_dict("index")

    patched = 0
    if dart_sector_map:
        for ticker, info in ticker_info.items():
            sector = str(info.get("sector") or "").strip()
            if sector in ("", "nan", "None", "기타") and ticker in dart_sector_map:
                ticker_info[ticker]["sector"] = dart_sector_map[ticker]
                patched += 1

    # 중앙 datastore(master.db) universe 보강: 신용등급·FnGuide·KSIC·최대주주(신규 보조필드)
    # + 스코어링용 sector는 비어있을 때만 gap-fill(기존 KSIC 택소노미 보존 → 스코어링 영향 없음)
    master_meta = _load_master_universe_meta()
    m_rating = m_fg = m_gap = 0
    if master_meta:
        for ticker, info in ticker_info.items():
            mm = master_meta.get(str(ticker))
            if not mm:
                continue
            if mm.get("rating_bond") or mm.get("rating_cp"):
                info["rating_bond"] = mm.get("rating_bond")
                info["rating_cp"]   = mm.get("rating_cp")
                m_rating += 1
            if mm.get("fg_sector"):
                info["fg_sector"]      = mm.get("fg_sector")
                info["fg_industry"]    = mm.get("fg_industry")
                info["ksic"]           = mm.get("ksic")
                info["largest_holder"] = mm.get("largest_holder")
                m_fg += 1
                sec = str(info.get("sector") or "").strip()
                if sec in ("", "nan", "None", "기타"):
                    info["sector"] = mm["fg_sector"]
                    m_gap += 1
        logger.info(f"master 보강: 등급 {m_rating}개 | FnGuide/KSIC {m_fg}개 | 섹터 gap-fill {m_gap}개")

    # 신용등급 하드 필터 (config filter.exclude_ratings) — 기본 빈 목록이면 무동작
    excl = {str(x).strip() for x in (cfg.get("filter", {}).get("exclude_ratings") or [])}
    if excl:
        def _rating_excluded(t):
            info = ticker_info.get(t, {})
            rb = str(info.get("rating_bond") or "").strip()
            rcp = str(info.get("rating_cp") or "").strip()
            return (rb in excl) or (rcp in excl)
        before = len(all_tickers)
        all_tickers = [t for t in all_tickers if not _rating_excluded(t)]
        logger.info(f"등급 필터: {before - len(all_tickers)}개 제외 "
                    f"(exclude_ratings={sorted(excl)})")

    for mkt in (["KOSPI","KOSDAQ"] if market=="ALL" else [market]):
        mkt_tickers = [t for t,i in ticker_info.items() if i.get("market")==mkt]
        sector_filled = sum(1 for t in mkt_tickers
                           if str(ticker_info[t].get("sector") or "").strip()
                           not in ("","nan","None","기타"))
        logger.info(f"{mkt}: {len(mkt_tickers)}개 | 섹터정보 {sector_filled}개"
                    + (f" (DART +{patched}개 보완)" if mkt=="KOSPI" and dart_sector_map else ""))

    logger.info(f"전체: {len(all_tickers)}개")
    if limit: all_tickers=all_tickers[:limit]; logger.info(f"제한: {limit}개")

    precomputed,failed={},[]
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures={executor.submit(_precompute_ticker,t,start_date,end_date,ticker_info.get(t,{}),ucfg):t for t in all_tickers}
        for future in tqdm(as_completed(futures),total=len(futures),desc="사전 계산",unit="종목"):
            t=futures[future]
            try:
                r=future.result()
                if r: precomputed[t]=r
            except Exception as e:
                logger.debug(f"future 결과 실패 ({t}): {e}")
                failed.append(t)
    if failed: logger.warning(f"실패: {len(failed)}개")
    for p in precomputed.values():
        for k,v in [("hype_latest",0.0),("hype_7d_ago",0.0),("hype_slope",0.0),("hype_rank",9999),("neg_ratio",0.0)]:
            p.setdefault(k,v)

    # A4: corp_codes 30일 만료 + last-known-good 폴백 (Phase A)
    corp_map = load_dart_corp_codes(os.getenv("DART_API_KEY", ""))

    mapped = 0
    for t, p in precomputed.items():
        cc = corp_map.get(t, "")
        p["corp_code"] = cc
        if cc: mapped += 1
    if corp_map:
        logger.info(f"DART corp_code 매핑: {mapped}/{len(precomputed)}개 ({mapped*100//max(len(precomputed),1)}%)")
    logger.info(f"유효 유니버스: {len(precomputed)}개")
    CACHE_DIR.mkdir(parents=True,exist_ok=True)
    with open(CACHE_DIR/f"universe_{date}.pkl","wb") as f: pickle.dump(precomputed,f)
    return precomputed

# ══════════════════════════════════════════════════════════════════════════════
# Step 1 — 신호 탐지
# ══════════════════════════════════════════════════════════════════════════════
def run_step1(precomputed,cfg):
    ea_cfg=cfg["engine_a"]; eb_cfg=cfg["engine_b"]
    neg_kws=cfg["negative_keywords"]
    naver=NaverClient()
    logger.info("관심 지수 및 부정 뉴스 비율 조회 중...")

    top_n = eb_cfg.get("hype_top_n", 100)
    max_workers = eb_cfg.get("hype_workers", 6)
    targets = sorted(precomputed.items(),
                     key=lambda x:x[1].get("vol_5d_avg",0), reverse=True)[:top_n]

    def _enrich(item):
        ticker, p = item
        name = p.get("name", ticker)

        trend = naver.get_trend(name, eb_cfg["hype_trend_days"])
        if len(trend) >= 2:
            p["hype_latest"] = float(trend[-1])
            p["hype_7d_ago"] = float(trend[0])
            p["hype_slope"]  = linear_slope(trend)

        headlines = naver.get_news_headlines(name, target=5, fetch=15)
        if headlines:
            neg_count = sum(1 for h in headlines if any(kw in h for kw in neg_kws))
            p["neg_ratio"] = round(neg_count / len(headlines), 3)
            return p["neg_ratio"] > 0
        return False

    neg_detected = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for hit in ex.map(_enrich, targets):
            if hit:
                neg_detected += 1

    logger.info(f"부정 키워드 감지: {neg_detected}개 종목 (neg_ratio > 0)")
    items=sorted(precomputed.items(),key=lambda x:x[1].get("hype_latest",0),reverse=True)
    for rank,(t,_) in enumerate(items,1): precomputed[t]["hype_rank"]=rank

    pool_a={}; ea_hit=eb_hit=both_hit=0; tier_counts={}
    surge_pct=ea_cfg.get("vol_surge_pct",0.50)
    for ticker,p in precomputed.items():
        vol_base=p.get("vol_60ma",p.get("vol_20ma",1))
        vol_rising=(vol_base>0 and p.get("vol_5d_avg",0)>=vol_base*(1+surge_pct))
        inst_buy=p.get("net_buy_days",0)>=ea_cfg["net_buy_min_days"]
        a=vol_rising or inst_buy
        b=(p.get("hype_slope", 0) > 0 and
           p.get("neg_ratio",  0) < eb_cfg["max_negative_sentiment"])
        if not (a or b): continue
        # Phase A.2: neg_ratio gate (Engine B only -> POOL_A whole gate)
        if p.get("neg_ratio", 0) >= eb_cfg["max_negative_sentiment"]:
            continue
        source="both" if (a and b) else ("engine_a" if a else "engine_b")
        if a and b: both_hit+=1
        elif a: ea_hit+=1
        else: eb_hit+=1
        tier=p.get("cap_tier","large"); tier_counts[tier]=tier_counts.get(tier,0)+1
        pool_a[ticker]={
            "source":source,"engine_a":a,"engine_b":b,
            "name":p.get("name",ticker),"sector":p.get("sector","기타"),
            "cap_tier":tier,"market_cap":p.get("market_cap",0),
            "vol_slope":p.get("vol_slope",0),"net_buy_days":p.get("net_buy_days",0),
            "vol_5d_avg":p.get("vol_5d_avg",0),"vol_60ma":p.get("vol_60ma",0),
            "vol_20ma":p.get("vol_20ma",0),"vol_rising":vol_rising,
            "net_buy_total":p.get("net_buy_total",0),"retail_buy_days":p.get("retail_buy_days",0),
            "inst_net":p.get("inst_net",0),"foreign_net":p.get("foreign_net",0),
            "current_price":p.get("current_price",0),"change_pct":p.get("change_pct",0.0),
            "rsi":p.get("rsi",50.0),"bb_pos":p.get("bb_pos",50.0),
            "hype_slope":p.get("hype_slope",0),"hype_rank":p.get("hype_rank",9999),
        }
    logger.info(f"POOL_A: {len(pool_a)}개 (A:{ea_hit} B:{eb_hit} 동시:{both_hit})\n"
                f"  대형:{tier_counts.get('large',0)} 중형:{tier_counts.get('mid',0)} 소형:{tier_counts.get('small',0)}")
    today=datetime.now().strftime("%Y%m%d")
    save_engine_b_history([t for t,m in pool_a.items() if m["engine_b"]], precomputed, today)
    return pool_a

# ══════════════════════════════════════════════════════════════════════════════
# Step 2 — 하드 필터링
# ══════════════════════════════════════════════════════════════════════════════
def run_step2(pool_a, precomputed, cfg):
    fcfg = cfg["filter"]

    upper = {
        "large": fcfg.get("max_disparity_large", 115),
        "mid":   fcfg.get("max_disparity_mid",   120),
        "small": fcfg.get("max_disparity_small",  130),
    }
    min_disp        = fcfg.get("min_disparity",      93)
    require_trend   = fcfg.get("require_ma_trend",   True)
    max_rsi         = fcfg.get("max_rsi",            80)
    min_turnover    = fcfg.get("min_turnover_ratio",  0.01)
    min_amount      = fcfg.get("min_turnover_amount", 2_000_000_000)

    pool_b = {}
    removed = {"disp_upper": [], "disp_lower": [], "ma_trend": [], "rsi": [], "turnover": []}

    for ticker, meta in pool_a.items():
        p      = precomputed.get(ticker, {})
        disp   = p.get("disparity", 0)
        tier   = p.get("cap_tier", "large")
        rsi    = p.get("rsi", 50)
        ma20   = p.get("ma20",  0)
        ma60   = p.get("ma60",  0)
        ma120  = p.get("ma120", 0)
        mktcap = p.get("market_cap", 0)

        if disp >= upper[tier]:
            removed["disp_upper"].append(ticker)
            continue

        if disp < min_disp:
            removed["disp_lower"].append(ticker)
            continue

        if require_trend and ma20 > 0 and ma120 > 0 and ma20 <= ma120:
            removed["ma_trend"].append(ticker)
            continue

        if rsi > max_rsi:
            removed["rsi"].append(ticker)
            continue

        daily_amount = p.get("vol_5d_avg", 0) * p.get("current_price", 0)
        turnover_ratio = daily_amount / mktcap if mktcap > 0 else 0
        if turnover_ratio < min_turnover or daily_amount < min_amount:
            removed["turnover"].append(ticker)
            continue

        pool_b[ticker] = {
            **meta,
            "disparity":      disp,
            "cap_tier":       tier,
            "current_price":  p.get("current_price", 0),
            "vol_slope":      p.get("vol_slope", 0),
            "net_buy_days":   p.get("net_buy_days", 0),
            "net_buy_total":  p.get("net_buy_total", 0),
            "retail_buy_days":p.get("retail_buy_days", 0),
            "hype_slope":     p.get("hype_slope", 0),
            "hype_rank":      p.get("hype_rank", 9999),
            "ma20":  ma20, "ma60": ma60, "ma120": p.get("ma120", 0),
            "w52_high": p.get("w52_high", 0),
            "res_top":  p.get("res_top", float("inf")),
            "corp_code":  p.get("corp_code", ""),
            "market_cap": p.get("market_cap", 0),
            "sector":     p.get("sector", "기타"),
            "rsi":        rsi,
            "bb_pos":     p.get("bb_pos", 50),
        }

    total_removed = sum(len(v) for v in removed.values())
    logger.info(
        f"하드 필터 제거: {total_removed}개 | "
        f"이격도상한 {len(removed['disp_upper'])} | "
        f"이격도하한 {len(removed['disp_lower'])} | "
        f"MA추세 {len(removed['ma_trend'])} | "
        f"RSI과열 {len(removed['rsi'])} | "
        f"거래대금 {len(removed['turnover'])}"
    )
    tier_s = Counter(m["cap_tier"] for m in pool_b.values())
    logger.info(
        f"POOL_B: {len(pool_b)}개 | "
        f"대형:{tier_s.get('large',0)} 중형:{tier_s.get('mid',0)} 소형:{tier_s.get('small',0)}"
    )
    return pool_b

# ══════════════════════════════════════════════════════════════════════════════
# Step 3 — 스코어링 (FinBERT)
# ══════════════════════════════════════════════════════════════════════════════
def _calc_t(meta):
    score = 0
    ma20, ma60, ma120 = meta.get("ma20",0), meta.get("ma60",0), meta.get("ma120",0)
    price = meta.get("current_price", 0)

    if ma20 > ma60 > 0:          score += 25
    if ma60 > ma120 > 0:         score += 25
    if ma20 > ma60 > ma120 > 0:  score += 10

    h = meta.get("w52_high", 0)
    if h > 0 and price >= h * 0.95:
        score += 20

    if price > meta.get("res_top", float("inf")):
        score += 20

    return min(score, 100)

def _calc_d(ticker, meta, all_vol, all_hype, top_pct, scfg):
    source = meta.get("source", "engine_a")
    base   = 50 if source == "both" else 30 if source == "engine_a" else 20

    strength = 0
    if is_top_percentile(meta.get("vol_slope", 0), all_vol, top_pct):
        strength += 10

    nbd = meta.get("net_buy_days", 0)
    if nbd >= 5:        strength += 10
    elif nbd >= 4:      strength += 5

    if is_top_percentile(meta.get("hype_slope", 0), all_hype, top_pct):
        strength += 10

    if meta.get("has_sector_bonus", False):
        strength += 5

    cross = 0; n_accel = False; v_surge = False

    if meta.get("engine_a", False):
        if get_engine_b_history(ticker, scfg.get("n_accel_window", 3)):
            cross += 15; n_accel = True

    if meta.get("hype_rank", 9999) <= scfg.get("v_surge_rank", 20):
        cross += 10; v_surge = True

    return min(base + strength + cross, 100), n_accel, v_surge

def run_step3(pool_b,precomputed,cfg,date):
    scfg=cfg["scoring"]
    w_tech =scfg.get("w_tech",  scfg.get("w1",  0.35))
    w_text =scfg.get("w_text",  scfg.get("w2",  0.30))
    w_cross=scfg.get("w_cross", scfg.get("w3",  0.35))
    dart=DartClient(); naver=NaverClient()
    finbert=FinBertClient(scfg.get("finbert_model","snunlp/KR-FinBert-SC"))
    news_w=scfg.get("news_weight",0.60); dart_w=scfg.get("dart_weight",0.40)
    news_cnt=scfg.get("news_count",10); dart_days=scfg.get("dart_days",90)
    all_vol=[m.get("vol_slope",0) for m in pool_b.values()]
    all_hype=[m.get("hype_slope",0) for m in pool_b.values()]
    top_pct=scfg.get("strength_top_pct",0.20)

    min_peers = cfg.get("filter", {}).get("min_sector_peers", 2)
    sector_counts = Counter(
        m.get("sector","기타") for m in pool_b.values()
        if m.get("sector","기타") != "기타"
    )
    for ticker in pool_b:
        sec = pool_b[ticker].get("sector","기타")
        pool_b[ticker]["has_sector_bonus"] = (
            sec != "기타" and sector_counts.get(sec, 0) >= min_peers
        )

    # A2: 일자별 incremental 캐시 — 같은 날 재실행 시 외부 API 0건 (Phase A)
    news_texts, dart_texts = collect_texts_with_cache(
        pool_b, naver, dart, news_cnt, dart_days, date,
        workers=scfg.get("text_workers", 6),
    )

    logger.info(f"FinBERT 감성 분석 ({finbert.mode} 모드)...")
    finbert._load()
    # Phase A.1 — 매핑 신뢰도 동시 산출 (외부 보고서 6건 사례 대응)
    # v3 hotfix: news_texts 가 일별 캐시라 어제 종목이 남아있을 수 있음 → pool_b 가드
    news_data = {}
    for t, texts in news_texts.items():
        if t not in pool_b:
            continue
        nm = pool_b[t].get("name", t)
        news_data[t] = finbert.score_with_confidence(texts, name=nm)
    dart_scores = {}
    dart_best_titles = {}
    for ticker, texts in dart_texts.items():
        finbert_sc = finbert.score(texts)
        kw_sc, best_title = calc_dart_keyword_score(texts)
        if abs(kw_sc - 50) < 1:
            dart_scores[ticker] = finbert_sc
        else:
            dart_scores[ticker] = round(finbert_sc * 0.5 + kw_sc * 0.5, 1)
        dart_best_titles[ticker] = best_title

    results=[]
    skipped_low_confidence = 0
    for ticker,meta in pool_b.items():
        t_score=_calc_t(meta)
        n_sc, n_headline, n_pct, n_conf = news_data.get(ticker, (50.0,"",0.0,0.0))
        d_sc=dart_scores.get(ticker,50.0)
        # Phase A.1 (C1): 신뢰도 < 0.5 면 감성 점수 폐기 (중립 50 처리)
        if n_conf < 0.5:
            s_text = round(50.0*news_w + d_sc*dart_w, 1)
            news_skipped = True
            skipped_low_confidence += 1
        else:
            s_text = round(n_sc*news_w + d_sc*dart_w, 1)
            news_skipped = False
        d_score,n_accel,v_surge=_calc_d(ticker,meta,all_vol,all_hype,top_pct,scfg)
        score=round(t_score*w_tech + s_text*w_text + d_score*w_cross, 2)
        results.append({
            "ticker":ticker,"name":meta.get("name",ticker),"sector":meta.get("sector","기타"),
            "rating_bond":meta.get("rating_bond"),"rating_cp":meta.get("rating_cp"),
            "fg_sector":meta.get("fg_sector"),"fg_industry":meta.get("fg_industry"),
            "ksic":meta.get("ksic"),"largest_holder":meta.get("largest_holder"),
            "cap_tier":meta.get("cap_tier","large"),"score":score,
            "t":t_score,"s_text":s_text,"news_score":n_sc,"dart_score":d_sc,"d":d_score,
            "source":meta.get("source","?"),"n_accel":n_accel,"v_surge":v_surge,
            "finbert_mode":finbert.mode,
            "best_headline":n_headline,"best_headline_pct":n_pct,
            "news_confidence":n_conf,"news_skipped":news_skipped,
            "best_dart_title":dart_best_titles.get(ticker,""),
            "news_count":len(news_texts.get(ticker,[])),
            "dart_count":len(dart_texts.get(ticker,[])),
            "vol_slope":meta.get("vol_slope",0),"net_buy_days":meta.get("net_buy_days",0),
            "vol_5d_avg":meta.get("vol_5d_avg",0),"vol_60ma":meta.get("vol_60ma",0),
            "vol_20ma":meta.get("vol_20ma",0),"vol_rising":meta.get("vol_rising",False),
            "net_buy_total":meta.get("net_buy_total",0),"retail_buy_days":meta.get("retail_buy_days",0),
            "current_price":meta.get("current_price",0),"inst_net":meta.get("inst_net",0),"foreign_net":meta.get("foreign_net",0),
            "rsi":meta.get("rsi",50.0),"bb_pos":meta.get("bb_pos",50.0),
            "change_pct":meta.get("change_pct",0.0),
            "hype_slope":meta.get("hype_slope",0),"hype_rank":meta.get("hype_rank",9999),
            "disparity":meta.get("disparity",0),
        })
    results.sort(key=lambda x:x["score"],reverse=True)
    hi=cfg["grade"]["high_interest"]; mi=cfg["grade"]["interest"]
    for r in results:
        r["grade"]="집중" if r["score"]>=hi else "주시" if r["score"]>=mi else "참고"
    if skipped_low_confidence:
        logger.info(f"Phase A.1: 매핑 신뢰도 < 0.5 → 감성 점수 폐기 {skipped_low_confidence}건")
    logger.info(f"스코어링: {len(results)}개 | 집중:{sum(1 for r in results if r['grade']=='집중')} | FinBERT:{finbert.mode}")
    save_scan_results(results,date)
    CACHE_DIR.mkdir(parents=True,exist_ok=True)
    with open(CACHE_DIR/f"results_{date}.pkl","wb") as f: pickle.dump(results,f)
    return results

# ══════════════════════════════════════════════════════════════════════════════
# Step 4 — 텔레그램 발송
# ══════════════════════════════════════════════════════════════════════════════
def _esc(text: str) -> str:
    return str(text).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

GRADE_RANK={"집중":2,"주시":1,"참고":0}

def run_step4(results,cfg,dry_run=False):
    today=datetime.now().strftime("%Y%m%d"); tg=TelegramClient()
    min_score=cfg.get("grade",{}).get("min_display_score",0)
    results=[r for r in results if r["score"]>=min_score]
    to_send = list(results)
    high=[r for r in to_send if r["grade"]=="집중"]
    mid=[r for r in to_send if r["grade"]=="주시"]

    def fmt(i,r):
        tier_icon={"large":"[대형]","mid":"[중형]","small":"[소형]"}.get(r.get("cap_tier","large"),"[?]")
        cross=("  ✦ N-Accel" if r.get("n_accel") else "")+("  ✦ V-Surge" if r.get("v_surge") else "")

        price      = r.get("current_price",0)
        change_pct = r.get("change_pct",0)
        change_str = (f"+{change_pct:.1f}%" if change_pct>=0 else f"{change_pct:.1f}%")
        change_icon= "▲" if change_pct>0 else ("▼" if change_pct<0 else "─")
        price_str  = f"{price:,.0f}원 {change_icon}{change_str}" if price>0 else "─"
        # master 보강 신용등급 — 값 있을 때만 표시 (채권/CP)
        _rb=str(r.get("rating_bond") or "").strip(); _rcp=str(r.get("rating_cp") or "").strip()
        rating_str=(f"  |  🏷 {_esc(_rb)}"+(f"/{_esc(_rcp)}" if _rcp else "")) if _rb else ""

        inst_v    = r.get("inst_net",    0)
        foreign_v = r.get("foreign_net", 0)
        no_data = (inst_v == 0 and foreign_v == 0 and r.get("net_buy_days", 0) == 0)

        if no_data:
            inst_str    = "정보없음"
            foreign_str = "정보없음"
        else:
            inst_str    = (f"+{inst_v:,}백만" if inst_v > 0 else f"{inst_v:,}백만" if inst_v < 0 else "0")
            foreign_str = (f"+{foreign_v:,}백만" if foreign_v > 0 else f"{foreign_v:,}백만" if foreign_v < 0 else "0")
        retail_tag = "  ✦ 개인주도" if r.get("retail_buy_days",0)>=3 and r.get("net_buy_days",0)<3 else ""

        bb  = r.get("bb_pos",50)
        bb_label = ("상단돌파" if bb>=95 else "상단근접" if bb>=80
                    else "중립"   if bb>=40 else "하단근접" if bb>=20 else "하단돌파")

        rsi = r.get("rsi",50)
        rsi_label = ("과매수" if rsi>=70 else "과매도" if rsi<=30 else "중립")

        vol_5d  = r.get("vol_5d_avg", 0)
        vol_60  = r.get("vol_60ma", 0) or r.get("vol_20ma", 0)
        if vol_5d > 0 and vol_60 > 0:
            vol_ratio = vol_5d / vol_60
            vol_pct = vol_ratio * 100
            vol_icon = "🔥" if vol_ratio >= 2.0 else "📈" if vol_ratio >= 1.5 else "📊"
            vol_line = f"  |  {vol_icon} 거래량 {vol_pct:.0f}%"
        else:
            vol_line = ""

        headline=r.get("best_headline",""); h_pct=r.get("best_headline_pct",0)
        n_conf=r.get("news_confidence", 1.0)
        news_skipped=r.get("news_skipped", False)

        # Phase A.1 (C2): 매핑 신뢰도 낮음 경고
        if news_skipped:
            headline_line=f"\n   ⚠️ 뉴스 매핑 신뢰도 낮음 ({n_conf*100:.0f}%) — 감성 점수 폐기"
        elif headline and h_pct>=70:
            h_short=_esc(headline[:45])+("..." if len(headline)>45 else "")
            headline_line=f"\n   📝 [호재] {h_short} ({h_pct:.0f}%)"
        else:
            headline_line=""

        return (
            f"\n\n<b>{i}. {_esc(r['name'])} ({r['ticker']})</b>\n"
            f"   {_esc(r['sector'])}  {tier_icon}{rating_str}  |  💵 {price_str}\n"
            f"   🏦 기관 {inst_str}  |  🌏 외인 {foreign_str}  ({r['net_buy_days']}일){retail_tag}\n"
            f"   📊 BB {bb:.0f}% {bb_label}  |  RSI {rsi:.0f} {rsi_label}{vol_line}\n"
            f"   📐 이격도 {r['disparity']:.1f}%  (20일 평균 대비 현재가 위치)\n"
            f"   🏆 <b>총점 {r['score']:.1f}</b>  기술 {r['t']:.0f}  수급 {r['d']:.0f}  감성 {r['s_text']:.0f}{cross}{headline_line}"
        )

    now=datetime.now().strftime("%Y-%m-%d %H:%M")
    hi_c=sum(1 for r in results if r["grade"]=="집중")
    mi_c=sum(1 for r in results if r["grade"]=="주시")
    lo_c=sum(1 for r in results if r["grade"]=="참고")

    msg_items = []
    msg_items.append(("header",
        f"📡 <b>AlphaRadar 관망 리스트</b>\n📅 {now}\n━━━━━━━━━━━━━━━━━━━━\n"
        f"탐지: {len(results)}종목  →  발송: {len(to_send)}종목\n"
        f"🔴 집중 {hi_c}  🟠 주시 {mi_c}  ⚪ 참고 {lo_c}\n"
        f"정형:비정형 = 6:4  |  S=T×0.30+ST×0.40+D×0.30"))
    if high:
        msg_items.append(("high",
            "<b>🔴 집중</b>\n━━━━━━━━━━━━━━━━━━━━"
            + "".join(fmt(i+1,r) for i,r in enumerate(high))
            + "\n━━━━━━━━━━━━━━━━━━━━"))
    if mid:
        msg_items.append(("mid",
            "<b>🟠 주시</b>\n━━━━━━━━━━━━━━━━━━━━"
            + "".join(fmt(i+1,r) for i,r in enumerate(mid))
            + "\n━━━━━━━━━━━━━━━━━━━━"))

    low=[r for r in results if r["grade"]=="참고" and r["score"]>=min_score]
    if low:
        low_lines=["⚪ <b>참고</b>","━━━━━━━━━━━━━━━━━━━━"]
        for r in low:
            low_lines.append(f"  {_esc(r['name'])} ({r['ticker']})  |  {_esc(r['sector'])}")
        low_lines.append("━━━━━━━━━━━━━━━━━━━━")
        msg_items.append(("low", "\n".join(low_lines)))

    sg = defaultdict(lambda: {"집중": 0, "주시": 0, "참고": 0})
    for r in results:
        sg[r["sector"]][r["grade"]] += 1
    icon_map={"집중":"🔴","주시":"🟠","참고":"⚪"}
    sl=["🗂 <b>섹터별 현황</b>","━━━━━━━━━━━━━━━━━━━━"]
    for sec,cnt in sorted(sg.items(),key=lambda x:sum(x[1].values()),reverse=True)[:10]:
        total=sum(cnt.values())
        det=" ".join(f"{icon_map[g]}{cnt[g]}" for g in ["집중","주시","참고"] if cnt[g]>0)
        sl.append(f"🔷 {_esc(sec)} ({total}종목)  {det}")
    w=cfg["scoring"]
    sl+=["━━━━━━━━━━━━━━━━━━━━",
         f"⚙️ T×{w.get('w_tech',w.get('w1',0.35))} · ST×{w.get('w_text',w.get('w2',0.30))} · D×{w.get('w_cross',w.get('w3',0.35))}",
         "💾 scores_history.db 저장 완료"]
    msg_items.append(("sector", "\n".join(sl)))

    msgs = [m for _, m in msg_items]

    if dry_run:
        logger.info("▶ [DRY RUN] 콘솔 출력")
        for msg in msgs: print("\n"+"─"*60+"\n"+msg)
    else:
        send_results = tg.send_many(msgs)
        kind_ok = {kind: ok for (kind, _), ok in zip(msg_items, send_results)}
        sent_count = 0
        skipped_count = 0
        for r in to_send:
            group = "high" if r["grade"] == "집중" else "mid"
            if kind_ok.get(group, False):
                mark_sent(r["ticker"], today, r["grade"], r["score"])
                sent_count += 1
            else:
                skipped_count += 1
        if skipped_count:
            logger.warning(
                f"텔레그램 발송 실패 그룹 → mark_sent skip {skipped_count}개 "
                f"(다음 실행 시 재발송 대상)"
            )
        logger.info(f"DB 기록: {sent_count}개 / 전체 to_send {len(to_send)}개")
    logger.info(f"발송: 집중 {len(high)} / 주시 {len(mid)}")

# ══════════════════════════════════════════════════════════════════════════════
# DART 법인코드 초기화
# ══════════════════════════════════════════════════════════════════════════════
def setup_dart():
    api_key=os.getenv("DART_API_KEY","")
    if not api_key: print("DART_API_KEY가 .env에 없습니다."); return
    print("OpenDART 법인코드 다운로드 중...")
    r=requests.get("https://opendart.fss.or.kr/api/corpCode.xml",
                   params={"crtfc_key":api_key},timeout=30)
    r.raise_for_status()
    z=zipfile.ZipFile(io.BytesIO(r.content))
    root=ET.fromstring(z.read("CORPCODE.xml").decode("utf-8"))
    corp_map={item.findtext("stock_code","").strip():item.findtext("corp_code","").strip()
              for item in root.findall("list") if len(item.findtext("stock_code","").strip())==6}
    CACHE_DIR.mkdir(parents=True,exist_ok=True)
    with open(CACHE_DIR/"dart_corp_codes.pkl","wb") as f: pickle.dump(corp_map,f)
    print(f"완료: {len(corp_map)}개")

# ══════════════════════════════════════════════════════════════════════════════
# 진입점
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser=argparse.ArgumentParser(description="AlphaRadar v3.3.1")
    parser.add_argument("--step",    type=int,   default=None,  choices=[0,1,2,3,4])
    parser.add_argument("--date",    type=str,   default=None)
    parser.add_argument("--market",  type=str,   default="ALL", choices=["KOSPI","KOSDAQ","ALL"])
    parser.add_argument("--limit",   type=int,   default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--mock",    action="store_true")
    parser.add_argument("--setup-dart", action="store_true")
    args=parser.parse_args()

    date_str=args.date or datetime.now().strftime("%Y%m%d")
    setup_logging(date_str); init_db()

    if args.setup_dart: setup_dart(); return

    try: cfg=load_config(); validate_config(cfg); logger.info("config 검증 완료 ✓")
    except AssertionError as e: logger.error(f"설정 오류: {e}"); sys.exit(1)

    start=time.time()
    logger.info("="*60)
    logger.info(f"AlphaRadar v3.3.1  |  기준일: {date_str}")
    logger.info(f"S = T×{cfg['scoring'].get('w_tech',cfg['scoring'].get('w1',0.35))} + S_text×{cfg['scoring'].get('w_text',cfg['scoring'].get('w2',0.30))} + D×{cfg['scoring'].get('w_cross',cfg['scoring'].get('w3',0.35))}")
    logger.info("="*60)

    cache_path=CACHE_DIR/f"universe_{date_str}.pkl"

    if args.step in (None,0):
        logger.info("▶ Step 0: 유니버스 필터링")
        if args.mock:
            precomputed=generate_mock_precomputed()
            CACHE_DIR.mkdir(parents=True,exist_ok=True)
            with open(cache_path,"wb") as f: pickle.dump(precomputed,f)
            logger.info(f"  → [MOCK] {len(precomputed)}개")
        else:
            precomputed=run_step0(date_str,cfg,market=args.market,limit=args.limit)
        logger.info(f"  → {len(precomputed)}개")
        if args.step==0: return
    else:
        if not cache_path.exists(): logger.error("캐시 없음. --step 0 먼저 실행"); sys.exit(1)
        with open(cache_path,"rb") as f: precomputed=pickle.load(f)
        logger.info(f"캐시 로드: {len(precomputed)}개")

    if args.step in (None,1):
        logger.info("▶ Step 1: 신호 탐지")
        pool_a=run_step1(precomputed,cfg)
        CACHE_DIR.mkdir(parents=True,exist_ok=True)
        with open(CACHE_DIR/f"pool_a_{date_str}.pkl","wb") as f: pickle.dump(pool_a,f)
        logger.info(f"  → POOL_A: {len(pool_a)}개")
        if args.step==1:
            for t,m in list(pool_a.items())[:20]: print(f"  {t} {m['name']:<14} [{m['source']}]")
            return
    else:
        pool_a_path=CACHE_DIR/f"pool_a_{date_str}.pkl"
        if pool_a_path.exists():
            with open(pool_a_path,"rb") as f: pool_a=pickle.load(f)
        else: pool_a={}

    if args.step in (None,2):
        logger.info("▶ Step 2: 하드 필터링")
        pool_b=run_step2(pool_a,precomputed,cfg)
        logger.info(f"  → POOL_B: {len(pool_b)}개")
        if args.step==2: return
    else: pool_b=pool_a

    if args.step in (None,3):
        logger.info("▶ Step 3: 스코어링 (FinBERT 감성 분석)")
        results=run_step3(pool_b,precomputed,cfg,date_str)
        logger.info(f"  → {len(results)}개")
        if args.step==3:
            print(f"\n{'종목':<14} {'S':>6} {'T':>5} {'ST':>6} {'D':>5} {'등급'}")
            print("-"*50)
            for r in results:
                print(f"  {r['name']:<14} {r['score']:>6.1f} {r['t']:>5.0f} {r['s_text']:>6.1f} {r['d']:>5.0f} {r['grade']}")
            return
    else: results=[]

    if args.step in (None,4):
        logger.info("▶ Step 4: 발송")
        run_step4(results,cfg,dry_run=args.dry_run)

    logger.info(f"완료  |  소요: {time.time()-start:.1f}초")

if __name__=="__main__":
    main()
