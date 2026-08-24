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
from datetime import datetime, timedelta, timezone
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

# ── 날짜 라벨은 항상 KST ───────────────────────────────────────────────────────
# GitHub Actions 러너는 UTC로 돈다. 아침 크론(22:40 UTC = 07:40 KST)에서 그냥
# datetime.now()를 쓰면 scan_date가 '전날'로 찍혀, 저녁 런과 아침 런이 같은
# 날짜에 뒤섞이고 대시보드에는 오늘 날짜가 저녁까지 나타나지 않는다.
KST = timezone(timedelta(hours=9))


def today_kst() -> str:
    """스캔 날짜 라벨(YYYYMMDD). 실행 환경의 TZ와 무관하게 KST 기준."""
    return datetime.now(KST).strftime("%Y%m%d")
LOG_DIR   = Path("data/logs")

DEFAULT_CONFIG = {
    "universe": {
        "min_price": 500, "min_market_cap": 200_000_000_000,
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
    w_tech  = s.get("w_tech",  s.get("w1", 0))
    w_cross = s.get("w_cross", s.get("w3", 0))
    # P1-4: w_news/w_dart 지정 시 이 둘이 S_text 가중을 대체 (합=w_text 자리)
    if "w_news" in s or "w_dart" in s:
        w_text_eff = s.get("w_news", 0) + s.get("w_dart", 0)
    else:
        w_text_eff = s.get("w_text", s.get("w2", 0))
    w = round(w_tech + w_text_eff + w_cross, 10)
    assert w == 1.0, f"가중치 합계 오류: {w} (w_tech+{'w_news+w_dart' if ('w_news' in s or 'w_dart' in s) else 'w_text'}+w_cross)"

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

def percentile_rank(value, all_values) -> float:
    """value가 all_values 안에서 차지하는 분위(0.0~1.0). 상위일수록 1에 가깝다.

    is_top_percentile의 연속판. 이진 컷오프가 경계 한 칸 차이로 10점을 가르는 문제를
    피하려고 만들었다. 동률은 중간값으로 처리해(≤ 개수와 < 개수의 평균) 같은 값이
    몰려 있을 때 순위가 0이나 1로 쏠리지 않게 한다.
    """
    if value is None: return 0.0
    arr = [v for v in all_values if v is not None]
    if not arr: return 0.0
    below = sum(1 for v in arr if v < value)
    equal = sum(1 for v in arr if v == value)
    return (below + equal / 2.0) / len(arr)

def _subject_ok(text, name, position_threshold=0.3) -> bool:
    """종목명이 이 텍스트의 '주체'인지. 위치(앞 N%) + 횟수(1회면 15자 이내).

    같은 판정이 세 군데에 흩어져 있었다 — NaverClient._is_subject(수집),
    FinBertClient.score_with_confidence(집행), 그리고 아래 shadow. 한 곳으로
    모아 세 경로가 반드시 같은 규칙을 쓰게 한다. 규칙 자체는 그대로다.
    """
    if not text or not name:
        return False
    idx = text.find(name)
    if idx < 0:
        return False
    if idx / max(len(text), 1) >= position_threshold:
        return False
    if text.count(name) < 2 and idx > 15:
        return False
    return True

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

    def confidence_by_field(self, items, name, position_threshold=0.3):
        """shadow — 신뢰도 검사만 필드별(title OR desc)로 바로잡았을 때의 값.

        집행 경로(score_with_confidence)는 title과 desc를 이어붙인 문자열
        f"{title}. {desc[:100]}" 하나에 주체 검증을 건다. 그런데 수집 단계는
        title·desc 각각에 걸어 하나만 통과해도 채택한다. 그래서 desc로 채택된
        기사는 종목명이 len(title)+2 뒤에 놓여 위치 임계 30%를 구조적으로 넘고
        전부 탈락한다 — 저장 기사 1,735건 실측에서 65.5%가 이 경로였다.

        여기서는 그 검사만 필드별로 되돌린다. FinBERT에 넣는 문자열은 기존
        연결 문자열 그대로다 — 검사 방식과 입력 텍스트를 같이 바꾸면 shadow
        신구 차이가 어느 쪽 효과인지 귀속되지 않는다. 입력 텍스트 변경이
        필요한지는 이 컬럼이 쌓인 뒤 따로 판단한다(field_mix가 그 근거다).

        반환: (conf_fixed, raw_fixed, field_mix)
          conf_fixed — 필드별 검사 기준 통과 비율
          raw_fixed  — 통과분만 FinBERT로 채점한 원점수(게이트 미적용). 없으면 None
          field_mix  — "title=4,desc=12,both=2,none=3" 형태의 채택 경로 내역
        """
        if not items or not name:
            # 측정 불가(캐시에 원본 기사가 없음)와 '재보니 0'을 구분해야 나중에
            # 집계에서 섞이지 않는다.
            return None, None, ""
        valid, mix = [], {"title": 0, "desc": 0, "both": 0, "none": 0}
        for it in items:
            t_ok = _subject_ok(it.get("title", ""), name, position_threshold)
            d_ok = _subject_ok(it.get("desc", ""), name, position_threshold)
            mix["both" if (t_ok and d_ok) else
                "title" if t_ok else "desc" if d_ok else "none"] += 1
            if t_ok or d_ok:
                valid.append(NaverClient.item_to_text(it))
        conf = round(len(valid) / len(items), 3)
        raw = self.score_with_best(valid)[0] if valid else None
        return conf, raw, ",".join(f"{k}={v}" for k, v in mix.items())

    def score_with_confidence(self, texts, name=None, position_threshold=0.3):
        """
        Phase A.1 — 매핑 신뢰도(confidence) 동시 반환.

        반환: (score, best_headline, headline_pct, confidence)

        반환: (score, best_headline, headline_pct, confidence, raw_score)

        name 이 주어지면 각 텍스트가 _is_subject 통과 비율로 confidence 계산.
        confidence < 0.5 → score=50 (중립), 헤드라인 빈 문자열.

        raw_score는 게이트를 적용하기 '전' 점수다. 점수 자체에는 쓰지 않고 DB에만
        남긴다. 이 게이트는 뉴스 점수의 75%를 정확히 50으로 만들어 가중치 0.24가
        사실상 죽어 있는데(실측: 게이트 통과 기사 26%), 게이트를 열어야 하는지는
        raw와 confidence가 쌓여야 실측으로 판단할 수 있다. 지금은 판단을 보류하고
        근거만 모은다. 유효 기사가 없으면 None.
        """
        if not texts:
            return 50.0, "", 0.0, 0.0, None
        if name:
            valid = [t for t in texts if _subject_ok(t, name, position_threshold)]
            confidence = round(len(valid) / len(texts), 3) if texts else 0.0
            if not valid:
                return 50.0, "", 0.0, confidence, None
            # 게이트에 걸리더라도 raw는 계산해 남긴다 — 나중에 게이트 방식을
            # 바꿨을 때 과거 데이터로 전/후를 비교하려면 이 값이 필요하다.
            raw, headline, pct = self.score_with_best(valid)
            if confidence < 0.5:
                return 50.0, "", 0.0, confidence, raw
            return raw, headline, pct, confidence, raw
        score, headline, pct = self.score_with_best(texts)
        return score, headline, pct, 1.0, score

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
                t_presurge REAL, score_presurge REAL,
                overheat_pen REAL,
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
            CREATE TABLE IF NOT EXISTS gated_tickers (
                scan_date TEXT, ticker TEXT, reason TEXT,
                ret_5d REAL, ret_20d REAL, w52_proximity REAL,
                PRIMARY KEY (scan_date, ticker)
            );
            -- 대조군 표본. 스캔을 통과한 종목이 정말 나은지 재려면 통과 못 한 쪽이
            -- 있어야 한다. stage로 두 종류를 구분한다.
            --   no_signal : 유니버스는 통과했으나 신호 없음 (POOL_A 미진입)
            --   filtered  : 신호는 있었으나 하드필터 탈락 (reason에 사유)
            -- 전량이 아니라 사유별 표본이다. 비율을 되돌리려면 pool_total을 쓴다.
            CREATE TABLE IF NOT EXISTS pool_history (
                scan_date TEXT, ticker TEXT, stage TEXT, reason TEXT,
                name TEXT, sector TEXT, cap_tier TEXT,
                change_pct REAL, vol_slope REAL, hype_slope REAL,
                rsi REAL, disparity REAL, net_buy_days INTEGER,
                pool_total INTEGER,
                PRIMARY KEY (scan_date, ticker, stage)
            );
            CREATE TABLE IF NOT EXISTS news_articles (
                scan_date TEXT, ticker TEXT, title TEXT, description TEXT,
                link TEXT, pub_date TEXT,
                collected_at TEXT DEFAULT (datetime('now','localtime')),
                PRIMARY KEY (scan_date, ticker, link)
            );
            CREATE INDEX IF NOT EXISTS idx_news_ticker_date
                ON news_articles(ticker, scan_date DESC);
            CREATE TABLE IF NOT EXISTS dart_filings (
                scan_date TEXT, ticker TEXT, title TEXT,
                rcept_no TEXT, rcept_dt TEXT,
                collected_at TEXT DEFAULT (datetime('now','localtime')),
                PRIMARY KEY (scan_date, ticker, title)
            );
            CREATE INDEX IF NOT EXISTS idx_dart_ticker_date
                ON dart_filings(ticker, scan_date DESC);
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
        # 뉴스 감성 게이트 진단용 — news_conf(매핑 신뢰도), news_raw(게이트 적용 전 점수).
        # 이 둘이 없으면 게이트를 열었을 때 점수가 어떻게 달라지는지 과거 데이터로
        # 재계산할 수 없다(news_score는 이미 게이트가 적용된 값이라 되돌릴 수 없음).
        # A-3 shadow — 수급축 재설계안 병행 계산 결과. 발송·등급에 미사용.
        for _c in ("d_flow", "score_flow"):
            if _c not in cols:
                con.execute(f"ALTER TABLE scan_results ADD COLUMN {_c} REAL")
                logger.info(f"DB 마이그레이션: {_c} 컬럼 추가 완료")
        for _c in ("news_conf", "news_raw"):
            if _c not in cols:
                con.execute(f"ALTER TABLE scan_results ADD COLUMN {_c} REAL")
                logger.info(f"DB 마이그레이션: {_c} 컬럼 추가 완료")
        # master universe 보강 메타 컬럼 (없으면 추가)
        for _c in ("rating_bond","rating_cp","fg_sector","fg_industry","ksic","largest_holder"):
            if _c not in cols:
                con.execute(f"ALTER TABLE scan_results ADD COLUMN {_c} TEXT")
                logger.info(f"DB 마이그레이션: {_c} 컬럼 추가 완료")
        # Task 4: shadow presurge 점수 컬럼 (REAL)
        for _c in ("t_presurge","score_presurge"):
            if _c not in cols:
                con.execute(f"ALTER TABLE scan_results ADD COLUMN {_c} REAL")
                logger.info(f"DB 마이그레이션: {_c} 컬럼 추가 완료")
        # V-Surge → 연속 과열 감점 교체. D점수에서 실제로 뺀 값을 남긴다.
        # 이게 없으면 나중에 감점을 걷어냈을 때의 점수를 과거 데이터로 복원할 수 없다
        # (score_d는 이미 감점이 반영된 값이라 되돌릴 수 없음).
        # D-2 shadow — 뉴스 주체 게이트를 필드별 검사로 바로잡았을 때의 값.
        # 집행은 news_conf 그대로 두고 여기에 병행 기록만 한다. 집행 전환 여부는
        # 이 컬럼이 수 주 쌓인 뒤 개편 ①(s_text 랭킹 제외)과 함께 판단한다.
        for _c in ("news_conf_fixed", "news_raw_fixed"):
            if _c not in cols:
                con.execute(f"ALTER TABLE scan_results ADD COLUMN {_c} REAL")
                logger.info(f"DB 마이그레이션: {_c} 컬럼 추가 완료")
        if "news_field_mix" not in cols:
            con.execute("ALTER TABLE scan_results ADD COLUMN news_field_mix TEXT")
            logger.info("DB 마이그레이션: news_field_mix 컬럼 추가 완료")
        if "overheat_pen" not in cols:
            con.execute("ALTER TABLE scan_results ADD COLUMN overheat_pen REAL")
            logger.info("DB 마이그레이션: overheat_pen 컬럼 추가 완료")

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
                 rating_bond,rating_cp,fg_sector,fg_industry,ksic,largest_holder,
                 t_presurge,score_presurge,news_conf,news_raw,d_flow,score_flow,
                 overheat_pen,news_conf_fixed,news_raw_fixed,news_field_mix)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
                 r.get("fg_industry"), r.get("ksic"), r.get("largest_holder"),
                 r.get("t_presurge"), r.get("score_presurge"),
                 r.get("news_conf"), r.get("news_raw"),
                 r.get("d_flow"), r.get("score_flow"),
                 r.get("overheat_pen"),
                 r.get("news_conf_fixed"), r.get("news_raw_fixed"),
                 r.get("news_field_mix")))

def save_engine_b_history(tickers, precomputed, scan_date):
    with _conn() as con:
        for t in tickers:
            slope = precomputed.get(t,{}).get("hype_slope",0)
            con.execute("INSERT OR REPLACE INTO engine_b_history VALUES (?,?,?)", (scan_date,t,slope))

def save_gated_tickers(rows, scan_date):
    """Task 3 반사실 기록 — 과열 필터로 제거된 종목을 결과 추적용으로 영속."""
    with _conn() as con:
        for r in rows:
            con.execute(
                "INSERT OR REPLACE INTO gated_tickers VALUES (?,?,?,?,?,?)",
                (scan_date, r["ticker"], r["reason"],
                 r.get("ret_5d"), r.get("ret_20d"), r.get("w52_proximity")))

def sample_pool(tickers, precomputed, scan_date, stage, reason, n, pool_total):
    """대조군 표본을 뽑아 pool_history 행으로 만든다.

    시드를 scan_date+stage+reason으로 고정한다. 같은 날 두 런이 같은 종목을 뽑게
    되는데, PK가 (scan_date,ticker,stage)라 중복은 자연히 합쳐져 행이 낭비되지
    않는다. 재현도 되므로 나중에 "그날 왜 이 종목이 뽑혔나"를 되짚을 수 있다.

    pool_total은 표본을 뽑기 전 모집단 크기다. 이게 없으면 나중에 표본 비율을
    복원할 수 없어 "몇 개 중 몇 개"를 못 따진다.
    """
    pool = sorted(tickers)          # set 순회 순서는 실행마다 달라 시드가 무의미해진다
    if not pool:
        return []
    rng = random.Random(f"{scan_date}:{stage}:{reason}")
    picked = rng.sample(pool, min(n, len(pool)))
    rows = []
    for t in picked:
        p = precomputed.get(t, {})
        rows.append({
            "ticker": t, "stage": stage, "reason": reason,
            "name": p.get("name", t), "sector": p.get("sector", "기타"),
            "cap_tier": p.get("cap_tier", "large"),
            "change_pct": p.get("change_pct", 0.0), "vol_slope": p.get("vol_slope", 0.0),
            "hype_slope": p.get("hype_slope", 0.0), "rsi": p.get("rsi", 50.0),
            "disparity": p.get("disparity", 0.0), "net_buy_days": p.get("net_buy_days", 0),
            "pool_total": pool_total,
        })
    return rows


def save_pool_history(rows, scan_date):
    """대조군 표본 영속. 스캔 통과군과 비교할 상대가 없으면 성과를 해석할 수 없다."""
    if not rows:
        return 0
    with _conn() as con:
        for r in rows:
            con.execute(
                "INSERT OR REPLACE INTO pool_history VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (scan_date, r["ticker"], r["stage"], r["reason"],
                 r.get("name"), r.get("sector"), r.get("cap_tier"),
                 r.get("change_pct"), r.get("vol_slope"), r.get("hype_slope"),
                 r.get("rsi"), r.get("disparity"), r.get("net_buy_days"),
                 r.get("pool_total")))
    return len(rows)


def save_news_articles(news_items: dict, scan_date: str) -> int:
    """스캔에 실제로 쓰인 기사를 보존한다.

    이걸 남기지 않으면 '그때 왜 잡혔는지'를 나중에 확인할 수 없다. NAVER 검색
    API에는 기간 필터가 없어 지나간 날짜의 기사는 소급 조회가 불가능하다.
    PK가 (scan_date, ticker, link)라 같은 날 두 번 돌아도 중복되지 않는다.

    엔티티(&quot; 등) 디코딩은 여기서만 한다. 점수용 문자열(item_to_text)을 건드리면
    FinBERT 입력이 달라져 과거 점수와 비교가 깨진다.
    """
    import html as _html
    n = 0
    with _conn() as con:
        for ticker, items in news_items.items():
            for it in items:
                link = (it.get("link") or "").strip()
                if not link:
                    continue   # PK 구성요소라 링크 없으면 보관 불가
                con.execute(
                    "INSERT OR REPLACE INTO news_articles "
                    "(scan_date, ticker, title, description, link, pub_date) "
                    "VALUES (?,?,?,?,?,?)",
                    (scan_date, ticker, _html.unescape(it.get("title", "")),
                     _html.unescape(it.get("desc", "")), link, it.get("pub", "")))
                n += 1
    return n


def save_dart_filings(dart_items: dict, scan_date: str) -> int:
    """스캔에 쓰인 공시 제목을 보존한다.

    공시 점수(dart_score)는 7건 남짓을 평균 내어 50 근처로 뭉개지는데(실측:
    70%가 40~60 구간), 산식을 바꿔 재계산하려면 제목 원본이 필요하다. 뉴스에서
    겪은 '되돌릴 수 없음'을 반복하지 않기 위한 것이다.

    PK는 (scan_date, ticker, title) — 채점도 제목 기준으로 중복을 제거하므로
    같은 기준을 쓴다. rcept_no는 조회용으로 함께 남긴다.
    """
    n = 0
    with _conn() as con:
        for ticker, items in dart_items.items():
            for it in items:
                title = (it.get("title") or "").strip()
                if not title:
                    continue
                con.execute(
                    "INSERT OR REPLACE INTO dart_filings "
                    "(scan_date, ticker, title, rcept_no, rcept_dt) VALUES (?,?,?,?,?)",
                    (scan_date, ticker, title,
                     it.get("rcept_no", ""), it.get("rcept_dt", "")))
                n += 1
    return n


def get_engine_b_history(ticker, scan_date=None, window_days=3):
    """기준일(scan_date) '이전' window_days*2 캘린더일 창의 엔진B 이력만 반환.
    버그 수정: 기존 LIMIT 방식은 수개월 전 이력이 N-Accel을 영구 발화시키고,
    당일 자기 기록까지 포함해 'both' 종목이 자동 +15되던 문제가 있었다.
    scan_date < 기준일 → 당일 자기기록 제외, scan_date >= 하한 → 과거 이력 제외."""
    if not scan_date:
        scan_date = today_kst()
    try:
        base = datetime.strptime(str(scan_date), "%Y%m%d")
    except (ValueError, TypeError):
        scan_date = today_kst(); base = datetime.strptime(scan_date, "%Y%m%d")
    lower = (base - timedelta(days=window_days*2)).strftime("%Y%m%d")
    with _conn() as con:
        rows = con.execute(
            "SELECT scan_date FROM engine_b_history "
            "WHERE ticker=? AND scan_date < ? AND scan_date >= ? "
            "ORDER BY scan_date DESC",
            (ticker, scan_date, lower)).fetchall()
    return [r["scan_date"] for r in rows]

def was_sent_today(ticker, scan_date):
    with _conn() as con:
        row = con.execute("SELECT grade FROM sent_history WHERE ticker=? AND send_date=?",
                          (ticker,scan_date)).fetchone()
    return row["grade"] if row else None

def get_watch_history(tickers, scan_date):
    """종목별 누적 관찰 이력 — {ticker: (누적등장, 최초이후경과, 직전공백)}.

    등급을 대신할 지표다. 실측(3,024건/469종목/64일, D+1 시가 진입 후 10일 내
    구간 최고가)에서 점수보다 훨씬 강했다.

      누적 등장 Spearman +0.187 (20일 +0.152)   vs   종합점수 -0.068
      첫 등장   n=462  +10% 26.6%  평균 6.08  중앙 -0.14   ← 처음 뜬 종목은 무의미
      4~6회     n=727  +10% 42.0%  평균 13.52
      7회 이상 vs 1~2회: 평균 10.79 vs 6.91 (p=0.0000)

    최초 등장 후 경과도 같이 본다. 8~14일 구간이 +10% 54.0%, 평균 17.76으로 최고다.

    직전 공백(마지막 등장 이후 건너뛴 스캔일 수)은 별개 신호다. 매일 붙어 있는
    종목보다 쉬었다 다시 나온 쪽이 낫다 — 연속(공백0) 평균 11.09 vs 4~7일 공백 15.71.
    같은 누적 등장 안에서 연속 3회 이상은 오히려 성과가 낮았다(4~6회 구간에서
    15.84 → 11.94). 매일 뜨는 것은 이미 움직이는 종목이고, 쉬었다 나온 것은
    눌림 뒤 신호가 다시 붙은 것으로 보인다.

    공백은 달력일이 아니라 '스캔일 칸' 수로 센다(분석과 같은 기준). 하루 두 런은
    같은 scan_date라 DISTINCT로 하루 1칸으로 접는다.
    """
    if not tickers:
        return {}
    with _conn() as con:
        days = [r[0] for r in con.execute(
            "SELECT DISTINCT scan_date FROM scan_results WHERE scan_date < ? ORDER BY scan_date",
            (scan_date,))]
        idx = {d: i for i, d in enumerate(days)}
        cur = len(days)                      # 오늘은 마지막 칸 다음
        out = {}
        for t in tickers:
            rows = [r[0] for r in con.execute(
                "SELECT DISTINCT scan_date FROM scan_results "
                "WHERE ticker=? AND scan_date < ? ORDER BY scan_date", (t, scan_date))]
            if not rows:
                out[t] = (0, 0, None)
                continue
            out[t] = (len(rows), cur - idx[rows[0]], cur - idx[rows[-1]])
    return out


_GRADE_RANK = {"참고": 0, "주시": 1, "집중": 2}

def mark_sent(ticker, scan_date, grade, score):
    """그날 그 종목이 발송된 사실을 남긴다. 등급은 강등되지 않는다.

    PK가 (ticker, send_date)인데 하루 두 런이 같은 날짜를 쓴다. 예전에는
    INSERT OR REPLACE라 나중 런이 앞 런을 통째로 갈아쳤다. 아침에 집중으로 상세
    카드가 나간 종목이 저녁 런에서 참고로 떨어지면 기록이 참고로 바뀌어, 실제로
    받은 알림이 통계에서 사라졌다 — 실측으로 집중 5건·주시 26건이 그렇게 묻혔다
    (라온시큐어 집중 78.4 → 참고 64.9 등). 기록된 집중 17/주시 64는 실제
    집중 22/주시 90보다 적다.

    그래서 이미 더 높은 등급으로 기록돼 있으면 덮어쓰지 않는다. 같은 등급이면
    점수가 높은 쪽을 남겨 어느 런의 발송인지 되짚을 수 있게 한다.
    한계: 한 종목이 하루에 두 등급으로 두 번 발송되면 강한 쪽만 남는다.
    두 발송을 모두 남기려면 PK에 런 구분이 들어가야 한다(별도 과제).
    """
    with _conn() as con:
        row = con.execute(
            "SELECT grade, score FROM sent_history WHERE ticker=? AND send_date=?",
            (ticker, scan_date)).fetchone()
        if row is not None:
            old_rank = _GRADE_RANK.get(row["grade"], -1)
            new_rank = _GRADE_RANK.get(grade, -1)
            if new_rank < old_rank:
                return
            if new_rank == old_rank and (row["score"] or 0) >= (score or 0):
                return
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

    def get_report_items(self, corp_code, days=90):
        """공시 목록 — 제목에 접수번호·접수일자를 함께 반환.

        get_report_titles()는 여기서 제목만 뽑는 래퍼다. 점수 계산에 쓰이는
        제목 목록의 순서·중복제거 방식은 예전과 동일하게 유지한다.
        """
        if not self.api_key or not corp_code: return []
        bgn = (datetime.now()-timedelta(days=days)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")
        items = []
        for pty in self._DART_PUBLICATION_TYPES:
            data = self._get("list.json", {
                "corp_code":corp_code, "bgn_de":bgn, "end_de":end,
                "pblntf_ty":pty, "page_count":"20",
            })
            for r in data.get("list", []):
                nm = r.get("report_nm")
                if nm:
                    items.append({"title": nm,
                                  "rcept_no": r.get("rcept_no", ""),
                                  "rcept_dt": r.get("rcept_dt", "")})
            time.sleep(0.12)
        seen, unique = set(), []
        for it in items:
            if it["title"] not in seen:
                seen.add(it["title"]); unique.append(it)
        return unique

    def get_report_titles(self, corp_code, days=90):
        """기존 호출부 호환용 — 제목 문자열만 필요할 때."""
        return [it["title"] for it in self.get_report_items(corp_code, days)]


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


# 표시용 공시 제목은 점수용과 분리한다.
# calc_dart_keyword_score의 best_title은 '감성 영향도'로 뽑히고 그대로 max_impact가
# 되어 점수에 들어간다. 그 기준으로는 형식적 절차 공시가 실질 공시를 이긴다 —
# 20260813 금호타이어는 공시 5건 중 '주주총회소집결의'(mild_positive 60)가
# '연결재무제표기준영업(잠정)실적'(사전에 없어 neutral 50)을 눌렀다.
# 사전을 고치면 dart_score와 발송 구성까지 움직이므로, 표시 순위만 따로 매긴다.
_DART_PROCEDURAL: tuple[str, ...] = (
    "주주총회소집결의", "주주명부폐쇄", "기준일설정",
    "기업설명회", "IR개최", "결산실적공시예고",
    "정관변경", "특정증권등소유상황보고",
)


def pick_dart_display_title(titles: list[str]) -> str:
    """메시지에 띄울 공시 한 건. 실질 정보가 있는 쪽을 우선하고 절차성 공시는 뒤로 민다.
    점수에는 관여하지 않는다."""
    if not titles:
        return ""

    def _procedural(t: str) -> bool:
        s = t.replace(" ", "")
        return any(k.replace(" ", "") in s for k in _DART_PROCEDURAL)

    pool = [t for t in titles if not _procedural(t)] or titles
    # 같은 pool 안에서는 감성 영향도가 큰 것 → 동점이면 악재 우선.
    # 동점 처리를 빠뜨리면 거래정지가 가려진다: 데이타솔루션은 '주권매매거래정지'(25)와
    # '단일판매ㆍ공급계약체결'(75)이 |영향도|=25로 같아, 수집 순서만으로 호재가 이겼다.
    # 점수용 best_title도 같은 이유로 악재를 우선한다(min의 두 번째 키).
    best = max(pool,
               key=lambda t: (abs(classify_dart_title(t)[1] - 50),
                              -classify_dart_title(t)[1]))
    # DART 원문에는 제목 중간에 공백이 길게 들어간다
    # ("주권매매거래정지              (무상증자)"). 표시용이므로 여기서 정리한다.
    return " ".join(best.split())


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

    # 봇 생성 주가 요약 — "OO 주가, 7월 21일 6,040원 7.86% 상승 마감" 류.
    # 정보 가치가 0인데 '7.86% 상승' 같은 문구가 감성 모델에 들어가면 가짜 양성을
    # 만든다. 더 나쁜 건 이 유형이 종목명으로 시작해 매핑 신뢰도 게이트를 항상
    # 통과한다는 점이다(실측: 데이타솔루션 8/5 수집 6건 중 게이트를 통과한 유일한
    # 기사가 이 봇 기사였다). 거르지 않으면 감성 점수가 봇 기사만 반영하게 된다.
    # 날짜 + 금액/퍼센트를 모두 요구해 일반 기사("주가, 7월 들어 30% 상승")는
    # 걸리지 않게 한다.
    _BOT_PRICE_SUMMARY_RE = re.compile(
        r"주가,\s*\d{1,2}\s*월\s*\d{1,2}\s*일.*?[\d,]+\s*(?:원|%)")

    def _is_ad(self, text: str) -> bool:
        return any(m in text for m in self._AD_MARKERS)

    def _is_bot_summary(self, title: str) -> bool:
        return bool(self._BOT_PRICE_SUMMARY_RE.search(title))

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
        return _subject_ok(text, name, position_threshold)

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

    def get_news_items(self, query, target=10, fetch=30, max_age_days=None):
        """
        뉴스 수집 — 제목/요약/링크/발행일시를 함께 반환한다.

        get_news_headlines()는 이 결과를 FinBERT 입력용 문자열로 접어서 돌려주는
        래퍼다. 점수 계산에 쓰이는 문자열 포맷은 예전과 동일하게 유지한다.
        B6: 3자 이상 query 는 자동 따옴표(정확 매칭) — 자회사 흡수 1차 차단.
        max_age_days: 지정 시 pubDate 기준 기준일(오늘)로부터 그보다 오래된 기사 제외.
                      pubDate 파싱 실패 기사도 보수적으로 제외.
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

        items_out, seen = [], []
        stats = {"ad":0, "bot":0, "irrelevant":0, "duplicate":0, "pass":0, "stale":0}

        for item in raw_items:
            title = re.sub(r"<[^>]+>", "", item.get("title", "")).strip()
            desc  = re.sub(r"<[^>]+>", "", item.get("description", "")).strip()

            if not title:
                continue

            # 시점 필터 (Task 1): max_age_days 초과 또는 pubDate 파싱 실패 → 제외
            if max_age_days is not None:
                pub = item.get("pubDate", "")
                try:
                    pub_dt = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %z")
                    age = (datetime.now(pub_dt.tzinfo) - pub_dt).days
                    if age > max_age_days:
                        stats["stale"] += 1
                        continue
                except (ValueError, TypeError):
                    logger.debug(f"pubDate 파싱 실패 → 제외 [{query}]: {pub!r}")
                    stats["stale"] += 1
                    continue

            if self._is_ad(title):
                stats["ad"] += 1
                continue

            if self._is_bot_summary(title):
                stats["bot"] += 1
                continue

            if not self._is_relevant(title, query) and not self._is_relevant(desc, query):
                stats["irrelevant"] += 1
                continue

            if self._is_duplicate(title, seen):
                stats["duplicate"] += 1
                continue

            # 보관용 발행일시(ISO). 파싱 실패해도 기사 자체는 버리지 않는다.
            pub_iso = ""
            try:
                pub_iso = datetime.strptime(
                    item.get("pubDate", ""), "%a, %d %b %Y %H:%M:%S %z"
                ).astimezone(KST).strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                pass

            items_out.append({
                "title": title,
                "desc": desc,
                "link": item.get("originallink") or item.get("link", ""),
                "pub": pub_iso,
            })
            seen.append(title)
            stats["pass"] += 1

            if len(items_out) >= target:
                break

        logger.debug(
            f"뉴스 필터 [{query}]: 수집 {len(raw_items)}건 → "
            f"시점 -{stats['stale']} 광고 -{stats['ad']} 봇 -{stats['bot']} "
            f"무관 -{stats['irrelevant']} "
            f"중복 -{stats['duplicate']} → 최종 {len(items_out)}건"
        )
        if max_age_days is not None and stats["stale"]:
            logger.info(f"뉴스 시점 제외 [{query}]: {stats['stale']}건 (>{max_age_days}일/파싱실패)")
        # 봇 요약은 매핑 신뢰도 게이트를 항상 통과하는 유형이라 몇 건을 걸렀는지
        # 관측 가능해야 한다. 상세 내역은 위 debug 라인에 있고 여기는 발생 시에만.
        if stats["bot"]:
            logger.info(f"봇 주가요약 제외 [{query}]: {stats['bot']}건")
        time.sleep(0.10)
        return items_out

    @staticmethod
    def item_to_text(it: dict) -> str:
        """FinBERT 입력 문자열. 기존 포맷을 그대로 유지해야 점수가 안 바뀐다."""
        return f"{it['title']}. {it['desc'][:100]}" if it["desc"] else it["title"]

    def get_news_headlines(self, query, target=10, fetch=30, max_age_days=None):
        """기존 호출부 호환용 — 문자열 리스트만 필요할 때."""
        return [self.item_to_text(it) for it in
                self.get_news_items(query, target, fetch, max_age_days)]


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
                              date: str, workers: int = 6, news_max_age=None):
    """
    A2 — 뉴스·공시 텍스트 일자별 incremental 캐시.
    같은 날 가중치 튜닝 재실행 시 외부 API 0건.
    캐시 파일명 _v2: Task 1(시점 필터) 이전 구버전 당일 캐시 재사용 차단.
    """
    news_cache = CACHE_DIR / f"news_titles_{date}_v2.pkl"
    dart_cache = CACHE_DIR / f"dart_titles_{date}_v2.pkl"
    # 링크·발행일시가 붙은 원본. 텍스트 캐시만으로는 기사를 복원할 수 없어
    # 캐시 적중 런에서 news_articles를 채울 수 없다.
    items_cache = CACHE_DIR / f"news_items_{date}_v2.pkl"
    dart_items_cache = CACHE_DIR / f"dart_items_{date}_v2.pkl"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _load(path):
        if not path.exists():
            return {}
        try:
            with open(path, "rb") as f: return pickle.load(f)
        except Exception:
            return {}

    news_texts: dict = _load(news_cache)
    dart_texts: dict = _load(dart_cache)
    news_items: dict = _load(items_cache)
    dart_items: dict = _load(dart_items_cache)

    # 캐시에서 읽은 항목에는 수집 시점 필터가 적용돼 있지 않다. 필터를 나중에
    # 추가하면 그날 캐시에 남아 있던 옛 항목이 그대로 통과한다 — 실측으로 8/6 저녁
    # 런에서 봇 기사 1건이 캐시를 경유해 살아남았다. 로드 직후 한 번 더 거르고,
    # 점수용 문자열도 같은 기준으로 다시 만들어 캐시와 어긋나지 않게 한다.
    _dropped = 0
    for _t, _its in list(news_items.items()):
        _keep = [i for i in _its if not naver._is_bot_summary(i.get("title", ""))]
        if len(_keep) != len(_its):
            _dropped += len(_its) - len(_keep)
            news_items[_t] = _keep
            news_texts[_t] = [NaverClient.item_to_text(i) for i in _keep]
    if _dropped:
        logger.info(f"캐시 정제: 봇 주가요약 {_dropped}건 제외")

    def _persist_articles(items: dict, note: str):
        """기사 보존은 캐시 적중 여부와 무관하게 매 런 시도한다.
        아침 런의 저장이 실패해도 저녁 런이 같은 scan_date로 메워준다.
        PK가 (scan_date, ticker, link)라 중복 저장은 덮어쓰기로 끝난다."""
        if not items:
            return
        try:
            saved = save_news_articles(items, date)
            logger.info(f"기사 보존({note}): {saved}건 ({len(items)}종목) → news_articles")
        except Exception as e:
            logger.warning(f"기사 보존 실패(스캔은 계속): {e}")

    def _persist_filings(items: dict, note: str):
        if not items:
            return
        try:
            saved = save_dart_filings(items, date)
            logger.info(f"공시 보존({note}): {saved}건 ({len(items)}종목) → dart_filings")
        except Exception as e:
            logger.warning(f"공시 보존 실패(스캔은 계속): {e}")

    # 원본 캐시가 없는 종목은 텍스트가 있어도 다시 수집한다 —
    # 이 기능 이전에 만들어진 캐시를 물려받아도 기사가 비지 않게.
    missing = [(t, m) for t, m in pool_b.items()
               if t not in news_texts or t not in dart_texts
               or t not in news_items or t not in dart_items]

    if not missing:
        logger.info(f"텍스트 캐시 전체 적중: {len(pool_b)}종목 (외부 API 0건)")
        _persist_articles({t: news_items[t] for t in pool_b if t in news_items}, "캐시")
        _persist_filings({t: dart_items[t] for t in pool_b if t in dart_items}, "캐시")
        return news_texts, dart_texts

    logger.info(
        f"텍스트 신규 수집: {len(missing)}종목 "
        f"(캐시 적중 {len(pool_b) - len(missing)}종목)"
    )

    def _collect(item):
        ticker, meta = item
        nm = meta.get("name", ticker)
        news_it = naver.get_news_items(nm, target=news_cnt, fetch=news_cnt * 3,
                                       max_age_days=news_max_age)
        dart_it = (dart.get_report_items(meta.get("corp_code", ""), dart_days)
                   if meta.get("corp_code") else [])
        return ticker, news_it, dart_it

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for ticker, news_it, dart_it in ex.map(_collect, missing):
            news_items[ticker] = news_it
            news_texts[ticker] = [NaverClient.item_to_text(it) for it in news_it]
            dart_items[ticker] = dart_it
            dart_texts[ticker] = [it["title"] for it in dart_it]

    # 이번 런에서 본 종목 전체(신규 + 캐시분)를 보존한다
    _persist_articles({t: news_items[t] for t in pool_b if t in news_items}, "수집")
    _persist_filings({t: dart_items[t] for t in pool_b if t in dart_items}, "수집")

    # 원자적 저장 — 도중 실패해도 옛 캐시 보존
    for path, obj in ((news_cache, news_texts), (dart_cache, dart_texts),
                      (items_cache, news_items), (dart_items_cache, dart_items)):
        tmp = path.with_suffix(".pkl.tmp")
        with open(tmp, "wb") as f: pickle.dump(obj, f)
        tmp.replace(path)

    logger.info(
        f"수집 완료: 뉴스 {sum(len(v) for v in news_texts.values())}건 | "
        f"공시 {sum(len(v) for v in dart_texts.values())}건"
    )
    # news_items(title·desc 원본)는 뉴스 게이트 shadow 측정에 쓴다. 캐시에 이미
    # 있는 값이라 추가 조회 비용은 없다.
    return news_texts, dart_texts, news_items


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

        # 시총을 확인할 수 없으면 통과시키지 않는다. 예전 조건은
        # `marcap_real > 0 and marcap_real < 컷` 이라, 조회에 실패한 종목이
        # 컷을 그대로 우회했다 — 극소형주를 걸러내려는 필터인데 정작 검증이
        # 안 되는 종목만 무사통과하는 구조였다.
        # 정상 운영에서는 발동하지 않는다(실측: KOSPI 943 + KOSDAQ 1820 종목
        # 모두 시총 조회 100% 성공). 소스가 망가진 경우의 안전장치다.
        if marcap_real < ucfg.get("min_market_cap", 200_000_000_000):
            return None
        cap = marcap_real

        _ct       = ucfg.get("cap_tier", {})
        _large_th = _ct.get("large_threshold", 5_000_000_000_000)
        _mid_th   = _ct.get("mid_threshold",     500_000_000_000)
        cap_tier  = ("large" if cap >= _large_th
                     else "mid" if cap >= _mid_th
                     else "small")
        net_days,net_total,retail_days,inst_net,foreign_net=_get_investor_data(
            ticker,start_date,end_date,
            exclude_today=ucfg.get("investor_exclude_today", True))
        rsi    = calc_rsi(close_s)
        bb_pos = calc_bb_position(close_s)
        change_pct = float(df["Change"].iloc[-1]*100) if "Change" in df.columns else 0.0
        # Task 3: 과열 배제용 피처 (누적수익률·신고가 근접도)
        w52_high = float(close_s.max())
        ret_5d  = (current/float(close_s.iloc[-6])-1)  if len(close_s) >= 6  else 0.0
        ret_20d = (current/float(close_s.iloc[-21])-1) if len(close_s) >= 21 else 0.0
        w52_proximity = (current/w52_high) if w52_high > 0 else 0.0
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
            "ret_5d":ret_5d,"ret_20d":ret_20d,"w52_proximity":w52_proximity,
            "w52_high":w52_high,"res_top":resistance_top(hist_df.iloc[-60:]),
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
_KIS_FAIL_COUNT  = 0
_KIS_DISABLED    = False
_KIS_FAIL_LIMIT  = 3

def _kis_is_real() -> bool:
    raw = os.getenv("KIS_IS_REAL", "0").strip().lower()
    return raw in ("1", "true", "yes", "y", "real")

def _kis_base_url() -> str:
    return ("https://openapi.koreainvestment.com:9443" if _kis_is_real()
            else "https://openapivts.koreainvestment.com:29443")

def _kis_note_token_failure() -> None:
    """토큰 발급 실패 누적. 연속 _KIS_FAIL_LIMIT회면 이번 런 KIS 조회를 봉인한다.

    KIS(특히 VTS)가 죽은 날, 종목마다 재발급을 재시도하면 connect timeout 10초가
    수백 회 쌓여 workflow timeout으로 런 전체가 죽는다. 래치가 서면 수급 데이터만
    빠진 채(_kis_get이 {} 반환 — 기존 실패 경로) 런은 완주한다.
    """
    global _KIS_FAIL_COUNT, _KIS_DISABLED
    _KIS_FAIL_COUNT += 1
    if _KIS_FAIL_COUNT >= _KIS_FAIL_LIMIT and not _KIS_DISABLED:
        _KIS_DISABLED = True
        logger.warning(
            f"KIS 토큰 연속 {_KIS_FAIL_COUNT}회 실패 → 이번 런 KIS 조회 전체 스킵"
        )

def _kis_token() -> str:
    global _KIS_TOKEN_CACHE, _KIS_FAIL_COUNT
    if _KIS_DISABLED:
        return ""
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
            _kis_note_token_failure()
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
            _KIS_FAIL_COUNT = 0
            logger.info("KIS 토큰 발급 완료")
            return token
        except Exception as e:
            logger.warning(f"KIS 토큰 발급 실패: {e}")
            _kis_note_token_failure()
            return ""

def _kis_get(path: str, params: dict, tr_id: str, retries: int = 3) -> dict:
    global _KIS_LAST_CALL
    if _KIS_DISABLED:
        return {}
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

def _get_investor_data(ticker, start_date, end_date, exclude_today=True):
    global _KIS_WARNED
    ex_today = exclude_today
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

        # 당일 행은 창에서 뺀다. 응답은 stck_bsop_date 내림차순이라 행 0이 오늘이다.
        #
        # 빼는 이유: 같은 scan_date의 두 런이 서로 다른 5일 창을 보게 되기 때문이다.
        # 아침 런(06:10)은 당일 행이 아직 없어 D-1..D-5를 보는데, 저녁 런(18:40)은
        # 당일이 집계돼 D..D-4를 본다. 창이 하루 밀리면서 net_buy_days가 뒤집히고,
        # 그게 engine_a(net_buy_days>=3)를 껐다 켰다 한다. source가 both→engine_b로
        # 바뀌면 _calc_d의 base가 50→20으로 30점 움직이고, w_cross 0.30을 곱해도
        # 총점이 9점 흔들린다. 실측: 다중 런 1,500 종목일 중 576건에서 순매수일이
        # 변했고, 엔진이 바뀐 231건의 총점 변동은 평균 10.64(최대 25.5)로
        # 엔진이 그대로인 1,269건의 1.56보다 7배 컸다.
        # (예: 큐에스아이 20260527 — 06:07 both/순매수3일/62.2점 → 13:06 engine_b/0일/45.7점)
        #
        # 장중에는 미확정이라 빼는 게 맞고, 장 마감 후에는 값이 차 있지만 그래도 뺀다.
        # 한쪽 런만 최신 데이터를 보면 같은 날짜의 두 관측이 계속 어긋나기 때문이다.
        # 당일 수급은 버려지지 않고 다음 런(D+1 아침)에서 D-1로 들어온다.
        today = today_kst()
        if ex_today:
            valid = [r for r in output2
                     if _settled(r) and str(r.get("stck_bsop_date", "")).strip() != today]
        else:
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
            # 러너에는 master.db가 없다. 그때는 리포에 커밋된 캐시를 쓴다
            # (export_sector_map.py가 만든다). 이게 없으면 Actions 런에서
            # FnGuide 섹터·KSIC·등급 보강이 통째로 빠지고, sector가 소속부나
            # 기타로 남아 동조화 보너스가 엉뚱하게 발화한다 — 실제로
            # scores_history.db의 fg_sector 컬럼이 100% 비어 있었다.
            cache = Path("data/cache/sector_map_v1.pkl")
            if cache.exists():
                with open(cache, "rb") as f:
                    blob = pickle.load(f)
                meta = blob.get("meta", {})
                logger.info(f"master universe 메타: 캐시 사용 {len(meta)}개 "
                            f"(year={blob.get('year')})")
                return meta
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
                # "dept"는 넣지 않는다. FDR 종목목록에는 업종 컬럼이 없고
                # Dept(소속부)만 있는데, KOSDAQ은 이게 100% 채워져 있어 sector가
                # 우량기업부/중견기업부/벤처기업부/기술성장기업부 4종으로 뭉갠다.
                # 그러면 min_sector_peers=2가 항상 충족돼 POOL_B의 모든 KOSDAQ
                # 종목이 섹터 동조화 보너스(+5)를 무조건 받는다 — 정보가 없는
                # 상수 가산이다. KOSPI는 Dept가 비어 DART 섹터맵이 채워지므로,
                # 같은 필드에 두 taxonomy가 섞이기까지 했다.
                elif any(x in cl for x in ("sector","industry","업종")):
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

    # 중앙 datastore(master.db) universe 보강: 신용등급·FnGuide·KSIC·최대주주
    # + 스코어링용 sector를 fg_industry로 덮어쓴다(2026-08-21부터). 예전엔 비어
    # 있을 때만 채웠는데, KOSDAQ은 Dept(소속부)가 sector를 선점해 영영 안 채워졌다.
    master_meta = _load_master_universe_meta()
    m_rating = m_fg = m_sec = 0
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
                # 스코어링용 sector는 fg_industry를 우선한다(gap-fill이 아니라 덮어쓰기).
                # 동조화 보너스가 물어야 할 것은 "같은 업종이 POOL_B에 겹쳤는가"인데,
                # fg_sector 대분류 10종으로는 POOL_B(런당 15~20개)에서 83% 발화해
                # 변별력이 없다. fg_industry 62종이면 56%로 갈린다(실측).
                # DART 섹터맵은 KOSPI 88%·KOSDAQ 0%라 단독으로는 절반이 빈다.
                if mm.get("fg_industry"):
                    info["sector"] = mm["fg_industry"]
                    m_sec += 1
        logger.info(f"master 보강: 등급 {m_rating}개 | FnGuide/KSIC {m_fg}개 | 섹터(fg_industry) {m_sec}개")

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
def run_step1(precomputed,cfg,date=None):
    ea_cfg=cfg["engine_a"]; eb_cfg=cfg["engine_b"]
    neg_kws=cfg["negative_keywords"]
    naver=NaverClient()
    scan_date = date or today_kst()
    max_neg = eb_cfg["max_negative_sentiment"]
    news_max_age = cfg.get("scoring", {}).get("news_max_age_days", 14)
    logger.info("관심 지수 및 부정 뉴스 비율 조회 중...")

    top_n = eb_cfg.get("hype_top_n", 100)
    max_workers = eb_cfg.get("hype_workers", 6)
    targets = sorted(precomputed.items(),
                     key=lambda x:x[1].get("vol_5d_avg",0), reverse=True)[:top_n]
    enriched = {t for t,_ in targets}   # neg_ratio 산출을 시도한 종목 집합

    def _enrich(item):
        ticker, p = item
        name = p.get("name", ticker)

        trend = naver.get_trend(name, eb_cfg["hype_trend_days"])
        if len(trend) >= 2:
            p["hype_latest"] = float(trend[-1])
            p["hype_7d_ago"] = float(trend[0])
            p["hype_slope"]  = linear_slope(trend)

        headlines = naver.get_news_headlines(name, target=5, fetch=15, max_age_days=news_max_age)
        if headlines:
            neg_count = sum(1 for h in headlines if any(kw in h for kw in neg_kws))
            p["neg_ratio"] = round(neg_count / len(headlines), 3)
        return p.get("neg_ratio", 0.0) > 0

    neg_detected = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for hit in ex.map(_enrich, targets):
            if hit:
                neg_detected += 1

    logger.info(f"부정 키워드 감지: {neg_detected}개 종목 (neg_ratio > 0)")
    # 버그 수정: V-Surge 순위는 hype_slope 기준 (hype_latest는 DataLab 자기정규화라 100 동률 다수 → 난수 순위)
    items=sorted(precomputed.items(),key=lambda x:x[1].get("hype_slope",0),reverse=True)
    for rank,(t,_) in enumerate(items,1): precomputed[t]["hype_rank"]=rank

    surge_pct=ea_cfg.get("vol_surge_pct",0.50)
    require_pos=ea_cfg.get("require_positive_total", True)  # Task 2: 수급 부호 가드
    sign_guarded=0
    # 1차: 신호 후보 판정 (a=수급/거래량, b=관심상승+부정낮음)
    candidates=[]
    for ticker,p in precomputed.items():
        vol_base=p.get("vol_60ma",p.get("vol_20ma",1))
        vol_rising=(vol_base>0 and p.get("vol_5d_avg",0)>=vol_base*(1+surge_pct))
        # 수급 부호 가드: 일수 충족이어도 순매수 합계가 음수면 매집 아님 → inst_buy 불인정
        days_ok=p.get("net_buy_days",0)>=ea_cfg["net_buy_min_days"]
        inst_buy=days_ok and ((not require_pos) or p.get("net_buy_total",0) > 0)
        if require_pos and days_ok and not inst_buy:
            sign_guarded += 1
        a=vol_rising or inst_buy
        b=(p.get("hype_slope", 0) > 0 and p.get("neg_ratio", 0) < max_neg)
        if a or b:
            candidates.append((ticker, a, b, vol_rising))
    logger.info(f"수급일수충족·합계음수 제외: {sign_guarded}개")

    # 버그 수정: top_n 밖의 POOL_A 후보는 neg_ratio 미조회 상태 → 부정뉴스 게이트가 무조건 통과됨.
    # 후보 중 미조회 종목만 뉴스 2차 조회해 neg_ratio 채운 뒤 게이트 적용.
    need = [t for (t,a,b,_) in candidates if t not in enriched]
    if need:
        def _neg_only(t):
            name = precomputed[t].get("name", t)
            hl = naver.get_news_headlines(name, target=5, fetch=15, max_age_days=news_max_age)
            if hl:
                nc = sum(1 for h in hl if any(kw in h for kw in neg_kws))
                precomputed[t]["neg_ratio"] = round(nc/len(hl), 3)
            else:
                precomputed[t]["neg_ratio"] = 0.0
            return t
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            list(ex.map(_neg_only, need))
        logger.info(f"POOL_A 후보 부정뉴스 2차 조회: {len(need)}개")

    # 2차: 부정뉴스 게이트 + POOL_A 구성
    pool_a={}; ea_hit=eb_hit=both_hit=0; tier_counts={}; neg_gated=0
    for ticker,a,b,vol_rising in candidates:
        p=precomputed[ticker]
        b=(p.get("hype_slope", 0) > 0 and p.get("neg_ratio", 0) < max_neg)  # neg_ratio 갱신 반영
        if not (a or b):
            continue
        # Phase A.2: neg_ratio gate (POOL_A 전체 게이트)
        if p.get("neg_ratio", 0) >= max_neg:
            neg_gated += 1
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
    logger.info(f"POOL_A: {len(pool_a)}개 (A:{ea_hit} B:{eb_hit} 동시:{both_hit}) | 부정게이트 제외:{neg_gated}\n"
                f"  대형:{tier_counts.get('large',0)} 중형:{tier_counts.get('mid',0)} 소형:{tier_counts.get('small',0)}")
    save_engine_b_history([t for t,m in pool_a.items() if m["engine_b"]], precomputed, scan_date)

    # 대조군 ①: 유니버스는 통과했으나 어느 엔진도 켜지지 않은 종목.
    # 신호 탐지 자체가 값을 만드는지 재려면 이 집단이 있어야 한다.
    cs_cfg = cfg.get("control_sample", {}) or {}
    if cs_cfg.get("enabled", True):
        no_sig = set(precomputed) - set(pool_a)
        rows = sample_pool(no_sig, precomputed, scan_date, "no_signal", "no_signal",
                           cs_cfg.get("no_signal", 40), len(no_sig))
        n = save_pool_history(rows, scan_date)
        logger.info(f"대조군 표본(무신호): {n}개 / 모집단 {len(no_sig)}개 → pool_history")
    return pool_a

# ══════════════════════════════════════════════════════════════════════════════
# Step 2 — 하드 필터링
# ══════════════════════════════════════════════════════════════════════════════
def run_step2(pool_a, precomputed, cfg, date=None):
    fcfg = cfg["filter"]
    scan_date = date or today_kst()

    upper = {
        "large": fcfg.get("max_disparity_large", 110),
        "mid":   fcfg.get("max_disparity_mid",   115),
        "small": fcfg.get("max_disparity_small",  120),
    }
    min_disp        = fcfg.get("min_disparity",      93)
    require_trend   = fcfg.get("require_ma_trend",   True)
    max_rsi         = fcfg.get("max_rsi",            70)
    min_turnover    = fcfg.get("min_turnover_ratio",  0.01)
    min_amount      = fcfg.get("min_turnover_amount", 2_000_000_000)

    # Task 3: 과열 배제 필터 (기본 ON, config filter.exclude_overheat)
    oh_cfg   = fcfg.get("exclude_overheat", {}) or {}
    oh_on    = oh_cfg.get("enabled", True)
    oh_r5    = oh_cfg.get("max_ret_5d",       0.20)
    oh_r20   = oh_cfg.get("max_ret_20d",      0.40)
    oh_prox  = oh_cfg.get("max_w52_proximity", 0.97)

    pool_b = {}
    removed = {"overheat": [], "disp_upper": [], "disp_lower": [], "ma_trend": [], "rsi": [], "turnover": []}
    gated_rows = []

    for ticker, meta in pool_a.items():
        p      = precomputed.get(ticker, {})
        disp   = p.get("disparity", 0)
        tier   = p.get("cap_tier", "large")
        rsi    = p.get("rsi", 50)
        ma20   = p.get("ma20",  0)
        ma60   = p.get("ma60",  0)
        ma120  = p.get("ma120", 0)
        mktcap = p.get("market_cap", 0)

        # 과열 배제: 이미 급등(누적수익률)·고점 근접 → 물밑 목적과 반대이므로 제외 + 반사실 기록
        if oh_on:
            r5   = p.get("ret_5d", 0.0)
            r20  = p.get("ret_20d", 0.0)
            prox = p.get("w52_proximity", 0.0)
            oh_reason = []
            if r5   > oh_r5:   oh_reason.append("ret5d")
            if r20  > oh_r20:  oh_reason.append("ret20d")
            if prox > oh_prox: oh_reason.append("w52prox")
            if oh_reason:
                removed["overheat"].append(ticker)
                gated_rows.append({"ticker": ticker, "reason": "overheat:" + "+".join(oh_reason),
                                   "ret_5d": r5, "ret_20d": r20, "w52_proximity": prox})
                continue

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
        f"과열배제 {len(removed['overheat'])} | "
        f"이격도상한 {len(removed['disp_upper'])} | "
        f"이격도하한 {len(removed['disp_lower'])} | "
        f"MA추세 {len(removed['ma_trend'])} | "
        f"RSI과열 {len(removed['rsi'])} | "
        f"거래대금 {len(removed['turnover'])}"
    )
    if gated_rows:
        save_gated_tickers(gated_rows, scan_date)
        logger.info(f"과열 gated 기록: {len(gated_rows)}개 → gated_tickers")

    # 대조군 ②: 신호는 켜졌지만 하드필터에서 탈락한 종목을 사유별로 표본 추출.
    # 과열(overheat)도 함께 넣는다 — gated_tickers에 전수가 있긴 하지만, 전수와
    # 표본을 한 분석에서 섞으면 비율이 왜곡된다. pool_history 안에서는 모든 사유가
    # 같은 방식으로 뽑혀 있어야 서로 비교할 수 있다.
    cs_cfg = cfg.get("control_sample", {}) or {}
    if cs_cfg.get("enabled", True):
        per = cs_cfg.get("per_reason", 40)
        sampled = []
        for reason, tickers in removed.items():
            sampled += sample_pool(tickers, precomputed, scan_date, "filtered",
                                   reason, per, len(tickers))
        n = save_pool_history(sampled, scan_date)
        logger.info(f"대조군 표본(필터탈락): {n}개 / 모집단 {sum(len(v) for v in removed.values())}개 "
                    f"→ pool_history")

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

def _calc_t_presurge(meta):
    """Task 4 — shadow 기술점수(물밑 축적 가설). legacy _calc_t 무변경, 병행 계산용.
    신고가 근접·저항 돌파 보상 없음. 중장기 구조 생존 + 눌림목/저이격/에너지 응축 보상."""
    score = 0
    ma20  = meta.get("ma20", 0)
    ma60  = meta.get("ma60", 0)
    ma120 = meta.get("ma120", 0)
    price = meta.get("current_price", 0)
    disp  = meta.get("disparity", 0)       # (price/ma20)*100
    bb    = meta.get("bb_pos", 50)
    v5    = meta.get("vol_5d_avg", 0)
    v60   = meta.get("vol_60ma", 0)
    w52h  = meta.get("w52_high", 0)

    if ma60 > ma120 > 0:                       # 중장기 구조 생존
        score += 20
    if ma20 > 0 and 0.97 <= price/ma20 <= 1.05:  # MA20 밀착 눌림목
        score += 20
    if 93 <= disp <= 105:                       # 저이격
        score += 15
    if bb <= 60:                                # 볼밴 하단~중립 (에너지 응축)
        score += 15
    if v60 > 0 and v5/v60 <= 1.3:               # 거래량 미폭발
        score += 15
    if w52h > 0 and 0.75 <= price/w52h <= 0.95:  # 바닥 탈출·고점 미도달
        score += 15

    return min(score, 100)

def load_overheat_reference(scan_date, ocfg):
    """과열 감점의 분위 비교 모집단 — 최근 N일 누적 풀 분포를 DB에서 읽는다.

    ref_window_days 기본값은 0이다. 즉 평상시 이 함수는 None을 돌려주고 호출부는
    당일 풀을 쓴다. 켜지 말 것 — 아래 반박이 남아 있는 한 재검증 전용이다.

    만들게 된 우려: 당일 풀이 8월 들어 4~26종목까지 줄어(시총 하한 2000억 상향
    이후) n=21이면 순위 한 칸이 4.8%p라 knee 0.80~full 0.85 사이에 자리가 한 칸뿐,
    n<=10이면 램프 전체가 한 칸 안 — 감점이 0 아니면 만점으로만 나온다는 것.

    리플레이 반박(2026-08-24, scan_results 5,380행 / 72 스캔일 / 130런):
      ① 이진성은 풀 축소의 결과가 아니다. 램프값이 0이나 1로만 나오는 비율은
         캘리브레이션 구간(풀 40~236)에서 이미 95.0%였고, 8월 풀에서 94.8%,
         20일 누적으로 바꿔도 94.6%다. 밴드에 드는 종목 비율은 모집단 크기가
         아니라 램프 폭이 정한다 — 폭 0.05는 어느 분포에서든 5%, 0.20이면 19%,
         0.50이면 48%다(당일 풀과 누적 풀이 동일하게 나온다). 늘어나는 건 밴드
         '안'의 해상도뿐이고(8월 고유값 12 → 19) 그 밴드는 전체의 5%다.
      ② IC로도 이득이 없다. 일자(런)별 Spearman 평균이 당일 풀 대비
         fwd1 -0.0319→-0.0321 / fwd5 -0.0645→-0.0655 / fwd10 -0.0410→-0.0443 /
         fwd20 -0.0803→-0.0815 — 4개 지평 전부 같거나 미세하게 나쁘다.
      ③ 부작용이 있다. 누적 분포는 절대 기준이라 시장 전체가 달아오른 날 다수가
         함께 감점된다. 런별 '감점 받은 종목 비율'의 표준편차가 15.3%p에서
         21.2%p로 벌어져, 감점을 같은 날 동료 대비로 매긴다는 설계가 깨진다.
         ref_demean=True로 일자평균을 빼면 16.2%p로 돌아오지만 IC 이득은
         fwd10·fwd20에만 있고 주력 지평인 fwd1·fwd5는 당일 풀이 낫다.

    남겨둔 이유: 풀이 지금보다 더 줄면(런당 한 자릿수) ①의 결론이 바뀔 수 있다.
    그때 이 옵션으로 재측정한다. 돌려주는 값이 None이면 호출부는 당일 풀을 쓴다
    (기본값 0 / cold start / 누적 표본 부족 / DB 장애 폴백).
    """
    win = int(ocfg.get("ref_window_days", 0) or 0)
    if win <= 0:
        return None
    min_n  = int(ocfg.get("ref_min_n", 100))
    demean = bool(ocfg.get("ref_demean", False))
    try:
        with _conn() as con:
            dates = [r[0] for r in con.execute(
                "SELECT DISTINCT scan_date FROM scan_results "
                "WHERE scan_date < ? ORDER BY scan_date DESC LIMIT ?",
                (str(scan_date), win)).fetchall()]
            if not dates:
                logger.info("과열 감점 모집단: 과거 스캔 없음 → 당일 풀 사용")
                return None
            qs = ",".join("?" * len(dates))
            rows = con.execute(
                f"SELECT scan_date, vol_slope, hype_slope FROM scan_results "
                f"WHERE scan_date IN ({qs})", dates).fetchall()
    except sqlite3.Error as e:
        logger.warning(f"과열 감점 모집단 조회 실패 → 당일 풀 사용: {e}")
        return None

    by_date = defaultdict(lambda: {"vol": [], "hype": []})
    for r in rows:
        if r["vol_slope"] is not None:
            by_date[r["scan_date"]]["vol"].append(float(r["vol_slope"]))
        if r["hype_slope"] is not None:
            by_date[r["scan_date"]]["hype"].append(float(r["hype_slope"]))

    ref = {"vol": [], "hype": [], "demean": demean, "n_dates": len(by_date)}
    for key in ("vol", "hype"):
        for _d, buckets in by_date.items():
            vals = buckets[key]
            if not vals:
                continue
            if demean:
                # 감점의 원래 의미는 '같은 날 동료 대비'다. 누적 분포를 쓰되 각 날의
                # 평균을 뺀 잔차끼리 비교해 그 의미를 지킨다. 근거 통계도 일자평균을
                # 차감해 측정했으므로 이쪽이 측정과 집행의 기준을 일치시킨다.
                c = sum(vals) / len(vals)
                ref[key].extend(v - c for v in vals)
            else:
                ref[key].extend(vals)

    n_min = min(len(ref["vol"]), len(ref["hype"]))
    if n_min < min_n:
        logger.info(f"과열 감점 모집단 부족 (vol {len(ref['vol'])} / hype {len(ref['hype'])} "
                    f"< {min_n}) → 당일 풀 사용")
        return None
    logger.info(f"과열 감점 모집단: 최근 {len(by_date)}일 누적 "
                f"vol {len(ref['vol'])}건 / hype {len(ref['hype'])}건"
                f"{' (일자평균 차감)' if demean else ''}")
    return ref

def _overheat_penalty(meta, all_vol, all_hype, ocfg, ref=None):
    """과열(관심·거래량 급증) 연속 감점. 0 이상의 실수를 돌려주며 D점수에서 뺀다.

    기존 V-Surge(검색량 순위 20위 이내 → cross +10)를 대체한다. 교체 근거는
    outcomes 2,715건 / 39일(20260523~20260710 진입, KOSDAQ 초과수익 fwd20).

    측정 기준: 감점은 같은 날 후보들 사이의 상대 순위로 매긴다. 그래서 근거도
    일자평균을 뺀 "같은 날 동료 대비"로 측정해야 한다. pooled 통계를 그대로 쓰면
    일자 효과를 종목 선별력으로 착각한다 — 실제로 hype_slope의 pooled IC -0.062는
    전부 일자 효과였다(날짜평균 hype vs 날짜평균 ex20 = -0.393, p=0.013).

      ① V-Surge 플래그는 예측력이 없다.
         Spearman IC ex5 -0.015(p=0.44) / ex10 -0.002(p=0.92) / ex20 +0.010(p=0.62).
         발화군(n=74) 초과 -5.03%p vs 미발화군(n=2,641) -4.53%p — 사실상 무차별.
         그런데 발화하면 +10을 준다. 근거 없는 가점이라 제거한다.

      ② 세 신호 모두 '일자 내 상위 20% 꼬리'에서만 작동한다. 전 구간에 걸친
         단조 관계가 아니라서 Spearman으로는 안 잡히고 꼬리 검정으로만 보인다.
         (일자평균 차감 후, 해당군 vs 나머지 — 셋 다 p<0.0001)
             V: vol_slope  일자내 상위20%  -3.81%p (t=-4.58)
                           60~80% 구간은 +0.16%p로 무효과 → knee를 0.6에 두면 헛발질
             H: hype_slope 상위20% & slope>0  -4.08%p (t=-4.39)
                           부호 조건을 빼면 -2.22로 약해진다. 관심이 '오를 때'만 과열
             C: change_pct >=+7%  -3.75%p (t=-4.17) | >=+5% -2.96 | >=+10% -4.41

      ③ 효과크기가 셋이 거의 같고(-3.8/-4.1/-3.8), 발화 개수에 따라 단조로 누적된다.
             0개 n=1730 +1.39%p | 1개 n=566 -1.07 | 2개 n=362 -3.74 | 3개 n=57 -7.73
         → 가중치를 균등(5/5/5)하게 두고 합산 상한을 15로 잡으면 이 계단을 그대로 재현한다.

      ④ 관계의 모양은 '기울기'가 아니라 경계에서의 '계단'이다. 처음엔 knee 0.60에서
         1.00까지 완만한 램프를 걸었는데, 무효과 구간(60~80%)까지 깎느라 신호가
         희석돼 리플레이에서 IC 개선이 +0.003에 그쳤다. knee~full 폭을 좁혀
         계단에 가깝게 만들되, 경계 한 칸 차이로 5점이 갈리지는 않게 남겨둔다.

      ⑤ change_pct는 변동성 정규화하지 않는다. 종목 20일 일간표준편차로 나눈
         z임계를 같이 재봤더니 오히려 약했다 — 고정 >=7% 는 t=-4.17(p<0.0001)인데
         z>=1.5 는 t=-2.34(p=0.020), z>=2.0 은 t=-1.91(p=0.058)로 무너진다.
         '평소 대비 큰 움직임'보다 '절대적으로 큰 하루 상승'이 더 정보가 많다.
         고정 임계가 고변동 종목만 때리지도 않는다(발화율 저변동 12.4% ~ 고변동 15.8%).
         다만 변별력은 저변동 종목에서 가장 크다(발화 vs 미발화 차 -5.07%p) —
         고변동 종목은 애초에 안 걸려도 부진해서 한계 신호가 작다(-1.80%p).

      ⑥ 시장 변동성 국면에 따라 강도가 달라지지만 부호는 유지된다(발화 개수별 dm):
             저변동장  0개 +1.59 | 1개 -1.44 | 2개 -4.48 | 3개 -9.84
             고변동장  0개 +1.15 | 1개 -0.74 | 2개 -2.93 | 3개 -5.83
         고변동 국면에서 폭이 약 40% 줄지만 뒤집히지 않는다. 표본의 시장 변동성이
         이미 일간 2.4~4.5%로 높은 구간이었다는 점은 감안할 것.

    주의: 표본이 전 구간 평균 -22%인 하락장 한 국면이다. '관심 급증이 나쁘다'는
    하락장에서 특히 강하게 나타나는 성질이라 상승장에서 약해지거나 뒤집힐 수 있다.
    config의 overheat_penalty.enabled를 false로 두면 기존 V-Surge +10으로 즉시 되돌아간다.
    """
    def _ramp(x, knee, cap):
        """knee 이하면 0, cap 이상이면 1, 사이는 선형."""
        if cap <= knee: return 1.0 if x >= cap else 0.0
        return max(0.0, min(1.0, (x - knee) / (cap - knee)))

    def _pct(value, today_pool, key):
        """분위의 비교 모집단을 고른다 — ref가 없으면 기존대로 당일 풀."""
        if not ref or not ref.get(key):
            return percentile_rank(value, today_pool)
        if ref.get("demean"):
            pool = [v for v in today_pool if v is not None]
            center = (sum(pool) / len(pool)) if pool else 0.0
            return percentile_rank(value - center, ref[key])
        return percentile_rank(value, ref[key])

    knee = ocfg.get("pct_knee", 0.80)          # 분위 몇 부터 감점을 시작할지
    full = ocfg.get("pct_full", 0.85)          # 어디서 만점 감점에 도달할지
    pen  = 0.0

    # 거래량 기울기 — 일자 내 상위 20%에서만 유효. 좁은 램프 = 완만한 계단.
    pen += ocfg.get("w_vol", 5.0) * _ramp(
        _pct(meta.get("vol_slope", 0), all_vol, "vol"), knee, full)

    # 검색량 기울기 — V-Surge가 이진으로 잡던 차원. 관심이 오를 때만 과열로 본다
    # (hype_slope<=0인데 순위만 높은 건 그냥 원래 관심 많은 종목이다).
    if meta.get("hype_slope", 0) > 0:
        pen += ocfg.get("w_hype", 5.0) * _ramp(
            _pct(meta.get("hype_slope", 0), all_hype, "hype"), knee, full)

    # 당일 등락률 — 편측(상단) 램프. 여기는 진짜 완만하다(>=5% -2.96 → >=10% -4.41).
    pen += ocfg.get("w_chg", 5.0) * _ramp(
        meta.get("change_pct", 0.0) or 0.0,
        ocfg.get("chg_knee", 5.0), ocfg.get("chg_cap", 10.0))

    return round(min(pen, ocfg.get("max_total", 15.0)), 2)

def _calc_d(ticker, meta, all_vol, all_hype, top_pct, scfg, scan_date=None, ref=None):
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

    cross = 0; n_accel = False

    if meta.get("engine_a", False):
        if get_engine_b_history(ticker, scan_date, scfg.get("n_accel_window", 3)):
            cross += 15; n_accel = True

    # V-Surge 발화 조건. 점수에는 더 이상 쓰지 않고 진단 플래그로만 남긴다 —
    # 과거 스캔과 정의가 같아야 outcomes로 교체 전후를 비교할 수 있다.
    # (hype_slope>0 조건은 기존 버그 수정분: 순위만 보면 관심 하락 종목도 발화했다)
    v_surge = (meta.get("hype_slope", 0) > 0
               and meta.get("hype_rank", 9999) <= scfg.get("v_surge_rank", 20))

    ocfg = scfg.get("overheat_penalty", {}) or {}
    if ocfg.get("enabled", True):
        # 연속 과열 감점으로 교체 (근거는 _overheat_penalty docstring)
        overheat_pen = _overheat_penalty(meta, all_vol, all_hype, ocfg, ref)
    else:
        # 되돌리기 스위치 — 기존 V-Surge 이진 가점 그대로
        overheat_pen = 0.0
        if v_surge:
            cross += 10

    score = min(base + strength + cross, 100) - overheat_pen
    return round(max(0.0, score), 1), n_accel, v_surge, overheat_pen

def _calc_d_flow(ticker, meta, all_vol, all_hype, top_pct, scfg, scan_date=None):
    """A-3 shadow — 수급축 재설계안. 발송·등급에 미사용, 결과 측정 전용.

    실측 근거(2,757건 / 59일 / 450종목, outcomes 매칭 2,411건):

      ① 구성요소의 부호가 서로 반대인데 모두 가산이라 상쇄된다.
         순매수 일수 IC5 +0.055 · 외인 순매수 +0.067  (자금 유입 = 양)
         거래량 기울기 -0.115 · 검색량 기울기 -0.099   (관심 급증 = 음)
         → 합산한 D수급 종합은 +0.018로 신호가 사라진다.
         여기서는 관심 급증을 감산으로 뒤집는다.

      ② 기본점 순서가 실측 성과와 반대다.
         현행 both 50 > engine_a 30 > engine_b 20
         실측 engine_a -20.38% > engine_b -26.39% > both -25.27%
         → both를 과열 신호로 보고 낮춘다.

      ③ 관계가 단조가 아니라 역U자다. 중간 구간이 유의하게 낫다
         (중간 -20.62% vs 극단 -25.12%, p<0.0001).
         → 50에서 멀어질수록 감점해 중앙을 우대한다.

    주의: 표본이 전 구간 평균 -22%인 하락장 한 국면이다. '관심 급증이 나쁘다'는
    하락장에서 특히 강한 성질이라 상승장에서 뒤집힐 수 있다. 그래서 legacy
    _calc_d를 그대로 두고 병행 계산만 한다.
    """
    source = meta.get("source", "engine_a")
    base   = 25 if source == "both" else 40 if source == "engine_a" else 30

    score = base

    # 자금 유입 — 양의 IC. 가산 유지
    nbd = meta.get("net_buy_days", 0)
    if nbd >= 5:        score += 10
    elif nbd >= 4:      score += 5
    if meta.get("foreign_net", 0) > 0:
        score += 5

    # 관심 급증 — 음의 IC. 부호를 뒤집어 감산
    if is_top_percentile(meta.get("vol_slope", 0), all_vol, top_pct):
        score -= 10
    if is_top_percentile(meta.get("hype_slope", 0), all_hype, top_pct):
        score -= 10
    if meta.get("hype_slope", 0) > 0 and meta.get("hype_rank", 9999) <= scfg.get("v_surge_rank", 20):
        score -= 5

    if meta.get("has_sector_bonus", False):
        score += 5

    score = max(0, min(score, 100))

    # 역U자 — 중앙(50)에서 벗어난 만큼 감점. 최대 -12.5
    score -= abs(score - 50) * 0.25

    return round(max(0.0, min(100.0, score)), 1)


def run_step3(pool_b,precomputed,cfg,date):
    scfg=cfg["scoring"]
    w_tech =scfg.get("w_tech",  scfg.get("w1",  0.35))
    w_text =scfg.get("w_text",  scfg.get("w2",  0.30))
    w_cross=scfg.get("w_cross", scfg.get("w3",  0.35))
    dart=DartClient(); naver=NaverClient()
    finbert=FinBertClient(scfg.get("finbert_model","snunlp/KR-FinBert-SC"))
    news_w=scfg.get("news_weight",0.60); dart_w=scfg.get("dart_weight",0.40)
    # P1-4: news/dart를 최종 score에서 독립 가중. config에 w_news/w_dart가 있으면 활성,
    #       없으면 기존값(w_text×news_w, w_text×dart_w)으로 폴백 → 기존 동작과 완전 동일.
    split_text = ("w_news" in scfg) or ("w_dart" in scfg)
    w_news = scfg.get("w_news", round(w_text * news_w, 6))
    w_dart = scfg.get("w_dart", round(w_text * dart_w, 6))
    if split_text:
        logger.info(f"[P1-4] news/dart 독립 가중 활성 — w_news={w_news}, w_dart={w_dart} (w_text 대체)")
    news_cnt=scfg.get("news_count",10); dart_days=scfg.get("dart_days",30)
    news_max_age=scfg.get("news_max_age_days",14)
    all_vol=[m.get("vol_slope",0) for m in pool_b.values()]
    all_hype=[m.get("hype_slope",0) for m in pool_b.values()]
    top_pct=scfg.get("strength_top_pct",0.20)
    # 과열 감점 분위의 비교 모집단. 런당 1회만 읽는다(종목마다 DB를 때리지 않는다).
    overheat_ref = load_overheat_reference(date, scfg.get("overheat_penalty", {}) or {})

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
    news_texts, dart_texts, news_items = collect_texts_with_cache(
        pool_b, naver, dart, news_cnt, dart_days, date,
        workers=scfg.get("text_workers", 6), news_max_age=news_max_age,
    )

    logger.info(f"FinBERT 감성 분석 ({finbert.mode} 모드)...")
    finbert._load()
    # Phase A.1 — 매핑 신뢰도 동시 산출 (외부 보고서 6건 사례 대응)
    # v3 hotfix: news_texts 가 일별 캐시라 어제 종목이 남아있을 수 있음 → pool_b 가드
    news_data = {}
    news_fixed = {}
    for t, texts in news_texts.items():
        if t not in pool_b:
            continue
        nm = pool_b[t].get("name", t)
        news_data[t] = finbert.score_with_confidence(texts, name=nm)
        # shadow — 검사만 필드별로 바로잡은 값. 집행에는 쓰지 않는다(D-2).
        news_fixed[t] = finbert.confidence_by_field(news_items.get(t, []), nm)
    dart_scores = {}
    dart_best_titles = {}
    for ticker, texts in dart_texts.items():
        finbert_sc = finbert.score(texts)
        kw_sc, _ = calc_dart_keyword_score(texts)   # best_title은 표시에 쓰지 않는다
        if abs(kw_sc - 50) < 1:
            dart_scores[ticker] = finbert_sc
        else:
            dart_scores[ticker] = round(finbert_sc * 0.5 + kw_sc * 0.5, 1)
        # 표시용 제목은 점수와 분리해서 따로 뽑는다(pick_dart_display_title 주석 참조).
        # dart_scores는 위에서 확정됐고 여기서 건드리지 않는다.
        dart_best_titles[ticker] = pick_dart_display_title(texts)

    results=[]
    skipped_low_confidence = 0
    skipped_fixed = 0
    measured_fixed = 0
    for ticker,meta in pool_b.items():
        t_score=_calc_t(meta)
        t_presurge=_calc_t_presurge(meta)   # Task 4: shadow 기술점수 병행 계산
        n_sc, n_headline, n_pct, n_conf, n_raw = news_data.get(
            ticker, (50.0,"",0.0,0.0,None))
        d_sc=dart_scores.get(ticker,50.0)
        # Phase A.1 (C1): 신뢰도 < 0.5 면 뉴스 감성 폐기 (중립 50 처리)
        n_conf_fix, n_raw_fix, n_field_mix = news_fixed.get(ticker, (None, None, ""))
        # 집행은 구값(n_conf) 그대로 — shadow 는 기록만 한다.
        news_skipped = n_conf < 0.5
        news_eff = 50.0 if news_skipped else n_sc
        if news_skipped:
            skipped_low_confidence += 1
        if n_conf_fix is not None:
            measured_fixed += 1
            if n_conf_fix < 0.5:
                skipped_fixed += 1
        # s_text: 표시·DB용 결합 감성 (0~100 스케일 유지 위해 news_w/dart_w 사용)
        s_text = round(news_eff*news_w + d_sc*dart_w, 1)
        # P1-4: S_text의 최종점수 기여 — split_text=False면 기존식(s_text×w_text)과 동일
        if split_text:
            text_contrib = news_eff*w_news + d_sc*w_dart
        else:
            text_contrib = s_text * w_text
        d_score,n_accel,v_surge,overheat_pen=_calc_d(ticker,meta,all_vol,all_hype,top_pct,scfg,date,overheat_ref)
        d_flow=_calc_d_flow(ticker,meta,all_vol,all_hype,top_pct,scfg,date)   # A-3 shadow
        score=round(t_score*w_tech + text_contrib + d_score*w_cross, 2)
        # Task 4: shadow 종합점수(발송·등급에 미사용, 결과 측정 전용)
        score_presurge=round(t_presurge*w_tech + text_contrib + d_score*w_cross, 2)
        # A-3 shadow 종합 — 수급축만 교체, 나머지는 동일
        score_flow=round(t_score*w_tech + text_contrib + d_flow*w_cross, 2)
        results.append({
            "ticker":ticker,"name":meta.get("name",ticker),"sector":meta.get("sector","기타"),
            "rating_bond":meta.get("rating_bond"),"rating_cp":meta.get("rating_cp"),
            "fg_sector":meta.get("fg_sector"),"fg_industry":meta.get("fg_industry"),
            "ksic":meta.get("ksic"),"largest_holder":meta.get("largest_holder"),
            "cap_tier":meta.get("cap_tier","large"),"score":score,
            "t":t_score,"t_presurge":t_presurge,"score_presurge":score_presurge,
            "s_text":s_text,"news_score":n_sc,"dart_score":d_sc,"d":d_score,
            "news_conf":n_conf,"news_raw":n_raw,
            "news_conf_fixed":n_conf_fix,"news_raw_fixed":n_raw_fix,
            "news_field_mix":n_field_mix,
            "d_flow":d_flow,"score_flow":score_flow,
            "source":meta.get("source","?"),"n_accel":n_accel,"v_surge":v_surge,
            "overheat_pen":overheat_pen,
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
    if measured_fixed:
        # D-2 shadow — 집행에는 반영하지 않는다. 같은 종목을 필드별 검사로
        # 다시 재면 몇 건이 걸리는지만 기록한다.
        logger.info(f"[shadow] 필드별 검사 기준 폐기 {skipped_fixed}/{measured_fixed}종목 "
                    f"(집행 {skipped_low_confidence}건 — 변동 없음)")
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

def run_step4(results,cfg,date=None,dry_run=False):
    # 발송 날짜는 스캔이 쓴 날짜와 같아야 한다. 예전에는 여기서 today_kst()를 따로
    # 계산해, 런이 KST 자정을 넘기면 save_scan_results가 쓴 scan_date와 하루 어긋났다.
    # 실측: 발송 점수가 그날 스캔에 존재하지 않는 기록 128건, 그중 51건은 정확히
    # 하루씩 밀려 있었다(예: 티씨머티리얼즈 6/9 60.9 발송 → 6/10에 기록, 6/10 실제 45.9).
    # 크론을 앞당겨 최근에는 안 보이지만 구조는 그대로였다.
    scan_date = date or today_kst()
    today = scan_date
    tg=TelegramClient()
    min_score=cfg.get("grade",{}).get("min_display_score",0)
    # 필터 전 개수를 따로 잡는다. 예전에는 results를 필터 결과로 덮어쓴 뒤 그걸로
    # '탐지' 수를 세서, 헤더가 구조적으로 항상 "탐지 N종목 → 발송 N종목"이었다.
    detected = len(results)
    results=[r for r in results if r["score"]>=min_score]
    to_send = list(results)
    # 등급으로 나누지 않는다. 점수는 성과 순위를 만들지 못했고(일자내 IC -0.068,
    # 70점 이상 구간이 평균 최고상승 6.80으로 꼴찌), 🔴집중이라는 표시는 읽는 쪽에서
    # 매수 신호로 받아들여진다. 대신 누적 관찰 이력을 붙여 보낸다(IC +0.187).
    watch = get_watch_history([r["ticker"] for r in to_send], scan_date)
    for r in to_send:
        n, since, gap = watch.get(r["ticker"], (0, 0, None))
        r["watch_n"], r["watch_since"], r["watch_gap"] = n, since, gap
    # 누적 관찰이 많은 순 → 같으면 공백이 큰 순(쉬었다 나온 쪽이 나았다)
    to_send.sort(key=lambda r: (r["watch_n"], r["watch_gap"] or 0), reverse=True)

    def phase_tag(r):
        """물밑 점수와 총점의 격차 = 기술축 관점 불일치. 감성·수급은 두 점수가 같으므로
        격차는 오직 '이미 튀어나왔나 / 아직 물밑인가'만 말한다.
        숫자로 병기하지 않는 이유: 실측 IC가 어느 지평에서도 유의하지 않아(fwd10
        -0.054 p=0.33) 다섯 번째 점수로 읽히면 오해만 만든다. 라벨로만 남겨
        누적 뒤 outcomes로 검증한다.
        기준선은 실측 분포(n=418)의 사분위 — 격차 중앙값이 0이 아니라 +6이라
        대칭 ±5로 끊으면 절반이 물밑으로 찍힌다."""
        ps = r.get("score_presurge")
        if ps is None:
            return ""
        gap = ps - r.get("score", 0)
        if gap >= 12:  return "  ⟨물밑⟩"
        if gap <= -1:  return "  ⟨돌출⟩"
        return ""

    def fmt(i,r):
        tier_icon={"large":"[대형]","mid":"[중형]","small":"[소형]"}.get(r.get("cap_tier","large"),"[?]")
        # V-Surge는 가점이 아니라 감점 근거가 됐다(연속 과열 감점으로 교체). 성과 배지로
        # 계속 띄우면 읽는 쪽이 정반대로 받아들이므로, 실제로 깎인 점수를 경고로 보여준다.
        _pen = r.get("overheat_pen") or 0
        cross=("  ✦ N-Accel" if r.get("n_accel") else "")+(f"  ⚠ 과열 -{_pen:.1f}" if _pen >= 1.0 else "")

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

        # 공시는 수집만 하고 버려지고 있었다(best_dart_title을 읽는 곳이 없었다).
        # 뉴스보다 단단한 재료인 경우가 많아 헤드라인과 같은 자리에 붙인다.
        dart_title=str(r.get("best_dart_title") or "").strip()
        dart_line=(f"\n   📄 [공시] {_esc(dart_title[:45])}"
                   + ("..." if len(dart_title)>45 else "")) if dart_title else ""

        return (
            f"\n\n<b>{i}. {_esc(r['name'])} ({r['ticker']})</b>\n"
            f"   {_esc(r['sector'])}  {tier_icon}{rating_str}  |  💵 {price_str}\n"
            f"   🏦 기관 {inst_str}  |  🌏 외인 {foreign_str}  ({r['net_buy_days']}일){retail_tag}\n"
            f"   📊 BB {bb:.0f}% {bb_label}  |  RSI {rsi:.0f} {rsi_label}{vol_line}\n"
            f"   📐 이격도 {r['disparity']:.1f}%  (20일 평균 대비 현재가 위치)\n"
            f"   🏆 <b>총점 {r['score']:.1f}</b>  기술 {r['t']:.0f}  수급 {r['d']:.0f}  감성 {r['s_text']:.0f}{cross}{phase_tag(r)}"
            f"{headline_line}{dart_line}"
        )

    now=datetime.now(KST).strftime("%Y-%m-%d %H:%M")

    def watch_tag(r):
        """관찰 이력 한 줄. 강조 구간은 실측 근거가 있는 곳만 표시한다."""
        n, since, gap = r.get("watch_n",0), r.get("watch_since",0), r.get("watch_gap")
        if n == 0:
            return "신규"                      # 첫 등장은 중앙값 -0.14 — 기대할 것이 없다
        s = f"{n+1}회째 · {since}일 관찰"
        if gap is not None and gap >= 2:
            s += f" · {gap}일 만에 재등장"
        mark = []
        if 3 <= n <= 9:   mark.append("주목")   # 누적 4~10회 = 성과 최상 구간
        if 8 <= since <= 14: mark.append("적기") # 최초 등장 후 8~14일 = +10% 54.0%
        if gap is not None and 4 <= gap <= 7: mark.append("눌림후")  # 공백 4~7일 = 평균 15.71
        return s + ("  ⟨" + "·".join(mark) + "⟩" if mark else "")

    msg_items = []
    msg_items.append(("header",
        f"📡 <b>AlphaRadar 관망 리스트</b>\n📅 {now}\n━━━━━━━━━━━━━━━━━━━━\n"
        f"탐지 {detected}종목 → 발송 {len(to_send)}종목  (기준 {min_score}점 이상)\n"
        f"<i>매수 신호 아님. 차트·수급 확인 후 판단하세요.</i>"))
    if to_send:
        # 정렬이 점수순이 아니라는 걸 밝혀둔다. 번호만 보면 1번이 최고점으로 읽힌다.
        lines = ["<b>관찰 종목</b>  <i>— 누적 관찰 순 (점수순 아님)</i>",
                 "━━━━━━━━━━━━━━━━━━━━"]
        # 등급을 없애면서 상세 블록까지 같이 떨어져 나갔었다. 상세는 등급에 딸린 것이
        # 아니라 종목 판단에 필요한 정보였으므로, 옛 집중/주시가 받던 fmt() 블록을
        # 등급 구분 없이 전 종목에 붙인다. 관찰 이력 줄은 그 아래 유지.
        for i, r in enumerate(to_send, 1):
            lines.append(fmt(i, r) + f"\n   👁 {watch_tag(r)}")
        lines.append("\n━━━━━━━━━━━━━━━━━━━━")
        lines.append("⟨주목⟩ 누적 4~10회 · ⟨적기⟩ 최초 후 8~14일 · ⟨눌림후⟩ 4~7일 공백")
        lines.append("⟨물밑⟩ 아직 안 튀어나온 구조 · ⟨돌출⟩ 이미 움직인 구조  (검증 중)")
        lines.append("차트·수급·관찰 이력 전체는 대시보드에서")
        msg_items.append(("list", "\n".join(lines)))

    # 섹터 요약도 등급 대신 관찰 이력으로. 재등장이 몰린 섹터를 보이게 한다.
    sg = defaultdict(lambda: {"n": 0, "watched": 0})
    for r in to_send:
        sg[r["sector"]]["n"] += 1
        if r.get("watch_n", 0) >= 3:
            sg[r["sector"]]["watched"] += 1
    sl=["🗂 <b>섹터별 현황</b>","━━━━━━━━━━━━━━━━━━━━"]
    for sec,cnt in sorted(sg.items(),key=lambda x:x[1]["n"],reverse=True)[:10]:
        det=f"  (누적 4회+ {cnt['watched']})" if cnt["watched"] else ""
        sl.append(f"🔷 {_esc(sec)} ({cnt['n']}종목){det}")
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
        # 등급마다 실려 나가는 메시지가 다르다. 집중은 "high", 주시는 "mid",
        # 참고는 "low"(이름·섹터 한 줄) 블록이다. 예전에는 집중이 아니면 전부 "mid"로
        # 판정해서, 참고의 발송 성공 여부를 주시 메시지로 물었다. 주시가 하나도 없는
        # 날은 "mid" 메시지 자체가 만들어지지 않아 kind_ok에 키가 없고, ⚪참고 목록은
        # 정상 발송됐는데도 전부 skip으로 빠졌다(20260812·0805·0804·0727 등).
        # 그러면서 실패한 것이 없는데도 아래 경고가 남았다(8/4·8/7 로그 실측).
        # 등급별 블록을 없애 전부 "list" 하나로 나간다. 예전에는 참고를 "mid"(주시)
        # 메시지로 판정해, 주시가 없는 날 ⚪참고가 정상 발송됐는데도 전부 skip으로
        # 빠지고 허위 경고까지 남았다(20260812·0805·0804·0727 등, 8/4·8/7 로그 실측).
        sent_count = 0
        skipped_count = 0
        for r in to_send:
            group = "list"
            if kind_ok.get(group, False):
                mark_sent(r["ticker"], scan_date, r["grade"], r["score"])
                sent_count += 1
            else:
                skipped_count += 1
        if skipped_count:
            logger.warning(
                f"텔레그램 발송 실패 그룹 → mark_sent skip {skipped_count}개 "
                f"(다음 실행 시 재발송 대상)"
            )
        logger.info(f"DB 기록: {sent_count}개 / 전체 to_send {len(to_send)}개")
    _w4 = sum(1 for r in to_send if 3 <= r.get("watch_n", 0) <= 9)
    _new = sum(1 for r in to_send if r.get("watch_n", 0) == 0)
    logger.info(f"발송: {len(to_send)}종목 | 누적 4~10회 {_w4} | 신규 {_new}")

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

    date_str=args.date or today_kst()
    setup_logging(date_str); init_db()

    if args.setup_dart: setup_dart(); return

    try: cfg=load_config(); validate_config(cfg); logger.info("config 검증 완료 ✓")
    except AssertionError as e: logger.error(f"설정 오류: {e}"); sys.exit(1)

    logger.info(f"KIS 조회 도메인: {'실전' if _kis_is_real() else 'VTS'} ({_kis_base_url()})")

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
        # 유니버스가 비면 이후 단계가 전부 0으로 흘러 '발송 0건'으로 조용히 끝난다.
        # 2026-08-07 아침 런이 그랬다 — fdr.StockListing()이 KOSPI·KOSDAQ 모두
        # HTTP 404를 반환했는데 목록 조회 실패가 warning으로 처리돼 워크플로는
        # success로 끝났다. 로그를 열어보기 전에는 알 방법이 없었다.
        # 데이터 소스 장애는 통제할 수 없어도, 그것이 성공으로 보고되는 것은 막는다.
        if not precomputed:
            logger.error("유니버스가 비었다 — 종목 목록 조회 실패로 보인다. "
                         "위 '목록 조회 실패' 경고를 확인할 것. 스캔을 중단한다.")
            sys.exit(1)
        if args.step==0: return
    else:
        if not cache_path.exists(): logger.error("캐시 없음. --step 0 먼저 실행"); sys.exit(1)
        with open(cache_path,"rb") as f: precomputed=pickle.load(f)
        logger.info(f"캐시 로드: {len(precomputed)}개")

    if args.step in (None,1):
        logger.info("▶ Step 1: 신호 탐지")
        pool_a=run_step1(precomputed,cfg,date_str)
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
        pool_b=run_step2(pool_a,precomputed,cfg,date_str)
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
        run_step4(results,cfg,date=date_str,dry_run=args.dry_run)

    logger.info(f"완료  |  소요: {time.time()-start:.1f}초")

if __name__=="__main__":
    main()
