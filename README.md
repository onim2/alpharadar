# AlphaRadar v3.2

한국 주식(KOSPI/KOSDAQ 약 2,770종목) 일일 퀀트 스캐너. 매일 장 마감 후 등급별로 텔레그램에 알림.

- **데이터**: KIS(수급) · Naver(검색트렌드+뉴스) · DART(공시) · FinBERT(감성)
- **점수**: S = T×0.30 + S_text×0.40 + D×0.30
- **등급**: 75↑ 집중 / 65↑ 주시 / 그 이하 참고

---

## 새로 시작하기 — 최단 경로 (30분)

### 1. 파일 배치

이 폴더의 모든 파일을 작업 디렉토리에 풀어둡니다.

```
your_folder/
├── alpharadar.py           # 메인 스캐너 (2,016줄)
├── backtest.py             # 백테스트
├── config.yaml             # 설정 (가중치·필터)
├── requirements.txt        # Python 의존성
├── .env.example            # 환경변수 템플릿
├── .gitignore
├── README.md               ← 이 파일
├── GITHUB_SETUP.md         # GitHub Actions 셋업 가이드
├── check_setup.py          # 환경 점검 스크립트
└── .github/workflows/
    └── daily.yml           # GitHub Actions 워크플로우
```

### 2. Python 환경

```bash
# Python 3.11 권장 (3.10~3.12 호환)
python3 -m venv venv
source venv/bin/activate                    # macOS/Linux
# venv\Scripts\activate                     # Windows

pip install -r requirements.txt

# FinBERT (선택 — 없으면 키워드 매칭으로 폴백, 정확도 낮음)
pip install 'transformers>=4.30' 'torch>=2.0'
```

### 3. API 키 발급

| 서비스 | 발급처 | 용도 | 필수 |
|---|---|---|---|
| KIS Open API | https://apiportal.koreainvestment.com | 수급 데이터 | ✅ |
| OpenDART | https://opendart.fss.or.kr | 공시 | ✅ |
| Naver Open API | https://developers.naver.com | 검색트렌드+뉴스 | ✅ (키 2~3개 권장) |
| Telegram Bot | @BotFather (텔레그램) | 알림 발송 | ✅ |

> **Naver 키 팁**: 1개 키만으론 DataLab 한도(1,000건/일)에 금방 도달. 2,770종목 풀 실행하려면 키 2~3개 필요. 같은 네이버 개발자 계정에서 앱 여러 개 등록 가능.

### 4. .env 설정

```bash
cp .env.example .env
# .env를 열어서 위에서 발급한 키들을 채워넣기
```

### 5. 환경 점검

```bash
python3 check_setup.py
```

모든 항목 ✅면 준비 완료.

### 6. 첫 실행 (소량 dry-run)

```bash
# 50종목만 dry-run (텔레그램 발송 X, 콘솔 출력만)
python3 alpharadar.py --limit 50 --dry-run
```

- 정상이면 약 1~2분 후 콘솔에 결과 표시
- KIS 토큰 발급 메시지 확인 (`KIS 토큰 발급 완료`)
- DART 섹터맵 첫 생성 시 10~15분 추가 (이후 30일 캐시)

### 7. 풀 실행

```bash
python3 alpharadar.py
```

- 약 13~20분 소요 (2,770종목)
- 결과는 `.env`의 `TELEGRAM_CHAT_ID`로 전송

---

## 자주 쓰는 옵션

```bash
# dry-run (텔레그램 발송 X)
python3 alpharadar.py --dry-run

# 종목 수 제한 (테스트용)
python3 alpharadar.py --limit 100

# 시장 지정
python3 alpharadar.py --market KOSPI       # KOSPI만
python3 alpharadar.py --market KOSDAQ      # KOSDAQ만

# 특정 종목 KIS 응답 디버그
KIS_DEBUG_TICKER=001820 python3 alpharadar.py --limit 10 --dry-run
```

---

## GitHub Actions로 자동화 (강력 권장)

로컬 실행은 macOS DNS 막힘 등 환경 문제를 자주 만남. GitHub Actions로 옮기면:
- 매일 평일 KST 16:00 자동 실행
- 컴퓨터 꺼져 있어도 작동
- Linux 환경이라 DNS 안정적

→ `GITHUB_SETUP.md` 참고 (30분 소요)

---

## 폴더 구조 (실행 후 자동 생성)

```
data/
├── cache/
│   ├── dart_sector_map_v2.pkl       # DART 섹터맵 (30일 캐시)
│   └── precompute_YYYYMMDD.pkl      # 일별 캐시
├── logs/
│   └── scanner_YYYYMMDD.log         # 실행 로그
└── scores_history.db                # 점수·발송 이력 SQLite

.kis_token                            # KIS 토큰 (24시간 유효)
```

---

## 트러블슈팅

### "KIS_APP_KEY 미설정 → 수급 데이터를 가져올 수 없습니다"
→ `.env`의 KIS_APP_KEY 확인. KIS API 포털 IP 등록도 확인 (`0.0.0.0/0` 권장).

### "DataLab 한도/인증 오류(429)"
→ 키 2~3개 등록 권장. 키 1만 있으면 1,000종목 정도에서 막힘.

### macOS에서 DNS 막힘 (`Failed to resolve openapi.naver.com`)
→ `sudo dscacheutil -flushcache; sudo killall -HUP mDNSResponder` 후 재시도. 또는 GitHub Actions로 이전 권장.

### "텔레그램 발송 실패"
→ Bot 토큰 형식: `1234567890:ABC...` (콜론 포함). Chat ID는 숫자 또는 `-1001234567890`.

### Naver burst rate limit (초당 한도)
→ `config.yaml`의 `engine_b.hype_workers`, `scoring.text_workers`를 2로 낮추기.

---

## v3.2 주요 변경 (2026-05-22 기준)

- **NaverClient N-키 회전**: DataLab/검색 API별 독립 인덱스, 키 1~9 자동 로드
- **KSIC 한글 변환**: DART 섹터를 코드(26291) 대신 한글(전자·통신장비)로 표시
- **TelegramClient circuit breaker**: 연속 3개 실패 시 즉시 중단 (DNS 막힘 시 3시간 hang 방지)
- **mark_sent 정합성**: 발송 실패 그룹은 DB 미기록 → 다음 실행 시 재시도
- **KIS 응답 방어**: output 타입·dict 검사, 빈 값 행 자동 스킵 (장중 행 처리)

전체 변경 이력은 git log 참고.
