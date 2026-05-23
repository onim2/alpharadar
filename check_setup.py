#!/usr/bin/env python3
"""
AlphaRadar 환경 점검 스크립트.
첫 실행 전에 돌려서 누락된 게 있는지 확인.

사용:
    python3 check_setup.py

모든 항목 ✅면 실행 가능 상태.
"""
import os
import sys
from pathlib import Path

GREEN = "\033[92m"
RED   = "\033[91m"
YELLOW= "\033[93m"
RESET = "\033[0m"

def ok(msg):    print(f"  {GREEN}✅{RESET} {msg}")
def warn(msg):  print(f"  {YELLOW}⚠️{RESET}  {msg}")
def fail(msg):  print(f"  {RED}❌{RESET} {msg}")

errors = 0
warnings = 0

print("=" * 70)
print(" AlphaRadar 환경 점검")
print("=" * 70)

# ─── 1. Python 버전 ───────────────────────────────────────────
print("\n[1] Python 버전")
v = sys.version_info
if v.major == 3 and 10 <= v.minor <= 12:
    ok(f"Python {v.major}.{v.minor}.{v.micro}")
elif v.major == 3 and v.minor >= 9:
    warn(f"Python {v.major}.{v.minor} — 3.11 권장 (3.10~3.12 호환)")
    warnings += 1
else:
    fail(f"Python {v.major}.{v.minor} — 3.10 이상 필요")
    errors += 1

# ─── 2. 필수 파일 ─────────────────────────────────────────────
print("\n[2] 필수 파일")
required_files = ["alpharadar.py", "config.yaml", "requirements.txt"]
optional_files = [".env", "backtest.py", ".gitignore"]
for f in required_files:
    if Path(f).exists():
        ok(f"{f}")
    else:
        fail(f"{f} 없음")
        errors += 1
for f in optional_files:
    if Path(f).exists():
        ok(f"{f}")
    else:
        if f == ".env":
            fail(f"{f} 없음 → cp .env.example .env 후 키 입력")
            errors += 1
        else:
            warn(f"{f} 없음 (선택)")

# ─── 3. Python 패키지 ─────────────────────────────────────────
print("\n[3] Python 패키지")
required_pkgs = {
    "FinanceDataReader": "finance-datareader",
    "pandas": "pandas",
    "numpy": "numpy",
    "requests": "requests",
    "dotenv": "python-dotenv",
    "yaml": "pyyaml",
    "scipy": "scipy",
    "tqdm": "tqdm",
}
optional_pkgs = {
    "transformers": "transformers (FinBERT용)",
    "torch": "torch (FinBERT용)",
}
for mod, pip_name in required_pkgs.items():
    try:
        __import__(mod)
        ok(f"{pip_name}")
    except ImportError:
        fail(f"{pip_name} 미설치 → pip install {pip_name}")
        errors += 1
for mod, label in optional_pkgs.items():
    try:
        __import__(mod)
        ok(f"{label}")
    except ImportError:
        warn(f"{label} 미설치 — 없으면 키워드 매칭 폴백 (정확도 ↓)")
        warnings += 1

# ─── 4. 환경변수 (.env) ───────────────────────────────────────
print("\n[4] 환경변수 (.env)")
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    fail("python-dotenv 미설치 — 위 패키지 점검 참고")
    errors += 1

required_env = {
    "KIS_APP_KEY":         "KIS 앱키",
    "KIS_APP_SECRET":      "KIS 앱시크릿",
    "DART_API_KEY":        "OpenDART API 키",
    "NAVER_CLIENT_ID":     "Naver 키1 ID",
    "NAVER_CLIENT_SECRET": "Naver 키1 SECRET",
    "TELEGRAM_BOT_TOKEN":  "Telegram 봇 토큰",
    "TELEGRAM_CHAT_ID":    "Telegram 채팅 ID",
}
optional_env = {
    "KIS_ACCOUNT_NO":        "KIS 계좌번호",
    "KIS_IS_REAL":           "KIS 실전/모의 (1/0)",
    "NAVER_CLIENT_ID_2":     "Naver 키2 ID (한도 안전)",
    "NAVER_CLIENT_SECRET_2": "Naver 키2 SECRET",
    "NAVER_CLIENT_ID_3":     "Naver 키3 ID",
    "NAVER_CLIENT_SECRET_3": "Naver 키3 SECRET",
}
for k, label in required_env.items():
    v = os.getenv(k, "")
    if v and v != f"your_{k.lower()}":
        masked = v[:4] + "..." + v[-4:] if len(v) > 12 else "***"
        ok(f"{label} ({masked})")
    else:
        fail(f"{label} 미설정 (.env의 {k})")
        errors += 1

# Naver 키 개수 카운트
naver_count = sum(
    1 for i in [""] + [f"_{n}" for n in range(2, 10)]
    if os.getenv(f"NAVER_CLIENT_ID{i}") and os.getenv(f"NAVER_CLIENT_SECRET{i}")
)
print()
if naver_count == 0:
    fail(f"Naver 키 0개 — 1개 이상 필수")
    errors += 1
elif naver_count == 1:
    warn(f"Naver 키 1개 — 풀 실행 시 한도 초과 위험, 2~3개 권장")
    warnings += 1
elif naver_count >= 2:
    ok(f"Naver 키 {naver_count}개 (한도 안전)")

for k, label in optional_env.items():
    if k.startswith("NAVER"):
        continue  # 위에서 카운트했음
    v = os.getenv(k, "")
    if v:
        ok(f"{label}: 설정됨")
    else:
        warn(f"{label} 미설정 (선택)")

# ─── 5. config.yaml 유효성 ────────────────────────────────────
print("\n[5] config.yaml")
try:
    import yaml
    cfg = yaml.safe_load(open("config.yaml"))
    s = cfg.get("scoring", {})
    wt = s.get("w_tech", 0)
    wx = s.get("w_text", 0)
    wc = s.get("w_cross", 0)
    total = wt + wx + wc
    if abs(total - 1.0) < 0.001:
        ok(f"가중치 합 1.0 (T:{wt} ST:{wx} D:{wc})")
    else:
        warn(f"가중치 합 {total} (1.0 권장)")
        warnings += 1
    g = cfg.get("grade", {})
    hi = g.get("high_interest", 0)
    mi = g.get("interest", 0)
    if hi > mi:
        ok(f"등급 임계값 (집중:{hi} 주시:{mi})")
    else:
        fail(f"등급 임계값 비정상 (집중:{hi} ≤ 주시:{mi})")
        errors += 1
except Exception as e:
    fail(f"config.yaml 로드 실패: {e}")
    errors += 1

# ─── 6. 디렉토리 쓰기 권한 ────────────────────────────────────
print("\n[6] 쓰기 권한")
for d in ["data", "data/cache", "data/logs"]:
    p = Path(d)
    p.mkdir(parents=True, exist_ok=True)
    try:
        test_file = p / ".write_test"
        test_file.write_text("test")
        test_file.unlink()
        ok(f"{d}/ 쓰기 가능")
    except Exception as e:
        fail(f"{d}/ 쓰기 실패: {e}")
        errors += 1

# ─── 7. DNS 연결성 (간단 체크) ────────────────────────────────
print("\n[7] DNS 연결성 (외부 API 도메인)")
import socket
hosts = [
    "openapi.koreainvestment.com",
    "openapi.naver.com",
    "opendart.fss.or.kr",
    "api.telegram.org",
]
for h in hosts:
    try:
        socket.gethostbyname(h)
        ok(f"{h} 해석 OK")
    except Exception as e:
        fail(f"{h} 해석 실패: {e}")
        errors += 1

# ─── 결과 요약 ────────────────────────────────────────────────
print()
print("=" * 70)
if errors == 0 and warnings == 0:
    print(f" {GREEN}✅ 모두 정상 — 실행 준비 완료{RESET}")
    print()
    print(" 첫 실행 명령:")
    print("   python3 alpharadar.py --limit 50 --dry-run")
elif errors == 0:
    print(f" {YELLOW}⚠️  경고 {warnings}건 (실행 가능, 권장사항 검토){RESET}")
    print()
    print(" 첫 실행 명령:")
    print("   python3 alpharadar.py --limit 50 --dry-run")
else:
    print(f" {RED}❌ 에러 {errors}건 / 경고 {warnings}건 — 해결 후 재실행{RESET}")
    sys.exit(1)
print("=" * 70)
