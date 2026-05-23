# GitHub Actions 자동 실행 셋업 가이드

매일 평일 KST 16:00에 GitHub Actions가 알아서 AlphaRadar를 돌려서 텔레그램에 보내줍니다. 컴퓨터를 꺼놔도 작동합니다.

---

## 1단계 — GitHub에 Private 저장소 생성

⚠️ **반드시 Private** (Public이면 .env 안 올려도 코드에서 비밀이 노출될 위험)

1. https://github.com/new
2. Repository name: `alpha-radar` (원하는 이름)
3. **Private 선택** ← 중요
4. "Create repository" 클릭

생성 후 보이는 명령어 중 "push an existing repository" 섹션의 URL 기억:
```
https://github.com/YOUR_USERNAME/alpha-radar.git
```

---

## 2단계 — 로컬 코드를 GitHub에 푸시

터미널에서:

```bash
cd "alpha_radar 2"

# Git 초기화 (이미 .git이 있으면 스킵)
git init
git branch -M main

# .gitignore 적용 — .env는 절대 안 올라감
git status
# → .env가 목록에 안 나오면 OK

# 첫 커밋
git add .
git commit -m "initial: AlphaRadar v3.2"

# 원격 연결
git remote add origin https://github.com/YOUR_USERNAME/alpha-radar.git

# 푸시
git push -u origin main
```

푸시 후 GitHub 페이지에서 파일들이 보이는지 확인. **.env가 안 보여야 정상**입니다.

---

## 3단계 — GitHub Secrets 등록 (가장 중요)

`.env` 안의 값을 GitHub Secrets에 옮겨야 워크플로우가 사용 가능.

1. 저장소 페이지 → **Settings** → 좌측 **Secrets and variables → Actions**
2. **New repository secret** 클릭, 아래 11개를 하나씩 등록:

| Secret 이름 | 값 (.env에서 복사) |
|---|---|
| `KIS_APP_KEY` | KIS_APP_KEY 값 |
| `KIS_APP_SECRET` | KIS_APP_SECRET 값 |
| `KIS_ACCOUNT_NO` | KIS_ACCOUNT_NO 값 |
| `KIS_IS_REAL` | `1` (실전) 또는 `0` (모의) |
| `DART_API_KEY` | DART_API_KEY 값 |
| `NAVER_CLIENT_ID` | 키1 ID |
| `NAVER_CLIENT_SECRET` | 키1 SECRET |
| `NAVER_CLIENT_ID_2` | 키2 ID |
| `NAVER_CLIENT_SECRET_2` | 키2 SECRET |
| `NAVER_CLIENT_ID_3` | 키3 ID (선택 — 한도 안전마진) |
| `NAVER_CLIENT_SECRET_3` | 키3 SECRET (선택) |
| `TELEGRAM_BOT_TOKEN` | 봇 토큰 |
| `TELEGRAM_CHAT_ID` | 채팅 ID |

⚠️ Secret 값은 한 번 저장하면 다시 볼 수 없습니다. (수정만 가능)

---

## 4단계 — 권한 활성화 (DB 자동 커밋용)

워크플로우가 매일 실행 결과(DB·캐시)를 자동 커밋하려면 쓰기 권한 필요.

1. 저장소 **Settings** → 좌측 **Actions → General**
2. 스크롤 내려 **Workflow permissions** 섹션
3. **Read and write permissions** 선택
4. **Save**

---

## 5단계 — 수동 테스트 실행

자동 실행(평일 16:00) 기다리지 말고 지금 한 번 돌려보기:

1. 저장소 페이지 → **Actions** 탭
2. 좌측 목록에서 **AlphaRadar Daily Run** 클릭
3. 우측 **Run workflow** 드롭다운 클릭
4. 옵션 설정:
   - `dry_run`: **true** ← 첫 테스트는 dry-run으로!
   - `limit`: `100` ← 빠르게 100종목만
5. **Run workflow** 버튼

3~5분 후 결과 확인:
- ✅ 녹색 체크 → 정상
- ❌ 빨간 X → 클릭해서 어느 step에서 실패했는지 확인

---

## 6단계 — 실 발송 테스트

위 5단계가 성공하면:

1. **Run workflow** 다시 클릭
2. 이번엔:
   - `dry_run`: **false** (실 발송)
   - `limit`: (빈 값 — 전체 2770종목)
3. **Run workflow**

13~15분 후 텔레그램에 결과 도착하면 셋업 완료.

---

## 7단계 — 자동 실행 확인

별도 작업 없이도 매주 월~금 **KST 16:00**에 자동 실행됩니다.

`.github/workflows/daily.yml`의 cron 설정:
```yaml
- cron: '0 7 * * 1-5'  # UTC 07:00 = KST 16:00
```

스케줄 변경하려면 이 줄 수정 후 커밋.

---

## 문제 해결

### 실패: KIS API 인증 에러
- KIS_APP_KEY/SECRET이 정확히 등록됐는지 확인
- KIS API 포털에서 IP 등록을 안 했는지(또는 0.0.0.0/0 등록) 확인 — GitHub Actions IP는 매번 바뀜

### 실패: Naver 429 (한도 초과)
- 어제 로컬에서 너무 많이 호출한 경우 발생. 자정(KST 00:00) 리셋 후 재시도
- 키 2도 등록했는지 확인

### 실패: Telegram 발송 안 됨
- TELEGRAM_BOT_TOKEN 형식: `1234567890:ABC...` (콜론 포함)
- TELEGRAM_CHAT_ID 형식: 숫자 (개인 채팅) 또는 `-1001234567890` (그룹/채널)

### 실행은 됐는데 DB 커밋 실패
- 4단계의 "Read and write permissions" 활성화 확인

### 로그 보기
- 실패한 run 클릭 → 좌측 **Upload logs on failure** artifact 다운로드 → scanner_YYYYMMDD.log 확인

---

## 비용

GitHub Actions 무료 한도:
- 월 2,000분 (Private 저장소)
- 알파레이더 한 번 실행 = 약 15분
- 평일 22일 × 15분 = **월 330분 사용** → 무료 한도 안에서 여유

---

## 일일 운영 후 — 컴퓨터에서 결과 보기

로컬에서 GitHub의 최신 DB 가져오려면:

```bash
cd "alpha_radar 2"
git pull
# data/scores_history.db 최신화됨
# backtest.py 등으로 분석 가능
```
