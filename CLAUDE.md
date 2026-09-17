# AlphaRadar — 작업 규칙

## 실행
- 항상 저장소 루트에서 실행한다 (경로 상수가 상대경로: `data/cache`, `data/scores_history.db`, `data/logs`).
  스크래치패드에서 스크립트를 돌릴 때는 `load_dotenv()`가 `.env`를 못 찾는다 — 경로를 명시할 것.
- 브랜치 체크아웃 전에 `data/scores_history.db` 상태를 확인하고 보고한다.
  git이 추적하는 DB라 브랜치 이동이 데이터를 되돌린 사고 이력이 있다(8/24).
  로컬 main은 Actions 자동 커밋 때문에 상시 origin보다 뒤처진다 —
  작업 전 `git merge-base --is-ancestor HEAD origin/main`으로 안전을 확인하고 `reset --hard origin/main`.
- 크론 시각(KST 18:40 본 런, 20:30 애프터마켓) 직전에는 푸시하지 않는다.
  저녁 런은 Actions 큐 지연 중앙 117분이라 실제 도착이 KST 21~22시대다. 애매하면 다음 날 아침 런 이후로.

## 데이터
- 종목코드는 문자열 6자리: `dtype={'ticker': str}` + `.str.zfill(6)`.
- 결측은 null. 0으로 쓰지 않는다. "조회 실패"와 "값이 0"은 다른 사실이다.
- 재현 가능한 데이터(PNG·이미지·캐시)는 git에 커밋하지 않는다. artifact 또는 재생성.
  git은 지운 파일도 히스토리에 영구 보관한다.
- pykrx 사용 금지(KRX 응답 포맷 변경으로 고장, 8월부터). 시세는 FDR, 수급·신용·대차는 KIS.
- 통계는 일자내로 낸다(pooled 금지). 절대 수익률·pooled 비교는 결론이 뒤집힌 실측이 있다(8/24).
- outcomes 검정 표 없이 판정 임계값을 바꾸지 않는다. 대안값은 shadow 컬럼으로 병행 기록.

## KIS API
- 접근토큰은 파일 캐시(`.kis_token`)를 재사용한다. 프로세스마다 새로 발급하지 않는다.
- 토큰 발급 API는 연속 호출 제한이 있다. 거부되면 캐시를 확인하지 재발급을 반복하지 않는다.
- 앱키·앱시크릿 재발급을 요청하지 않는다. 키는 `.env` / Actions secrets에만 두고
  코드·커밋·문서에 기재하지 않는다.

### KIS 함정 (실측으로 확인, 2026-09-16)
1. **대차잔고 `daily-loan-trans`(`HHPST074500C0`)는 `MRKT_DIV_CLS_CODE=3`(종목) 필수.**
   `1`(코스피)/`2`(코스닥)를 넣으면 에러 없이 **시장 전체 집계가 조용히 돌아온다.**
2. **신용잔고 `daily-credit-balance`(`FHPST04760000`)는 3영업일 지연.**
   결제일 기준 공표라 9/16을 조회해도 최신 매매일은 9/11이다. 대차는 당일치가 나온다 —
   카드에서 두 줄의 기준일이 다르므로 각각 날짜를 병기한다.
3. **`whol_loan_rmnd_amt`의 단위는 미확인.** 금액이 필요하면 `rmnd_stcn × 종가`로 직접 계산한다.
   비율 필드(`whol_loan_rmnd_rate`, `whol_loan_gvrt`)는 백분율로 바로 쓸 수 있고,
   `rmnd_rate`는 상장주식수 기준이다(실측 대조 확인).
4. 투자자 API(`inquire-investor`)는 개인·외국인·기관 3종만 준다.
   기타법인·투신·연기금 세분화가 없어 자사주(기타법인) 매입은 판별 불가 → null로 표시한다.
5. **2026-09-14부터 KIS `stck_clpr` 와 FDR `Close` 가 다르다.** [확인]
   9/11 이전은 두 소스가 완전히 일치하는데 9/14·15·16·17 은 전부 어긋난다
   (우리넷 9/16 KIS 9,670 vs FDR 9,660 · 하이닉스 9/15 KIS 169.0만 vs FDR 171.2만).
   부호가 일정하지 않아 한쪽이 늦은 시점을 보는 것으로 읽힌다 — 9/14 KRX 애프터마켓
   시행과 날짜가 같다 [추정, 미확인].
   **지표(RSI·이격도·MA)가 전부 FDR 일봉에서 나오므로 카드의 가격도 FDR 을 정본으로 쓴다.**
   investor_flow_daily.close 는 KIS 값이라 다르다 — 같은 줄에 나란히 놓지 말 것.
6. 당일 행은 장중에도 응답에 들어오고 값이 계속 움직인다(종가가 아니라 현재가).
   as_of 와 upsert 가 그 갱신을 받아내는 구조다 — 당일 값을 확정치로 읽지 말 것.

## DART (OpenDART)
- `corp_code`는 8자리 DART 자체 채번이다. 6자리 종목코드가 아니다 — 매핑 캐시가 선행 조건.
- 호출 한도 일 20,000건. 분당 제한은 공식 문서에 없다(미확인).
- 구조화 API로 되는 것: 발행주식·자사주·유통주식수(`stockTotqySttus`), 최대주주(`hyslrSttus`),
  담보 주식수·비율·이력(`majorstock`), CB/BW 발행조건(`cvbdIsDecsn`/`bdwtIsDecsn`), 기업개요(`company`).
- 구조화 API가 없는 것: 담보계약 명세(대출금액·유지비율·만기일), CB 미상환 잔액,
  사업의 개요·매출 구성 → `document.xml` 파싱뿐이다.

### DART 함정 (실측으로 확인, 2026-09-16)
1. **`cvbdIsDecsn`은 "발행결정 시점 스냅샷"이지 미상환 현황이 아니다.**
   전환청구기간이 끝난 CB도 그대로 반환된다 — 우리넷(115440)은 만료 2건이 잡혀
   그대로 쓰면 `cb_potential_pct`가 0이 아니라 28.8%로 나오고 붕괴형 태그가 잘못 켜진다.
   **`cvrqpd_edd < today`는 제외**하고, 이름은 `cb_potential_pct_max`,
   카드 라벨은 "(상한, 상환분 미반영)"으로 정직하게 쓴다.
2. **`document.xml`은 Content-Type이 `application/x-msdownload`로 온다.**
   XML이 아니다 — **선두 2바이트가 `PK`인지로 ZIP 여부를 판별**하고, 아니면 에러 XML로 분기한다.
   내부 XML 인코딩은 **UTF-8**이다(EUC-KR이라는 문서 밖 통설은 실측에서 틀렸다).
3. `majorstock.ctr_stkqy`는 "주요계약체결 주식수"라 담보 외 계약(신탁·대차 등)이 섞일 수 있다.
   파서가 계약 종류를 "담보계약"으로 확인한 건만 `collateral_pct`에 쓰고,
   종류 미확인이면 `contract_pct`로 따로 둔다.
4. 담보 명세 파싱은 **공시가 제공하는 합계 행과 대조**한다. 합계가 어긋나면 부분 결과를 쓰지 말고
   전부 null로 버린다 — 틀린 트리거 가격은 없는 것보다 나쁘다.

## stockcard 로컬 실행
- CLI 는 기본적으로 **실전 DB(`data/scores_history.db`)에 쓰지 않는다.**
  `--db <경로>` 로 사본을 주거나, Actions 처럼 `STOCKCARD_ALLOW_LIVE_DB=1` 을 켜야 한다.
  둘 다 없으면 아무것도 하지 않고 종료한다.
  까닭: 2026-09-16 에 로컬 CLI 한 번이 추적 중인 DB에 빈 테이블을 만들었고,
  그게 브랜치 diff 로 새어 나갔다. 되돌렸지만 다음에도 같은 실수를 한다.

      cp data/scores_history.db /tmp/t.db
      python stockcard.py --step flow --db /tmp/t.db

## 파이프라인
- 기존 스캐너·점수·발송 경로는 건드리지 않는다. 새 기능은 새 테이블·새 스크립트·새 스텝으로.
  stockcard 레이어는 `alpharadar.py`를 수정하지 않고 `import`로 헬퍼만 재사용한다
  (테이블 생성도 `init_db()`가 아니라 stockcard 쪽 `ensure_schema()`에서 한다).
- 새 워크플로는 기존 concurrency 그룹(`alpharadar-daily`)에 넣는다. DB 푸시 경합을 막는 장치다.
- `run_type`은 크론 환경변수로 박는다(시각 추론 금지). `ALPHARADAR_RUN_TYPE`으로 강제 가능.
- 새 모듈의 로거는 지연 초기화로 쓴다(`LOG_DIR` import 시점 고정 문제, 8/24 이월 과제).

## 용어
- 새 산출물은 "stockcard". 텔레그램 메시지 블록("카드")과 구분한다.
- `net_buy_days_5`(최근 5거래일 중 순매수 일수, **비연속**) ≠ `net_buy_streak`(연속 일수). 혼용 금지.
  `entry_candidate()`(9/11 진입 후보 규칙)가 쓰는 것은 전자다.

## 확인 태도
- 모르면 모른다고 한다. 추측 구현이 제일 비싸다.
- 문서보다 실측이 우선이다. 2026-09-16에 `document.xml` 인코딩을 EUC-KR로 추정했다가
  실제로 받아보니 UTF-8이었다. 엔드포인트 존재 여부·응답 형식은 찔러보고 정한다.

## 커밋
- 작업 단위: T0→T1→T2→T3→T5→T4→T6→T7. 브랜치 하나에 하나.
- 커밋 메시지에 전달문 버전을 남긴다: `feat(stockcard): ... (handoff 20260916 v2+reply)`
