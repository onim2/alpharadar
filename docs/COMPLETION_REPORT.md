# WORK_ORDER 완료 보고 — AlphaRadar v3.3 "물밑 정렬" (2026-07-06)

## 결과: Task별 판정
- Task 0 (교차신호 오류 5건): **완료** — 5건 모두 신규 구현, 수락 검증 PASS
- Task 1 (텍스트 시점 필터): **완료** — pubDate 14일 컷오프, 수락 PASS
- Task 2 (수급 부호 가드): **완료** — net_buy_total>0 가드, 수락 PASS
- Task 3 (과열 배제 필터 + gated 기록): **완료** — 유일한 행동 변경, 수락 PASS
- Task 4 (shadow presurge 스코어): **완료** — 발송 불변, 수락 PASS
- Task 5 (outcomes 추적): **완료** — 멱등 검증 PASS
- Task 6 (통합 검증·커밋): **완료** — 3검증 통과, 커밋 5개 분리

## Task 0 판정: v3.2.1 [신규 구현] — 근거
현재 HEAD(9119235)에 5건 모두 **미존재**로 확인 → 전부 신규 구현.
- `get_engine_b_history(ticker, window_days=3)` — scan_date 인자 없음, `ORDER BY scan_date DESC LIMIT window_days*2` (날짜 윈도우 아님). 수개월 전 이력이 N-Accel 영구 발화 + 당일 자기기록 'both' 자동 +15.
- V-Surge: 순위가 `hype_latest` 정렬(1916행), `_calc_d`에 hype_slope>0 조건 없음(2090행).
- run_step1: neg_ratio를 top_n(500)만 계산 → 그 밖 POOL_A 후보는 neg_ratio=0으로 게이트 무조건 통과.
- save_engine_b_history 호출이 `today=datetime.now()` 사용(1952행), run_step1에 date 인자 없음.
- run_step2 코드 기본값 115/120/130/max_rsi 80 ≠ config 110/115/120/70.
→ 별도 커밋 `75ecd9d fix: v3.2.1 교차신호 오류 5건`.

## 수락 기준 출력 (각 Task 테스트 실제 출력)

### Task 0
```
=== get_engine_b_history 날짜 윈도우 (3건 동시 삽입) ===
  입력: 3개월전=20260407, 오늘=20260706, 3일전=20260703
  반환: ['20260703']  (기대: ['20260703'])   PASS ✓
=== _calc_d n_accel — 시나리오별 (각 1건만) ===
  3개월전(20260407): n_accel=False  기대=False  PASS ✓
  오늘(20260706):    n_accel=False  기대=False  PASS ✓
  3일전(20260703):   n_accel=True   기대=True   PASS ✓
```

### Task 1
```
max_age_days=14 반환 1건: ['테스트종목 신제품 출시 기대. ...']   PASS ✓ (30일전 제외)
max_age_days=None 반환 2건 (기대 2)                              PASS ✓ (하위호환)
```

### Task 2
```
합계 음수(-100)·일수충족(4일)·거래량급증없음 → POOL_A 포함? False   PASS ✓
대조: 합계 양수(+100) → POOL_A 포함? True, engine_a=True          PASS ✓
```

### Task 3
```
POOL_B 포함? False (기대 False)
gated_tickers 행: [('005930','overheat:ret5d',0.25,0.1,0.8)]      PASS ✓
대조(비과열) POOL_B 포함? True                                     PASS ✓
```

### Task 4
```
pool_b 13개 → results 13개, scan_results 13행
score/score_presurge 둘 다 non-null: 13/13                        PASS ✓
예: 035720 legacy 62.0 vs presurge 42.5 (이미 오른 종목 저평가)
```

### Task 5
```
1회차: 대상 444건 | 신규 444건 (fwd1/5/10/20 전부 채움)
2~4회차: 신규 0건                                                  PASS ✓ (멱등)
가드 후 재실행: 신규 0건 | 갱신 0건 (완전 수렴)
gated origin: [('20260525','005930','gated',1.1706)]              PASS ✓
20260704 최신 2종목(009150,402340): 진입가 미도래로 NULL 유지(정상)
```

### Task 6 통합 검증
```
검증1 py_compile alpharadar.py track_outcomes.py → OK
검증2 mock --dry-run 완주: Step0→4, POOL_A 26 → POOL_B 13 → 발송 7, 완료 20.2초
검증3 신규 config 키 전부 제거(9119235 config)로 완주: config 검증 ✓, 완료 19.9초
```

## 변경 파일 목록 + 커밋 해시
| 커밋 | 파일 |
|------|------|
| `75ecd9d` fix: v3.2.1 교차신호 오류 5건 | alpharadar.py |
| `f142a3a` fix: 텍스트 시점·수급 부호 오류 | alpharadar.py, config.yaml |
| `1c40966` feat: 과열 배제 필터 + gated 반사실 기록 | alpharadar.py, config.yaml |
| `4f3eb50` feat: shadow presurge 스코어 (발송 불변) | alpharadar.py, config.yaml |
| `0d30c09` feat: outcomes 추적 인프라 | track_outcomes.py, .github/workflows/daily.yml |

브랜치: `feat/v3.3-presurge` (main 미머지, **푸시 안 함** — 샘 승인 대기).

## 스펙과 다르게 구현한 부분과 사유
- **Task 5 진입가 정의**: WORK_ORDER "T+1/5/10/20 거래일 종가 수익률"에서 진입가를 `scan_date 당일 이후 첫 거래일 종가`로 정의(searchsorted side=left). backtest.py의 기존 forward-return 관례(side=right, 익일 진입)와 다르되, "당일 종가 진입" 해석이 08:00 발송 스캐너에 더 부합. T+h는 진입 후 h거래일.
- **Task 5 멱등 강화**: 스펙의 "신규 행 0"에 더해, 값 불변 시 재기록도 생략(갱신 0)하도록 가드 추가. 스펙 위반 아님(더 엄격).
- 그 외 없음.

## 발견한 추가 문제 (수정하지 않고 보고만)
1. **`_calc_d` V-Surge 순위 산정 위치**: hype_rank는 run_step1에서 hype_slope 정렬로 부여되나, 이 정렬은 `precomputed` 전체(신호 미발화 종목 포함) 기준. POOL_A/B 상대 순위가 아니라 전체 유니버스 순위라 v_surge_rank=20의 의미가 유니버스 규모에 종속. 의도 확인 필요.
2. **DART 시점 필터 미완**: dart_days는 90→30으로 줄였으나(list.json bgn_de/end_de = 접수일 창이므로 30일 자체가 시점 컷), 뉴스처럼 개별 항목 rcept_dt 기반 추가 감쇠는 없음. 30일 창으로 충분하다는 전제.
3. **mock 실행이 실 scores_history.db에 기록**: `--mock`도 save_scan_results로 실 DB에 씀(이번에 20260706 26행 발생 → 삭제 정리함). mock은 별도 DB 경로를 쓰는 게 안전. 별도 개선 필요.
4. **outcomes 진입가와 과열필터 시점 불일치 가능성**: gated 종목의 fwd return은 "제외 안 했다면"의 반사실인데, scan_date 당일 종가 진입 가정. 실제 과열 종목은 당일에도 추가 급등/급락 변동성 큼 — 판정 시 median 사용으로 완화되나 해석 주의.

## 다음 실행(내일 08:00 KST Actions)에서 관찰할 로그 라인
1. `수급일수충족·합계음수 제외: N개` — Task2 가드가 실제로 걸러내는 수 (0이면 해당 케이스 없음)
2. `POOL_A: N개 (A:.. B:.. 동시:..) | 부정게이트 제외:N` — Task0-3 부정뉴스 전체 게이트 작동
3. `POOL_A 후보 부정뉴스 2차 조회: N개` — Task0-3 top_n 밖 후보 2차 조회 발생 수
4. `하드 필터 제거: N개 | 과열배제 N | ...` — Task3 과열배제가 실제로 거르는 수 (물밑 목적 핵심 지표)
5. `과열 gated 기록: N개 → gated_tickers` — 반사실 기록 적재 확인
6. `뉴스 시점 제외 [종목]: N건 (>14일/파싱실패)` — Task1 스테일 뉴스 제외 (증상 S1 해소 증거)
7. `DB 마이그레이션: t_presurge/score_presurge 컬럼 추가 완료` — 프로덕션 DB 최초 1회만
8. `forward outcomes 추적 실행` + `outcomes 적재: 신규 N건` — Task5 Actions 스텝 작동
```
```
