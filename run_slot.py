"""런 회차(slot) 판정 — 이 실행이 '어느 날짜의 아침/저녁 런'인지 정한다.

왜 필요한가: Actions schedule 은 발사 시각을 보장하지 않는다. 실측(KST, 크론 대비 지연):
  8/10~8/26  아침 +0.4~0.8h · 저녁 +0.4h
  9/28~10/8  아침 +2.6~4.6h · 저녁 +6~9h (다음 날 00:56~03:47 발사)
저녁 런이 자정을 넘기면 today_kst()·run_type_kst() 가 '다음 날 am' 을 찍어
진짜 아침 런과 같은 키를 썼다(10/2 저녁 → 20261003 am, 10/7 저녁 → 20261008 am).

회차 = 어느 크론이 쐈는가(github.event.schedule) 또는 수동 실행의 slot 입력.
회차 날짜 = 그 회차의 정시(아침 06:10, 저녁 18:40 KST)가 지금 이전인 가장 최근 평일.
지연이 24시간 미만이면 이것이 원래 의도한 날짜다. 휴장일은 여기서 거르지 않는다
(alpharadar.run_guard 가 데이터로 판정한다).

출력: GITHUB_ENV 에 SLOT_TYPE=am|pm, SLOT_DATE=YYYYMMDD.
회차를 정할 수 없으면(수동 실행에 slot 미지정) 둘 다 빈 값 — alpharadar.py 가
실행 시각으로 판정하고 '회차 폴백' 경고를 남긴다.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

# daily.yml 의 cron 문자열 → 회차. 크론을 바꾸면 여기도 같이 바꾼다.
CRON_SLOT = {
    "10 21 * * 0-4": "am",
    "40 9 * * 1-5": "pm",
}
NOMINAL = {"am": (6, 10), "pm": (18, 40)}
# 정시보다 조금 일찍 시작한 수동 실행도 그 회차로 본다.
EARLY_TOLERANCE = timedelta(minutes=30)


def resolve(slot_input="", cron="", date_input="", now=None):
    """(slot, date) 를 돌려준다. 정할 수 없으면 ("", "")."""
    now = (now or datetime.now(KST)).astimezone(KST)
    slot = (slot_input or "").strip().lower()
    if slot not in NOMINAL:
        slot = CRON_SLOT.get((cron or "").strip(), "")
    if not slot:
        return "", ""

    date = (date_input or "").strip().replace("-", "")
    if date:
        datetime.strptime(date, "%Y%m%d")  # 형식 검증
        return slot, date

    h, m = NOMINAL[slot]
    probe = now + EARLY_TOLERANCE
    d = probe.date()
    for _ in range(8):
        nominal = datetime(d.year, d.month, d.day, h, m, tzinfo=KST)
        if nominal <= probe and d.weekday() < 5:
            return slot, d.strftime("%Y%m%d")
        d -= timedelta(days=1)
    return "", ""  # 도달 불가 — 평일은 8일 안에 반드시 있다


def main():
    cron = os.getenv("EVENT_SCHEDULE", "")
    slot, date = resolve(os.getenv("SLOT_INPUT", ""), cron, os.getenv("DATE_INPUT", ""))
    if cron and not slot:
        # 크론을 바꾸고 CRON_SLOT 을 안 바꾼 경우. 폴백으로 돌되 크게 남긴다.
        print(f"::warning::알 수 없는 크론 '{cron}' — run_slot.CRON_SLOT 갱신 필요, 실행 시각으로 폴백")
    env = os.getenv("GITHUB_ENV")
    if env:
        with open(env, "a", encoding="utf-8") as f:
            f.write(f"SLOT_TYPE={slot}\nSLOT_DATE={date}\n")
    print(f"회차: {date} {slot}" if slot else "회차 미지정 — 실행 시각으로 판정(폴백)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
