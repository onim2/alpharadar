"""daily.yml 런의 '예정 시각 / 실제 시작 / 텔레그램 도착 / 가드 발동' 표 (2026-10-26~ 측정 주간용).

    python tools/run_timing_report.py 2026-10-26 [2026-10-30]

gh CLI 로 런 목록과 로그를 읽는다(읽기 전용). 시각은 전부 KST.
- 예정: 스케줄은 어느 크론이 쐈는지(로그의 EVENT_SCHEDULE), dispatch 는 slot=pm 이면 16:30(cron-job.org).
- 실제 시작: 런의 run_started_at. dispatch 는 발사(created)와 시작이 같다.
- 텔레그램 도착: alpharadar.py 의 '텔레그램 도착' 로그(텔레그램 서버 시각) 첫·마지막.
- 가드: gate 의 '같은 회차 발송 기록 있음'(건너뜀), FDR 당일 봉 대기 횟수·처음 잡힌 시각.
"""
import json, re, subprocess, sys
from datetime import date, datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
REPO = "onim2/alpharadar"
CRON_KST = {"53 22 * * 0-4": "07:53", "27 8 * * 1-5": "17:27", "10 21 * * 0-4": "06:10",
            "40 9 * * 1-5": "18:40", "0 8 * * 1-5": "17:00"}


def gh(*args):
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=True).stdout


def kst(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(KST)


def parse(log):
    g = {}
    if m := re.search(r"EVENT_SCHEDULE: (.+)", log):
        g["cron"] = m.group(1).strip()
    if m := re.search(r"회차: (\d{8}) (am|pm)", log):
        g["slot"] = f"{m.group(1)} {m.group(2)}"
    g["dry"] = "--dry-run" in (re.search(r"실행 인자: (.*)", log) or [""])[0]
    tg = re.findall(r"텔레그램 도착 (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)", log)
    g["tg"] = (tg[0][11:16], tg[-1][11:16]) if tg else None
    g["skip"] = "같은 회차 발송 기록 있음" in log
    if m := re.search(r"FDR 당일 봉 첫 감지 (\d\d:\d\d:\d\d) KST .*? 대기 (\d)회", log):
        g["bar"] = (m.group(1)[:5], int(m.group(2)))
    elif "대기 후에도 없음" in log:
        g["bar"] = ("없음", 3)
    return g


def main():
    since = date.fromisoformat(sys.argv[1])
    until = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date.today()
    runs = json.loads(gh("run", "list", "--repo", REPO, "--workflow", "daily.yml", "--branch", "main",
                         "--limit", "100", "--json",
                         "databaseId,event,createdAt,startedAt,updatedAt,status,conclusion"))
    rows = []
    for r in runs:
        c = kst(r["createdAt"])
        if not (since <= (c - timedelta(hours=5)).date() <= until):
            continue
        g = parse(gh("run", "view", str(r["databaseId"]), "--repo", REPO, "--log"))
        if r["event"] == "schedule":
            plan = CRON_KST.get(g.get("cron", ""), g.get("cron", "?")) + " 크론"
        else:
            plan = "16:30 dispatch" if (g.get("slot", "").endswith("pm")) else "수동"
        st = kst(r["startedAt"])
        guard = []
        if g["skip"]:
            guard.append("회차 중복 → 건너뜀")
        if "bar" in g:
            guard.append(f"FDR 봉 {g['bar'][0]} (대기 {g['bar'][1]}회{', 발동' if g['bar'][1] else ''})")
        rows.append((st, f"| {g.get('slot', '?')} | {plan} | {c:%m-%d %H:%M} | {st:%m-%d %H:%M} | "
                         f"{(g['tg'][0] + ('~' + g['tg'][1] if g['tg'][1] != g['tg'][0] else '')) if g['tg'] else '—'} | "
                         f"{', '.join(guard) or '없음'} | {r['event']}{' dry' if g['dry'] else ''} | "
                         f"{r['conclusion'] or r['status']} | {r['databaseId']} |"))
    print("| 회차 | 예정 | 발사(created) | 실제 시작 | 텔레그램 도착 | 가드 | 이벤트 | 결과 | 런 |")
    print("|---|---|---|---|---|---|---|---|---|")
    for _, line in sorted(rows):
        print(line)


if __name__ == "__main__":
    main()
