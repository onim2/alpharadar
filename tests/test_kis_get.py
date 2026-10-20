"""_kis_get 토큰 오류 재발급·집계·알림, 기존 차단(_KIS_FAIL_LIMIT·_KIS_DISABLED) 회귀.

네트워크는 전부 가짜다. .kis_token 은 임시 디렉터리에서만 만든다(monkeypatch.chdir).
"""
import pytest

import alpharadar as ar


class Resp:
    def __init__(self, status, body):
        self.status_code, self._body = status, body

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


OK = {"rt_cd": "0", "msg_cd": "MCA00000", "output": {"x": 1}}
TOKEN_EXPIRED = {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "기간이 만료된 token 입니다."}
RATE = {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다."}


@pytest.fixture
def kis(monkeypatch, tmp_path):
    """모듈 전역을 깨끗이 하고, 토큰 발급·조회·텔레그램을 가짜로 바꾼다."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KIS_APP_KEY", "k")
    monkeypatch.setenv("KIS_APP_SECRET", "s")
    monkeypatch.setattr(ar, "_KIS_TOKEN_CACHE", {})
    monkeypatch.setattr(ar, "_KIS_DISABLED", False)
    monkeypatch.setattr(ar, "_KIS_FAIL_COUNT", 0)
    monkeypatch.setattr(ar, "_KIS_STATS", {"calls": 0, "reissue": 0, "codes": {}})
    monkeypatch.setattr(ar, "_KIS_REPORT_ON", True)          # atexit 등록 막기
    monkeypatch.setattr(ar, "_KIS_INTERVAL", 0.0)
    monkeypatch.setattr(ar.time, "sleep", lambda s: None)
    st = {"issued": 0, "post_ok": True, "gets": [], "get_plan": [], "sent": []}

    def post(url, json=None, timeout=None):
        st["issued"] += 1
        if not st["post_ok"]:
            raise ConnectionError("token down")
        return Resp(200, {"access_token": f"T{st['issued']}", "expires_in": 86400})

    def get(url, headers=None, params=None, timeout=None):
        st["gets"].append(headers["authorization"])
        return st["get_plan"].pop(0) if st["get_plan"] else Resp(200, OK)

    class TG:
        def send(self, text):
            st["sent"].append(text)

    monkeypatch.setattr(ar.requests, "post", post)
    monkeypatch.setattr(ar.requests, "get", get)
    monkeypatch.setattr(ar, "TelegramClient", TG)
    return st


def call():
    return ar._kis_get("/x", {}, "TR")


def test_token_error_in_http500_body_is_detected_and_reissued(kis):
    """raise_for_status 전에 본문을 읽는다 — HTTP 500 으로 온 EGW00123 도 잡힌다."""
    kis["get_plan"] = [Resp(500, TOKEN_EXPIRED), Resp(200, OK)]
    assert call() == OK
    assert kis["issued"] == 2, "캐시를 무시한 강제 재발급이 1회 일어나야 한다"
    assert kis["gets"] == ["Bearer T1", "Bearer T2"], "재시도는 새 토큰으로"
    assert ar._KIS_STATS["reissue"] == 1 and ar._KIS_STATS["codes"] == {"EGW00123": 1}


def test_reissue_happens_only_once_and_alert_is_sent(kis):
    """확인 요청 5: 재발급 후에도 토큰 오류면 포기하고, 종료 요약이 텔레그램으로 간다."""
    kis["get_plan"] = [Resp(200, TOKEN_EXPIRED), Resp(200, TOKEN_EXPIRED), Resp(200, OK)]
    assert call() == {}
    assert kis["issued"] == 2, "재발급은 딱 1회"
    assert len(kis["gets"]) == 2, "재발급 뒤 재시도도 1회뿐"
    ar._kis_report()                                   # 프로세스 종료 시 atexit 가 부르는 것
    assert len(kis["sent"]) == 1 and "토큰 재발급 1회" in kis["sent"][0] and "EGW00123×2" in kis["sent"][0]


def test_rate_limit_is_counted_and_alerted(kis):
    kis["get_plan"] = [Resp(200, RATE), Resp(200, OK)]
    assert call() == OK
    ar._kis_report()
    assert ar._KIS_STATS["codes"] == {"EGW00201": 1}
    assert kis["sent"] and "EGW00201×1" in kis["sent"][0]


def test_clean_run_logs_but_does_not_alert(kis):
    for _ in range(3):
        call()
    ar._kis_report()
    assert ar._KIS_STATS["calls"] == 3 and kis["sent"] == []


def test_fail_limit_latch_still_blocks(kis):
    """확인 요청 3: 토큰 발급 연속 _KIS_FAIL_LIMIT 회 실패 → _KIS_DISABLED, 이후 조회 없음."""
    kis["post_ok"] = False
    for _ in range(ar._KIS_FAIL_LIMIT):
        assert call() == {}
    assert ar._KIS_DISABLED is True
    issued, gets = kis["issued"], len(kis["gets"])
    kis["post_ok"] = True
    assert call() == {}
    assert kis["issued"] == issued and len(kis["gets"]) == gets == 0, "차단 뒤에는 발급·조회 모두 없다"


def test_reissue_failure_counts_toward_latch(kis):
    """토큰 오류 → 강제 재발급이 실패하면 그 실패도 래치 계수에 들어간다."""
    assert call() == OK                                # 첫 발급 성공·조회 성공
    kis["post_ok"] = False
    kis["get_plan"] = [Resp(200, TOKEN_EXPIRED)]
    assert call() == {}                                # 토큰 오류 → 재발급 시도 실패
    assert ar._KIS_STATS["reissue"] == 1 and ar._KIS_FAIL_COUNT == 1


def test_reissue_is_capped_once_per_process(kis):
    """토큰이 계속 나쁘면 종목마다 재발급하지 않는다 — 프로세스 전체에서 1회."""
    kis["get_plan"] = [Resp(200, TOKEN_EXPIRED)] * 10
    for _ in range(4):
        assert call() == {}
    assert kis["issued"] == 2, "첫 발급 + 재발급 1회뿐"
    assert ar._KIS_STATS["reissue"] == 1
