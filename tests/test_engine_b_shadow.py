"""엔진 B shadow — 영업일 기울기와, 현행 기울기(get_trend)가 바뀌지 않았는지."""
import alpharadar as ar

# 2026-10-11(일) ~ 10-22(목), 삼성전자 실측(10/23 DataLab) 모양
SERIES = [("2026-10-11", 5.6), ("2026-10-12", 55.6), ("2026-10-13", 76.7), ("2026-10-14", 64.3),
          ("2026-10-15", 100), ("2026-10-16", 77.6), ("2026-10-17", 11.8), ("2026-10-18", 9.0),
          ("2026-10-19", 77.5), ("2026-10-20", 67.4), ("2026-10-21", 67.6), ("2026-10-22", 59.2)]


class Resp:
    status_code = 200
    def raise_for_status(self): pass
    def json(self): return {"results": [{"data": [{"period": d, "ratio": v} for d, v in SERIES]}]}


def _client(monkeypatch):
    monkeypatch.setenv("NAVER_CLIENT_ID", "i"); monkeypatch.setenv("NAVER_CLIENT_SECRET", "s")
    monkeypatch.setattr(ar.requests, "post", lambda *a, **k: Resp())
    return ar.NaverClient()


def test_get_trend_unchanged(monkeypatch):
    c = _client(monkeypatch)
    assert c.get_trend("x", 7) == [v for _, v in SERIES[-7:]]       # 10/16~10/22, 주말 포함 그대로
    assert c.get_trend_series("x", 7) == SERIES


def test_bizday_slope_drops_weekends():
    biz = [v for d, v in SERIES if d not in ("2026-10-11", "2026-10-17", "2026-10-18")][-7:]
    assert biz == [64.3, 100, 77.6, 77.5, 67.4, 67.6, 59.2]
    assert ar.bizday_slope(SERIES, 7) == ar.linear_slope(biz)


def test_weekend_position_no_longer_flips_sign():
    """주말이 창 끝에 오면 현행 기울기는 크게 음수, 영업일 기울기는 평탄한 계열에서 0 근처."""
    flat = [(d, 10.0 if d in ("2026-10-17", "2026-10-18", "2026-10-11") else 70.0) for d, _ in SERIES]
    upto_sun = flat[:8]                                    # 10/11~10/18, 마지막 두 점이 주말
    assert ar.linear_slope([v for _, v in upto_sun[-7:]]) < -0.1
    assert abs(ar.bizday_slope(upto_sun, 7)) < 1e-9


def test_shadow_not_written_in_dry_run(monkeypatch):
    called = []
    monkeypatch.setattr(ar, "_SCAN_RESULTS_READONLY", True)
    monkeypatch.setattr(ar, "_conn", lambda: called.append(1))
    ar.save_hype_shadow([("005930", 0.1, 0.0, 0.0, 1, 0, "2026-10-22")], "20261023", "pm")
    assert called == []
