"""파이프라인 단계 사이에서 메타 키가 새지 않는지 지킨다.

POOL_A dict(run_step1)는 명시 화이트리스트고 POOL_B는 그걸 {**meta}로 펼친다.
그래서 화이트리스트에서 빠진 키는 run_step3까지 영영 오지 않고, 읽는 쪽은
meta.get(k, 기본값) 이라 조용히 기본값으로 대체된다 — 예외도 경고도 없다.
prev_change_pct 가 그렇게 8/26~9/16 400행 내내 0으로 찍혔다.

같은 사고가 반복되지 않도록 '계산됐고 DB에 저장되는' 키가 실제로 건너오는지
검사한다. 새 메타 필드를 추가할 때 이 목록에도 넣을 것.
"""

import importlib.util
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def ar():
    spec = importlib.util.spec_from_file_location("ar", REPO / "alpharadar.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def cfg():
    with open(REPO / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _pool_a_for(ar, cfg, tmp_path, **overrides):
    """engine_a 가 켜지는 합성 종목 하나로 POOL_A 를 만든다.

    실전 DB와 네트워크를 둘 다 끊는다. run_step1 은 engine_b 이력·대조군 표본을
    DB에 쓰고, 네이버 DataLab(get_trend)과 뉴스(get_news_headlines)를 호출한다.
    DataLab 을 그대로 두면 타임아웃 8초가 종목마다 쌓여 한 번에 30초씩 걸린다.
    """
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(ar, "DB_PATH", tmp_path / "t.db")
        mp.setattr(ar.NaverClient, "get_trend", lambda self, *a, **k: [])
        mp.setattr(ar.NaverClient, "get_news_headlines", lambda self, *a, **k: [])
        ar.init_db()

        pre = ar.generate_mock_precomputed()
        tk = list(pre)[0]
        pre[tk].update({
            "net_buy_days": 4, "vol_5d_avg": 300.0, "vol_60ma": 100.0,
            "neg_ratio": 0.0, "hype_slope": 0.0,
            **overrides,
        })
        pool_a = ar.run_step1(pre, cfg, "20260917")
    assert tk in pool_a, "표본이 POOL_A 에 진입하지 못했다 — 테스트 전제가 깨졌다"
    return tk, pre, pool_a


@pytest.fixture(scope="module")
def built(ar, tmp_path_factory):
    """run_step1 은 한 번만 돌린다 — 파라미터마다 돌리면 테스트가 분 단위가 된다.

    반환: (기본 표본, 전일급등·당일급락 표본)
    """
    with open(REPO / "config.yaml", encoding="utf-8") as f:
        c = yaml.safe_load(f)
    d = tmp_path_factory.mktemp("meta")
    return (_pool_a_for(ar, c, d),
            _pool_a_for(ar, c, d, prev_change_pct=26.5, change_pct=-18.2))


# scan_results 에 저장되면서 run_step3 가 meta 에서 읽는 키들.
# 여기 있는 키가 POOL_A 화이트리스트에서 빠지면 조용히 기본값이 된다.
CARRIED = ["change_pct", "prev_change_pct", "rsi", "bb_pos",
           "net_buy_days", "inst_net", "foreign_net",
           "hype_slope", "hype_rank", "current_price"]


@pytest.mark.parametrize("key", CARRIED)
def test_POOL_A가_메타_키를_흘리지_않는다(built, key):
    (tk, _, pool_a), _ = built
    assert key in pool_a[tk], (
        f"'{key}' 가 POOL_A 화이트리스트(run_step1)에서 빠졌다. "
        f"POOL_B 는 {{**meta}} 라 여기 없으면 run_step3 까지 오지 않고, "
        f"읽는 쪽은 meta.get(기본값) 이라 조용히 기본값으로 찍힌다.")


def test_prev_change_pct_가_값_그대로_건너온다(built):
    """8/26~9/16 400행이 전부 0이었던 그 경로."""
    _, (tk, pre, pool_a) = built
    assert pre[tk]["prev_change_pct"] == 26.5
    assert pool_a[tk]["prev_change_pct"] == 26.5, "계산은 됐는데 POOL_A 에서 사라졌다"


def test_전일급등_당일급락이_shadow_플래그를_켠다(ar, cfg, built):
    """키가 건너와야 flag 가 선다. 0.0 으로 대체되면 15.0 문턱을 영원히 못 넘는다."""
    _, (tk, _, pool_a) = built
    meta = {**pool_a[tk]}                      # run_step2 의 POOL_B 구성과 같은 모양
    flag = ar.prev_spike_flag(cfg,
                              meta.get("prev_change_pct", 0.0),
                              meta.get("change_pct", 0.0))
    assert flag == 1


def test_shadow_플래그는_집행에_쓰이지_않는다(cfg):
    """킬스위치가 내려가 있어야 이 수정이 발송 결과를 바꾸지 않는다."""
    gate = (cfg.get("overheat", {}) or {}).get("prev_day_spike_gate", {}) or {}
    assert gate.get("enabled") is False, (
        "게이트가 켜져 있다. prev_change_pct 가 이제 실제 값으로 차므로 "
        "켜는 순간 발송 종목이 바뀐다 — outcomes 검정 표를 먼저 볼 것.")
