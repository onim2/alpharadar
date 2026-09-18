"""테스트 공통 설정.

stockcard 로거는 처음 쓰일 때 data/logs/stockcard_<날짜>.log 에 파일 핸들러를 연다.
그 파일은 워크플로가 커밋하는 추적 파일이라, 테스트가 거기 쓰면 작업 트리가 더러워지고
브랜치 diff 로 새어 나간다(2026-09-18 두 번 발생). 세션 동안 LOG_DIR 을 임시 디렉터리로
돌리고, 이미 열린 핸들러가 있으면 닫는다. 운영 코드는 건드리지 않는다.
"""
import logging

import pytest


def _reset_stockcard_logger():
    lg = logging.getLogger("stockcard")
    for h in list(lg.handlers):
        lg.removeHandler(h)
        h.close()


@pytest.fixture(autouse=True, scope="session")
def _isolate_stockcard_log(tmp_path_factory):
    import stockcard_common as sc
    old_dir, old_logger = sc.LOG_DIR, sc._LOGGER
    _reset_stockcard_logger()
    sc.LOG_DIR = tmp_path_factory.mktemp("logs")
    sc._LOGGER = None
    yield sc.LOG_DIR
    _reset_stockcard_logger()
    sc.LOG_DIR, sc._LOGGER = old_dir, None

