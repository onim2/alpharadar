"""conftest 의 로그 격리가 실제로 작동하는지 — 추적 로그(data/logs)에 쓰지 않는다."""
import logging


def test_stockcard_log_goes_to_tmp(_isolate_stockcard_log):
    """격리 확인: 로거가 여는 파일이 저장소 data/logs 가 아니라 임시 디렉터리에 있다."""
    import stockcard_common as sc
    sc.logger().info("isolation check")
    files = [h.baseFilename for h in logging.getLogger("stockcard").handlers
             if isinstance(h, logging.FileHandler)]
    assert files and all(str(_isolate_stockcard_log) in f for f in files), files
