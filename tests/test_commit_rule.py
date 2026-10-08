"""DB 커밋 규칙 — 스캔 완료 전 실패는 DB 를 커밋하지 않고, 로그는 언제나 커밋한다.

8/28·9/1·9/8 실패한 아침 런이 대조군만 반쯤 써 둔 DB 를 커밋해, 스캔 행 없는 키가
outcomes 추적 대상이 됐다. daily.yml 커밋 단계의 파일 선택 부분을 실제 git 저장소에서 돌린다.
"""

import subprocess
from pathlib import Path

import pytest
import yaml

import alpharadar as ar

ROOT = Path(__file__).resolve().parents[1]


def test_mark_scan_done_writes_github_env(tmp_path, monkeypatch):
    env = tmp_path / "github_env"
    monkeypatch.setenv("GITHUB_ENV", str(env))
    ar.mark_scan_done()
    assert env.read_text() == "SCAN_DONE=1\n"


def test_mark_scan_done_without_actions(monkeypatch):
    monkeypatch.delenv("GITHUB_ENV", raising=False)
    ar.mark_scan_done()   # 로컬 실행 — 아무 데도 쓰지 않고 넘어간다


def test_fail_at_only_named_point(monkeypatch):
    monkeypatch.setenv("ALPHARADAR_FAIL_AT", "scan_mid")
    ar.fail_at("scan_before")
    with pytest.raises(RuntimeError, match="scan_mid"):
        ar.fail_at("scan_mid")
    monkeypatch.delenv("ALPHARADAR_FAIL_AT")
    ar.fail_at("scan_mid")


def _select_block():
    """daily.yml 커밋 단계에서 '무엇을 스테이징하나'까지만 떼어 온다(커밋·푸시 앞)."""
    wf = yaml.safe_load((ROOT / ".github/workflows/daily.yml").read_text())
    step = next(s for s in wf["jobs"]["run"]["steps"] if s.get("name") == "Commit DB & DART master cache")
    lines = step["run"].splitlines()
    end = next(i for i, l in enumerate(lines) if "git diff --staged --quiet" in l)
    return "\n".join(lines[:end])


@pytest.fixture
def repo(tmp_path):
    r = tmp_path / "repo"
    (r / "data/logs").mkdir(parents=True); (r / "data/cache").mkdir()
    (r / "data/scores_history.db").write_bytes(b"db-v1")
    (r / "data/cache/dart_corp_codes.pkl").write_bytes(b"c1")
    (r / "data/logs/scanner_20261008.log").write_text("l1\n")
    for cmd in (["init", "-q"], ["add", "-A"], ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init"]):
        subprocess.run(["git", *cmd], cwd=r, check=True)
    # 런이 남긴 변경: DB·캐시·로그 모두 바뀌었다
    (r / "data/scores_history.db").write_bytes(b"db-partial")
    (r / "data/cache/dart_corp_codes.pkl").write_bytes(b"c2")
    (r / "data/logs/scanner_20261008.log").write_text("l1\nl2\n")
    return r


def run_select(repo, tmp_path, outcome, scan_done, **extra):
    env = {"PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin", "RUN_OUTCOME": outcome,
           "RUNNER_TEMP": str(tmp_path / "runner_temp"), "HOME": str(tmp_path), **extra}
    if scan_done:
        env["SCAN_DONE"] = "1"
    subprocess.run(["bash", "-e", "-c", _select_block()], cwd=repo, env=env, check=True, capture_output=True)
    staged = subprocess.run(["git", "diff", "--staged", "--name-only"], cwd=repo, capture_output=True, text=True).stdout.split()
    dirty = subprocess.run(["git", "diff", "--name-only"], cwd=repo, capture_output=True, text=True).stdout.split()
    return set(staged), set(dirty)


DB = {"data/scores_history.db", "data/cache/dart_corp_codes.pkl"}
LOG = {"data/logs/scanner_20261008.log"}


@pytest.mark.parametrize("case,outcome,scan_done,commit_db", [
    ("스캔 전 실패(가드·유니버스 0·Step0~2)", "failure", False, False),
    ("스캔 중 실패(Step3)",                    "failure", False, False),
    ("발송 중 실패(Step4)",                    "failure", True,  True),
    ("발송 후 실패(Step4 뒤)",                 "failure", True,  True),
    ("성공",                                   "success", True,  True),
    ("휴장 스킵(exit 0, 스캔 없음)",           "success", False, True),
])
def test_commit_selection(repo, tmp_path, case, outcome, scan_done, commit_db):
    staged, dirty = run_select(repo, tmp_path, outcome, scan_done)
    assert LOG <= staged, case
    if commit_db:
        assert DB <= staged, case
    else:
        assert not (DB & staged), case
        assert not (DB & dirty), f"{case}: 버린 DB 가 작업 트리에 남으면 pull --rebase 가 막힌다"
        assert (tmp_path / "runner_temp/partial/scores_history.db").read_bytes() == b"db-partial"


def test_fail_at_run_on_main_commits_nothing(repo, tmp_path):
    """검증 런(fail_at)이 main 에서 돌면 로그조차 스테이징하지 않고 끝난다."""
    staged, _ = run_select(repo, tmp_path, "failure", True, FAIL_AT="send", GITHUB_REF_NAME="main")
    assert staged == set()
    staged, _ = run_select(repo, tmp_path, "failure", True, FAIL_AT="send", GITHUB_REF_NAME="fix/commit-on-scan-done")
    assert DB <= staged and LOG <= staged


# ── main() 경로: dry-run 에서도 스캔 완료 시점에 SCAN_DONE 이 서는가 ─────────────

@pytest.fixture
def stub_pipeline(tmp_path, monkeypatch):
    """main() 을 단계 함수만 갈아끼워 돌린다. 로그 파일·DB·외부 호출 없음."""
    calls = []
    monkeypatch.setattr(ar, "DB_PATH", tmp_path / "t.db")
    monkeypatch.setattr(ar, "setup_logging", lambda *a, **k: ar.logger)
    monkeypatch.setattr(ar, "run_guard", lambda *a, **k: (True, 0))
    monkeypatch.setattr(ar, "run_step0", lambda *a, **k: calls.append("s0") or {"005930": {"name": "x"}})
    monkeypatch.setattr(ar, "run_step1", lambda *a, **k: calls.append("s1") or {})
    monkeypatch.setattr(ar, "run_step2", lambda *a, **k: calls.append("s2") or {})
    monkeypatch.setattr(ar, "run_step3", lambda *a, **k: calls.append("s3") or [])
    monkeypatch.setattr(ar, "run_step4", lambda *a, **k: calls.append("s4"))
    monkeypatch.setattr(ar, "CACHE_DIR", tmp_path / "cache")
    env = tmp_path / "github_env"
    monkeypatch.setenv("GITHUB_ENV", str(env))
    monkeypatch.delenv("ALPHARADAR_FAIL_AT", raising=False)
    return calls, env


def run_main(argv, monkeypatch):
    monkeypatch.setattr("sys.argv", ["alpharadar.py", *argv])
    ar.main()


@pytest.mark.parametrize("dry", [True, False])
@pytest.mark.parametrize("point,scan_done,reached", [
    ("scan_before", False, []),
    ("scan_mid",    False, ["s0", "s1", "s2"]),
    ("send",        True,  ["s0", "s1", "s2", "s3"]),
    ("send_after",  True,  ["s0", "s1", "s2", "s3", "s4"]),
])
def test_main_marks_scan_done_at_step3(stub_pipeline, monkeypatch, dry, point, scan_done, reached):
    calls, env = stub_pipeline
    monkeypatch.setenv("ALPHARADAR_FAIL_AT", point)
    with pytest.raises(RuntimeError, match=point):
        run_main(["--run-type", "am", "--date", "20261008"] + (["--dry-run"] if dry else []), monkeypatch)
    assert calls == reached
    got = env.read_text() if env.exists() else ""
    assert ("SCAN_DONE=1" in got) == scan_done, (dry, point, got)


def test_main_success_marks_scan_done(stub_pipeline, monkeypatch):
    calls, env = stub_pipeline
    run_main(["--run-type", "pm", "--date", "20261008", "--dry-run"], monkeypatch)
    assert calls == ["s0", "s1", "s2", "s3", "s4"] and "SCAN_DONE=1" in env.read_text()
