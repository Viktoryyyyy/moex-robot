"""Regression coverage for the registry child removed by calendar migration."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from moex_data.futures import registry_refresh_runner as runner

SCREEN_ID = "liquidity_history_metrics_probe"
EVIDENCE_ID = "registry_evidence_artifacts_producer"
SNAPSHOT_DATE = "2026-09-16"
RUN_DATE = "2026-09-17"


def _simulate_refresh(monkeypatch, tmp_path, *, fault="", bounds=(), workers=4):
    """Fake acquisition only; retain real runner, files, freshness and validators."""
    data_root = tmp_path / "fixture_data"
    outputs = runner.output_paths(ROOT, data_root, SNAPSHOT_DATE, RUN_DATE)
    calls = []
    rows = [
        {
            "secid": secid,
            "mapping_status": "mapped",
            "availability_status": "available",
            "liquidity_status": "pass",
            "history_depth_status": "pass",
            "validation_status": "metrics_computed",
            "review_status": "ready_for_pm_review",
        }
        for secid in runner.DEFAULT_WHITELIST
    ]
    evidence_keys = [key for key in runner.CONTRACTS if key not in ("liquidity_screen", "history_depth_screen")]
    screen_keys = ["liquidity_screen", "history_depth_screen"]

    def acquire(command, *, cwd, text, capture_output, env=None):
        is_screen = bool(calls)
        calls.append(list(command))
        assert cwd == str(ROOT)
        assert text and capture_output
        if fault == ("screen_exit" if is_screen else "evidence_exit"):
            return subprocess.CompletedProcess(command, 7, "", "injected child failure")
        for key in screen_keys if is_screen else evidence_keys:
            if fault == "missing:" + key:
                continue
            path = Path(outputs[key])
            path.parent.mkdir(parents=True, exist_ok=True)
            frame = pd.DataFrame(rows)
            if fault == "invalid:" + key:
                if key == "liquidity_screen":
                    frame.loc[0, "liquidity_status"] = "fail"
                elif key == "history_depth_screen":
                    frame.loc[0, "history_depth_status"] = "review_required"
                    frame.loc[0, "review_status"] = "pending"
                elif key == "algopack_fo_tradestats":
                    frame.loc[0, "availability_status"] = "unavailable"
                else:
                    frame = frame.iloc[:0]
            frame.to_parquet(path, index=False)
            if fault == "stale:" + key:
                os.utime(path, (1, 1))
        stdout = "" if is_screen else 'availability_probe_timing_summary: {"fixture": {"probe_count": 5}}\n'
        return subprocess.CompletedProcess(command, 0, stdout, "")

    with monkeypatch.context() as patch:
        patch.chdir(ROOT)
        patch.setattr(runner, "load_dotenv", None)
        patch.setattr(runner.subprocess, "run", acquire)
        patch.setattr(sys, "argv", [
            "registry_refresh_runner",
            "--snapshot-date", SNAPSHOT_DATE,
            "--run-date", RUN_DATE,
            "--data-root", str(data_root),
            "--timeout", "19.5",
            "--iss-base-url", "https://iss.invalid",
            "--apim-base-url", "https://apim.invalid",
            "--availability-max-workers", str(workers),
            *bounds,
        ])
        result = runner.main()
    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8"))
    return result, manifest, calls, outputs


@pytest.mark.parametrize("workers", [1, 4])
@pytest.mark.parametrize("bounds", [(), ("--from", "2026-09-01", "--till", "2026-09-16")])
def test_launch_arguments_and_valid_manifest(monkeypatch, tmp_path, workers, bounds):
    result, manifest, calls, outputs = _simulate_refresh(
        monkeypatch, tmp_path, workers=workers, bounds=bounds,
    )
    assert result == 0
    common = [
        "--snapshot-date", SNAPSHOT_DATE,
        "--data-root", str(tmp_path / "fixture_data"),
        "--timeout", "19.5",
        "--iss-base-url", "https://iss.invalid",
        "--apim-base-url", "https://apim.invalid",
        *bounds,
    ]
    assert calls == [
        [sys.executable, str(ROOT / "src/moex_data/futures/registry_evidence_artifacts_producer.py"),
         *common, "--availability-max-workers", str(workers)],
        [sys.executable, "-m", "moex_data.futures.liquidity_history_metrics_probe",
         *common, "--full-history-proven"],
    ]
    assert manifest["schema_version"] == "futures_registry_refresh_manifest.v1"
    assert manifest["snapshot_date"] == SNAPSHOT_DATE
    assert manifest["run_date"] == RUN_DATE
    assert manifest["refresh_from"] == ("2026-09-01" if bounds else None)
    assert manifest["refresh_till"] == (SNAPSHOT_DATE if bounds else None)
    assert manifest["component_execution_order"] == [EVIDENCE_ID, SCREEN_ID]
    assert [item["component_id"] for item in manifest["child_component_status"]] == [EVIDENCE_ID, SCREEN_ID]
    assert set(manifest["child_output_references"]) == {EVIDENCE_ID, SCREEN_ID}
    assert set(manifest["child_duration_summary"]) == {EVIDENCE_ID, SCREEN_ID}
    assert all(value >= 0 for value in manifest["child_duration_summary"].values())
    assert manifest["availability_probe_timing_summary"] == {"fixture": {"probe_count": 5}}
    assert manifest["runner_whitelist_applied"] == list(runner.DEFAULT_WHITELIST)
    assert manifest["excluded_instruments_confirmed"] == list(runner.DEFAULT_EXCLUDED)
    assert manifest["output_artifacts"] == outputs
    assert len(manifest["output_summaries"]) == 9
    assert all(item["validation_status"] == "pass" for item in manifest["output_summaries"].values())
    assert manifest["registry_refresh_result_verdict"] == "pass"
    assert manifest["artifact_validation_status"] == "pass"
    assert manifest["blockers"] == []


@pytest.mark.parametrize("fault,expected_calls,component", [
    ("evidence_exit", 1, EVIDENCE_ID),
    ("screen_exit", 2, SCREEN_ID),
])
def test_child_failure_is_not_promoted(monkeypatch, tmp_path, fault, expected_calls, component):
    result, manifest, calls, _ = _simulate_refresh(monkeypatch, tmp_path, fault=fault)
    assert result == 1
    assert len(calls) == expected_calls
    assert manifest["blockers"] == [component + ":component_returncode_nonzero"]
    assert manifest["child_component_status"][-1]["returncode"] == 7
    assert manifest["child_component_status"][-1]["stderr_tail"] == "injected child failure"
    assert manifest["registry_refresh_result_verdict"] == "fail"
    assert manifest["output_summaries"] == {}


@pytest.mark.parametrize("kind", ["missing", "stale"])
@pytest.mark.parametrize("key,expected_calls,component", [
    ("normalized_registry", 1, EVIDENCE_ID),
    ("liquidity_screen", 2, SCREEN_ID),
    ("history_depth_screen", 2, SCREEN_ID),
])
def test_zero_exit_does_not_admit_missing_or_stale_output(
    monkeypatch, tmp_path, kind, key, expected_calls, component,
):
    result, manifest, calls, _ = _simulate_refresh(monkeypatch, tmp_path, fault=kind + ":" + key)
    assert result == 1
    assert len(calls) == expected_calls
    assert manifest["blockers"][0].startswith(component + ":expected_output_" + kind + ": ")
    assert key + "=" in manifest["blockers"][0]
    assert manifest["artifact_validation_status"] == "fail"
    assert manifest["output_summaries"] == {}


@pytest.mark.parametrize("key", [
    "liquidity_screen", "history_depth_screen", "algopack_fo_tradestats", "registry_snapshot",
])
def test_real_output_validation_rejects_invalid_fresh_files(monkeypatch, tmp_path, key):
    result, manifest, calls, _ = _simulate_refresh(monkeypatch, tmp_path, fault="invalid:" + key)
    assert result == 1
    assert len(calls) == 2
    assert all(item["returncode"] == 0 for item in manifest["child_component_status"])
    assert key + "_validation_failed" in manifest["blockers"]
    assert manifest["output_summaries"][key]["validation_status"] == "fail"
    assert manifest["artifact_validation_status"] == "fail"


@pytest.mark.parametrize("inherited", [None, "", "relative_inherited_path"])
def test_child_source_path_and_environment_are_explicit(monkeypatch, tmp_path, inherited):
    if inherited is None:
        monkeypatch.delenv("PYTHONPATH", raising=False)
    else:
        monkeypatch.setenv("PYTHONPATH", inherited)
    monkeypatch.setenv("REGISTRY_TEST_SENTINEL", "preserved")
    before = dict(os.environ)
    code = (
        "import json,os; "
        "print('probe: '+json.dumps({'path':os.environ['PYTHONPATH'],"
        "'sentinel':os.environ['REGISTRY_TEST_SENTINEL'],'cwd':os.getcwd()}))"
    )
    result = runner.run_child(tmp_path, "environment_probe", [sys.executable, "-c", code], {})
    assert result["returncode"] == 0, result["stderr_tail"]
    expected = str((tmp_path / "src").resolve()) + (os.pathsep + inherited if inherited else "")
    assert result["json_line_outputs"]["probe"] == {
        "path": expected, "sentinel": "preserved", "cwd": str(tmp_path.resolve()),
    }
    assert dict(os.environ) == before


@pytest.mark.parametrize("invalid_option", [False, True])
@pytest.mark.parametrize("bounds", [(), ("--from", "2026-09-01", "--till", "2026-09-16")])
def test_actual_selected_producer_cli_without_network(monkeypatch, tmp_path, invalid_option, bounds):
    # Capture the command actually selected by main, then execute it for real.
    _, _, calls, _ = _simulate_refresh(monkeypatch, tmp_path, bounds=bounds)
    command = list(calls[1])
    data_root = tmp_path / "real_cli_no_outputs"
    command[command.index("--data-root") + 1] = str(data_root)
    if invalid_option:
        command.append("--invalid-registry-test-option")
    assert "--help" not in command
    guard_root = tmp_path / "network_guard"
    guard_root.mkdir()
    marker = tmp_path / "network_guard_loaded"
    (guard_root / "sitecustomize.py").write_text(
        "import os,sys\n"
        "from pathlib import Path\n"
        "Path(os.environ['REGISTRY_SMOKE_MARKER']).write_text('active')\n"
        "def deny_network(event, args):\n"
        "    if event.startswith('socket.'):\n"
        "        raise RuntimeError('network forbidden in registry CLI smoke')\n"
        "sys.addaudithook(deny_network)\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("PYTHONPATH", str(guard_root))
    monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
    monkeypatch.setenv("REGISTRY_SMOKE_MARKER", str(marker))
    real_run = subprocess.run

    def bounded_real_run(*args, **kwargs):
        return real_run(*args, **kwargs, timeout=30)

    monkeypatch.setattr(runner.subprocess, "run", bounded_real_run)
    result = runner.run_child(ROOT, SCREEN_ID, command, {})
    assert marker.read_text() == "active"
    assert not data_root.exists()
    if invalid_option:
        assert result["returncode"] == 2, result
        unknown = [
            line.split("error: unrecognized arguments:", 1)[1].strip()
            for line in result["stderr_tail"].splitlines()
            if "error: unrecognized arguments:" in line
        ]
        assert unknown == ["--invalid-registry-test-option"], result["stderr_tail"]
        assert "Missing normalized registry artifact" not in result["stderr_tail"]
        assert result["status"] == "fail"
        assert result["failure_reason"] == "component_returncode_nonzero"
    else:
        # Reaching this exact missing-input error proves all forwarded options parsed.
        expected_missing = runner.contract_path(
            ROOT, data_root, runner.CONTRACTS["normalized_registry"], SNAPSHOT_DATE,
        )
        assert result["returncode"] == 1, result
        assert result["stderr_tail"].strip() == (
            "ERROR: FileNotFoundError: Missing normalized registry artifact: " + str(expected_missing)
        )
        assert result["status"] == "fail"
        assert result["failure_reason"] == "component_returncode_nonzero"
        assert not result["stdout_tail"]
        assert "unrecognized arguments" not in result["stderr_tail"]
        origin = runner.run_child(ROOT, "module_origin", [
            sys.executable, "-c",
            "import json; from moex_data.futures import liquidity_history_metrics_probe as m; "
            "print('origin: '+json.dumps(m.__file__))",
        ], {})
        assert origin["returncode"] == 0, origin["stderr_tail"]
        assert Path(origin["json_line_outputs"]["origin"]).resolve() == (
            ROOT / "src/moex_data/futures/liquidity_history_metrics_probe.py"
        ).resolve()


def test_manifest_contract_tracks_current_and_historical_component_ids():
    text = (ROOT / "contracts/datasets/futures_registry_refresh_manifest_contract.md").read_text(encoding="utf-8")
    assert "component_execution_order for new executions must equal " + EVIDENCE_ID + ", " + SCREEN_ID + "." in text
    assert "python -m moex_data.futures.liquidity_history_metrics_probe" in text
    assert "Historical manifests may contain the legacy component ID liquidity_history_metrics_probe_apim_calendar" in text
    assert "without rewriting or reclassifying them" in text
    assert not (ROOT / "src/moex_data/futures/liquidity_history_metrics_probe_apim_calendar.py").exists()
    assert "manifest_history/{sha256}.json" in text
    assert "exact original bytes" in text
    assert "without replacing the daily manifest" in text
    assert "nonblocking POSIX flock" in text


def _manifest_path(tmp_path):
    directory = tmp_path / ("run_date=" + RUN_DATE)
    directory.mkdir()
    return directory / "manifest.json"


def _archive_path(path, payload):
    return path.parent / "manifest_history" / (hashlib.sha256(payload).hexdigest() + ".json")


@pytest.mark.parametrize("previous", [
    b'{ "run_id":"legacy", "registry_refresh_result_verdict":"fail", '
    b'"component_execution_order":["liquidity_history_metrics_probe_apim_calendar"] }\r\n',
    b'{"damaged": ',
    b"",
    '{"original_note":"Сбой источника"}\r\n'.encode("utf-8"),
])
def test_manifest_preserves_original_bytes_and_identical_retry(tmp_path, previous):
    path = _manifest_path(tmp_path)
    path.write_bytes(previous)
    path.chmod(0o640)
    value = {"run_id": "retry", "registry_refresh_result_verdict": "pass"}
    runner.write_manifest(path, value)
    published = path.read_bytes()
    archive = _archive_path(path, previous)
    assert archive.read_bytes() == previous
    assert _archive_path(path, published).read_bytes() == published
    assert json.loads(published) == value
    assert path.stat().st_mode & 0o777 == 0o640
    initial = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in archive.parent.glob("*.json")}
    runner.write_manifest(path, value)
    assert path.read_bytes() == published
    assert {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in archive.parent.glob("*.json")} == initial
    assert not [p for p in path.parent.rglob(".manifest.*") if p.name != ".manifest.lock"]


def test_same_day_main_retry_retains_legacy_failure_and_each_new_result(monkeypatch, tmp_path):
    outputs = runner.output_paths(ROOT, tmp_path / "fixture_data", SNAPSHOT_DATE, RUN_DATE)
    path = Path(outputs["manifest"])
    path.parent.mkdir(parents=True)
    legacy = b'{"run_id":"old", "registry_refresh_result_verdict":"fail", "stderr_tail":"missing calendar wrapper"}\n'
    path.write_bytes(legacy)
    failed, _, _, _ = _simulate_refresh(monkeypatch, tmp_path, fault="screen_exit")
    failed_bytes = path.read_bytes()
    succeeded, current, _, _ = _simulate_refresh(monkeypatch, tmp_path)
    assert failed == 1 and succeeded == 0
    assert current["registry_refresh_result_verdict"] == "pass"
    for payload in (legacy, failed_bytes, path.read_bytes()):
        assert _archive_path(path, payload).read_bytes() == payload
    assert json.loads(_archive_path(path, failed_bytes).read_bytes())["registry_refresh_result_verdict"] == "fail"


@pytest.mark.parametrize("which", ["previous", "new"])
def test_manifest_archive_failure_keeps_previous_daily_bytes(monkeypatch, tmp_path, which):
    path = _manifest_path(tmp_path)
    previous = b'{"run_id":"legacy", "status":"fail"}\n'
    path.write_bytes(previous)
    real_archive = runner._archive_manifest_bytes

    def fail_selected(directory, payload):
        if (payload == previous) == (which == "previous"):
            raise OSError("injected preservation failure")
        return real_archive(directory, payload)

    monkeypatch.setattr(runner, "_archive_manifest_bytes", fail_selected)
    with pytest.raises(OSError, match="injected preservation failure"):
        runner.write_manifest(path, {"run_id": "new", "status": "pass"})
    assert path.read_bytes() == previous


@pytest.mark.parametrize("stage", [
    "archive_temp", "archive_link", "archive_sync", "parent_sync", "current_temp", "replace",
])
def test_manifest_io_failure_before_replacement_is_fail_closed(monkeypatch, tmp_path, stage):
    path = _manifest_path(tmp_path)
    previous = b'{"run_id":"old", "status":"fail"}\n'
    path.write_bytes(previous)
    history = path.parent / "manifest_history"
    real_temp = runner._manifest_temp
    real_sync = runner._sync_manifest_directory

    def fail(*args, **kwargs):
        raise OSError("injected " + stage)

    def temporary(directory, payload, mode=None):
        if (stage == "archive_temp" and directory == history) or (stage == "current_temp" and directory == path.parent):
            fail()
        return real_temp(directory, payload, mode)

    def sync(directory):
        if (stage == "archive_sync" and directory == history) or (stage == "parent_sync" and directory == path.parent):
            fail()
        return real_sync(directory)

    monkeypatch.setattr(runner, "_manifest_temp", temporary)
    monkeypatch.setattr(runner, "_sync_manifest_directory", sync)
    if stage == "archive_link":
        monkeypatch.setattr(runner.os, "link", fail)
    if stage == "replace":
        monkeypatch.setattr(runner.os, "replace", fail)
    with pytest.raises(OSError, match="injected " + stage):
        runner.write_manifest(path, {"run_id": "new"})
    assert path.read_bytes() == previous
    assert not list(history.glob(".manifest.*"))
    assert not [p for p in path.parent.glob(".manifest.*") if p.name != ".manifest.lock"]


def test_manifest_partial_archive_write_never_poisoned_or_published(monkeypatch, tmp_path):
    path = _manifest_path(tmp_path)
    previous = b'{"status":"fail"}\n'
    path.write_bytes(previous)

    def fail_fsync(fd):
        raise OSError("injected flush failure")

    with monkeypatch.context() as patch:
        patch.setattr(runner.os, "fsync", fail_fsync)
        with pytest.raises(OSError, match="injected flush failure"):
            runner.write_manifest(path, {"status": "pass"})
    assert path.read_bytes() == previous
    assert not list((path.parent / "manifest_history").iterdir())
    # Retry after an interrupted/failed temporary write does not hit a poisoned archive.
    runner.write_manifest(path, {"status": "pass"})
    assert _archive_path(path, previous).read_bytes() == previous


def test_manifest_existing_archive_conflict_is_not_overwritten(monkeypatch, tmp_path):
    path = _manifest_path(tmp_path)
    previous = b'{"status":"fail"}\n'
    path.write_bytes(previous)
    archive = _archive_path(path, previous)
    archive.parent.mkdir()
    archive.write_bytes(b"conflicting evidence")
    with pytest.raises(RuntimeError, match="manifest_archive_conflict"):
        runner.write_manifest(path, {"status": "pass"})
    assert path.read_bytes() == previous
    assert archive.read_bytes() == b"conflicting evidence"


def test_manifest_read_failure_is_not_treated_as_absence(monkeypatch, tmp_path):
    path = _manifest_path(tmp_path)
    previous = b'{"status":"fail"}'
    path.write_bytes(previous)
    real_read = runner._read_manifest_bytes

    def denied(candidate):
        if candidate == path:
            raise PermissionError("injected prior read failure")
        return real_read(candidate)

    monkeypatch.setattr(runner, "_read_manifest_bytes", denied)
    with pytest.raises(PermissionError, match="injected prior read failure"):
        runner.write_manifest(path, {"status": "pass"})
    assert path.read_bytes() == previous
    assert not (path.parent / "manifest_history").exists()


@pytest.mark.parametrize("target", ["current", "archive", "history", "lock"])
def test_manifest_rejects_symlink_targets_without_changing_evidence(tmp_path, target):
    path = _manifest_path(tmp_path)
    previous = b'{"status":"fail"}\n'
    path.write_bytes(previous)
    outside = tmp_path / "outside"
    outside.mkdir()
    source = outside / "prior.json"
    source.write_bytes(previous)
    if target == "current":
        path.unlink()
        path.symlink_to(source)
    elif target == "archive":
        archive = _archive_path(path, previous)
        archive.parent.mkdir()
        archive.symlink_to(source)
    elif target == "history":
        (path.parent / "manifest_history").symlink_to(outside, target_is_directory=True)
    else:
        (path.parent / ".manifest.lock").symlink_to(source)
    with pytest.raises((OSError, RuntimeError)):
        runner.write_manifest(path, {"status": "pass"})
    assert path.read_bytes() == previous
    assert source.read_bytes() == previous
    assert list(outside.iterdir()) == [source]


def test_manifest_publication_lock_refuses_overlapping_writer(tmp_path):
    import fcntl

    path = _manifest_path(tmp_path)
    previous = b'{"status":"fail"}'
    path.write_bytes(previous)
    with (path.parent / ".manifest.lock").open("wb") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            runner.write_manifest(path, {"status": "pass"})
    assert path.read_bytes() == previous
    assert not (path.parent / "manifest_history").exists()
    runner.write_manifest(path, {"status": "pass"})
    assert _archive_path(path, previous).read_bytes() == previous


def test_manifest_serialization_failure_keeps_previous_bytes(tmp_path):
    path = _manifest_path(tmp_path)
    previous = b'{"status":"fail"}'
    path.write_bytes(previous)
    circular = {}
    circular["self"] = circular
    with pytest.raises(ValueError, match="Circular reference"):
        runner.write_manifest(path, circular)
    assert path.read_bytes() == previous
    assert not (path.parent / "manifest_history").exists()


def test_main_does_not_report_success_when_manifest_preservation_fails(monkeypatch, tmp_path, capsys):
    outputs = runner.output_paths(ROOT, tmp_path / "fixture_data", SNAPSHOT_DATE, RUN_DATE)
    path = Path(outputs["manifest"])
    path.parent.mkdir(parents=True)
    previous = b'{"status":"fail"}'
    path.write_bytes(previous)

    def fail_archive(directory, payload):
        raise OSError("injected preservation failure")

    monkeypatch.setattr(runner, "_archive_manifest_bytes", fail_archive)
    with pytest.raises(OSError, match="injected preservation failure"):
        _simulate_refresh(monkeypatch, tmp_path)
    assert path.read_bytes() == previous
    assert "registry_refresh_result_verdict:" not in capsys.readouterr().out
