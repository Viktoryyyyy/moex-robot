from __future__ import annotations

from pathlib import Path

import pytest

from moex_data.futures.controlled_materialization_proof import main, run_controlled_materialization_proof


REPO_ROOT = Path(__file__).resolve().parents[2]
PROOF_RUNNER_PATH = REPO_ROOT / "src/moex_data/futures/controlled_materialization_proof.py"


def _token(*codes: int) -> str:
    return "".join(chr(code) for code in codes)


def _base_args(tmp_path: Path) -> list[str]:
    return [
        "--repo-root",
        REPO_ROOT.as_posix(),
        "--moex-data-root",
        (tmp_path / "moex_data_root").as_posix(),
        "--artifact-bundle-root",
        (tmp_path / "artifact_bundle").as_posix(),
        "--run-id",
        "controlled_proof_test_run",
        "--family",
        "TEST_FAMILY_A",
        "--secid",
        "TEST_FAMILY_A_1",
        "--board",
        "RFUD",
        "--market",
        "FORTS",
        "--series-type",
        "native",
        "--trade-date",
        "2026-06-02",
        "--derived-timeframes",
        "10m",
        "--raw-manifest-ref",
        "${MOEX_DATA_ROOT}/futures/manifests/run_date={YYYY-MM-DD}/run_id={RUN_ID}/raw_5m_manifest.json",
        "--raw-quality-report-ref",
        "${MOEX_DATA_ROOT}/futures/quality/run_date={YYYY-MM-DD}/run_id={RUN_ID}/raw_5m_quality_report.json",
        "--derived-manifest-ref",
        "${MOEX_DATA_ROOT}/futures/manifests/run_date={YYYY-MM-DD}/run_id={RUN_ID}/timeframe={TIMEFRAME}/derived_manifest.json",
        "--derived-quality-report-ref",
        "${MOEX_DATA_ROOT}/futures/quality/run_date={YYYY-MM-DD}/run_id={RUN_ID}/timeframe={TIMEFRAME}/derived_quality_report.json",
    ]


def _without_option(args: list[str], option_name: str) -> list[str]:
    output: list[str] = []
    skip = False
    for value in args:
        if skip:
            skip = False
            continue
        if value == option_name:
            skip = True
            continue
        output.append(value)
    return output


def _with_option(args: list[str], option_name: str, replacement: str) -> list[str]:
    result = list(args)
    index = result.index(option_name) + 1
    result[index] = replacement
    return result


def _imported_names_from_source(source: str) -> tuple[str, ...]:
    names: list[str] = []
    for raw_line in source.splitlines():
        line = raw_line.strip().casefold()
        if line.startswith("import "):
            modules = line.removeprefix("import ").split(",")
            names.extend(module.strip().split(" as ")[0] for module in modules)
        elif line.startswith("from ") and " import " in line:
            names.append(line.removeprefix("from ").split(" import ", 1)[0].strip())
    return tuple(name for name in names if name)


def test_controlled_proof_writes_expected_files_and_summary(tmp_path):
    result = run_controlled_materialization_proof(_base_args(tmp_path))

    assert result.raw_storage_path.exists()
    assert result.raw_manifest_path.exists()
    assert result.raw_quality_report_path.exists()
    assert result.proof_summary_path.exists()
    assert len(result.derived_outputs) == 1
    derived = result.derived_outputs[0]
    assert derived.timeframe == "10m"
    assert derived.storage_path.exists()
    assert derived.manifest_path.exists()
    assert derived.quality_report_path.exists()
    assert result.proof_summary["status"] == "succeeded"
    assert result.proof_summary["real_iss_fetch_performed"] is False
    assert result.proof_summary["strategy_execution_performed"] is False
    assert result.proof_summary["runtime_live_performed"] is False


def test_missing_moex_data_root_or_cli_root_fails_closed(tmp_path, monkeypatch):
    monkeypatch.delenv("MOEX_DATA_ROOT", raising=False)
    monkeypatch.setattr("moex_data.futures.controlled_materialization_proof._load_dotenv", lambda: None)
    exit_code = main(_without_option(_base_args(tmp_path), "--moex-data-root"))
    assert exit_code == 2


def test_missing_artifact_bundle_root_fails_closed(tmp_path):
    with pytest.raises(SystemExit):
        run_controlled_materialization_proof(_without_option(_base_args(tmp_path), "--artifact-bundle-root"))


@pytest.mark.parametrize(
    "marker",
    [
        _token(108, 97, 116, 101, 115, 116),
        _token(99, 117, 114, 114, 101, 110, 116),
        _token(97, 117, 116, 111, 100, 101, 116, 101, 99, 116),
    ],
)
def test_latest_current_autodetect_rejected(tmp_path, marker):
    exit_code = main(_with_option(_base_args(tmp_path), "--run-id", marker))
    assert exit_code == 2


def test_continuous_series_type_rejected(tmp_path):
    exit_code = main(_with_option(_base_args(tmp_path), "--series-type", "continuous"))
    assert exit_code == 2


@pytest.mark.parametrize("option_name", ["--raw-manifest-ref", "--raw-quality-report-ref", "--derived-manifest-ref", "--derived-quality-report-ref"])
def test_manifest_and_quality_refs_are_required(tmp_path, option_name):
    with pytest.raises(SystemExit):
        run_controlled_materialization_proof(_without_option(_base_args(tmp_path), option_name))


def test_no_strategy_backtest_runtime_live_imports_in_proof_runner():
    source = PROOF_RUNNER_PATH.read_text(encoding="utf-8")
    imported_names = _imported_names_from_source(source)
    forbidden_roots = (
        "moex_strategy_sdk",
        "strategies",
        "moex_backtest",
        "moex_runtime",
    )
    for forbidden_root in forbidden_roots:
        assert not any(imported_name.startswith(forbidden_root) for imported_name in imported_names)
