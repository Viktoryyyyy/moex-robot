from __future__ import annotations

import json
from pathlib import Path

CONTRACT_PATH = Path("contracts/experiments/usdrubf_ema_3_19_d1_research_baseline_v1.json")

EXPECTED_OUTPUT_FILENAMES = [
    "run_metadata.json",
    "usdrubf_d1_ohlc.csv",
    "usdrubf_d1_ema_3_19_cross_context.csv",
    "usdrubf_d1_ema_3_19_cross_labels.csv",
    "usdrubf_ema_3_19_raw_baseline_summary.csv",
    "usdrubf_ema_3_19_quality_report.json",
]

EXPECTED_REQUIRED_CLI_ARGS = [
    "--source-dataset-path",
    "--output-dir",
    "--run-id",
]


def _load_contract() -> dict[str, object]:
    with CONTRACT_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _walk_string_values(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        out: list[str] = []
        for item in value.values():
            out.extend(_walk_string_values(item))
        return out
    if isinstance(value, list):
        out = []
        for item in value:
            out.extend(_walk_string_values(item))
        return out
    return []


def test_contract_json_required_fields() -> None:
    payload = _load_contract()

    for field in (
        "experiment_id",
        "producer",
        "input_contracts",
        "output_artifacts",
        "required_cli_args",
        "formats",
        "schema_version",
        "no_runtime_consumption",
        "no_broker_execution",
        "no_latest_autodetect",
    ):
        assert field in payload

    assert payload["experiment_id"] == "usdrubf_ema_3_19_d1_research_baseline_v1"
    assert payload["schema_version"] == 1
    assert payload["producer"]["module"] == "src.moex_research.runners.usdrubf_ema_3_19_d1_research_baseline"


def test_output_artifacts_exactly_declared() -> None:
    payload = _load_contract()

    filenames = [item["filename"] for item in payload["output_artifacts"]]
    assert filenames == EXPECTED_OUTPUT_FILENAMES
    assert sorted(payload["formats"]["json"]) == sorted(
        [
            "run_metadata.json",
            "usdrubf_ema_3_19_quality_report.json",
        ]
    )
    assert sorted(payload["formats"]["csv"]) == sorted(
        [
            "usdrubf_d1_ohlc.csv",
            "usdrubf_d1_ema_3_19_cross_context.csv",
            "usdrubf_d1_ema_3_19_cross_labels.csv",
            "usdrubf_ema_3_19_raw_baseline_summary.csv",
        ]
    )


def test_required_cli_args_exactly_declared() -> None:
    payload = _load_contract()
    assert payload["required_cli_args"] == EXPECTED_REQUIRED_CLI_ARGS
    assert payload["artifact_write_policy"]["output_dir_arg"] == "--output-dir"
    assert payload["artifact_write_policy"]["declared_outputs_only"] is True
    assert payload["artifact_write_policy"]["no_external_writes"] is True


def test_research_no_runtime_or_broker_flags() -> None:
    payload = _load_contract()
    assert payload["no_runtime_consumption"] is True
    assert payload["no_broker_execution"] is True
    assert payload["no_latest_autodetect"] is True
    assert payload["research_semantics"]["labels_are_research_only"] is True
    assert payload["research_semantics"]["feature_context_uses_d_plus_1_values"] is False


def test_contract_rejects_hidden_dynamic_and_absolute_server_path_semantics() -> None:
    payload = _load_contract()
    rejections = payload["path_semantics_rejections"]

    assert rejections["latest"] is True
    assert rejections["current"] is True
    assert rejections["autodetect"] is True
    assert rejections["absolute_hardcoded_server_paths"] is True

    for value in _walk_string_values(payload):
        lowered = value.lower()
        assert "latest" not in lowered
        assert "current" not in lowered
        assert "autodetect" not in lowered
        assert not value.startswith("/")
        assert not value.startswith("~/")
        assert "/home/trader" not in value
        assert "\\\\" not in value
