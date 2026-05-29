import json
from pathlib import Path

import pytest

from moex_research.runners.artifact_manifest_draft import ArtifactManifestDraft, validate_artifact_manifest_draft
from moex_research.runners.synthetic_signal_artifact import (
    SYNTHETIC_SIGNAL_ARTIFACT_WRITE_REQUEST_FIELDS,
    SYNTHETIC_SIGNAL_ARTIFACT_WRITE_RESULT_FIELDS,
    SYNTHETIC_SIGNAL_ROW_FIELDS,
    SYNTHETIC_SIGNAL_TABLE_ARTIFACT_FIELDS,
    SyntheticSignalArtifactValidationError,
    SyntheticSignalArtifactWriteRequest,
    SyntheticSignalArtifactWriteResult,
    SyntheticSignalRow,
    SyntheticSignalTableArtifact,
    validate_synthetic_signal_artifact_write_request,
    validate_synthetic_signal_artifact_write_result,
    validate_synthetic_signal_row,
    validate_synthetic_signal_table_artifact,
    write_synthetic_signal_artifact_dry_run,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "synthetic_signal_artifact.py"
EXPECTED_ROW_FIELDS = frozenset(
    {
        "strategy_id",
        "strategy_test_id",
        "signal_id",
        "instrument_id",
        "timestamp",
        "signal_value",
        "signal_version",
        "source_type",
    }
)
EXPECTED_ARTIFACT_FIELDS = frozenset(
    {
        "artifact_id",
        "strategy_id",
        "strategy_test_id",
        "artifact_role",
        "artifact_class",
        "schema_version",
        "rows",
        "source_type",
    }
)
EXPECTED_REQUEST_FIELDS = frozenset(
    {
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "signal_artifact",
        "output_path",
        "artifact_manifest_ref",
        "write_mode",
    }
)
EXPECTED_RESULT_FIELDS = frozenset(
    {
        "write_status",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "output_path",
        "artifact_id_or_none",
        "artifact_manifest_ref_or_none",
        "error_message_or_none",
    }
)


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _legacy_strategy_marker() -> str:
    return "d1_" + "ts" + "mom"


def _market_access() -> str:
    return "li" + "ve"


def _scheduler_access() -> str:
    return "run" + "time"


def _external_actor() -> str:
    return "bro" + "ker"


def _intent_marker() -> str:
    return "or" + "der"


def _host_marker() -> str:
    return "ser" + "ver"


def _storage_root_marker() -> str:
    return "data" + "_" + "root"


def _row_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "strategy_id": "strategy_fixture",
        "strategy_test_id": "strategy_test.fixture.v1",
        "signal_id": "signal.fixture.v1",
        "instrument_id": "SYNTH_FIXTURE",
        "timestamp": "2026-01-05T10:00:00Z",
        "signal_value": "1",
        "signal_version": "signal_schema.v1",
        "source_type": "synthetic_test_only",
    }
    values.update(overrides)
    return values


def _artifact_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "artifact_id": "synthetic_signal_artifact.fixture.v1",
        "strategy_id": "strategy_fixture",
        "strategy_test_id": "strategy_test.fixture.v1",
        "artifact_role": "synthetic_signal_table",
        "artifact_class": "temporary_test_path",
        "schema_version": "synthetic_signal_table.v1",
        "rows": (SyntheticSignalRow(**_row_values()),),
        "source_type": "synthetic_test_only",
    }
    values.update(overrides)
    return values


def _request_values(tmp_path: Path, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "request_id": "synthetic_signal_write_request.fixture.v1",
        "strategy_id": "strategy_fixture",
        "strategy_test_id": "strategy_test.fixture.v1",
        "signal_artifact": SyntheticSignalTableArtifact(**_artifact_values()),
        "output_path": str(tmp_path / "synthetic_signal_table.json"),
        "artifact_manifest_ref": "artifact_manifest.strategy_test.fixture.v1",
        "write_mode": "dry_run_test_only",
    }
    values.update(overrides)
    return values


def test_valid_synthetic_signal_row_passes():
    row = SyntheticSignalRow(**_row_values())

    assert validate_synthetic_signal_row(row) is row
    assert frozenset(row.__dict__) == EXPECTED_ROW_FIELDS
    assert frozenset(SYNTHETIC_SIGNAL_ROW_FIELDS) == EXPECTED_ROW_FIELDS


@pytest.mark.parametrize(
    "field_name",
    (
        "strategy_id",
        "strategy_test_id",
        "signal_id",
        "instrument_id",
        "timestamp",
        "signal_value",
        "signal_version",
    ),
)
def test_invalid_synthetic_signal_row_fails_closed(field_name: str):
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalRow(**_row_values(**{field_name: ""}))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_synthetic_signal_row_selection_markers_fail_closed(marker: str):
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalRow(**_row_values(signal_id="signal." + marker + ".fixture"))


def test_synthetic_signal_row_unsupported_source_type_fails_closed():
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalRow(**_row_values(source_type="fixture_source"))


def test_valid_synthetic_signal_table_artifact_passes():
    artifact = SyntheticSignalTableArtifact(**_artifact_values())

    assert validate_synthetic_signal_table_artifact(artifact) is artifact
    assert frozenset(artifact.__dict__) == EXPECTED_ARTIFACT_FIELDS
    assert frozenset(SYNTHETIC_SIGNAL_TABLE_ARTIFACT_FIELDS) == EXPECTED_ARTIFACT_FIELDS


@pytest.mark.parametrize(
    "field_name",
    (
        "artifact_id",
        "strategy_id",
        "strategy_test_id",
        "schema_version",
    ),
)
def test_invalid_synthetic_signal_table_artifact_fails_closed(field_name: str):
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalTableArtifact(**_artifact_values(**{field_name: ""}))


def test_empty_rows_fail_closed():
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalTableArtifact(**_artifact_values(rows=()))


def test_invalid_artifact_role_class_and_source_type_fail_closed():
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalTableArtifact(**_artifact_values(artifact_role="other_role"))
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalTableArtifact(**_artifact_values(artifact_class="other_class"))
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalTableArtifact(**_artifact_values(source_type="other_source"))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_synthetic_signal_table_artifact_selection_markers_fail_closed(marker: str):
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalTableArtifact(**_artifact_values(artifact_id="artifact." + marker + ".fixture"))


def test_valid_synthetic_signal_artifact_write_request_passes(tmp_path: Path):
    request = SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path))

    assert validate_synthetic_signal_artifact_write_request(request) is request
    assert frozenset(request.__dict__) == EXPECTED_REQUEST_FIELDS
    assert frozenset(SYNTHETIC_SIGNAL_ARTIFACT_WRITE_REQUEST_FIELDS) == EXPECTED_REQUEST_FIELDS


@pytest.mark.parametrize(
    "field_name",
    (
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "output_path",
        "artifact_manifest_ref",
    ),
)
def test_invalid_synthetic_signal_artifact_write_request_fails_closed(tmp_path: Path, field_name: str):
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path, **{field_name: ""}))


def test_invalid_signal_artifact_fails_closed(tmp_path: Path):
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path, signal_artifact=object()))


def test_invalid_write_mode_fails_closed(tmp_path: Path):
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path, write_mode="unexpected_mode"))


def test_signal_artifact_strategy_identity_must_match_request(tmp_path: Path):
    mismatched_artifact = SyntheticSignalTableArtifact(**_artifact_values(strategy_id="other_strategy"))

    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path, signal_artifact=mismatched_artifact))


def test_valid_temporary_test_signal_artifact_write_succeeds(tmp_path: Path):
    request = SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path))

    result = write_synthetic_signal_artifact_dry_run(request)

    assert validate_synthetic_signal_artifact_write_result(result) is result
    assert result.write_status == "written"
    assert result.error_message_or_none is None
    assert result.artifact_id_or_none == request.signal_artifact.artifact_id
    assert result.artifact_manifest_ref_or_none == request.artifact_manifest_ref
    assert Path(result.output_path).exists()


def test_written_artifact_content_matches_synthetic_signal_rows_exactly(tmp_path: Path):
    rows = (
        SyntheticSignalRow(**_row_values(signal_id="signal.fixture.one", signal_value="1")),
        SyntheticSignalRow(**_row_values(signal_id="signal.fixture.two", signal_value="-1")),
    )
    artifact = SyntheticSignalTableArtifact(**_artifact_values(rows=rows))
    request = SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path, signal_artifact=artifact))

    result = write_synthetic_signal_artifact_dry_run(request)
    written = json.loads(Path(result.output_path).read_text(encoding="utf-8"))

    assert result.write_status == "written"
    assert written["rows"] == [row.__dict__ for row in rows]
    assert written["artifact_id"] == artifact.artifact_id
    assert written["artifact_role"] == "synthetic_signal_table"
    assert written["artifact_class"] == "temporary_test_path"
    assert written["source_type"] == "synthetic_test_only"


def test_jsonl_write_contains_only_synthetic_signal_rows(tmp_path: Path):
    output_path = tmp_path / "synthetic_signal_table.jsonl"
    request = SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path, output_path=str(output_path)))

    result = write_synthetic_signal_artifact_dry_run(request)
    lines = Path(result.output_path).read_text(encoding="utf-8").splitlines()

    assert result.write_status == "written"
    assert [json.loads(line) for line in lines] == [row.__dict__ for row in request.signal_artifact.rows]


def test_non_temporary_path_fails_closed(tmp_path: Path):
    non_temp_path = str(REPO_ROOT / "artifacts" / "synthetic_signal_table.json")

    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path, output_path=non_temp_path))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_request_selection_markers_fail_closed(tmp_path: Path, marker: str):
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalArtifactWriteRequest(
            **_request_values(tmp_path, request_id="synthetic_signal_request." + marker + ".fixture")
        )


@pytest.mark.parametrize(
    "marker",
    (
        _host_marker(),
        _scheduler_access(),
        _market_access(),
        "data" + "lake",
        "data" + " " + "lake",
        _storage_root_marker(),
    ),
)
def test_platform_path_markers_fail_closed(tmp_path: Path, marker: str):
    bad_path = str(tmp_path / marker / "synthetic_signal_table.json")

    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path, output_path=bad_path))


def test_writer_returns_rejected_for_invalid_object():
    result = write_synthetic_signal_artifact_dry_run(object())

    assert result.write_status == "rejected"
    assert result.error_message_or_none is not None
    assert result.artifact_id_or_none is None
    assert result.artifact_manifest_ref_or_none is None


def test_result_object_contains_only_write_status_and_identifiers(tmp_path: Path):
    result = write_synthetic_signal_artifact_dry_run(SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path)))

    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(SYNTHETIC_SIGNAL_ARTIFACT_WRITE_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS


def test_result_object_rejects_extra_fields(tmp_path: Path):
    result = write_synthetic_signal_artifact_dry_run(SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path)))
    result.metrics = {}

    with pytest.raises(SyntheticSignalArtifactValidationError):
        validate_synthetic_signal_artifact_write_result(result)


def test_result_object_does_not_contain_forbidden_outputs(tmp_path: Path):
    result = write_synthetic_signal_artifact_dry_run(SyntheticSignalArtifactWriteRequest(**_request_values(tmp_path)))
    blocked_fields = (
        "metrics",
        "metrics_output",
        "report_output",
        "registry_entry_write_status",
        "promotion_" + "verdict",
        "back" + "test_result",
        "research_result",
        _scheduler_access() + "_" + _market_access() + "_authorization",
        _market_access() + "_authorization",
    )

    for field_name in blocked_fields:
        assert field_name not in result.__dict__


def test_artifact_manifest_draft_ref_linkage_is_explicit():
    artifact = SyntheticSignalTableArtifact(**_artifact_values())
    manifest_ref = "artifact_manifest.strategy_test.fixture.v1"

    draft = ArtifactManifestDraft(
        artifact_manifest_draft_id="artifact_manifest_draft.strategy_test.fixture.v1",
        request_id="synthetic_signal_write_request.fixture.v1",
        strategy_id="strategy_fixture",
        strategy_test_id="strategy_test.fixture.v1",
        planned_artifacts=(artifact.artifact_id,),
        artifact_manifest_ref=manifest_ref,
    )

    assert validate_artifact_manifest_draft(draft) is draft
    assert artifact.artifact_id in draft.planned_artifacts
    assert draft.artifact_manifest_ref == manifest_ref
    assert draft.write_allowed is False
    assert draft.registry_write_allowed is False
    assert getattr(draft, "promotion_" + "verdict_allowed") is False


def test_result_schema_rejects_forbidden_extra_output_field():
    with pytest.raises(SyntheticSignalArtifactValidationError):
        SyntheticSignalArtifactWriteResult(
            write_status="written",
            request_id="request.fixture.v1",
            strategy_id="strategy_fixture",
            strategy_test_id="strategy_test.fixture.v1",
            output_path="/tmp/synthetic_signal_table.json",
            artifact_id_or_none="synthetic_signal_artifact.fixture.v1",
            artifact_manifest_ref_or_none="artifact_manifest.strategy_test.fixture.v1",
            error_message_or_none=None,
            metrics={},
        )


def test_no_real_ema_calculation_is_introduced():
    source_text = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    assert "calculate_" + "ema" not in source_text


def test_no_real_signal_generation_over_data_is_introduced():
    source_text = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    assert "generate_" + "signals" not in source_text
    assert "materialize_" + "signals" + "_from_" + "data" not in source_text


def test_no_market_data_loading_is_introduced():
    source_text = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    assert "load_" + "market_" + "data" not in source_text
    assert "moex_data" not in source_text


def test_source_has_no_forbidden_execution_responsibility_markers():
    forbidden_markers = (
        "load_" + "market_" + "data",
        "calculate_" + "ema",
        "execute_" + "strategy",
        "generate_" + "signals",
        "materialize_" + "signals" + "_from_" + "data",
        "run_" + "back" + "test",
        "execute_" + "back" + "test",
        "run_" + "research",
        "calculate_" + "pnl",
        "calculate_" + "metrics",
        "write_" + "report",
        "generate_" + "report",
        "write_" + "registry",
        "create_" + "promotion_" + "verdict",
        _external_actor(),
        _intent_marker(),
        _market_access() + "_execution",
        _scheduler_access() + "_execution",
        _storage_root_marker(),
        _host_marker(),
        _freshness_marker(),
        _active_marker(),
        _implicit_marker(),
        _legacy_strategy_marker(),
    )
    source_text = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    for marker in forbidden_markers:
        assert marker not in source_text, marker


def test_source_does_not_import_legacy_strategy_or_external_data_infra():
    forbidden_markers = (
        _legacy_strategy_marker(),
        "data" + "_" + "lake",
        "moex_data",
        _scheduler_access(),
        _host_marker(),
        "/home/",
        "/var/",
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "glob(",
        "os.",
    )
    source_text = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    for marker in forbidden_markers:
        assert marker not in source_text, marker
