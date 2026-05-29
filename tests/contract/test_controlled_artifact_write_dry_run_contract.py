from pathlib import Path

import pytest

from moex_research.publishers.artifact_writer import (
    ARTIFACT_WRITE_REQUEST_FIELDS,
    ARTIFACT_WRITE_RESULT_FIELDS,
    ArtifactWriteRequest,
    ArtifactWriteResult,
    ArtifactWriteValidationError,
    validate_artifact_write_request,
    validate_artifact_write_result,
    write_planning_artifact_dry_run,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "publishers" / "artifact_writer.py"
EXPECTED_REQUEST_FIELDS = frozenset(
    {
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "artifact_manifest_ref",
        "artifact_class",
        "artifact_role",
        "content",
        "output_path",
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
        "artifact_role",
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


def _request_values(tmp_path: Path, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "request_id": "artifact_write_request.strategy_test.fixture.v1",
        "strategy_id": "strategy_fixture",
        "strategy_test_id": "strategy_test.fixture.v1",
        "artifact_manifest_ref": "artifact_manifest.strategy_test.fixture.v1",
        "artifact_class": "temporary_test_path",
        "artifact_role": "planning_artifact_manifest",
        "content": '{"synthetic":"planning-only"}',
        "output_path": str(tmp_path / "planning_manifest.json"),
        "write_mode": "dry_run_test_only",
    }
    values.update(overrides)
    return values


def test_valid_temporary_test_artifact_write_succeeds(tmp_path: Path):
    request = ArtifactWriteRequest(**_request_values(tmp_path))

    result = write_planning_artifact_dry_run(request)

    assert validate_artifact_write_request(request) is request
    assert validate_artifact_write_result(result) is result
    assert result.write_status == "written"
    assert result.error_message_or_none is None
    assert Path(result.output_path).read_text(encoding="utf-8") == request.content


def test_written_content_is_exactly_synthetic_planning_content(tmp_path: Path):
    content = "synthetic planning dry-run artifact manifest"
    request = ArtifactWriteRequest(**_request_values(tmp_path, content=content))

    result = write_planning_artifact_dry_run(request)

    assert result.write_status == "written"
    assert Path(result.output_path).read_text(encoding="utf-8") == content


@pytest.mark.parametrize(
    "field_name",
    (
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "artifact_manifest_ref",
        "content",
        "output_path",
    ),
)
def test_empty_required_fields_fail_closed(tmp_path: Path, field_name: str):
    with pytest.raises(ArtifactWriteValidationError):
        ArtifactWriteRequest(**_request_values(tmp_path, **{field_name: ""}))


def test_invalid_artifact_class_fails_closed(tmp_path: Path):
    with pytest.raises(ArtifactWriteValidationError):
        ArtifactWriteRequest(**_request_values(tmp_path, artifact_class="unexpected_class"))


def test_invalid_artifact_role_fails_closed(tmp_path: Path):
    with pytest.raises(ArtifactWriteValidationError):
        ArtifactWriteRequest(**_request_values(tmp_path, artifact_role="unexpected_role"))


def test_invalid_write_mode_fails_closed(tmp_path: Path):
    with pytest.raises(ArtifactWriteValidationError):
        ArtifactWriteRequest(**_request_values(tmp_path, write_mode="unexpected_mode"))


def test_non_temporary_path_fails_closed(tmp_path: Path):
    non_temp_path = str(REPO_ROOT / "artifacts" / "planning_manifest.json")

    with pytest.raises(ArtifactWriteValidationError):
        ArtifactWriteRequest(**_request_values(tmp_path, output_path=non_temp_path))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_marker_paths_fail_closed(tmp_path: Path, marker: str):
    bad_path = str(tmp_path / marker / "planning_manifest.json")

    with pytest.raises(ArtifactWriteValidationError):
        ArtifactWriteRequest(**_request_values(tmp_path, output_path=bad_path))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_marker_refs_fail_closed(tmp_path: Path, marker: str):
    bad_ref = "artifact_manifest." + marker + ".fixture.v1"

    with pytest.raises(ArtifactWriteValidationError):
        ArtifactWriteRequest(**_request_values(tmp_path, artifact_manifest_ref=bad_ref))


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
    bad_path = str(tmp_path / marker / "planning_manifest.json")

    with pytest.raises(ArtifactWriteValidationError):
        ArtifactWriteRequest(**_request_values(tmp_path, output_path=bad_path))


def test_invalid_object_writer_result_is_rejected():
    result = write_planning_artifact_dry_run(object())

    assert result.write_status == "rejected"
    assert result.error_message_or_none is not None


def test_result_object_contains_only_write_status_and_identifiers(tmp_path: Path):
    result = write_planning_artifact_dry_run(ArtifactWriteRequest(**_request_values(tmp_path)))

    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(ARTIFACT_WRITE_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS
    assert frozenset(ARTIFACT_WRITE_REQUEST_FIELDS) == EXPECTED_REQUEST_FIELDS


def test_result_object_rejects_extra_fields(tmp_path: Path):
    result = write_planning_artifact_dry_run(ArtifactWriteRequest(**_request_values(tmp_path)))
    result.extra_field = "not allowed"

    with pytest.raises(ArtifactWriteValidationError):
        validate_artifact_write_result(result)


def test_result_object_does_not_contain_forbidden_outputs(tmp_path: Path):
    result = write_planning_artifact_dry_run(ArtifactWriteRequest(**_request_values(tmp_path)))
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


def test_result_schema_rejects_forbidden_extra_output_field():
    with pytest.raises(ArtifactWriteValidationError):
        ArtifactWriteResult(
            write_status="written",
            request_id="request.fixture.v1",
            strategy_id="strategy_fixture",
            strategy_test_id="strategy_test.fixture.v1",
            output_path="/tmp/planning_manifest.json",
            artifact_role="planning_artifact_manifest",
            error_message_or_none=None,
            metrics={},
        )


def test_source_has_no_forbidden_execution_responsibility_markers():
    forbidden_markers = (
        "execute_" + "strategy",
        "generate_" + "signals",
        "materialize_" + "signals",
        "calculate_" + "ema",
        "run_" + "back" + "test",
        "execute_" + "back" + "test",
        "run_" + "research",
        "calculate_" + "pnl",
        "calculate_" + "metrics",
        "generate_" + "report",
        "write_" + "registry",
        "create_" + "promotion_" + "verdict",
        _external_actor(),
        _intent_marker(),
        _market_access() + "_execution",
        _scheduler_access() + "_execution",
        _storage_root_marker(),
        _freshness_marker(),
        _active_marker(),
        _implicit_marker(),
        _legacy_strategy_marker(),
    )
    source_text = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    for marker in forbidden_markers:
        assert marker not in source_text, marker


def test_source_does_not_import_legacy_strategy_or_data_infra():
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
