import json
from dataclasses import dataclass

import pytest

from moex_data.futures import refresh_forts_raw_5m_incremental_pointer as refresh


@dataclass(frozen=True)
class FakeResult:
    payload: dict[str, object]


def write_registry(path):
    path.write_text(
        "registry_id: forts_instrument_registry.v1\n"
        "instruments:\n"
        "  - instrument_id: forts.usdrubf\n"
        "    secid: USDRUBF\n"
        "    enabled_for_raw_5m_materialization: true\n"
        "rules:\n"
        "  family_partition_key_allowed: false\n",
        encoding="utf-8",
    )


def write_base_manifest(path, *, last_valid="2026-06-09"):
    path.write_text(
        json.dumps(
            {
                "artifact_id": refresh.ARTIFACT_ID,
                "source_artifact_id": refresh.SOURCE_ARTIFACT_ID,
                "artifact_version": "base_v1",
                "instrument_id_scope": ["forts.usdrubf"],
                "secid_scope": ["USDRUBF"],
                "requested_data_start": "2020-01-01",
                "data_start": "2022-04-26",
                "data_end": last_valid,
                "last_valid_trade_date": last_valid,
                "row_count": 100,
                "partition_count": 5,
                "partition_hashes": {"/base/part.parquet": "hash-base"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def write_pointer(path, manifest_path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "artifact_id": refresh.POINTER_ARTIFACT_ID,
                "target_artifact_id": refresh.ARTIFACT_ID,
                "accepted_manifest_reference": manifest_path.as_posix(),
                "accepted_artifact_version": "base_v1",
                "quality_status": "passed",
                "instrument_id_scope": ["forts.usdrubf"],
                "secid_scope": ["USDRUBF"],
                "latest_autodetect_used": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def calendar(*dates):
    return [{"trade_date": item[0], "is_trading_day": item[1]} for item in dates]


def test_stable_pointer_path_is_used_as_base_manifest_source(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    accepted_base = tmp_path / "accepted_base.json"
    unrelated_latest = tmp_path / "unrelated_latest_manifest.json"
    write_registry(registry)
    write_base_manifest(accepted_base, last_valid="2026-06-09")
    write_base_manifest(unrelated_latest, last_valid="2026-06-10")
    pointer_path = refresh.build_accepted_manifest_pointer_path()
    write_pointer(pointer_path, accepted_base)
    calls = []

    def runner(**kwargs):
        calls.append(kwargs["trade_date"])
        return FakeResult(
            payload={
                "storage_partition_path": "/data/2026-06-10.parquet",
                "content_hash": "hash-new",
                "data_start": kwargs["trade_date"],
                "data_end": kwargs["trade_date"],
                "last_valid_trade_date": kwargs["trade_date"],
                "row_count": 1,
            }
        )

    summary = refresh.refresh_incremental(
        instrument_id="forts.usdrubf",
        secid="USDRUBF",
        artifact_version="refresh_pointer_v1",
        registry_path=registry,
        date_end="2026-06-10",
        calendar_rows=calendar(("2026-06-09", True), ("2026-06-10", True)),
        runner=runner,
    )

    assert calls == ["2026-06-10"]
    assert summary.payload["base_manifest_pointer_reference"] == pointer_path.as_posix()
    assert summary.payload["base_manifest_reference"] == accepted_base.as_posix()
    assert summary.payload["latest_autodetect_used"] is False
    assert unrelated_latest.as_posix() not in json.dumps(summary.payload, sort_keys=True)


def test_passed_quality_atomically_advances_pointer_to_new_manifest(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    accepted_base = tmp_path / "accepted_base.json"
    write_registry(registry)
    write_base_manifest(accepted_base, last_valid="2026-06-09")
    pointer_path = refresh.build_accepted_manifest_pointer_path()
    write_pointer(pointer_path, accepted_base)

    def runner(**kwargs):
        return FakeResult(
            payload={
                "storage_partition_path": "/data/2026-06-10.parquet",
                "content_hash": "hash-new",
                "data_start": kwargs["trade_date"],
                "data_end": kwargs["trade_date"],
                "last_valid_trade_date": kwargs["trade_date"],
                "row_count": 17,
            }
        )

    summary = refresh.refresh_incremental(
        instrument_id="forts.usdrubf",
        secid="USDRUBF",
        artifact_version="refresh_pointer_passed_v1",
        registry_path=registry,
        date_end="2026-06-10",
        calendar_rows=calendar(("2026-06-09", True), ("2026-06-10", True)),
        runner=runner,
    )

    pointer_payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert summary.payload["quality_status"] == "passed"
    assert summary.payload["accepted_manifest_pointer_updated"] is True
    assert pointer_payload["accepted_manifest_reference"] == summary.manifest_path.as_posix()
    assert pointer_payload["accepted_quality_report_reference"] == summary.quality_report_path.as_posix()
    assert pointer_payload["previous_accepted_manifest_reference"] == accepted_base.as_posix()
    assert pointer_payload["atomic_update_rule"] == "write_temp_file_in_pointer_directory_then_replace"


def test_pointer_no_op_date_end_equal_base_last_skips_calendar_and_runner(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    accepted_base = tmp_path / "accepted_base.json"
    write_registry(registry)
    write_base_manifest(accepted_base, last_valid="2026-06-09")
    pointer_path = refresh.build_accepted_manifest_pointer_path()
    write_pointer(pointer_path, accepted_base)

    def calendar_loader(*args, **kwargs):
        raise AssertionError("calendar loader must not be called for pointer no_op")

    def runner(**kwargs):
        raise AssertionError("runner must not be called for pointer no_op")

    summary = refresh.refresh_incremental(
        instrument_id="forts.usdrubf",
        secid="USDRUBF",
        artifact_version="refresh_pointer_noop_v1",
        registry_path=registry,
        date_end="2026-06-09",
        calendar_loader=calendar_loader,
        runner=runner,
    )

    pointer_payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert summary.payload["status"] == "no_op"
    assert summary.payload["quality_status"] == "passed"
    assert summary.payload["incremental_requested_dates"] == []
    assert summary.payload["incremental_requested_date_count"] == 0
    assert summary.payload["accepted_manifest_pointer_updated"] is True
    assert pointer_payload["accepted_manifest_reference"] == summary.manifest_path.as_posix()
    assert pointer_payload["quality_status"] == "passed"


def test_failed_quality_does_not_advance_pointer(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    accepted_base = tmp_path / "accepted_base.json"
    write_registry(registry)
    write_base_manifest(accepted_base, last_valid="2026-06-09")
    pointer_path = refresh.build_accepted_manifest_pointer_path()
    write_pointer(pointer_path, accepted_base)
    original_pointer_text = pointer_path.read_text(encoding="utf-8")

    def runner(**kwargs):
        raise RuntimeError("network down")

    summary = refresh.refresh_incremental(
        instrument_id="forts.usdrubf",
        secid="USDRUBF",
        artifact_version="refresh_pointer_failed_v1",
        registry_path=registry,
        date_end="2026-06-10",
        calendar_rows=calendar(("2026-06-09", True), ("2026-06-10", True)),
        runner=runner,
    )

    assert summary.payload["status"] == "failed"
    assert summary.payload["quality_status"] == "failed"
    assert summary.payload["accepted_manifest_pointer_updated"] is False
    assert pointer_path.read_text(encoding="utf-8") == original_pointer_text


def test_non_json_calendar_failure_is_classified_and_does_not_advance_pointer(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    accepted_base = tmp_path / "accepted_base.json"
    write_registry(registry)
    write_base_manifest(accepted_base, last_valid="2026-06-09")
    pointer_path = refresh.build_accepted_manifest_pointer_path()
    write_pointer(pointer_path, accepted_base)
    original_pointer_text = pointer_path.read_text(encoding="utf-8")

    def calendar_loader(*args, **kwargs):
        raise json.JSONDecodeError("Expecting value", "", 0)

    def runner(**kwargs):
        raise AssertionError("runner must not be called when calendar fetch fails")

    with pytest.raises(ValueError, match="calendar_fetch_non_json_response") as excinfo:
        refresh.refresh_incremental(
            instrument_id="forts.usdrubf",
            secid="USDRUBF",
            artifact_version="refresh_pointer_calendar_failure_v1",
            registry_path=registry,
            date_end="2026-06-10",
            calendar_loader=calendar_loader,
            runner=runner,
        )

    assert "JSONDecodeError" not in str(excinfo.value)
    assert pointer_path.read_text(encoding="utf-8") == original_pointer_text
