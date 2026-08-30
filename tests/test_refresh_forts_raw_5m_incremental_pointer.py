import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from moex_data.futures import refresh_forts_raw_5m_incremental_pointer as refresh


USDRUBF_INSTRUMENT_ID = "forts.usdrubf"
USDRUBF_SECID = "USDRUBF"
SBERF_INSTRUMENT_ID = "forts.sberf"
SBERF_SECID = "SBERF"


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
        "  - instrument_id: forts.sberf\n"
        "    secid: SBERF\n"
        "    enabled_for_raw_5m_materialization: true\n"
        "rules:\n"
        "  family_partition_key_allowed: false\n",
        encoding="utf-8",
    )


def write_base_manifest(
    path,
    *,
    instrument_id=USDRUBF_INSTRUMENT_ID,
    secid=USDRUBF_SECID,
    last_valid="2026-06-09",
):
    path.write_text(
        json.dumps(
            {
                "artifact_id": refresh.ARTIFACT_ID,
                "source_artifact_id": refresh.SOURCE_ARTIFACT_ID,
                "artifact_version": "base_v1",
                "instrument_id_scope": [instrument_id],
                "secid_scope": [secid],
                "requested_data_start": "2020-01-01",
                "data_start": "2022-04-26",
                "data_end": last_valid,
                "last_valid_trade_date": last_valid,
                "row_count": 100,
                "partition_count": 5,
                "partition_hashes": {"/base/" + secid + "/part.parquet": "hash-base"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def write_pointer(path, manifest_path, *, instrument_id=USDRUBF_INSTRUMENT_ID, secid=USDRUBF_SECID):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "artifact_id": refresh.POINTER_ARTIFACT_ID,
                "target_artifact_id": refresh.ARTIFACT_ID,
                "accepted_manifest_reference": manifest_path.as_posix(),
                "accepted_artifact_version": "base_v1",
                "quality_status": "passed",
                "instrument_id_scope": [instrument_id],
                "secid_scope": [secid],
                "latest_autodetect_used": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def result_for_trade_date(trade_date, row_count=1):
    return FakeResult(
        payload={
            "storage_partition_path": "/data/" + trade_date + ".parquet",
            "content_hash": "hash-" + trade_date,
            "data_start": trade_date,
            "data_end": trade_date,
            "last_valid_trade_date": trade_date,
            "row_count": row_count,
        }
    )


def prepare_usdrubf(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    accepted_base = tmp_path / "accepted_base.json"
    write_registry(registry)
    write_base_manifest(accepted_base)
    pointer_path = refresh.build_accepted_manifest_pointer_path(USDRUBF_INSTRUMENT_ID, USDRUBF_SECID)
    write_pointer(pointer_path, accepted_base)
    return registry, accepted_base, pointer_path


def test_per_instrument_pointer_path_contains_instrument_and_secid(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))

    pointer_path = refresh.build_accepted_manifest_pointer_path(SBERF_INSTRUMENT_ID, SBERF_SECID)

    assert pointer_path.as_posix().endswith(
        "/state/datasets/artifact_id=dataset.forts.raw_5m.tradestats.v1/"
        "instrument_id=forts.sberf/secid=SBERF/current_accepted_manifest.json"
    )


def test_missing_stable_pointer_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    write_registry(registry)

    with pytest.raises(ValueError, match="accepted_manifest_pointer does not exist"):
        refresh.refresh_incremental(
            instrument_id=SBERF_INSTRUMENT_ID,
            secid=SBERF_SECID,
            artifact_version="missing_pointer_v1",
            registry_path=registry,
            date_end="2026-06-10",
            observed_dates=["2026-06-09", "2026-06-10"],
            runner=lambda **kwargs: result_for_trade_date(kwargs["trade_date"]),
        )


def test_stable_pointer_is_base_and_only_observed_new_dates_are_materialized(tmp_path, monkeypatch):
    registry, accepted_base, pointer_path = prepare_usdrubf(tmp_path, monkeypatch)
    calls = []

    def runner(**kwargs):
        calls.append(kwargs["trade_date"])
        return result_for_trade_date(kwargs["trade_date"])

    summary = refresh.refresh_incremental(
        instrument_id=USDRUBF_INSTRUMENT_ID,
        secid=USDRUBF_SECID,
        artifact_version="observed_dates_v1",
        registry_path=registry,
        date_end="2026-06-12",
        observed_dates=["2026-06-09", "2026-06-10", "2026-06-12"],
        runner=runner,
    )

    assert calls == ["2026-06-10", "2026-06-12"]
    assert summary.payload["incremental_requested_dates"] == ["2026-06-10", "2026-06-12"]
    assert summary.payload["base_manifest_pointer_reference"] == pointer_path.as_posix()
    assert summary.payload["base_manifest_reference"] == accepted_base.as_posix()
    assert summary.payload["latest_autodetect_used"] is False


def test_authoritative_source_loader_is_used_when_dates_not_injected(tmp_path, monkeypatch):
    registry, _, _ = prepare_usdrubf(tmp_path, monkeypatch)
    source_calls = []
    materialized = []

    def source_loader(date_start, date_end, *, secid, timeout, apim_base_url):
        source_calls.append((date_start, date_end, secid, timeout, apim_base_url))
        return ["2026-06-09", "2026-06-11"]

    def runner(**kwargs):
        materialized.append(kwargs["trade_date"])
        return result_for_trade_date(kwargs["trade_date"])

    refresh.refresh_incremental(
        instrument_id=USDRUBF_INSTRUMENT_ID,
        secid=USDRUBF_SECID,
        artifact_version="source_loader_v1",
        registry_path=registry,
        date_end="2026-06-11",
        timeout=7.0,
        apim_base_url="https://apim.example",
        source_date_loader=source_loader,
        runner=runner,
    )

    assert source_calls == [("2026-06-09", "2026-06-11", USDRUBF_SECID, 7.0, "https://apim.example")]
    assert materialized == ["2026-06-11"]


def test_source_loader_failure_fails_closed_without_pointer_mutation(tmp_path, monkeypatch):
    registry, _, pointer_path = prepare_usdrubf(tmp_path, monkeypatch)
    before = pointer_path.read_text(encoding="utf-8")

    def source_loader(*args, **kwargs):
        raise ValueError("authoritative TradeStats unavailable")

    with pytest.raises(ValueError, match="authoritative TradeStats unavailable"):
        refresh.refresh_incremental(
            instrument_id=USDRUBF_INSTRUMENT_ID,
            secid=USDRUBF_SECID,
            artifact_version="source_failure_v1",
            registry_path=registry,
            date_end="2026-06-10",
            source_date_loader=source_loader,
            runner=lambda **kwargs: result_for_trade_date(kwargs["trade_date"]),
        )

    assert pointer_path.read_text(encoding="utf-8") == before


def test_observed_date_outside_requested_range_fails_without_pointer_mutation(tmp_path, monkeypatch):
    registry, _, pointer_path = prepare_usdrubf(tmp_path, monkeypatch)
    before = pointer_path.read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="observed trade date escaped requested source range"):
        refresh.refresh_incremental(
            instrument_id=USDRUBF_INSTRUMENT_ID,
            secid=USDRUBF_SECID,
            artifact_version="bad_observed_date_v1",
            registry_path=registry,
            date_end="2026-06-10",
            observed_dates=["2026-06-09", "2026-06-11"],
            runner=lambda **kwargs: result_for_trade_date(kwargs["trade_date"]),
        )

    assert pointer_path.read_text(encoding="utf-8") == before


def test_passed_quality_atomically_advances_pointer_with_observed_date_metadata(tmp_path, monkeypatch):
    registry, accepted_base, pointer_path = prepare_usdrubf(tmp_path, monkeypatch)

    summary = refresh.refresh_incremental(
        instrument_id=USDRUBF_INSTRUMENT_ID,
        secid=USDRUBF_SECID,
        artifact_version="pointer_passed_v1",
        registry_path=registry,
        date_end="2026-06-10",
        observed_dates=["2026-06-09", "2026-06-10"],
        runner=lambda **kwargs: result_for_trade_date(kwargs["trade_date"], row_count=17),
    )

    pointer_payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert summary.payload["quality_status"] == "passed"
    assert summary.payload["accepted_manifest_pointer_updated"] is True
    assert pointer_payload["accepted_manifest_reference"] == summary.manifest_path.as_posix()
    assert pointer_payload["accepted_quality_report_reference"] == summary.quality_report_path.as_posix()
    assert pointer_payload["previous_accepted_manifest_reference"] == accepted_base.as_posix()
    assert pointer_payload["date_source_artifact_id"] == refresh.SOURCE_ARTIFACT_ID
    assert pointer_payload["date_source_id"] == refresh.base_refresh.OBSERVED_DATE_SOURCE_ID
    assert pointer_payload["date_source_endpoint"] == refresh.base_refresh.OBSERVED_DATE_SOURCE_ENDPOINT
    assert pointer_payload["date_selection_rule"] == "observed_trade_dates_only"
    assert pointer_payload["session_binding"] == "explicit_trade_date_session"
    assert pointer_payload["atomic_update_rule"] == "write_temp_file_in_pointer_directory_then_replace"
    assert pointer_payload["pointer_scope_strategy"] == "per_instrument"
    assert pointer_payload["latest_autodetect_used"] is False


def test_failed_partition_quality_does_not_advance_pointer(tmp_path, monkeypatch):
    registry, _, pointer_path = prepare_usdrubf(tmp_path, monkeypatch)
    before = pointer_path.read_text(encoding="utf-8")

    def runner(**kwargs):
        raise RuntimeError("partition failure")

    summary = refresh.refresh_incremental(
        instrument_id=USDRUBF_INSTRUMENT_ID,
        secid=USDRUBF_SECID,
        artifact_version="pointer_failed_quality_v1",
        registry_path=registry,
        date_end="2026-06-10",
        observed_dates=["2026-06-09", "2026-06-10"],
        runner=runner,
    )

    assert summary.payload["status"] == "failed"
    assert summary.payload["quality_status"] == "failed"
    assert summary.payload["accepted_manifest_pointer_updated"] is False
    assert pointer_path.read_text(encoding="utf-8") == before


def test_atomic_pointer_write_failure_leaves_previous_pointer_unchanged(tmp_path, monkeypatch):
    registry, _, pointer_path = prepare_usdrubf(tmp_path, monkeypatch)
    before = pointer_path.read_text(encoding="utf-8")
    original_write = refresh.base_refresh._write_json_atomic

    def flaky_write(path, values):
        if Path(path) == pointer_path:
            raise OSError("pointer replace failed")
        return original_write(path, values)

    monkeypatch.setattr(refresh.base_refresh, "_write_json_atomic", flaky_write)

    with pytest.raises(OSError, match="pointer replace failed"):
        refresh.refresh_incremental(
            instrument_id=USDRUBF_INSTRUMENT_ID,
            secid=USDRUBF_SECID,
            artifact_version="atomic_failure_v1",
            registry_path=registry,
            date_end="2026-06-10",
            observed_dates=["2026-06-09", "2026-06-10"],
            runner=lambda **kwargs: result_for_trade_date(kwargs["trade_date"]),
        )

    assert pointer_path.read_text(encoding="utf-8") == before


def test_no_new_observed_date_is_no_op_and_does_not_run_partition(tmp_path, monkeypatch):
    registry, _, pointer_path = prepare_usdrubf(tmp_path, monkeypatch)

    def runner(**kwargs):
        raise AssertionError("partition runner must not be called when no new observed date exists")

    summary = refresh.refresh_incremental(
        instrument_id=USDRUBF_INSTRUMENT_ID,
        secid=USDRUBF_SECID,
        artifact_version="no_new_date_v1",
        registry_path=registry,
        date_end="2026-06-09",
        observed_dates=["2026-06-09"],
        runner=runner,
    )

    pointer_payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert summary.payload["status"] == "no_op"
    assert summary.payload["quality_status"] == "passed"
    assert summary.payload["incremental_requested_dates"] == []
    assert summary.payload["accepted_manifest_pointer_updated"] is True
    assert pointer_payload["last_valid_trade_date"] == "2026-06-09"


def test_usdrubf_legacy_pointer_fallback_is_preserved(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    accepted_base = tmp_path / "accepted_base.json"
    write_registry(registry)
    write_base_manifest(accepted_base)
    legacy_pointer = refresh.build_legacy_usdrubf_accepted_manifest_pointer_path()
    per_instrument_pointer = refresh.build_accepted_manifest_pointer_path(USDRUBF_INSTRUMENT_ID, USDRUBF_SECID)
    write_pointer(legacy_pointer, accepted_base)
    assert not per_instrument_pointer.exists()

    summary = refresh.refresh_incremental(
        instrument_id=USDRUBF_INSTRUMENT_ID,
        secid=USDRUBF_SECID,
        artifact_version="legacy_pointer_v1",
        registry_path=registry,
        date_end="2026-06-10",
        observed_dates=["2026-06-09", "2026-06-10"],
        runner=lambda **kwargs: result_for_trade_date(kwargs["trade_date"]),
    )

    pointer_payload = json.loads(legacy_pointer.read_text(encoding="utf-8"))
    assert summary.accepted_manifest_pointer_path == legacy_pointer
    assert pointer_payload["legacy_artifact_level_pointer_compatibility_used"] is True
    assert not per_instrument_pointer.exists()


def test_new_instrument_prefers_per_instrument_pointer_and_never_mutates_legacy(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    sberf_base = tmp_path / "sberf_base.json"
    usdrubf_base = tmp_path / "usdrubf_base.json"
    write_registry(registry)
    write_base_manifest(sberf_base, instrument_id=SBERF_INSTRUMENT_ID, secid=SBERF_SECID)
    write_base_manifest(usdrubf_base)
    sberf_pointer = refresh.build_accepted_manifest_pointer_path(SBERF_INSTRUMENT_ID, SBERF_SECID)
    legacy_pointer = refresh.build_legacy_usdrubf_accepted_manifest_pointer_path()
    write_pointer(sberf_pointer, sberf_base, instrument_id=SBERF_INSTRUMENT_ID, secid=SBERF_SECID)
    write_pointer(legacy_pointer, usdrubf_base)
    legacy_before = legacy_pointer.read_text(encoding="utf-8")

    summary = refresh.refresh_incremental(
        instrument_id=SBERF_INSTRUMENT_ID,
        secid=SBERF_SECID,
        artifact_version="sberf_pointer_v1",
        registry_path=registry,
        date_end="2026-06-10",
        observed_dates=["2026-06-09", "2026-06-10"],
        runner=lambda **kwargs: result_for_trade_date(kwargs["trade_date"], row_count=23),
    )

    pointer_payload = json.loads(sberf_pointer.read_text(encoding="utf-8"))
    assert summary.accepted_manifest_pointer_path == sberf_pointer
    assert pointer_payload["instrument_id_scope"] == [SBERF_INSTRUMENT_ID]
    assert pointer_payload["secid_scope"] == [SBERF_SECID]
    assert pointer_payload["legacy_artifact_level_pointer_compatibility_used"] is False
    assert legacy_pointer.read_text(encoding="utf-8") == legacy_before
