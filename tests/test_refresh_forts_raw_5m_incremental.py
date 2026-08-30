import json
from dataclasses import dataclass

import pytest

from moex_data.futures import refresh_forts_raw_5m_incremental as refresh


@dataclass(frozen=True)
class FakeResult:
    payload: dict[str, object]


def write_registry(path, nested_alias=False):
    nested_instrument = "forts.alias" if nested_alias else "forts.usdrubf"
    nested_secid = "ALIAS" if nested_alias else "USDRUBF"
    path.write_text(
        "registry_id: forts_instrument_registry.v1\n"
        "instruments:\n"
        "  - instrument_id: forts.usdrubf\n"
        "    secid: USDRUBF\n"
        "    enabled_for_raw_5m_materialization: true\n"
        "    storage_partition_values:\n"
        "      instrument_id: " + nested_instrument + "\n"
        "      secid: " + nested_secid + "\n"
        "rules:\n"
        "  family_partition_key_allowed: false\n",
        encoding="utf-8",
    )


def write_base_manifest(path, *, instrument_id="forts.usdrubf", secid="USDRUBF", last_valid="2026-06-09"):
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
                "partition_hashes": {"/base/part.parquet": "hash-base"},
            }
        ),
        encoding="utf-8",
    )


def test_no_op_when_base_manifest_is_current(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    base_manifest = tmp_path / "base_manifest.json"
    write_registry(registry)
    write_base_manifest(base_manifest, last_valid="2026-06-09")

    def runner(**kwargs):
        raise AssertionError("runner must not be called for no_op")

    summary = refresh.refresh_incremental(
        instrument_id="forts.usdrubf",
        secid="USDRUBF",
        base_manifest=base_manifest,
        artifact_version="refresh_noop_v1",
        registry_path=registry,
        as_of_date="2026-06-10",
        observed_dates=["2026-06-09"],
        runner=runner,
    )

    assert summary.payload["status"] == "no_op"
    assert summary.payload["quality_status"] == "passed"
    assert summary.payload["incremental_requested_dates"] == []
    assert summary.payload["last_completed_valid_trading_day"] == "2026-06-09"
    assert summary.payload["date_source_id"] == refresh.OBSERVED_DATE_SOURCE_ID
    assert summary.payload["date_selection_rule"] == "observed_trade_dates_only"
    assert summary.manifest_path.exists()
    assert summary.quality_report_path.exists()


def test_one_missing_observed_trading_day_refresh(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    base_manifest = tmp_path / "base_manifest.json"
    write_registry(registry)
    write_base_manifest(base_manifest, last_valid="2026-06-09")
    calls = []

    def runner(**kwargs):
        calls.append(kwargs)
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
        base_manifest=base_manifest,
        artifact_version="refresh_one_day_v1",
        registry_path=registry,
        as_of_date="2026-06-11",
        observed_dates=["2026-06-09", "2026-06-10"],
        runner=runner,
    )

    assert [call["trade_date"] for call in calls] == ["2026-06-10"]
    assert summary.payload["status"] == "succeeded"
    assert summary.payload["quality_status"] == "passed"
    assert summary.payload["incremental_start"] == "2026-06-10"
    assert summary.payload["row_count"] == 117
    assert summary.payload["partition_count"] == 6
    assert summary.payload["data_end"] == "2026-06-10"
    assert summary.payload["last_valid_trade_date"] == "2026-06-10"


def test_registry_mismatch_rejection_uses_top_level_identity(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    base_manifest = tmp_path / "base_manifest.json"
    write_registry(registry, nested_alias=True)
    write_base_manifest(base_manifest, instrument_id="forts.alias", secid="ALIAS", last_valid="2026-06-09")

    with pytest.raises(ValueError, match="instrument is not enabled by registry"):
        refresh.refresh_incremental(
            instrument_id="forts.alias",
            secid="ALIAS",
            base_manifest=base_manifest,
            artifact_version="refresh_reject_v1",
            registry_path=registry,
            date_end="2026-06-10",
            observed_dates=["2026-06-09", "2026-06-10"],
        )


def test_explicit_base_manifest_used_no_autodetect(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    explicit_base = tmp_path / "explicit_base.json"
    unrelated_manifest = tmp_path / "unrelated_latest_manifest.json"
    write_registry(registry)
    write_base_manifest(explicit_base, last_valid="2026-06-09")
    write_base_manifest(unrelated_manifest, last_valid="2026-06-10")
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
        base_manifest=explicit_base,
        artifact_version="refresh_explicit_v1",
        registry_path=registry,
        date_end="2026-06-10",
        observed_dates=["2026-06-09", "2026-06-10"],
        runner=runner,
    )

    assert calls == ["2026-06-10"]
    assert summary.payload["base_manifest_reference"] == explicit_base.as_posix()
    assert summary.payload["latest_autodetect_used"] is False


def test_failed_date_keeps_quality_status_failed(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    base_manifest = tmp_path / "base_manifest.json"
    write_registry(registry)
    write_base_manifest(base_manifest, last_valid="2026-06-09")

    def runner(**kwargs):
        raise RuntimeError("network down")

    summary = refresh.refresh_incremental(
        instrument_id="forts.usdrubf",
        secid="USDRUBF",
        base_manifest=base_manifest,
        artifact_version="refresh_failed_v1",
        registry_path=registry,
        date_end="2026-06-10",
        observed_dates=["2026-06-09", "2026-06-10"],
        runner=runner,
    )

    quality = json.loads(summary.quality_report_path.read_text(encoding="utf-8"))
    assert summary.payload["status"] == "failed"
    assert summary.payload["quality_status"] == "failed"
    assert summary.payload["failed_dates"] == [{"trade_date": "2026-06-10", "error": "network down"}]
    assert quality["quality_status"] == "failed"
    assert quality["failed_dates_count"] == 1


def test_weekend_is_not_fabricated_from_date_range(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    base_manifest = tmp_path / "base_manifest.json"
    write_registry(registry)
    write_base_manifest(base_manifest, last_valid="2026-06-12")

    summary = refresh.refresh_incremental(
        instrument_id="forts.usdrubf",
        secid="USDRUBF",
        base_manifest=base_manifest,
        artifact_version="refresh_weekend_v1",
        registry_path=registry,
        date_end="2026-06-14",
        observed_dates=["2026-06-12"],
        runner=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("weekend must not be requested")),
    )

    assert summary.payload["status"] == "no_op"
    assert summary.payload["incremental_requested_dates"] == []
    assert summary.payload["last_completed_valid_trading_day"] == "2026-06-12"


def test_observed_dates_are_the_only_refresh_candidates(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = tmp_path / "registry.yaml"
    base_manifest = tmp_path / "base_manifest.json"
    write_registry(registry)
    write_base_manifest(base_manifest, last_valid="2026-06-12")
    calls = []

    def runner(**kwargs):
        calls.append(kwargs["trade_date"])
        return FakeResult(
            payload={
                "storage_partition_path": "/data/" + kwargs["trade_date"] + ".parquet",
                "content_hash": "hash-" + kwargs["trade_date"],
                "data_start": kwargs["trade_date"],
                "data_end": kwargs["trade_date"],
                "last_valid_trade_date": kwargs["trade_date"],
                "row_count": 1,
            }
        )

    summary = refresh.refresh_incremental(
        instrument_id="forts.usdrubf",
        secid="USDRUBF",
        base_manifest=base_manifest,
        artifact_version="refresh_observed_v1",
        registry_path=registry,
        date_end="2026-06-17",
        observed_dates=["2026-06-12", "2026-06-15", "2026-06-17"],
        runner=runner,
    )

    assert calls == ["2026-06-15", "2026-06-17"]
    assert summary.payload["incremental_requested_dates"] == ["2026-06-15", "2026-06-17"]
    assert "2026-06-13" not in calls
    assert "2026-06-14" not in calls
    assert "2026-06-16" not in calls
