from dataclasses import dataclass
from pathlib import Path

from moex_data.futures import backfill_forts_raw_5m_instrument as backfill


@dataclass(frozen=True)
class FakeResult:
    payload: dict[str, object]


def test_date_range_is_inclusive_and_limited():
    assert backfill._date_range("2026-06-01", "2026-06-03") == [
        "2026-06-01",
        "2026-06-02",
        "2026-06-03",
    ]
    assert backfill._date_range("2026-06-01", "2026-06-05", max_dates=2) == [
        "2026-06-01",
        "2026-06-02",
    ]


def test_registry_allows_only_explicit_instrument(tmp_path):
    registry = tmp_path / "registry.yaml"
    registry.write_text(
        "instrument_id: forts.usdrubf\n"
        "secid: USDRUBF\n"
        "enabled_for_raw_5m_materialization: true\n"
        "family_partition_key_allowed: false\n",
        encoding="utf-8",
    )

    assert backfill.registry_allows_instrument(registry, "forts.usdrubf", "USDRUBF") is True
    assert backfill.registry_allows_instrument(registry, "forts.si", "SiM6") is False


def test_backfill_writes_rollup_manifest_and_quality(tmp_path, monkeypatch):
    registry = tmp_path / "registry.yaml"
    registry.write_text(
        "instrument_id: forts.usdrubf\n"
        "secid: USDRUBF\n"
        "enabled_for_raw_5m_materialization: true\n"
        "family_partition_key_allowed: false\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))

    def fake_runner(*, trade_date, instrument_id, secid, artifact_version, timeout, apim_base_url):
        if trade_date == "2026-06-01":
            raise ValueError("APIM tradestats response contains no rows for requested secid/date")
        return FakeResult(
            payload={
                "storage_partition_path": "/data/part.parquet",
                "content_hash": "abc",
                "data_start": trade_date,
                "data_end": trade_date,
                "last_valid_trade_date": trade_date,
                "row_count": 179,
            }
        )

    summary = backfill.backfill_range(
        date_start="2026-06-01",
        date_end="2026-06-02",
        instrument_id="forts.usdrubf",
        secid="USDRUBF",
        artifact_version="test_run_v1",
        registry_path=registry,
        runner=fake_runner,
    )

    assert summary.payload["status"] == "succeeded"
    assert summary.payload["quality_status"] == "passed"
    assert summary.payload["instrument_id_scope"] == ["forts.usdrubf"]
    assert summary.payload["secid_scope"] == ["USDRUBF"]
    assert summary.payload["row_count"] == 179
    assert summary.payload["partition_count"] == 1
    assert summary.payload["skipped_empty_source_dates"] == ["2026-06-01"]
    assert summary.payload["latest_autodetect_used"] is False
    assert summary.payload["hardcoded_server_path_used"] is False
    assert "family_scope" not in summary.payload
    assert Path(summary.payload["manifest_reference"]).exists()
    assert Path(summary.payload["quality_report_reference"]).exists()
