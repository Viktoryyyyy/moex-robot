from pathlib import Path

from moex_data.futures import materialize_forts_raw_5m_tradestats as target


def test_build_target_paths_uses_pr150_contract_pattern(tmp_path):
    paths = target.build_target_paths(
        trade_date="2026-06-02",
        family="Si",
        secid="SiM6",
        artifact_version="pilot_v1",
        env={"MOEX_DATA_ROOT": str(tmp_path)},
    )

    assert paths.partition_path == (
        tmp_path
        / "forts"
        / "raw_5m"
        / "tradestats"
        / "trade_date=2026-06-02"
        / "family=Si"
        / "secid=SiM6"
        / "part.parquet"
    )
    assert paths.manifest_path == (
        tmp_path
        / "manifests"
        / "artifact_id=dataset.forts.raw_5m.tradestats.v1"
        / "artifact_version=pilot_v1"
        / "manifest.json"
    )
    assert paths.quality_report_path == (
        tmp_path
        / "quality_reports"
        / "artifact_id=dataset.forts.raw_5m.tradestats.v1"
        / "artifact_version=pilot_v1"
        / "quality_report.json"
    )


def test_legacy_request_targets_new_artifact_and_source_contract():
    request = target.build_legacy_request(
        trade_date="2026-06-02",
        family="Si",
        secid="SiM6",
        artifact_version="pilot_v1",
    )

    assert request.dataset_id == "dataset.forts.raw_5m.tradestats.v1"
    assert request.contract_id == "dataset.forts.raw_5m.tradestats.v1"
    assert request.source_candidate == "external.apim.fo.tradestats.v1"
    assert request.source_endpoint == "/iss/datashop/algopack/fo/tradestats.json"
    assert request.trade_date == "2026-06-02"
    assert request.family == "Si"
    assert request.secid == "SiM6"


def test_manifest_and_quality_report_have_readiness_fields(tmp_path):
    paths = target.build_target_paths(
        trade_date="2026-06-02",
        family="Si",
        secid="SiM6",
        artifact_version="pilot_v1",
        env={"MOEX_DATA_ROOT": str(tmp_path)},
    )
    metrics = {
        "rows": 10,
        "data_start": "2026-06-02",
        "data_end": "2026-06-02",
        "last_valid_trade_date": "2026-06-02",
        "duplicate_key_count": 0,
        "gap_count": 0,
        "null_ohlc_count": 0,
        "invalid_ohlc_count": 0,
    }

    manifest = target._manifest(
        paths=paths,
        artifact_version="pilot_v1",
        metrics=metrics,
        content_hash="abc",
        family="Si",
        secid="SiM6",
        build_started_at="2026-06-10T00:00:00+00:00",
        build_finished_at="2026-06-10T00:00:01+00:00",
    )
    quality = target._quality_report("pilot_v1", metrics, "Si", "SiM6")

    assert manifest["artifact_id"] == "dataset.forts.raw_5m.tradestats.v1"
    assert manifest["source_artifact_id"] == "external.apim.fo.tradestats.v1"
    assert manifest["schema_version"] == "dataset.forts.raw_5m.tradestats.v1"
    assert manifest["path_contract_type"] == "external_pattern"
    assert manifest["data_start"] == "2026-06-02"
    assert manifest["data_end"] == "2026-06-02"
    assert manifest["last_valid_trade_date"] == "2026-06-02"
    assert manifest["row_count"] == 10
    assert manifest["calendar_contract"] == "moex_iss_futures_calendar"
    assert manifest["session_binding"] == "explicit_trade_date_session"
    assert manifest["quality_report_reference"] == paths.quality_report_path.as_posix()

    assert quality["artifact_id"] == "reports.data_asset.quality.v1"
    assert quality["target_artifact_id"] == "dataset.forts.raw_5m.tradestats.v1"
    assert quality["quality_status"] == "passed"
    assert quality["row_count"] == 10


def test_result_payload_declares_no_latest_or_hardcoded_path(tmp_path):
    result = target.MaterializationResult(
        partition_path=tmp_path / "part.parquet",
        manifest_path=tmp_path / "manifest.json",
        quality_report_path=tmp_path / "quality_report.json",
        row_count=10,
        quality_status="passed",
        data_start="2026-06-02",
        data_end="2026-06-02",
        last_valid_trade_date="2026-06-02",
        family="Si",
        secid="SiM6",
        content_hash="abc",
    )

    payload = target.result_payload(result)

    assert payload["latest_autodetect_used"] is False
    assert payload["hardcoded_server_path_used"] is False
    assert payload["artifact_id"] == "dataset.forts.raw_5m.tradestats.v1"
    assert payload["source_artifact_id"] == "external.apim.fo.tradestats.v1"
