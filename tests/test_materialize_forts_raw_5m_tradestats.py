from moex_data.futures import materialize_forts_raw_5m_instrument as target


def test_target_paths_use_instrument_id_partition(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))

    paths = target.target_paths(
        trade_date="2026-06-02",
        instrument_id="forts.usdrubf",
        secid="USDRUBF",
        artifact_version="pilot_v1",
    )

    assert paths.partition_path == (
        tmp_path
        / "market"
        / "raw"
        / "timeframe=5m"
        / "instrument_id=forts.usdrubf"
        / "trade_date=2026-06-02"
        / "source=moex_algopack_fo_tradestats_5m"
        / "part.parquet"
    )
    assert "family=" not in paths.partition_path.as_posix()
    assert "secid=" not in paths.partition_path.as_posix()


def test_result_payload_has_instrument_scope_and_no_latest_flags(tmp_path):
    payload = {
        "status": "succeeded",
        "artifact_id": "dataset.forts.raw_5m.tradestats.v1",
        "source_artifact_id": "external.apim.fo.tradestats.v1",
        "storage_partition_path": (tmp_path / "part.parquet").as_posix(),
        "manifest_reference": (tmp_path / "manifest.json").as_posix(),
        "quality_report_reference": (tmp_path / "quality_report.json").as_posix(),
        "quality_status": "passed",
        "data_start": "2026-06-02",
        "data_end": "2026-06-02",
        "last_valid_trade_date": "2026-06-02",
        "row_count": 10,
        "instrument_id_scope": ["forts.usdrubf"],
        "secid_scope": ["USDRUBF"],
        "schema_version": "dataset.forts.raw_5m.tradestats.v1",
        "calendar_session_binding": "moex_iss_futures_calendar/explicit_trade_date_session",
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "content_hash": "abc",
    }

    assert payload["instrument_id_scope"] == ["forts.usdrubf"]
    assert "family_scope" not in payload
    assert payload["latest_autodetect_used"] is False
    assert payload["hardcoded_server_path_used"] is False
