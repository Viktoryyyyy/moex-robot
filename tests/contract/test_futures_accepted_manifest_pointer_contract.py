import json

import pytest

from moex_data.futures import (
    FuturesAcceptedManifestError,
    read_accepted_manifest_pointer,
    write_accepted_manifest_pointer,
)


DATASET_ID = "futures_raw_5m"
INSTRUMENT_ID = "forts.test.si"
SOURCE_ID = "moex_algopack_fo_tradestats_snapshot.v1"
RUN_ID = "refresh_run.accepted_manifest.v1"
QUALITY_REF = "${MOEX_DATA_ROOT}/state/quality/run_date=2026-06-02/run_id=refresh_run.accepted_manifest.v1/quality_report.json"
MANIFEST_REF = "${MOEX_DATA_ROOT}/state/refresh/run_date=2026-06-02/run_id=refresh_run.accepted_manifest.v1/manifest.json"
POINTER_FILE = "_".join(("current", "accepted", "manifest")) + ".json"


def _accepted_ref(instrument_id=INSTRUMENT_ID):
    return (
        "${MOEX_DATA_ROOT}/state/datasets/dataset_id="
        + DATASET_ID
        + "/instrument_id="
        + instrument_id
        + "/"
        + POINTER_FILE
    )


def _manifest(status="succeeded", instrument_id=INSTRUMENT_ID):
    return {
        "run_id": RUN_ID,
        "run_date": "2026-06-02",
        "dataset_contract_refs": (
            "futures_raw_5m.v1",
            "futures_futoi_raw.v1",
            "futures_derived_d1.v1",
            "futures_derived_w1.v1",
            "futures_data_refresh_manifest.v1",
            "futures_quality_report.v1",
            "futures_continuous_5m.v1",
        ),
        "instrument_scope": (instrument_id,),
        "source_scope": (SOURCE_ID,),
        "partitions_written": (
            "${MOEX_DATA_ROOT}/market/raw/timeframe=5m/instrument_id="
            + instrument_id
            + "/trade_date=2026-06-02/source="
            + SOURCE_ID
            + "/part.parquet",
        ),
        "partitions_skipped": (),
        "quality_report_ref": QUALITY_REF,
        "accepted_manifest_ref": _accepted_ref(instrument_id),
        "refresh_status": status,
    }


def _quality_row(status="pass", instrument_id=INSTRUMENT_ID):
    return {
        "run_id": RUN_ID,
        "dataset_id": DATASET_ID,
        "instrument_id": instrument_id,
        "source_id": SOURCE_ID,
        "secid": "SiM6",
        "board": "RFUD",
        "market": "FORTS",
        "engine": "futures",
        "trade_date": "2026-06-02",
        "rows": 2,
        "duplicate_key_count": 0,
        "gap_count": 0,
        "null_ohlc_count": 0,
        "invalid_ohlc_count": 0,
        "futoi_missing_count": 0,
        "calendar_status": "not_checked",
        "quality_status": status,
    }


def test_accepted_manifest_pointer_writes_and_reads_explicit_dataset_instrument(tmp_path):
    env = {"MOEX_DATA_ROOT": str(tmp_path / "data")}

    written = write_accepted_manifest_pointer(
        env=env,
        dataset_id=DATASET_ID,
        instrument_id=INSTRUMENT_ID,
        manifest_ref=MANIFEST_REF,
        manifest_values=_manifest(),
        quality_rows=(_quality_row(),),
    )

    assert written.dataset_id == DATASET_ID
    assert written.instrument_id == INSTRUMENT_ID
    assert written.accepted_manifest_path == tmp_path / "data" / "state" / "datasets" / ("dataset_id=" + DATASET_ID) / ("instrument_id=" + INSTRUMENT_ID) / POINTER_FILE
    payload = json.loads(written.accepted_manifest_path.read_text(encoding="utf-8"))
    assert payload["manifest_ref"] == MANIFEST_REF
    assert payload["quality_status"] == "pass"

    read = read_accepted_manifest_pointer(
        env=env,
        dataset_id=DATASET_ID,
        instrument_id=INSTRUMENT_ID,
        accepted_manifest_ref=_accepted_ref(),
    )
    assert read == written


def test_accepted_manifest_pointer_rejects_non_pass_quality(tmp_path):
    with pytest.raises(FuturesAcceptedManifestError, match="quality pass"):
        write_accepted_manifest_pointer(
            env={"MOEX_DATA_ROOT": str(tmp_path / "data")},
            dataset_id=DATASET_ID,
            instrument_id=INSTRUMENT_ID,
            manifest_ref=MANIFEST_REF,
            manifest_values=_manifest(),
            quality_rows=(_quality_row("warn"),),
        )


def test_accepted_manifest_pointer_rejects_non_succeeded_manifest(tmp_path):
    with pytest.raises(FuturesAcceptedManifestError, match="succeeded refresh"):
        write_accepted_manifest_pointer(
            env={"MOEX_DATA_ROOT": str(tmp_path / "data")},
            dataset_id=DATASET_ID,
            instrument_id=INSTRUMENT_ID,
            manifest_ref=MANIFEST_REF,
            manifest_values=_manifest("partial"),
            quality_rows=(_quality_row(),),
        )


def test_accepted_manifest_pointer_rejects_scope_mismatch_without_scanning(tmp_path):
    env = {"MOEX_DATA_ROOT": str(tmp_path / "data")}
    other = "forts.test.other"
    with pytest.raises(FuturesAcceptedManifestError, match="instrument_scope"):
        write_accepted_manifest_pointer(
            env=env,
            dataset_id=DATASET_ID,
            instrument_id=INSTRUMENT_ID,
            manifest_ref=MANIFEST_REF,
            manifest_values=_manifest(instrument_id=other),
            quality_rows=(_quality_row(instrument_id=other),),
        )

    with pytest.raises(FuturesAcceptedManifestError, match="does not exist"):
        read_accepted_manifest_pointer(
            env=env,
            dataset_id=DATASET_ID,
            instrument_id=INSTRUMENT_ID,
            accepted_manifest_ref=_accepted_ref(),
        )
