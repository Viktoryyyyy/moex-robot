"""Repository-wide compatibility boundaries for issue #547.

This is a source-only inventory guard, not a production data probe.
New identity-sensitive consumers require explicit review before a live schema
change may be treated as complete. Historical v1 defaults remain protected.
"""
from pathlib import Path
import re

from moex_data.futures import materialize_futoi_instrument as materializer


# These concrete consumers were inspected in the repository before this guard
# was introduced. A match outside this set is a review requirement, not a
# reason to silently broaden v2 admission or to alter historical evidence.
REVIEWED_IDENTITY_CONSUMERS = frozenset({
    # The accepted-pointer legacy implementation defines its own helper.
    "src/moex_data/futures/futoi_live_factual_refresh.py",
    # This caller reads historical v1 partitions; its default API stays v1.
    "src/moex_data/rub_cr_futoi_observed_statistics.py",
    "src/moex_data/futures/materialize_futoi_instrument.py",
    "src/moex_data/futures/futoi_live_factual_refresh_source_native.py",
    "src/moex_data/futures/futoi_intraday_previous_session_context.py",
    "src/moex_data/futures/futoi_intraday_previous_session_context_fast.py",
    "src/moex_data/futures/futoi_delta_statistics_context.py",
    "src/moex_data/futures/futoi_publication_audit.py",
    "src/moex_data/rub_si_futoi_dated_context.py",
    "src/moex_data/rub_cr_futoi_dated_context.py",
    "src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot_current_context.py",
    "src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot_futoi.py",
})
IDENTITY_SYMBOL = re.compile(r"\b(?:source_identity|latest_aligned_factual|_materialize_target)\s*\(")


def test_futoi_identity_consumer_inventory_is_explicit():
    root = Path(__file__).resolve().parents[1]
    assert (root / "src").is_dir(), "complete repository source checkout is required"
    unreviewed = {}
    for path in sorted((root / "src").rglob("*")):
        if not path.is_file() or path.suffix not in {".py", ".inc"}:
            continue
        text = path.read_text(encoding="utf-8")
        if "futoi" not in text.lower():
            continue
        matches = [f"{number}: {line.strip()}"
                   for number, line in enumerate(text.splitlines(), 1)
                   if IDENTITY_SYMBOL.search(line)]
        relative = path.relative_to(root).as_posix()
        if matches and relative not in REVIEWED_IDENTITY_CONSUMERS:
            unreviewed[relative] = matches
    assert not unreviewed, (
        "Unreviewed FUTOI identity-sensitive consumers; inspect before changing live schemas:\n"
        + "\n".join(path + "\n  " + "\n  ".join(lines)
                    for path, lines in unreviewed.items())
    )


def test_historical_raw_default_key_and_paths_remain_v1(monkeypatch, tmp_path):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    assert materializer.SOURCE_RECORD_KEY_FIELDS == (
        "trade_date", "sess_id", "seqnum", "secid", "clgroup",
    )
    assert materializer.RAW_CONTRACT_REF == "contracts/datasets/futures_futoi_raw.v1.yaml"
    assert materializer.SOURCE_CONTRACT_REF == "contracts/sources/futures/moex_algopack_futoi.v1.yaml"
    expected = (tmp_path / "market" / "supplementary"
                / "dataset_id=futures_futoi_raw" / "instrument_id=si_futures_family"
                / "trade_date=2026-09-17" / "source=moex_algopack_futoi" / "part.parquet")
    assert materializer._partition_path("2026-09-17", "si_futures_family", "moex_algopack_futoi") == expected
    assert materializer._quality_path("2026-09-17", "legacy") == (
        tmp_path / "state" / "quality" / "dataset_id=futures_futoi_raw"
        / "run_date=2026-09-17" / "run_id=legacy" / "quality_report.json")
    assert materializer._manifest_path("2026-09-17", "legacy") == (
        tmp_path / "state" / "refresh" / "dataset_id=futures_futoi_raw"
        / "run_date=2026-09-17" / "run_id=legacy" / "manifest.json")


# The runtime call graph must also be explicit: source identity alone does not
# discover a caller which selects a refresh implementation by importing it.
REVIEWED_REFRESH_IMPORTERS = REVIEWED_IDENTITY_CONSUMERS | {
    "src/moex_data/step10_rub_refresh_entrypoint.py",
    # Direct source import: _data_root only. Keep historical loaders at v1
    # until their own caller explicitly selects the new version.
    "src/moex_data/rub_contract_price_market_oi_observed.py",
    "src/moex_data/rub_historical_basis_carry_context.py",
    "src/moex_data/rub_si_futoi_observed_statistics.py",
    # Actual fast-refresh service entrypoint, covered by the scoped wiring
    # amendment. An opt-in raw producer alone does not switch this runtime.
    "src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot_live_market_oi.py",
}


def test_futoi_refresh_importer_inventory_is_explicit():
    import ast

    root = Path(__file__).resolve().parents[1]
    tracked = {
        "futoi_live_factual_refresh_source_native",
        "futoi_intraday_previous_session_context",
        "futoi_intraday_previous_session_context_fast",
    }
    unreviewed = {}
    for path in sorted((root / "src").rglob("*.py")):
        relative = path.relative_to(root).as_posix()
        if relative in REVIEWED_REFRESH_IMPORTERS:
            continue
        text = path.read_text(encoding="utf-8")
        if not any(name in text for name in tracked):
            continue
        references = []
        for node in ast.walk(ast.parse(text)):
            if isinstance(node, ast.ImportFrom):
                names = [node.module or ""] + [item.name for item in node.names]
            elif isinstance(node, ast.Import):
                names = [item.name for item in node.names]
            else:
                continue
            if any(name.rsplit(".", 1)[-1] in tracked for name in names):
                references.append(f"{node.lineno}: {ast.get_source_segment(text, node)}")
        if references:
            unreviewed[relative] = references
    assert not unreviewed, (
        "Unreviewed FUTOI refresh importers; reconcile runtime version selection:\n"
        + "\n".join(path + "\n  " + "\n  ".join(lines) for path, lines in unreviewed.items())
    )


# Synthetic/reconstructed source rows. These are not captured HTTP responses.
import inspect
import json
import hashlib

import numpy as np
import pandas as pd
import pytest


def _synthetic_root_pair(ticker="Si", seqnum=2**53 + 1):
    shared = {
        "sess_id": 7655, "seqnum": seqnum, "tradedate": "2026-09-24",
        "tradetime": "10:35:00", "ticker": ticker,
        "systime": "2026-09-24 10:35:10",
        "trade_session_date": "2026-09-24",
        "pos_long_num": 1, "pos_short_num": 1,
    }
    return pd.DataFrame([
        dict(shared, clgroup="FIZ", pos=10, pos_long=20, pos_short=-10),
        dict(shared, clgroup="YUR", pos=-10, pos_long=30, pos_short=-40),
    ])


def _root_normalize(frame, **overrides):
    arguments = dict(trade_date="2026-09-24", instrument_id="si_futures_family",
                     ticker="si", received_at="2026-09-24T07:35:11+00:00",
                     ingest_at="2026-09-24T07:35:12+00:00")
    arguments.update(overrides)
    return materializer._normalize_root_source(frame, **arguments)


def _root_binding(instrument="si_futures_family"):
    ticker, secid = ("si", "SiU6") if instrument == "si_futures_family" else ("cr", "CRU6")
    return {
        "instrument_id": instrument, "canonical_symbol": ticker.upper(),
        "secid": secid, "board": "RFUD", "market": "forts", "engine": "futures",
        "futoi.source_id": materializer.SOURCE_ID, "futoi.ticker": ticker,
        "futoi.availability_status": "available", "futoi.probe_status": "completed",
        "futoi.enabled_for_materialization": False,
    }


def test_v2_opt_in_does_not_change_public_default():
    params = inspect.signature(materializer.materialize_futoi_partition).parameters
    assert params["raw_schema_version"].default == "v1"


@pytest.mark.parametrize("instrument,ticker", [("si_futures_family", "Si"), ("cr_futures_family", "CR")])
def test_v2_raw_is_root_scoped_and_preserves_exact_source_records(instrument, ticker):
    result, dropped = _root_normalize(
        _synthetic_root_pair(ticker), instrument_id=instrument, ticker=ticker.lower())
    assert dropped == 0
    assert "secid" not in result.columns
    assert set(result["source_ticker"]) == {ticker.lower()}
    assert set(result["instrument_id"]) == {instrument}
    assert set(result["raw_schema_version"]) == {"v2"}
    assert set(result["source_identity_scope"]) == {"source_ticker_root"}
    assert result["seqnum"].tolist() == [2**53 + 1, 2**53 + 1]
    assert str(result["seqnum"].dtype) == "int64"
    assert set(result["ts"]) == {pd.Timestamp("2026-09-24 10:35:00")}
    assert materializer.ROOT_SOURCE_RECORD_KEY_FIELDS == (
        "trade_date", "sess_id", "seqnum", "source_ticker", "clgroup")


def test_raw_v2_keeps_reconstructed_plus_six_for_separate_pair_refusal():
    frame = _synthetic_root_pair(seqnum=43)
    frame.loc[frame["clgroup"] == "FIZ", ["pos", "pos_long", "pos_short"]] = [726369, 927387, -201018]
    frame.loc[frame["clgroup"] == "YUR", ["pos", "pos_long", "pos_short"]] = [-726363, 4297389, -5023752]
    result, _ = _root_normalize(frame)
    # Raw structural pass must not hide bad latest data or grant pair admission.
    assert result["pos"].sum() == 6
    assert len(result) == 2


def test_root_exact_duplicates_collapse_but_all_distinct_revisions_remain():
    frame = _synthetic_root_pair()
    revised = frame.copy()
    revised["seqnum"] = 2**53 + 2
    result, dropped = _root_normalize(pd.concat([frame, frame, revised], ignore_index=True))
    assert dropped == 2
    assert len(result) == 4
    assert set(result["seqnum"]) == {2**53 + 1, 2**53 + 2}


def test_conflicting_root_key_is_not_silently_deduplicated():
    frame = _synthetic_root_pair()
    conflict = frame.iloc[[0]].copy()
    conflict["pos_long_num"] = 2
    with pytest.raises(materializer.FutoiMaterializationError, match="conflicting duplicate"):
        _root_normalize(pd.concat([frame, conflict], ignore_index=True))


@pytest.mark.parametrize("value", [float(2**53), np.float64(2**53 + 2),
                                  True, None, "1.5", "NaN", 2**63])
def test_raw_v2_unsafe_or_invalid_identifiers_refuse(value):
    frame = _synthetic_root_pair()
    frame["seqnum"] = pd.Series([value, value], dtype=object)
    with pytest.raises(materializer.FutoiMaterializationError):
        _root_normalize(frame)


@pytest.mark.parametrize("field,value,error", [
    ("ticker", "CR", "ticker"),
    ("tradedate", "2026-09-23", "trade_date"),
    ("tradetime", "bad", "timestamp"),
    ("clgroup", "UNKNOWN", "clgroup"),
    ("systime", "2026-09-24 10:34:00", "publication"),
    ("pos", 999, "per-row net"),
    ("pos_long_num", -1, "signs/counts"),
])
def test_raw_v2_wrong_source_rows_refuse(field, value, error):
    frame = _synthetic_root_pair()
    frame.loc[0, field] = value
    with pytest.raises(materializer.FutoiMaterializationError, match=error):
        _root_normalize(frame)


@pytest.mark.parametrize("received,ingest", [
    ("2026-09-24T07:35:09+00:00", "2026-09-24T07:35:12+00:00"),
    ("2026-09-24T07:35:11+00:00", "2026-09-24T07:35:10+00:00"),
    ("2026-09-24T07:35:11", "2026-09-24T07:35:12"),
])
def test_raw_v2_causal_clock_violations_refuse(received, ingest):
    with pytest.raises(materializer.FutoiMaterializationError, match="clocks"):
        _root_normalize(_synthetic_root_pair(), received_at=received, ingest_at=ingest)


@pytest.mark.parametrize("version", [None, "v3", "", 2, True])
def test_unknown_raw_version_refuses_before_binding_or_fetch(monkeypatch, version):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid version reached registry or source")
    monkeypatch.setattr(materializer, "_registry_binding", forbidden)
    monkeypatch.setattr(materializer, "_fetch_exact", forbidden)
    with pytest.raises(materializer.FutoiMaterializationError, match="raw_schema_version"):
        materializer.materialize_futoi_partition(
            trade_date="2026-09-24", instrument_id="si_futures_family",
            run_id="synthetic", raw_schema_version=version)


def test_raw_v2_paths_cannot_alias_v1(monkeypatch, tmp_path):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    raw_path = materializer._partition_path("2026-09-24", "si_futures_family",
                                           materializer.SOURCE_ID, raw_schema_version="v2")
    assert raw_path == (
        tmp_path / "market/supplementary/dataset_id=futures_futoi_raw/schema_version=v2"
        / "instrument_id=si_futures_family/trade_date=2026-09-24"
        / "source=moex_algopack_futoi/part.parquet")
    for method, args in (
        (materializer._partition_path, ("2026-09-24", "si_futures_family", materializer.SOURCE_ID)),
        (materializer._quality_path, ("2026-09-24", "run")),
        (materializer._manifest_path, ("2026-09-24", "run")),
    ):
        legacy = method(*args)
        assert legacy != method(*args, raw_schema_version="v2")
        assert "schema_version=v2" not in legacy.parts
    raw_path.parent.mkdir(parents=True)
    raw_path.symlink_to(tmp_path / "legacy.parquet")
    with pytest.raises(materializer.FutoiMaterializationError, match="symlink"):
        materializer._partition_path("2026-09-24", "si_futures_family",
                                    materializer.SOURCE_ID, raw_schema_version="v2")


@pytest.mark.parametrize("error", [
    materializer.FutoiMaterializationError("FUTOI APIM exact source returned no rows"),
    TimeoutError("synthetic timeout"),
    PermissionError("synthetic authorization failure"),
    materializer.FutoiMaterializationError("FUTOI APIM returned ERROR_MESSAGE instead of data"),
])
def test_raw_v2_failed_fetch_retains_reason_and_never_overwrites_v1(monkeypatch, tmp_path, error):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(materializer, "_registry_binding", lambda *args: _root_binding())
    calls = []
    def fail(ticker, day, *args):
        calls.append((ticker, day))
        raise error
    monkeypatch.setattr(materializer, "_fetch_exact", fail)
    legacy = materializer._partition_path("2026-09-24", "si_futures_family", materializer.SOURCE_ID)
    legacy.parent.mkdir(parents=True)
    legacy.write_bytes(b"synthetic immutable v1 bytes")
    with pytest.raises(type(error), match=str(error)):
        materializer.materialize_futoi_partition(
            trade_date="2026-09-24", instrument_id="si_futures_family",
            run_id="failed_si", raw_schema_version="v2")
    assert calls == [("si", "2026-09-24")]
    assert legacy.read_bytes() == b"synthetic immutable v1 bytes"
    quality = json.loads(materializer._quality_path(
        "2026-09-24", "failed_si", raw_schema_version="v2").read_text())
    manifest = json.loads(materializer._manifest_path(
        "2026-09-24", "failed_si", raw_schema_version="v2").read_text())
    assert quality["error_class"] == type(error).__name__
    assert quality["failure_reasons"] == [str(error)]
    assert manifest["refresh_status"] == "failed"
    assert manifest["partitions_written"] == []
    assert manifest["accepted_manifest_ref"] is None


@pytest.mark.parametrize("instrument,ticker", [("si_futures_family", "Si"), ("cr_futures_family", "CR")])
def test_raw_v2_parquet_roundtrip_and_post_response_receipt(monkeypatch, tmp_path, instrument, ticker):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(materializer, "_registry_binding", lambda *args: _root_binding(instrument))
    events = []
    clocks = iter(["2026-09-24T07:35:11+00:00", "2026-09-24T07:35:12+00:00"])
    def fetch(source_ticker, day, *args):
        assert source_ticker == ticker.lower() and day == "2026-09-24"
        events.append("response_received")
        return _synthetic_root_pair(ticker), "https://synthetic.invalid/exact"
    def clock():
        assert events
        events.append("clock")
        return next(clocks)
    def no_legacy(*args, **kwargs):
        pytest.fail("root raw was routed through legacy SECID normalization")
    monkeypatch.setattr(materializer, "_fetch_exact", fetch)
    monkeypatch.setattr(materializer, "_utc_now_root", clock)
    monkeypatch.setattr(materializer.legacy, "normalize_futoi", no_legacy)
    legacy = materializer._partition_path("2026-09-24", instrument, materializer.SOURCE_ID)
    legacy.parent.mkdir(parents=True)
    legacy.write_bytes(b"synthetic v1 preserved")
    result = materializer.materialize_futoi_partition(
        trade_date="2026-09-24", instrument_id=instrument, run_id="roundtrip_" + instrument,
        raw_schema_version="v2")
    path = Path(result["storage_partition_path"])
    frame = pd.read_parquet(path)
    assert events == ["response_received", "clock", "clock"]
    assert frame["seqnum"].tolist() == [2**53 + 1, 2**53 + 1]
    assert set(frame["availability_ts_utc"]) == {"2026-09-24T07:35:11+00:00"}
    assert set(frame["ingest_ts"]) == {"2026-09-24T07:35:12+00:00"}
    assert "secid" not in frame and "secid" not in result
    assert result["published_partition_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert result["factual_authority"] is False
    assert legacy.read_bytes() == b"synthetic v1 preserved"
    quality = json.loads(Path(result["quality_report_reference"]).read_text())
    manifest = json.loads(Path(result["manifest_reference"]).read_text())
    for metadata in (result, quality, manifest):
        assert metadata["raw_schema_version"] == "v2"
        assert metadata["source_identity_scope"] == "source_ticker_root"
        assert metadata["source_ticker"] == ticker.lower()
    assert manifest["published_partition_sha256"] == result["published_partition_sha256"]
    assert not materializer.accepted_pointer_path(instrument).exists()


def test_v2_run_slot_cannot_be_reused_for_another_instrument(monkeypatch, tmp_path):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(materializer, "_registry_binding", lambda *args: _root_binding("cr_futures_family"))
    path = materializer._quality_path("2026-09-24", "shared", raw_schema_version="v2")
    path.parent.mkdir(parents=True)
    payload = {"instrument_id": "si_futures_family", "raw_schema_version": "v2",
               "source_identity_scope": "source_ticker_root"}
    path.write_text(json.dumps(payload))
    def forbidden(*args, **kwargs):
        pytest.fail("run ownership conflict reached source")
    monkeypatch.setattr(materializer, "_fetch_exact", forbidden)
    with pytest.raises(materializer.FutoiMaterializationError, match="run_id"):
        materializer.materialize_futoi_partition(
            trade_date="2026-09-24", instrument_id="cr_futures_family",
            run_id="shared", raw_schema_version="v2")
    assert json.loads(path.read_text()) == payload
