"""Synthetic regressions for issue #547's native root-v2 prerequisite.

These are reconstructed examples, not production HTTP replay. Legacy integer
and factual-shape checks are retained. Fast/regular and downstream migration
remain separate, still-required parts of this same PR.
"""
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from moex_data.futures import futoi_live_factual_refresh_source_native as source


@pytest.mark.parametrize(
    "value, expected",
    [
        (2**53 + 1, 2**53 + 1),
        (str(2**53 + 1), 2**53 + 1),
        (Decimal(2**53 + 1), 2**53 + 1),
        (np.int64(2**53 + 1), 2**53 + 1),
        (2**63 - 1, 2**63 - 1),
        (-(2**53 + 1), -(2**53 + 1)),
        ("9.007199254740993e15", 2**53 + 1),
        (0, 0),
        ("43", 43),
        (43.0, 43),
        (np.float64(43), 43),
        (-201018, -201018),
    ],
)
def test_source_integer_is_exact(value, expected):
    assert source._as_int(value, "FIZ.seqnum") == expected


@pytest.mark.parametrize(
    "value",
    [
        None, pd.NA, True, False, np.bool_(True), np.bool_(False),
        float("nan"), float("inf"), float("-inf"),
        Decimal("NaN"), Decimal("Infinity"), 43.5, "43.5", "", "bad",
    ],
)
def test_invalid_integer_refuses(value):
    with pytest.raises(source.FutoiSourceNativeRefreshError):
        source._as_int(value, "FIZ.seqnum")


@pytest.mark.parametrize("value", [float(2**53), float(2**53 + 1), np.float64(2**53), -float(2**53)])
def test_already_ambiguous_float_refuses(value):
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="unsafe floating-point"):
        source._as_int(value, "FIZ.seqnum")


def _pair(*, ts="2026-09-24 10:35:00", seqnum=43, sess_id=7655, net=100):
    """Construct a legacy-v1-shaped pair; SECID is a fixture, not a source claim."""
    common = {
        "trade_date": "2026-09-24", "ts": ts,
        "systime": "2026-09-24 10:35:10",
        "availability_ts_utc": "2026-09-24T07:35:11+00:00",
        "ingest_ts": "2026-09-24T07:35:12+00:00",
        "sess_id": sess_id, "seqnum": seqnum,
        "source_id": source.SOURCE_ID,
        "instrument_id": source.SI_INSTRUMENT_ID,
        "source_ticker": "si", "secid": "SiU6",
        "pos_long_num": 2, "pos_short_num": 1,
    }
    return pd.DataFrame([
        dict(common, clgroup="FIZ", pos=net, pos_long=net + 10, pos_short=-10),
        dict(common, clgroup="YUR", pos=-net, pos_long=20, pos_short=-(net + 20)),
    ])


def _fact(frame):
    return source.latest_aligned_factual(
        frame, expected_trade_date="2026-09-24",
        expected_instrument_id=source.SI_INSTRUMENT_ID,
        expected_source_ticker="si", expected_secid="SiU6",
    )


def test_newest_revision_above_binary_float_precision_is_selected():
    frame = pd.concat([
        _pair(seqnum=2**53, net=100),
        _pair(seqnum=2**53 + 1, net=200),
    ], ignore_index=True)
    result = _fact(frame)
    assert result["fiz"]["net"] == 200
    assert result["yur"]["net"] == -200


def test_distinct_large_sessions_are_not_merged():
    frame = _pair(sess_id=2**53)
    frame.loc[frame["clgroup"] == "YUR", "sess_id"] = 2**53 + 1
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="share sess_id"):
        _fact(frame)


def test_large_session_identity_survives_factual_serialization():
    result = _fact(_pair(sess_id=2**53 + 1))
    assert result["sess_id"] == 2**53 + 1


def test_latest_si_plus_six_is_not_replaced_by_older_balanced_pair():
    latest = _pair()
    latest.loc[latest["clgroup"] == "FIZ", ["pos", "pos_long", "pos_short"]] = [
        726369, 927387, -201018,
    ]
    latest.loc[latest["clgroup"] == "YUR", ["pos", "pos_long", "pos_short"]] = [
        -726363, 4297389, -5023752,
    ]
    frame = pd.concat([_pair(ts="2026-09-24 10:30:00", seqnum=42), latest], ignore_index=True)
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="do not balance to zero"):
        _fact(frame)


def test_normal_legacy_factual_shape_and_values_are_unchanged():
    assert _fact(_pair()) == {
        "trade_date": "2026-09-24",
        "snapshot_ts": "2026-09-24T07:35:00+00:00",
        "source_publication_time": "2026-09-24T07:35:10+00:00",
        "availability_ts_utc": "2026-09-24T07:35:11+00:00",
        "ingest_ts_utc": "2026-09-24T07:35:12+00:00",
        "source_ticker": "si", "secid": "SiU6", "sess_id": 7655,
        "fiz": {"long": 110, "short": 10, "net": 100, "long_participants": 2, "short_participants": 1},
        "yur": {"long": 20, "short": 120, "net": -100, "long_participants": 2, "short_participants": 1},
        "total_open_interest": 130,
        "short_semantics": "absolute_contract_count",
        "timestamp_semantics": "source_event_and_publication_localized_from_Europe/Moscow_to_UTC",
        "fiz_yur_alignment": "latest_exact_shared_source_event_ts_and_sess_id_after_max_seqnum_revision_resolution",
    }


# All v2 examples below are synthetic. They are not production HTTP replay.
def _root_frame(*, day="2026-09-24", instrument="si_futures_family", seqnum=43, net=100):
    frame = _pair(seqnum=seqnum, net=net).drop(columns="secid")
    frame["trade_date"] = day
    frame["tradedate"] = day
    frame["tradetime"] = "10:35:00"
    frame["ts"] = day + " 10:35:00"
    frame["moment"] = frame["ts"]
    frame["systime"] = day + " 10:35:10"
    frame["availability_ts_utc"] = day + "T07:35:11+00:00"
    frame["ingest_ts"] = day + "T07:35:12+00:00"
    frame["instrument_id"] = instrument
    frame["source_ticker"] = "si" if instrument == source.SI_INSTRUMENT_ID else "cr"
    frame["ticker"] = frame["source_ticker"].str.upper()
    frame["raw_schema_version"] = "v2"
    frame["source_identity_scope"] = "source_ticker_root"
    return frame


def _root_fact(frame, *, day="2026-09-24", instrument="si_futures_family"):
    return source.latest_aligned_factual(
        frame, expected_trade_date=day, expected_instrument_id=instrument,
        expected_source_ticker="si" if instrument == source.SI_INSTRUMENT_ID else "cr",
        raw_schema_version="v2",
    )


@pytest.mark.parametrize("instrument", source.LIVE_INSTRUMENT_IDS)
def test_root_factual_serializes_exact_selected_revisions_without_secid(instrument):
    frame = pd.concat([
        _root_frame(instrument=instrument, seqnum=2**53, net=100),
        _root_frame(instrument=instrument, seqnum=2**53 + 1, net=200),
    ], ignore_index=True)
    result = _root_fact(frame, instrument=instrument)
    assert result["fiz"]["net"] == 200
    assert result["source_identity_scope"] == "source_ticker_root"
    assert result["raw_schema_version"] == "v2"
    assert "secid" not in result
    assert all(r["seqnum"] == 2**53 + 1 for r in result["selected_source_records"].values())


def test_root_latest_plus_six_refuses_even_with_older_balanced_revision():
    older = _root_frame(seqnum=42)
    latest = _root_frame()
    latest.loc[0, ["pos", "pos_long", "pos_short"]] = [726369, 927387, -201018]
    latest.loc[1, ["pos", "pos_long", "pos_short"]] = [-726363, 4297389, -5023752]
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="balance to zero"):
        _root_fact(pd.concat([older, latest], ignore_index=True))


def test_root_incomplete_frontier_never_selects_an_older_pair():
    older = _root_frame(seqnum=42)
    older[["ts", "moment"]] = "2026-09-24 10:30:00"
    older["tradetime"] = "10:30:00"
    frame = pd.concat([older, _root_frame().iloc[[0]]], ignore_index=True)
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="fallback forbidden"):
        _root_fact(frame)


@pytest.mark.parametrize("column,value", [
    ("raw_schema_version", "v1"), ("raw_schema_version", "v9"),
    ("source_identity_scope", "contract"), ("source_ticker", "cr"),
    ("ticker", "CR"), ("instrument_id", "cr_futures_family"),
    ("source_id", "other"), ("trade_date", "2026-09-23"),
    ("clgroup", "UNKNOWN"), ("moment", "2026-09-24 10:30:00"),
    ("systime", "2026-09-24 10:34:00"),
    ("availability_ts_utc", "2026-09-24T07:35:09+00:00"),
    ("availability_ts_utc", "2026-09-24T07:35:11"),
    ("ingest_ts", "2026-09-24T07:35:10+00:00"),
    ("pos", 999), ("pos_short_num", -1),
    ("seqnum", "9007199254740992.5"), ("seqnum", str(2**63)),
])
def test_root_partition_refuses_invalid_version_identity_clocks_or_numbers(column, value):
    frame = _root_frame()
    frame[column] = value
    with pytest.raises(source.FutoiSourceNativeRefreshError):
        _root_fact(frame)


def test_root_duplicate_source_key_refuses():
    frame = _root_frame()
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="duplicate source-record"):
        _root_fact(pd.concat([frame, frame.iloc[[0]]], ignore_index=True))


def test_root_distinct_sessions_above_float_precision_refuse():
    frame = _root_frame()
    frame["sess_id"] = [2**53, 2**53 + 1]
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="share sess_id"):
        _root_fact(frame)


def test_root_multiple_sessions_at_newest_timestamp_refuse():
    frame = _root_frame()
    extra = frame.iloc[[0]].copy()
    extra["sess_id"] = 7656
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="multiple sess_id"):
        _root_fact(pd.concat([frame, extra], ignore_index=True))


def test_root_contract_secid_is_not_a_compatibility_adapter():
    frame = _root_frame()
    frame["secid"] = "SiZ6"
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="contract secid"):
        _root_fact(frame)
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="contract secid"):
        source.latest_aligned_factual(
            _root_frame(), expected_trade_date="2026-09-24",
            expected_instrument_id=source.SI_INSTRUMENT_ID, expected_source_ticker="si",
            expected_secid="SiU6", raw_schema_version="v2",
        )


def test_v1_reader_does_not_silently_admit_a_v2_frame():
    frame = _root_frame()
    frame["secid"] = "SiU6"
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="v1 reader"):
        _fact(frame)


@pytest.mark.parametrize("version", ["v0", "v3", None, 2, True, " v2"])
def test_unknown_reader_version_fails_closed(version):
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="unsupported"):
        source.latest_aligned_factual(
            _root_frame(), expected_trade_date="2026-09-24",
            expected_instrument_id=source.SI_INSTRUMENT_ID,
            expected_source_ticker="si", raw_schema_version=version,
        )


def _binding_for(instrument):
    ticker = "si" if instrument == source.SI_INSTRUMENT_ID else "cr"
    return {
        "instrument_id": instrument, "canonical_symbol": ticker,
        "secid": "SiU6" if ticker == "si" else "CRU6",
        "board": "RFUD", "market": "forts", "engine": "futures",
        "futoi.source_id": source.SOURCE_ID, "futoi.ticker": ticker,
        "futoi.availability_status": "available", "futoi.probe_status": "completed",
        "futoi.enabled_for_materialization": False,
    }


@pytest.mark.parametrize("instrument", source.LIVE_INSTRUMENT_IDS)
def test_root_identity_ignores_expired_registry_contract_metadata(monkeypatch, instrument):
    monkeypatch.setattr(source, "_binding", _binding_for)
    assert source.source_identity(instrument)["secid"].endswith("U6")
    root = source.source_identity(instrument, raw_schema_version="v2")
    assert "secid" not in root and root["source_ticker"] in ("si", "cr")
    assert root["source_identity_scope"] == "source_ticker_root"


def test_v2_current_path_cannot_overwrite_v1_or_follow_symlinks(tmp_path):
    legacy = source._current_path(tmp_path, source.SI_INSTRUMENT_ID)
    new = source._current_path(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2")
    assert legacy != new
    assert "schema_version=v2" in new.parts
    root = tmp_path / "state" / "datasets" / ("dataset_id=" + source.DATASET_ID)
    root.mkdir(parents=True)
    (root / "schema_version=v2").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="symlink"):
        source._current_path(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2")


@pytest.mark.parametrize("day", ["2026-09-19", "2026-09-20", "2026-09-23"])
def test_completed_root_date_uses_usdrubf_witness_including_weekends(monkeypatch, day):
    calls = []
    def observed(start, end, *, instrument_id, timeout):
        calls.append((start, end, instrument_id))
        return [day]
    monkeypatch.setattr(source.observed_dates, "observed_dates", observed)
    chosen, evidence = source._root_date_candidate(
        day, timeout=1, started=pd.Timestamp("2026-09-24T07:00:00Z"),
    )
    assert chosen == day
    assert calls[0][2] == "usdrubf_futures_family"
    assert evidence[-1]["witness_secid"] == "USDRUBF"
    assert evidence[-1]["futoi_availability_proven"] is False


def test_completed_root_date_keeps_future_date_restriction(monkeypatch):
    monkeypatch.setattr(source.observed_dates, "observed_dates",
                        lambda *a, **k: pytest.fail("must fail before a witness query"))
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="completed"):
        source._root_date_candidate(
            "2026-09-24", timeout=1, started=pd.Timestamp("2026-09-24T07:00:00Z"),
        )


@pytest.mark.parametrize("kind", ["empty_witness", "timeout", "auth"])
def test_native_root_witness_failure_does_not_materialize_or_relabel_old_context(monkeypatch, tmp_path, kind):
    import json
    import requests

    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(source, "_binding", _binding_for)
    def observed(*args, **kwargs):
        if kind == "empty_witness":
            return []
        if kind == "timeout":
            raise requests.Timeout("synthetic witness timeout")
        raise requests.HTTPError("synthetic 401")
    monkeypatch.setattr(source.observed_dates, "observed_dates", observed)
    monkeypatch.setattr(source, "_materialize_target",
                        lambda *a, **k: pytest.fail("FUTOI must not be queried without witness"))
    old_path = source._current_path(tmp_path, source.SI_INSTRUMENT_ID)
    old_path.parent.mkdir(parents=True)
    old_path.write_text('{"legacy":true}')
    with pytest.raises(Exception) as failure:
        source.run_refresh(
            through_date="2026-09-23", instrument_id=source.SI_INSTRUMENT_ID,
            run_id="root_failure", raw_schema_version="v2",
            now_fn=lambda: pd.Timestamp("2026-09-24T07:00:00Z").to_pydatetime(),
        )
    result = json.loads(source._current_path(
        tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2",
    ).read_text())
    assert result["status"] == "FAILED"
    assert result["factual"] is None
    assert result["error_class"] == type(failure.value).__name__
    assert result["freshness"]["status"] == "UNAVAILABLE"
    assert old_path.read_text() == '{"legacy":true}'


def test_native_root_failure_is_isolated_from_other_instrument(monkeypatch):
    calls = []
    def run_refresh(**kwargs):
        calls.append(kwargs)
        if kwargs["instrument_id"] == source.SI_INSTRUMENT_ID:
            raise TimeoutError("synthetic Si timeout")
        return {"status": "PASS", "schema_version": source.SCHEMA_VERSION_V2}
    monkeypatch.setattr(source, "run_refresh", run_refresh)
    result = source.run_refresh_all(
        through_date="2026-09-23", run_id="isolated", raw_schema_version="v2",
    )
    assert result["status"] == "PARTIAL_FAILURE"
    assert result["failed_instrument_ids"] == [source.SI_INSTRUMENT_ID]
    assert all(c["raw_schema_version"] == "v2" for c in calls)
    assert result["instrument_results"][source.SI_INSTRUMENT_ID]["error_class"] == "TimeoutError"


# These tests use real Parquet and the normal materializer/audit imports in CI.
def _parquet_setup(monkeypatch, tmp_path, instrument, *, imbalance=False, day="2026-09-23"):
    from moex_data.futures import materialize_futoi_instrument as materializer

    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(materializer, "_registry_binding", lambda _path, inst: _binding_for(inst))
    raw = _root_frame(day=day, instrument=instrument)
    if imbalance:
        raw.loc[0, ["pos", "pos_long", "pos_short"]] = [726369, 927387, -201018]
        raw.loc[1, ["pos", "pos_long", "pos_short"]] = [-726363, 4297389, -5023752]
    calls = []
    def fetch(ticker, trade_date, timeout, base):
        calls.append((ticker, trade_date))
        return raw.copy(), "https://apim.moex.com/iss/analyticalproducts/futoi/securities/" + ticker + ".json"
    monkeypatch.setattr(materializer, "_fetch_exact", fetch)
    clock = iter(["2026-09-24T07:00:01+00:00", "2026-09-24T07:00:02+00:00"])
    monkeypatch.setattr(materializer, "_utc_now_root", lambda: next(clock))
    monkeypatch.setattr(source.observed_dates, "observed_dates", lambda *a, **k: [day])
    return materializer, calls


@pytest.mark.parametrize("instrument", source.LIVE_INSTRUMENT_IDS)
def test_parquet_native_root_completed_chain_preserves_exact_proof_and_v1(monkeypatch, tmp_path, instrument):
    import json
    materializer, calls = _parquet_setup(monkeypatch, tmp_path, instrument)
    legacy = source._current_path(tmp_path, instrument)
    legacy.parent.mkdir(parents=True)
    legacy.write_text('{"frozen_v1":true}')
    old_raw = materializer._partition_path("2026-09-23", instrument, source.SOURCE_ID)
    old_raw.parent.mkdir(parents=True)
    old_raw.write_bytes(b"untouched-v1")
    clock = iter([pd.Timestamp("2026-09-24T07:00:00Z").to_pydatetime(),
                  pd.Timestamp("2026-09-24T07:00:03Z").to_pydatetime()])
    result = source.run_refresh(
        through_date="2026-09-23", instrument_id=instrument, run_id="native_root",
        raw_schema_version="v2", now_fn=lambda: next(clock),
    )
    assert calls == [(source.ROOT_TICKERS[instrument], "2026-09-23")]
    assert result["status"] == "PASS"
    assert result["schema_version"] == source.SCHEMA_VERSION_V2
    assert result["factual"] == source.replay_root_factual(
        tmp_path, result["provenance"], instrument_id=instrument, trade_date="2026-09-23",
    )
    assert "publication_audit" in result["provenance"]
    assert legacy.read_text() == '{"frozen_v1":true}'
    assert old_raw.read_bytes() == b"untouched-v1"
    assert result["factual_authority"] is False
    audit_ref = result["provenance"]["publication_audit"]["ref"]
    audit = json.loads((tmp_path / audit_ref.removeprefix(source.ROOT_REF_PREFIX)).read_text())
    assert audit["latest_status"] == "PASS" and audit["latest_factual"] == result["factual"]


@pytest.mark.parametrize("instrument", source.LIVE_INSTRUMENT_IDS)
def test_parquet_root_rejected_latest_pair_has_frozen_audit_for_both_roots(monkeypatch, tmp_path, instrument):
    import json
    _parquet_setup(monkeypatch, tmp_path, instrument, imbalance=True)
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="balance to zero") as failure:
        source._materialize_target(
            tmp_path, "2026-09-23", "rejected", instrument_id=instrument,
            timeout=1, raw_schema_version="v2",
        )
    proof = failure.value.attempt_provenance
    audit = json.loads((tmp_path / proof["publication_audit"]["ref"].removeprefix(source.ROOT_REF_PREFIX)).read_text())
    assert audit["latest_status"] == "REJECTED"
    assert audit["rejected_count"] == 1
    assert proof["raw_schema_version"] == "v2"
    assert "secid" not in proof


def test_parquet_v2_proof_corruption_and_version_mismatch_never_fall_back(monkeypatch, tmp_path):
    from copy import deepcopy
    _parquet_setup(monkeypatch, tmp_path, source.SI_INSTRUMENT_ID)
    path, proof = source._materialize_target(
        tmp_path, "2026-09-23", "proof", instrument_id=source.SI_INSTRUMENT_ID,
        timeout=1, raw_schema_version="v2",
    )
    bad = deepcopy(proof)
    bad["raw_schema_version"] = "v1"
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="version/source/contract"):
        source.replay_root_factual(tmp_path, bad, instrument_id=source.SI_INSTRUMENT_ID, trade_date="2026-09-23")
    path.write_bytes(b"corrupt")
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="SHA mismatch"):
        source.replay_root_factual(tmp_path, proof, instrument_id=source.SI_INSTRUMENT_ID, trade_date="2026-09-23")


def test_parquet_native_v2_fails_on_pre_receipt_validation_clock(monkeypatch, tmp_path):
    _parquet_setup(monkeypatch, tmp_path, source.SI_INSTRUMENT_ID)
    clock = iter([pd.Timestamp("2026-09-24T07:00:00Z").to_pydatetime(),
                  pd.Timestamp("2026-09-24T07:00:01.500Z").to_pydatetime(),
                  pd.Timestamp("2026-09-24T07:00:04Z").to_pydatetime()])
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="validation clocks"):
        source.run_refresh(
            through_date="2026-09-23", instrument_id=source.SI_INSTRUMENT_ID,
            run_id="bad_clock", raw_schema_version="v2", now_fn=lambda: next(clock),
        )


def test_root_evidence_symlink_refuses_before_materialization(monkeypatch, tmp_path):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(source, "_binding", _binding_for)
    target = tmp_path / "redirect"
    target.mkdir()
    evidence = tmp_path / "state" / "datasets" / ("dataset_id=" + source.DATASET_ID) / "evidence"
    evidence.parent.mkdir(parents=True)
    evidence.symlink_to(target, target_is_directory=True)
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="symlink"):
        source._archive_root_context(tmp_path, source._root_current_path(tmp_path, source.SI_INSTRUMENT_ID), {})
    assert list(target.iterdir()) == []


def test_root_clocks_with_fractional_seconds_use_timestamp_order():
    raw = _root_frame()
    raw.loc[0, "systime"] = "2026-09-24 10:35:10.100"
    raw.loc[1, "systime"] = "2026-09-24 10:35:10"
    result = _root_fact(raw)
    assert pd.Timestamp(result["source_publication_time"]) == pd.Timestamp("2026-09-24T07:35:10.100Z")


def _snapshot_reader():
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as snapshot
    return snapshot


@pytest.mark.parametrize("version", ["v0", "v3", "", None])
def test_snapshot_candidate_unknown_version_refuses_before_read(monkeypatch, tmp_path, version):
    snapshot = _snapshot_reader()
    monkeypatch.setattr(snapshot, "_load_json", lambda *a: pytest.fail("version must be rejected before read"))
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="raw_schema_version"):
        snapshot._load_candidate(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version=version)


def test_snapshot_selected_v2_does_not_read_present_v1_candidate(monkeypatch, tmp_path):
    snapshot = _snapshot_reader()
    old = source._current_path(tmp_path, source.SI_INSTRUMENT_ID)
    old.parent.mkdir(parents=True)
    old.write_text('{"legacy":true}')
    with pytest.raises(snapshot.FutoiSnapshotComponentError, match="regular non-symlink"):
        snapshot._load_candidate(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2")
    assert old.read_text() == '{"legacy":true}'


def _parquet_root_payload(monkeypatch, tmp_path, instrument):
    _parquet_setup(monkeypatch, tmp_path, instrument)
    clock = iter([pd.Timestamp("2026-09-24T07:00:00Z").to_pydatetime(),
                  pd.Timestamp("2026-09-24T07:00:03Z").to_pydatetime()])
    return source.run_refresh(
        through_date="2026-09-23", instrument_id=instrument, run_id="reader_root",
        raw_schema_version="v2", now_fn=lambda: next(clock),
    )


@pytest.mark.parametrize("instrument", source.LIVE_INSTRUMENT_IDS)
def test_parquet_snapshot_root_candidate_replays_exact_source_and_audit(monkeypatch, tmp_path, instrument):
    result = _parquet_root_payload(monkeypatch, tmp_path, instrument)
    loaded = _snapshot_reader()._load_candidate(
        tmp_path, instrument, raw_schema_version="v2",
        now=pd.Timestamp("2026-09-24T07:00:04Z").to_pydatetime(),
    )
    assert loaded == result
    assert "secid" not in loaded["factual"]
    assert loaded["freshness"]["scope"] == "latest_completed_observed_date_not_current_intraday"
    assert loaded["factual_authority"] is False


@pytest.mark.parametrize("alteration", ["unbound_fact", "bound_bad_proof", "future_clock", "v1_path"])
def test_parquet_snapshot_root_candidate_rejects_tampering_and_version_path_mismatch(
    monkeypatch, tmp_path, alteration,
):
    import json
    from copy import deepcopy

    snapshot = _snapshot_reader()
    result = _parquet_root_payload(monkeypatch, tmp_path, source.SI_INSTRUMENT_ID)
    path = source._root_current_path(tmp_path, source.SI_INSTRUMENT_ID)
    read_at = pd.Timestamp("2026-09-24T07:00:04Z").to_pydatetime()
    version = "v2"
    if alteration == "unbound_fact":
        result["factual"]["fiz"]["net"] += 1
        path.write_text(json.dumps(result))
    elif alteration == "bound_bad_proof":
        result = deepcopy(result)
        result.pop("run_evidence_ref")
        result.pop("run_evidence_sha256")
        result["provenance"]["raw_schema_version"] = "v1"
        source._archive_root_context(tmp_path, path, result)
    elif alteration == "future_clock":
        read_at = pd.Timestamp("2026-09-24T07:00:02Z").to_pydatetime()
    else:
        old = source._current_path(tmp_path, source.SI_INSTRUMENT_ID)
        old.parent.mkdir(parents=True, exist_ok=True)
        old.write_text(json.dumps(result))
        version = "v1"
    with pytest.raises((source.FutoiSourceNativeRefreshError, snapshot.FutoiSnapshotComponentError)):
        snapshot._load_candidate(
            tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version=version, now=read_at,
        )


def test_parquet_snapshot_root_failure_cannot_retain_old_pass_as_fresh(monkeypatch, tmp_path):
    import json
    _parquet_root_payload(monkeypatch, tmp_path, source.SI_INSTRUMENT_ID)
    monkeypatch.setattr(source.observed_dates, "observed_dates",
                        lambda *a, **k: (_ for _ in ()).throw(TimeoutError("synthetic later timeout")))
    with pytest.raises(TimeoutError):
        source.run_refresh(
            through_date="2026-09-23", instrument_id=source.SI_INSTRUMENT_ID, run_id="later_failure",
            raw_schema_version="v2",
            now_fn=lambda: pd.Timestamp("2026-09-24T07:01:00Z").to_pydatetime(),
        )
    path = source._root_current_path(tmp_path, source.SI_INSTRUMENT_ID)
    failed = json.loads(path.read_text())
    assert failed["status"] == "FAILED" and failed["factual"] is None
    with pytest.raises(_snapshot_reader().FutoiSnapshotComponentError, match="status mismatch"):
        _snapshot_reader()._load_candidate(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2")


def test_native_root_context_contract_and_config_declare_isolated_opt_in_path(tmp_path):
    from pathlib import Path
    import inspect
    import json
    from moex_data.futures.contract_io import load_simple_yaml_mapping

    root = Path(__file__).resolve().parents[1]
    # Exercise the production config reader, not an undeclared YAML dependency.
    config = load_simple_yaml_mapping(root, "configs/datasets/futures_data_lake.v1.yaml")
    declared = config["futoi_source_native_context_v2"]
    assert declared["contract_ref"] == "contracts/datasets/futoi_live_factual_context.v2.yaml"
    contract_lines = (root / declared["contract_ref"]).read_text(encoding="utf-8").splitlines()
    # These two contract fields are explicit top-level scalars. Check their exact
    # declarations, including uniqueness, without introducing another YAML parser.
    schema_lines = [line for line in contract_lines if line.startswith("schema_version:")]
    path_lines = [line for line in contract_lines if line.startswith("current_path_pattern:")]
    assert schema_lines == ["schema_version: " + source.SCHEMA_VERSION_V2]
    assert len(path_lines) == 1
    contract_pattern = json.loads(path_lines[0].partition(":")[2].strip())
    assert isinstance(contract_pattern, str)
    assert declared["context_artifact_path_pattern"] == contract_pattern
    expected = contract_pattern.replace("${MOEX_DATA_ROOT}", str(tmp_path)).replace(
        "{INSTRUMENT_ID}", source.SI_INSTRUMENT_ID,
    )
    assert source._current_path(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2") == Path(expected)
    assert declared["failed_attempt_publication_policy"] == "replace_old_pass_with_explicit_failure"
    assert declared["live_runtime_enabled"] is False
    for function in (source.run_refresh, source.run_refresh_all, source.source_identity,
                     source.latest_aligned_factual, source._materialize_target, _snapshot_reader().build_snapshot):
        assert inspect.signature(function).parameters["raw_schema_version"].default == "v1"


def test_native_root_witness_secid_is_verified_before_query(monkeypatch):
    monkeypatch.setattr(source.observed_dates, "reference_secid", lambda _id: "SiU6")
    monkeypatch.setattr(source.observed_dates, "observed_dates",
                        lambda *a, **k: pytest.fail("wrong witness must fail before query"))
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="witness registry binding"):
        source._root_date_candidate("2026-09-23", timeout=1, started=pd.Timestamp("2026-09-24T07:00:00Z"))


@pytest.mark.parametrize("location", ["envelope", "factual", "provenance"])
def test_legacy_snapshot_candidate_rejects_v2_tags(monkeypatch, tmp_path, location):
    snapshot = _snapshot_reader()
    value = {"project": source.PROJECT, "schema_version": source.SCHEMA_VERSION}
    tagged = value if location == "envelope" else value.setdefault(location, {})
    tagged["raw_schema_version"] = "v2"
    monkeypatch.setattr(source, "source_identity", lambda _id: {"source_ticker": "si", "secid": "SiU6"})
    monkeypatch.setattr(snapshot, "_load_json", lambda *a: value)
    with pytest.raises(snapshot.FutoiSnapshotComponentError, match="another raw version"):
        snapshot._load_candidate(tmp_path, source.SI_INSTRUMENT_ID)


@pytest.mark.parametrize("marker", ["current", "latest", "autodetect"])
def test_native_context_config_keeps_legacy_dynamic_selection_guard(marker):
    from moex_data.futures.contract_io import FuturesContractIoError, reject_dynamic_markers

    # Describing a separately versioned live artifact does not authorize dynamic
    # selection through the historical controlled-read interface.
    for value in (marker + "_path_pattern", "${MOEX_DATA_ROOT}/" + marker + ".json"):
        with pytest.raises(FuturesContractIoError, match="unsupported dynamic marker"):
            reject_dynamic_markers(value, "regression")


@pytest.fixture(params=["regular", "fast"])
def role_v2_module(request):
    from moex_data.futures import futoi_intraday_previous_session_context as regular
    from moex_data.futures import futoi_intraday_previous_session_context_fast as fast
    return regular if request.param == "regular" else fast


def _role_v2_setup(monkeypatch, tmp_path):
    """Synthetic orchestration inputs; materialization/replay are mocked here."""
    from moex_data.futures import futoi_intraday_previous_session_context as core

    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    now = pd.Timestamp("2026-09-24T07:36:00.123456Z").to_pydatetime()
    observed = {"2026-09-24", "2026-09-23"}
    witness_errors = {}
    source_errors = {}
    witness_calls = []
    source_calls = []
    monkeypatch.setattr(source.observed_dates, "reference_secid", lambda instrument_id: (
        "USDRUBF" if instrument_id == "usdrubf_futures_family" else pytest.fail("expired witness used")
    ))
    def witness(day, *, secid, timeout, apim_base_url):
        assert secid == "USDRUBF"
        witness_calls.append(day.isoformat())
        if day.isoformat() in witness_errors:
            raise witness_errors[day.isoformat()]
        return day.isoformat() in observed
    monkeypatch.setattr(source.observed_dates, "_exact_date_has_secid", witness)
    def materialize(root, target, run_id, *, instrument_id, timeout, raw_schema_version):
        assert raw_schema_version == "v2"
        source_calls.append((instrument_id, target, run_id))
        error = source_errors.get((instrument_id, target))
        if error is not None:
            raise error
        return root / "unused_synthetic.parquet", {
            "synthetic": True, "trade_date": target, "instrument_id": instrument_id,
            "source_id": source.SOURCE_ID, "source_ticker": source.ROOT_TICKERS[instrument_id],
            "raw_schema_version": "v2", "source_identity_scope": "source_ticker_root",
        }
    monkeypatch.setattr(source, "_materialize_target", materialize)
    def replay(root, proof, *, instrument_id, trade_date):
        assert proof["trade_date"] == trade_date
        return {
            "trade_date": trade_date, "snapshot_ts": trade_date + "T07:35:00+00:00",
            "source_publication_time": trade_date + "T07:35:10+00:00",
            "availability_ts_utc": now.isoformat(), "ingest_ts_utc": now.isoformat(),
            "raw_schema_version": "v2", "source_identity_scope": "source_ticker_root",
            "source_ticker": source.ROOT_TICKERS[instrument_id],
            "fiz": {"net": 100}, "yur": {"net": -100},
        }
    monkeypatch.setattr(source, "replay_root_factual", replay)
    monkeypatch.setattr(source, "_probe_exact_date", lambda *a, **k: pytest.fail("legacy duplicate probe used"))
    monkeypatch.setattr(core, "_resolve_observed_trade_dates", lambda *a, **k: pytest.fail("coupled v1 resolver used"))
    return now, observed, witness_errors, source_errors, witness_calls, source_calls


@pytest.mark.parametrize("failed_role", ["current_intraday", "previous_completed_session"])
@pytest.mark.parametrize("failure_kind", ["witness_timeout", "no_witness", "source_timeout", "source_empty"])
def test_role_v2_independence_in_both_directions(
    role_v2_module, monkeypatch, tmp_path, failed_role, failure_kind,
):
    from moex_data.futures import futoi_intraday_previous_session_context as core
    from moex_data.futures import materialize_futoi_instrument as materializer

    now, observed, witness_errors, source_errors, witness_calls, source_calls = _role_v2_setup(monkeypatch, tmp_path)
    failed_date = "2026-09-24" if failed_role == core.CURRENT_ROLE else "2026-09-23"
    other = core.PREVIOUS_ROLE if failed_role == core.CURRENT_ROLE else core.CURRENT_ROLE
    if failure_kind == "witness_timeout":
        witness_errors[failed_date] = TimeoutError("synthetic witness timeout")
    elif failure_kind == "no_witness":
        observed.remove(failed_date)
    elif failure_kind == "source_empty":
        source_errors[(source.SI_INSTRUMENT_ID, failed_date)] = materializer.FutoiMaterializationError(
            source.EXPLICIT_EMPTY_ERROR,
        )
    else:
        source_errors[(source.SI_INSTRUMENT_ID, failed_date)] = TimeoutError("synthetic source timeout")
    result = role_v2_module.run_refresh(
        through_date="2026-09-24", instrument_id=source.SI_INSTRUMENT_ID,
        run_id="role_failure", now_fn=lambda: now, raw_schema_version="v2",
    )
    expected_status = {"no_witness": "UNAVAILABLE", "source_empty": "PENDING"}.get(failure_kind, "ERROR")
    assert result["status"] == "PARTIAL"
    assert result[failed_role]["status"] == expected_status
    assert result[failed_role]["factual"] is None
    assert result[other]["status"] == "FRESH"
    assert result[other]["factual"]["trade_date"] != failed_date
    assert result[other]["consumer_factual_use_allowed"] is False
    if failure_kind == "witness_timeout":
        assert result[failed_role]["refresh_error_class"] == "TimeoutError"
        assert len(witness_calls) == 2  # Do not skip the uncertain previous date.
    elif failure_kind == "no_witness" and failed_role == core.PREVIOUS_ROLE:
        assert len(witness_calls) == core.SOURCE_LOOKBACK_DAYS
    if failure_kind in ("source_empty", "source_timeout"):
        assert result[failed_role]["expected_trade_date"] == failed_date
        assert sorted(call[1] for call in source_calls) == ["2026-09-23", "2026-09-24"]
    assert result["refresh_attempted_at"] == now.isoformat()
    assert result["raw_schema_version"] == "v2"


def test_role_v2_si_failure_does_not_cancel_cr(role_v2_module, monkeypatch, tmp_path):
    now, _, _, errors, _, _ = _role_v2_setup(monkeypatch, tmp_path)
    for day in ("2026-09-23", "2026-09-24"):
        errors[(source.SI_INSTRUMENT_ID, day)] = TimeoutError("synthetic Si timeout")
    result = role_v2_module.run_refresh_all(
        through_date="2026-09-24", run_id="isolated_roots", now_fn=lambda: now, raw_schema_version="v2",
    )
    assert result["status"] == "PARTIAL"
    assert result["instrument_results"][source.SI_INSTRUMENT_ID]["status"] == "FAILED"
    assert result["instrument_results"][source.CR_INSTRUMENT_ID]["status"] == "PASS"
    assert result["failed_instrument_ids"] == [source.SI_INSTRUMENT_ID]


def test_role_v2_failed_pair_evidence_and_previous_are_preserved(role_v2_module, monkeypatch, tmp_path):
    from moex_data.futures import futoi_intraday_previous_session_context as core
    now, _, _, errors, _, calls = _role_v2_setup(monkeypatch, tmp_path)
    error = source.FutoiSourceNativeRefreshError("FIZ/YUR net positions do not balance to zero")
    error.attempt_provenance = {"synthetic_reconstructed": True, "imbalance": 6}
    errors[(source.SI_INSTRUMENT_ID, "2026-09-24")] = error
    result = role_v2_module.run_refresh(
        through_date="2026-09-24", instrument_id=source.SI_INSTRUMENT_ID,
        run_id="latest_plus_six", now_fn=lambda: now, raw_schema_version="v2",
    )
    assert result[core.CURRENT_ROLE]["status"] == "ERROR"
    assert result[core.CURRENT_ROLE]["failed_attempt_evidence"] == error.attempt_provenance
    assert result[core.PREVIOUS_ROLE]["status"] == "FRESH"
    assert sorted(call[1] for call in calls) == ["2026-09-23", "2026-09-24"]


@pytest.mark.parametrize("day", ["2026-09-19", "2026-09-20"])
def test_role_v2_witness_keeps_observed_weekends(monkeypatch, day):
    from moex_data.futures import futoi_intraday_previous_session_context as core
    from datetime import date, timedelta

    previous = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
    monkeypatch.setattr(source.observed_dates, "reference_secid", lambda _id: "USDRUBF")
    monkeypatch.setattr(source.observed_dates, "_exact_date_has_secid",
                        lambda requested, **kwargs: requested.isoformat() in {day, previous})
    assert core._root_role_witness(day, core.CURRENT_ROLE, timeout=1, observations=[]) == day
    assert core._root_role_witness(day, core.PREVIOUS_ROLE, timeout=1, observations=[]) == previous


def test_role_v2_persisted_envelope_is_isolated_and_hash_bound(role_v2_module, monkeypatch, tmp_path):
    from moex_data.futures import futoi_intraday_previous_session_context as core
    import json

    now, _, _, errors, _, _ = _role_v2_setup(monkeypatch, tmp_path)
    legacy = core._artifact_path(tmp_path, source.SI_INSTRUMENT_ID)
    legacy.parent.mkdir(parents=True)
    legacy.write_text('{"unchanged_legacy_evidence":true}')
    result = role_v2_module.run_refresh(
        through_date="2026-09-24", instrument_id=source.SI_INSTRUMENT_ID,
        run_id="versioned_roles", now_fn=lambda: now, raw_schema_version="v2",
    )
    path = core._artifact_path(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2")
    assert path != legacy and "schema_version=v2" in path.parts
    assert core._load_previous(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2") == result
    errors[(source.SI_INSTRUMENT_ID, "2026-09-24")] = TimeoutError("synthetic later failure")
    failed = role_v2_module.run_refresh(
        through_date="2026-09-24", instrument_id=source.SI_INSTRUMENT_ID,
        run_id="later_roles", now_fn=lambda: now, raw_schema_version="v2",
    )
    assert failed[core.CURRENT_ROLE]["factual"] is None
    assert failed[core.PREVIOUS_ROLE]["status"] == "FRESH"
    frozen = json.loads(source._root_proof_bytes(
        tmp_path, result["run_evidence_ref"], result["run_evidence_sha256"], ".json",
    ))
    assert frozen[core.CURRENT_ROLE]["status"] == "FRESH"  # Original evidence, not this attempt.
    assert legacy.read_text() == '{"unchanged_legacy_evidence":true}'
    failed[core.CURRENT_ROLE]["status"] = "FRESH"
    path.write_text(json.dumps(failed))
    with pytest.raises(core.FutoiIntradayContextError, match="frozen evidence"):
        core._load_previous(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2")


@pytest.mark.parametrize("version", ["v9", None, True])
def test_role_v2_unknown_version_fails_before_io(role_v2_module, monkeypatch, version):
    monkeypatch.setattr(source, "_data_root", lambda: pytest.fail("version must fail before I/O"))
    with pytest.raises(source.FutoiSourceNativeRefreshError, match="raw_schema_version"):
        role_v2_module.run_refresh_all(through_date="2026-09-24", run_id="wrong_version", raw_schema_version=version)


def test_role_v2_validation_clock_error_keeps_attempt_provenance(role_v2_module, monkeypatch, tmp_path):
    from moex_data.futures import futoi_intraday_previous_session_context as core
    now, _, _, _, _, _ = _role_v2_setup(monkeypatch, tmp_path)
    replay = source.replay_root_factual
    def bad_clock(root, proof, *, instrument_id, trade_date):
        value = replay(root, proof, instrument_id=instrument_id, trade_date=trade_date)
        if trade_date == "2026-09-24":
            value["ingest_ts_utc"] = (pd.Timestamp(now) + pd.Timedelta(seconds=1)).isoformat()
        return value
    monkeypatch.setattr(source, "replay_root_factual", bad_clock)
    result = role_v2_module.run_refresh(
        through_date="2026-09-24", instrument_id=source.SI_INSTRUMENT_ID,
        run_id="clock_roles", now_fn=lambda: now, raw_schema_version="v2",
    )
    assert result[core.CURRENT_ROLE]["status"] == "ERROR"
    assert result[core.CURRENT_ROLE]["failed_attempt_evidence"]["synthetic"] is True
    assert result[core.PREVIOUS_ROLE]["status"] == "FRESH"


@pytest.mark.parametrize("bad_current", [False, True])
@pytest.mark.parametrize("instrument", source.LIVE_INSTRUMENT_IDS)
def test_role_v2_parquet_end_to_end_with_expired_registry(
    role_v2_module, monkeypatch, tmp_path, bad_current, instrument,
):
    from moex_data.futures import futoi_intraday_previous_session_context as core
    from moex_data.futures import materialize_futoi_instrument as materializer

    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    now = pd.Timestamp("2026-09-24T07:36:00.123456Z").to_pydatetime()
    monkeypatch.setattr(materializer, "_registry_binding", lambda _path, instrument: _binding_for(instrument))
    monkeypatch.setattr(materializer, "_utc_now_root", lambda: now.isoformat())
    monkeypatch.setattr(source.observed_dates, "reference_secid", lambda instrument: (
        "USDRUBF" if instrument == "usdrubf_futures_family" else pytest.fail("expired witness")
    ))
    monkeypatch.setattr(source.observed_dates, "_exact_date_has_secid", lambda day, **kwargs: day.isoformat() in {
        "2026-09-23", "2026-09-24",
    })
    calls = []
    def fetch(ticker, day, timeout, base):
        calls.append((ticker, day))
        raw = _root_frame(day=day, instrument=instrument)
        if bad_current and day == "2026-09-24":
            raw.loc[0, ["pos", "pos_long", "pos_short"]] = [726369, 927387, -201018]
            raw.loc[1, ["pos", "pos_long", "pos_short"]] = [-726363, 4297389, -5023752]
        return raw, "https://apim.moex.com/iss/analyticalproducts/futoi/securities/" + ticker + ".json"
    monkeypatch.setattr(materializer, "_fetch_exact", fetch)
    monkeypatch.setattr(source, "_probe_exact_date", lambda *a, **k: pytest.fail("legacy probe used"))
    result = role_v2_module.run_refresh(
        through_date="2026-09-24", instrument_id=instrument,
        run_id="parquet_roles", raw_schema_version="v2", now_fn=lambda: now,
    )
    assert sorted(calls) == [(source.ROOT_TICKERS[instrument], day) for day in ("2026-09-23", "2026-09-24")]
    assert result["status"] == ("PARTIAL" if bad_current else "PASS")
    assert result[core.PREVIOUS_ROLE]["status"] == "FRESH"
    assert "secid" not in result[core.PREVIOUS_ROLE]["factual"]
    assert result[core.PREVIOUS_ROLE]["factual"] == source.replay_root_factual(
        tmp_path, result[core.PREVIOUS_ROLE]["provenance"],
        instrument_id=instrument, trade_date="2026-09-23",
    )
    if bad_current:
        assert result[core.CURRENT_ROLE]["factual"] is None
        assert "publication_audit" in result[core.CURRENT_ROLE]["failed_attempt_evidence"]
    else:
        assert result[core.CURRENT_ROLE]["status"] == "FRESH"
        assert "publication_audit" in result[core.CURRENT_ROLE]["provenance"]


@pytest.mark.parametrize("day", ["2026-09-23", "2026-09-25"])
def test_role_v2_intraday_date_cannot_be_a_past_or_future_date(role_v2_module, monkeypatch, tmp_path, day):
    from moex_data.futures import futoi_intraday_previous_session_context as core
    now, _, _, _, witness_calls, source_calls = _role_v2_setup(monkeypatch, tmp_path)
    with pytest.raises(core.FutoiIntradayContextError, match="refresh-start"):
        role_v2_module.run_refresh(
            through_date=day, instrument_id=source.SI_INSTRUMENT_ID,
            run_id="wrong_intraday_date", now_fn=lambda: now, raw_schema_version="v2",
        )
    assert witness_calls == [] and source_calls == []


def test_role_v2_default_api_stays_legacy(role_v2_module, tmp_path):
    import inspect
    from moex_data.futures import futoi_intraday_previous_session_context as core
    for function in (role_v2_module.run_refresh, role_v2_module.run_refresh_all,
                     core._artifact_path, core._load_previous):
        assert inspect.signature(function).parameters["raw_schema_version"].default == "v1"
    assert core._artifact_path(tmp_path, source.SI_INSTRUMENT_ID) == (
        source._current_path(tmp_path, source.SI_INSTRUMENT_ID).parent / core.ARTIFACT_FILENAME
    )


def test_role_v2_reader_rejects_legacy_envelope_in_versioned_path(tmp_path):
    from moex_data.futures import futoi_intraday_previous_session_context as core
    import json

    path = core._artifact_path(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2")
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({
        "project": source.PROJECT, "schema_version": core.SCHEMA_VERSION,
        "instrument_id": source.SI_INSTRUMENT_ID, "raw_schema_version": "v1",
    }))
    with pytest.raises(core.FutoiIntradayContextError, match="version/identity"):
        core._load_previous(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2")


def test_role_v2_contract_config_and_explicit_path_agree(tmp_path):
    from pathlib import Path
    import json
    from moex_data.futures import futoi_intraday_previous_session_context as core
    from moex_data.futures.contract_io import load_simple_yaml_mapping

    root = Path(__file__).resolve().parents[1]
    values = load_simple_yaml_mapping(root, "configs/datasets/futures_data_lake.v1.yaml")
    declared = values["futoi_intraday_context_v2"]
    assert declared["contract_ref"] == "contracts/datasets/futoi_intraday_previous_session_context.v2.yaml"
    lines = (root / declared["contract_ref"]).read_text(encoding="utf-8").splitlines()
    assert [line for line in lines if line.startswith("schema_version:")] == [
        "schema_version: " + core.SCHEMA_VERSION_V2,
    ]
    paths = [line for line in lines if line.startswith("artifact_path_pattern:")]
    assert len(paths) == 1
    pattern = json.loads(paths[0].partition(":")[2].strip())
    assert declared["artifact_path_pattern"] == pattern
    assert core._artifact_path(tmp_path, source.SI_INSTRUMENT_ID, raw_schema_version="v2") == Path(
        pattern.replace("${MOEX_DATA_ROOT}", str(tmp_path)).replace("{INSTRUMENT_ID}", source.SI_INSTRUMENT_ID)
    )
    assert declared["roles"] == (core.CURRENT_ROLE, core.PREVIOUS_ROLE)
    assert declared["live_runtime_enabled"] is False
