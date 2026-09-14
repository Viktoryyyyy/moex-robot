"""Synthetic regression cases for retained Stage7 build-witness integrity."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest

from moex_data import rub_fx_observed_context as fx

UTC = timezone.utc
BUILT = datetime(2026, 9, 12, 21, 34, 30, 123456, tzinfo=UTC)
NOW = datetime(2026, 9, 13, 13, tzinfo=UTC)
PREBUILD = datetime(2026, 9, 12, 12, tzinfo=UTC)
EARLIER = datetime(2026, 9, 12, 6, tzinfo=UTC)
IDENTITIES = [
    ("usdrubf_futures_family", "USDRUBF"),
    ("cnyrubf_futures_family", "CNYRUBF"),
]


def retained_block(timeframe="1D", count=3, identity=IDENTITIES[0]):
    instrument, secid = identity
    last = datetime(2026, 9, 11 if timeframe == "1D" else 6, tzinfo=UTC)
    step = timedelta(days=1 if timeframe == "1D" else 7)
    rows = []
    for index in range(count):
        end = last - step * (count - 1 - index)
        start = end if timeframe == "1D" else end - timedelta(days=6)
        rows.append({
            "instrument_id": instrument, "secid": secid, "timeframe": timeframe,
            "trade_date": end.date().isoformat(),
            "period_start_date": start.date().isoformat(),
            "period_end_date": end.date().isoformat(),
            "availability_ts_utc": (end + timedelta(days=1, hours=3)).isoformat(),
            "build_ts_utc": BUILT.isoformat(),
            "open": 80.0 + index, "high": 82.0 + index,
            "low": 79.0 + index, "close": 81.0 + index,
            "volume": 100.0, "value": 8000.0, "num_trades": 10.0,
        })
    provenance = {"acceptance_run_id": "synthetic_existing_acceptance"}
    evidence = {
        "schema_version": fx.SCHEMA, "instrument_id": instrument,
        "timeframe": timeframe, "rows": rows,
        "source_provenance": deepcopy(provenance),
    }
    return {
        "block_id": "stage7.ohlcv." + timeframe + "." + instrument,
        "stage": 7, "dataset_id": "rub_native_ohlcv_htf",
        "instrument_id": instrument, "timeframe": timeframe, "status": "ready",
        "selected_observation": deepcopy(rows[-1]),
        "selected_causal_ts_utc": rows[-1]["availability_ts_utc"],
        "provenance": provenance, "observed_context_evidence": evidence,
    }


def change_rows(value, which):
    rows = value["observed_context_evidence"]["rows"]
    targets = rows if which == "all" else [rows[0 if which == "first" else -1]]
    for row in targets:
        row["build_ts_utc"] = EARLIER.isoformat()


@pytest.mark.parametrize("identity", IDENTITIES)
@pytest.mark.parametrize("timeframe", ["1D", "1W"])
@pytest.mark.parametrize("count", [1, 3])
@pytest.mark.parametrize("which", ["first", "last", "all"])
def test_syntactically_valid_backdating_cannot_replace_selected_witness(identity, timeframe, count, which):
    value = retained_block(timeframe, count, identity)
    change_rows(value, which)
    original = deepcopy(value)
    for clock in (PREBUILD, NOW):
        fx.apply(value, clock)
        result = value["observed_context"]
        assert result["status"] == "UNAVAILABLE"
        expected_reason = ("noncausal_hole_no_lag_shift"
                           if clock == PREBUILD and count > 1 and which == "last"
                           else "retained_build_witness_mismatch")
        assert result["reason"] == expected_reason
        assert result["observations"] == [] and result["comparisons"] == {}
        assert {key: item for key, item in value.items() if key != "observed_context"} == original


@pytest.mark.parametrize("timeframe", ["1D", "1W"])
@pytest.mark.parametrize("defect", ["missing", "null", "naive", "invalid", "early", "late"])
def test_selected_build_witness_is_required_and_must_agree(timeframe, defect):
    value = retained_block(timeframe)
    if defect == "missing":
        value["selected_observation"].pop("build_ts_utc")
    else:
        value["selected_observation"]["build_ts_utc"] = {
            "null": None, "naive": "2026-09-12T21:34:30",
            "invalid": "bad-clock", "early": EARLIER.isoformat(),
            "late": (BUILT + timedelta(microseconds=1)).isoformat(),
        }[defect]
    before = deepcopy(value)
    fx.apply(value, NOW)
    assert value["observed_context"]["status"] == "UNAVAILABLE"
    assert value["observed_context"]["reason"] == "retained_build_witness_mismatch"
    assert value["observed_context_evidence"] == before["observed_context_evidence"]


@pytest.mark.parametrize("identity", IDENTITIES)
@pytest.mark.parametrize("timeframe", ["1D", "1W"])
def test_same_instant_offsets_microsecond_boundary_and_repeated_read(identity, timeframe):
    value = retained_block(timeframe, 3, identity)
    for row, hours in zip(value["observed_context_evidence"]["rows"], [-7, 3, 14]):
        row["build_ts_utc"] = BUILT.astimezone(timezone(timedelta(hours=hours))).isoformat()
    original = deepcopy(value)
    fx.apply(value, BUILT - timedelta(microseconds=1))
    assert value["observed_context"]["status"] == "UNAVAILABLE"
    fx.apply(value, BUILT)
    assert value["observed_context"]["status"] == "AVAILABLE"
    assert value["observed_context"]["observations"] == original["observed_context_evidence"]["rows"]
    fx.apply(value, NOW)
    final = deepcopy(value["observed_context"])
    fx.apply(value, NOW)
    assert value["observed_context"] == final
    assert final == fx.describe(original["observed_context_evidence"], now=NOW)
    assert {key: item for key, item in value.items() if key != "observed_context"} == original


@pytest.mark.parametrize("timeframe", ["1D", "1W"])
@pytest.mark.parametrize("count", [1, 3])
@pytest.mark.parametrize("which", ["first", "all"])
def test_independent_oracle_refuses_matching_source_output_backdating(timeframe, count, which):
    from moex_data.rub_factual_release_acceptance import _fx_arithmetic_completeness

    value = retained_block(timeframe, count)
    claimed = fx.describe(value["observed_context_evidence"], now=NOW)
    change_rows(value, which)
    targets = claimed["observations"] if which == "all" else [claimed["observations"][0]]
    for row in targets:
        row["build_ts_utc"] = EARLIER.isoformat()
    # Inject matching changes into source and output without calling apply.
    # An oracle that trusts the shared descriptor's rows would miss this.
    value["observed_context"] = claimed
    before = deepcopy(value)
    with pytest.raises(AssertionError, match="FX retained build witness refusal"):
        _fx_arithmetic_completeness(value, now=NOW)
    assert value == before
    fx.apply(value, NOW)
    assert value["observed_context"]["status"] == "UNAVAILABLE"
    _fx_arithmetic_completeness(value, now=NOW)


@pytest.mark.parametrize("timeframe", ["1D", "1W"])
@pytest.mark.parametrize("count", [1, 3])
def test_full_release_and_export_refuse_rewritten_builds_without_archive_changes(timeframe, count, tmp_path):
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness

    value = retained_block(timeframe, count)
    change_rows(value, "all")
    snapshot = {
        "identity": {"generated_at_utc": PREBUILD.isoformat()},
        "components": {"stage9_daily": {"status": "READY", "data": {
            "server_core": {"blocks": [value]}}}},
    }
    before = deepcopy(snapshot)
    result = release.build(snapshot, now=PREBUILD, code_revision="a" * 40)
    assert result["timeframe_context"] == []
    assert release.compact(snapshot, now=PREBUILD, code_revision="a" * 40)["timeframe_context"] == []
    later = release.build(snapshot, now=NOW, code_revision="a" * 40)
    selected = later["timeframe_context"][0]["values"]
    assert selected["selected_observation"] == before["components"]["stage9_daily"]["data"]["server_core"]["blocks"][0]["selected_observation"]
    assert selected["observed_context"]["status"] == "UNAVAILABLE"
    projection_completeness(snapshot, result, now=PREBUILD)
    directory = release.export(snapshot, now=PREBUILD, code_revision="a" * 40, output=tmp_path)
    frozen_bytes = (directory / "input_snapshot.json").read_bytes()
    frozen = json.loads(frozen_bytes)
    assert release.build(frozen, now=PREBUILD, code_revision="a" * 40) == result
    assert (directory / "input_snapshot.json").read_bytes() == frozen_bytes
    assert snapshot == before


@pytest.mark.parametrize("component", ["stage9_daily", "stage9_weekly"])
@pytest.mark.parametrize("timeframe", ["1H", "1D", "1W"])
@pytest.mark.parametrize("kind", ["ohlcv", "technical"])
@pytest.mark.parametrize("defect", ["future", "missing", "null", "naive", "invalid", "equal", "offset"])
def test_whole_stage7_build_gate_and_independent_oracle(component, timeframe, kind, defect):
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness

    value = {
        "block_id": "stage7." + kind + "." + timeframe,
        "stage": 7, "timeframe": timeframe, "status": "ready",
        "dataset_id": "rub_native_ohlcv_htf" if kind == "ohlcv" else "rub_technical_features_htf",
        "selected_causal_ts_utc": EARLIER.isoformat(),
        "selected_observation": {"close": 81.0, "build_ts_utc": EARLIER.isoformat()},
    }
    valid = deepcopy(value)
    valid["block_id"] += ".independent"
    if defect == "missing":
        value["selected_observation"].pop("build_ts_utc")
    else:
        value["selected_observation"]["build_ts_utc"] = {
            "future": (PREBUILD + timedelta(microseconds=1)).isoformat(),
            "null": None, "naive": PREBUILD.replace(tzinfo=None).isoformat(),
            "invalid": "bad-clock", "equal": PREBUILD.isoformat(),
            "offset": PREBUILD.astimezone(timezone(timedelta(hours=3))).isoformat(),
        }[defect]
    snapshot = {"identity": {"generated_at_utc": PREBUILD.isoformat()}, "components": {
        component: {"status": "READY", "data": {"server_core": {"blocks": [value, valid]}}}}}
    before = deepcopy(snapshot)
    result = release.build(snapshot, now=PREBUILD, code_revision="a" * 40)
    allowed = defect in ("equal", "offset")
    expected_ids = {valid["block_id"]} | ({value["block_id"]} if allowed else set())
    assert {entry["values"]["block_id"] for entry in result["timeframe_context"]} == expected_ids
    projection_completeness(snapshot, result, now=PREBUILD)
    compact = release.compact(snapshot, now=PREBUILD, code_revision="a" * 40)
    assert {entry["values"]["block_id"] for entry in compact["timeframe_context"]} == expected_ids
    if not allowed:
        forged = deepcopy(result)
        forged["timeframe_context"].append({"values": deepcopy(value)})
        with pytest.raises(AssertionError, match="timeframe completeness"):
            projection_completeness(snapshot, forged, now=PREBUILD)
    assert snapshot == before
