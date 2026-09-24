"""Synthetic regressions for the integer prerequisite of issue #547.

These are reconstructed examples, not production HTTP replay. The joint
date/root-v2 repair remains a separate, still-required part of the same PR.
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
