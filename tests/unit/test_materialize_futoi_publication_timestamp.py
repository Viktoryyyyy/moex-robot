from __future__ import annotations

import pandas as pd
import pytest

from moex_data.futures import materialize_futoi_instrument as target


def _raw_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sess_id": 6263,
                "seqnum": 195,
                "tradedate": "2021-04-06",
                "tradetime": "18:30:00",
                "ticker": "Si",
                "clgroup": "FIZ",
                "pos": 53870,
                "pos_long": 668969,
                "pos_short": -615099,
                "pos_long_num": 14657,
                "pos_short_num": 9598,
                "systime": "2021-04-06 18:30:05",
            },
            {
                "sess_id": 6263,
                "seqnum": 195,
                "tradedate": "2021-04-06",
                "tradetime": "18:30:00",
                "ticker": "Si",
                "clgroup": "YUR",
                "pos": -53870,
                "pos_long": 814948,
                "pos_short": -868818,
                "pos_long_num": 267,
                "pos_short_num": 138,
                "systime": "2021-04-06 18:30:05",
            },
        ]
    )


def _normalized_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2021-04-06",
                "ts": pd.Timestamp("2021-04-06 18:30:00"),
                "moment": pd.Timestamp("2021-04-06 18:30:00"),
                "systime": pd.Timestamp("2021-04-06 18:30:05"),
                "secid": "SiU6",
                "clgroup": "FIZ",
                "sess_id": 6263,
                "seqnum": 195,
                "source_ticker": "SI",
                "pos": 53870,
                "pos_long": 668969,
                "pos_short": -615099,
                "pos_long_num": 14657,
                "pos_short_num": 9598,
            },
            {
                "trade_date": "2021-04-06",
                "ts": pd.Timestamp("2021-04-06 18:30:00"),
                "moment": pd.Timestamp("2021-04-06 18:30:00"),
                "systime": pd.Timestamp("2021-04-06 18:35:05"),
                "secid": "SiU6",
                "clgroup": "FIZ",
                "sess_id": 6263,
                "seqnum": 215,
                "source_ticker": "SI",
                "pos": 53415,
                "pos_long": 668882,
                "pos_short": -615467,
                "pos_long_num": 14665,
                "pos_short_num": 9613,
            },
        ]
    )


def _binding() -> dict[str, object]:
    return {
        "instrument_id": "si_futures_family",
        "canonical_symbol": "Si",
        "secid": "SiU6",
        "board": "RFUD",
        "market": "forts",
        "engine": "futures",
        "futoi.source_id": target.SOURCE_ID,
        "futoi.ticker": "si",
        "futoi.availability_status": "available",
        "futoi.probe_status": "completed",
        "futoi.enabled_for_materialization": False,
    }


def test_raw_source_validation_accepts_exact_rows() -> None:
    result = target._validate_raw_source_rows(_raw_rows(), "2021-04-06", "si")

    assert len(result) == 2


def test_raw_source_validation_rejects_malformed_tradetime_before_normalization() -> None:
    frame = _raw_rows()
    frame.loc[frame.index[0], "tradetime"] = "not-a-time"

    with pytest.raises(target.FutoiMaterializationError, match="invalid tradedate/tradetime reference timestamp"):
        target._validate_raw_source_rows(frame, "2021-04-06", "si")


def test_raw_source_validation_rejects_rows_outside_explicit_trade_date() -> None:
    frame = _raw_rows()
    frame.loc[frame.index[0], "tradedate"] = "2021-04-05"
    frame.loc[frame.index[0], "systime"] = "2021-04-05 18:30:05"

    with pytest.raises(target.FutoiMaterializationError, match="rows outside explicit trade_date"):
        target._validate_raw_source_rows(frame, "2021-04-06", "si")


def test_raw_source_validation_rejects_wrong_ticker() -> None:
    frame = _raw_rows()
    frame.loc[frame.index[0], "ticker"] = "CR"

    with pytest.raises(target.FutoiMaterializationError, match="ticker does not match explicit registry ticker"):
        target._validate_raw_source_rows(frame, "2021-04-06", "si")


def test_raw_source_validation_rejects_unsupported_clgroup() -> None:
    frame = _raw_rows()
    frame.loc[frame.index[0], "clgroup"] = "OTHER"

    with pytest.raises(target.FutoiMaterializationError, match="unsupported clgroup"):
        target._validate_raw_source_rows(frame, "2021-04-06", "si")


def test_raw_source_validation_allows_publication_after_midnight() -> None:
    frame = _raw_rows().iloc[:1].copy()
    frame.loc[frame.index[0], "tradetime"] = "23:59:55"
    frame.loc[frame.index[0], "systime"] = "2021-04-07 00:00:05"

    result = target._validate_raw_source_rows(frame, "2021-04-06", "si")

    assert len(result) == 1


def test_publication_systime_becomes_canonical_ts_and_preserves_distinct_snapshots() -> None:
    frame = target._enforce_publication_timestamp(_normalized_rows())
    cleaned, dropped = target._deduplicate_exact_source_duplicates(frame)

    assert dropped == 0
    assert len(cleaned) == 2
    assert cleaned["moment"].nunique() == 1
    assert cleaned["ts"].nunique() == 2
    assert cleaned["ts"].tolist() == [
        pd.Timestamp("2021-04-06 18:30:05"),
        pd.Timestamp("2021-04-06 18:35:05"),
    ]


def test_missing_publication_systime_fails_closed() -> None:
    frame = _normalized_rows().drop(columns=["systime"])

    with pytest.raises(target.FutoiMaterializationError, match="missing systime publication timestamp"):
        target._enforce_publication_timestamp(frame)


def test_invalid_publication_systime_fails_closed() -> None:
    frame = _normalized_rows()
    frame["systime"] = frame["systime"].astype(object)
    frame.loc[frame.index[0], "systime"] = "not-a-timestamp"

    with pytest.raises(target.FutoiMaterializationError, match="invalid systime publication timestamp"):
        target._enforce_publication_timestamp(frame)


def test_publication_after_midnight_is_allowed_for_same_source_trade_date() -> None:
    frame = _normalized_rows().iloc[:1].copy()
    frame.loc[frame.index[0], "moment"] = pd.Timestamp("2021-04-06 23:59:55")
    frame.loc[frame.index[0], "systime"] = pd.Timestamp("2021-04-07 00:00:05")

    result = target._enforce_publication_timestamp(frame)

    assert result.loc[result.index[0], "trade_date"] == "2021-04-06"
    assert result.loc[result.index[0], "moment"] == pd.Timestamp("2021-04-06 23:59:55")
    assert result.loc[result.index[0], "ts"] == pd.Timestamp("2021-04-07 00:00:05")


def test_source_reference_date_mismatch_fails_closed() -> None:
    frame = _normalized_rows().iloc[:1].copy()
    frame.loc[frame.index[0], "moment"] = pd.Timestamp("2021-04-07 00:00:00")
    frame.loc[frame.index[0], "systime"] = pd.Timestamp("2021-04-07 00:00:05")

    with pytest.raises(target.FutoiMaterializationError, match="source reference moment date does not match trade_date"):
        target._enforce_publication_timestamp(frame)


def test_publication_before_source_reference_time_fails_closed() -> None:
    frame = _normalized_rows().iloc[:1].copy()
    frame.loc[frame.index[0], "moment"] = pd.Timestamp("2021-04-06 18:30:05")
    frame.loc[frame.index[0], "systime"] = pd.Timestamp("2021-04-06 18:30:00")

    with pytest.raises(target.FutoiMaterializationError, match="publication systime precedes source reference moment"):
        target._enforce_publication_timestamp(frame)


def test_required_source_identifiers_accept_numeric_values() -> None:
    frame = _normalized_rows().copy()
    frame["sess_id"] = frame["sess_id"].astype(str)
    frame["seqnum"] = frame["seqnum"].astype(str)

    result = target._validate_required_source_identifiers(frame)

    assert result["sess_id"].tolist() == [6263, 6263]
    assert result["seqnum"].tolist() == [195, 215]


@pytest.mark.parametrize("field", ["sess_id", "seqnum"])
def test_large_integer_source_identifier_preserves_exact_precision(field: str) -> None:
    frame = _normalized_rows().iloc[:1].copy()
    frame[field] = ["9007199254740993"]

    result = target._validate_required_source_identifiers(frame)

    assert result.loc[result.index[0], field] == 9007199254740993


@pytest.mark.parametrize("field", ["sess_id", "seqnum"])
def test_missing_required_source_identifier_fails_closed(field: str) -> None:
    frame = _normalized_rows().drop(columns=[field])

    with pytest.raises(target.FutoiMaterializationError, match="missing required source identifier"):
        target._validate_required_source_identifiers(frame)


@pytest.mark.parametrize("field", ["sess_id", "seqnum"])
@pytest.mark.parametrize("invalid_value", [True, 1.5, float("inf"), float("-inf"), "not-a-number"])
def test_invalid_required_source_identifier_fails_closed(field: str, invalid_value: object) -> None:
    frame = _normalized_rows()
    frame[field] = frame[field].astype(object)
    frame.loc[frame.index[0], field] = invalid_value

    with pytest.raises(target.FutoiMaterializationError, match="invalid required source identifier"):
        target._validate_required_source_identifiers(frame)


def test_materializer_rejects_boolean_identifier_before_legacy_normalization(monkeypatch) -> None:
    frame = _raw_rows()
    frame["sess_id"] = frame["sess_id"].astype(object)
    frame.loc[frame.index[0], "sess_id"] = True
    normalize_called = False

    def fake_normalize(*args, **kwargs):
        nonlocal normalize_called
        normalize_called = True
        raise AssertionError("legacy normalization must not see invalid raw identifiers")

    monkeypatch.setattr(target, "_registry_binding", lambda *_: _binding())
    monkeypatch.setattr(target, "_fetch_exact", lambda *_: (frame, "https://apim.moex.com/futoi"))
    monkeypatch.setattr(target.legacy, "normalize_futoi", fake_normalize)

    with pytest.raises(target.FutoiMaterializationError, match="invalid required source identifier"):
        target.materialize_futoi_partition(
            trade_date="2021-04-06",
            instrument_id="si_futures_family",
            run_id="raw_identifier_pre_normalization_test",
        )

    assert normalize_called is False


def test_fetch_exact_rejects_case_normalized_duplicate_columns(monkeypatch) -> None:
    frame = pd.DataFrame(
        [[6263, True, 195, "2021-04-06", "18:30:00", "Si", "FIZ", 1, 2, -1, 2, 1, "2021-04-06 18:30:05"]],
        columns=[
            "sess_id",
            "SESS_ID",
            "seqnum",
            "tradedate",
            "tradetime",
            "ticker",
            "clgroup",
            "pos",
            "pos_long",
            "pos_short",
            "pos_long_num",
            "pos_short_num",
            "systime",
        ],
    )

    monkeypatch.setenv("MOEX_API_KEY", "test-token")
    monkeypatch.setattr(target.availability, "fetch_paged_frame", lambda *args, **kwargs: frame)

    with pytest.raises(target.FutoiMaterializationError, match="duplicate columns after case normalization"):
        target._fetch_exact("si", "2021-04-06", 5.0, "https://apim.moex.com")


def test_fetch_exact_requires_official_publication_fields(monkeypatch) -> None:
    def fake_fetch(*args, **kwargs):
        return pd.DataFrame(
            [
                {
                    "sess_id": 1,
                    "seqnum": 1,
                    "tradedate": "2026-08-17",
                    "tradetime": "07:05:00",
                    "ticker": "SI",
                    "clgroup": "FIZ",
                    "pos": 1,
                    "pos_long": 2,
                    "pos_short": -1,
                    "pos_long_num": 2,
                    "pos_short_num": 1,
                }
            ]
        )

    monkeypatch.setenv("MOEX_API_KEY", "test-token")
    monkeypatch.setattr(target.availability, "fetch_paged_frame", fake_fetch)

    with pytest.raises(target.FutoiMaterializationError, match="FUTOI APIM schema mismatch"):
        target._fetch_exact("si", "2026-08-17", 5.0, "https://apim.moex.com")
