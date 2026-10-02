from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase07a_data_expansion_admission as phase07a


def _scope(records):
    return SimpleNamespace(records=tuple(records))


def test_prefix_records_accepts_exact_frozen_prefix(monkeypatch):
    records = [
        {"trade_date": "2022-04-26", "sha256": "a" * 64, "row_count": 10},
        {"trade_date": "2022-04-27", "sha256": "b" * 64, "row_count": 20},
    ]
    monkeypatch.setattr(phase07a, "PREFIX_START", "2022-04-26")
    monkeypatch.setattr(phase07a, "PREFIX_END", "2022-04-27")
    monkeypatch.setattr(phase07a, "EXPECTED_DATE_COUNT", 2)
    monkeypatch.setattr(phase07a, "EXPECTED_RAW_ROW_COUNT", 30)
    import hashlib
    expected = hashlib.sha256(
        "".join(f"{row['trade_date']}\t{row['sha256']}\n" for row in records).encode()
    ).hexdigest()
    monkeypatch.setattr(phase07a, "EXPECTED_PREFIX_CONTENT_SHA256", expected)
    got, dates, rows, digest = phase07a._prefix_records(_scope(records))
    assert len(got) == 2
    assert dates == ("2022-04-26", "2022-04-27")
    assert rows == 30
    assert digest == expected


def test_target_identity_uses_previous_admitted_date(monkeypatch):
    monkeypatch.setattr(phase07a, "EXPECTED_TARGET_COUNT", 3)
    monkeypatch.setattr(phase07a, "PREFIX_END", "2026-10-01")
    frame = phase07a._target_identities(
        ("2022-04-26", "2022-04-27", "2024-08-05", "2026-10-01")
    )
    assert frame["prior_trade_date"].tolist() == ["2022-04-26", "2022-04-27", "2024-08-05"]
    assert frame["evaluation_segment"].tolist() == [
        "pre_discovery_extension",
        "discovery_reference",
        "forward_extension",
    ]


def test_prefix_sha_mismatch_fails_closed(monkeypatch):
    records = [{"trade_date": "2022-04-26", "sha256": "a" * 64, "row_count": 10}]
    monkeypatch.setattr(phase07a, "PREFIX_START", "2022-04-26")
    monkeypatch.setattr(phase07a, "PREFIX_END", "2022-04-26")
    monkeypatch.setattr(phase07a, "EXPECTED_DATE_COUNT", 1)
    monkeypatch.setattr(phase07a, "EXPECTED_RAW_ROW_COUNT", 10)
    monkeypatch.setattr(phase07a, "EXPECTED_PREFIX_CONTENT_SHA256", "0" * 64)
    with pytest.raises(phase07a.Phase07AError, match="content SHA256 mismatch"):
        phase07a._prefix_records(_scope(records))


def test_segmentation_boundaries():
    assert phase07a._segment("2024-08-04") == "pre_discovery_extension"
    assert phase07a._segment("2024-08-05") == "discovery_reference"
    assert phase07a._segment("2026-06-11") == "discovery_reference"
    assert phase07a._segment("2026-06-12") == "forward_extension"


def test_validate_brent_minimal_valid_grid():
    identities = pd.DataFrame(
        [
            {
                "target_trade_date": "2024-08-05",
                "target_instrument_id": "forts.usdrubf",
                "prior_trade_date": "2024-08-02",
                "evaluation_segment": "discovery_reference",
            },
            {
                "target_trade_date": "2026-06-12",
                "target_instrument_id": "forts.usdrubf",
                "prior_trade_date": "2026-06-11",
                "evaluation_segment": "forward_extension",
            },
            {
                "target_trade_date": "2024-08-02",
                "target_instrument_id": "forts.usdrubf",
                "prior_trade_date": "2024-08-01",
                "evaluation_segment": "pre_discovery_extension",
            },
        ]
    )
    identities = identities.iloc[[2, 0, 1]].reset_index(drop=True)
    universe = pd.DataFrame(
        {
            "source_id": [phase07a.phase84a.SOURCE_ID],
            "asset_code": [phase07a.phase84a.ASSET_CODE],
            "board_id": [phase07a.phase84a.BOARD_ID],
            "metadata_route": ["https://iss.moex.com/iss/securities/BRQ4.json"],
            "enumeration_route": ["https://iss.moex.com/iss/history/x"],
            "metadata_raw_payload_sha256": ["a" * 64],
            "enumeration_raw_payload_sha256": ["b" * 64],
            "metadata_retrieved_at_utc": ["2026-10-02T10:00:00+00:00"],
            "enumeration_retrieved_at_utc": ["2026-10-02T10:00:01+00:00"],
        }
    )
    candles = pd.DataFrame(
        {
            "source_id": [phase07a.phase84a.SOURCE_ID],
            "source_route": [
                "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/BRQ4/candles.json"
            ],
            "raw_payload_sha256": ["c" * 64],
            "retrieved_at_utc": ["2026-10-02T10:00:02+00:00"],
        }
    )
    matrix = pd.DataFrame(
        {
            "target_trade_date": identities["target_trade_date"],
            "target_instrument_id": identities["target_instrument_id"],
            "prior_trade_date": identities["prior_trade_date"],
            "brent_trade_date": identities["prior_trade_date"],
            "brent_days_to_expiration": [30, 30, 30],
            "brent_candle_end": [
                "2024-08-01T23:50:00+03:00",
                "2024-08-02T23:50:00+03:00",
                "2026-06-11T23:50:00+03:00",
            ],
            "brent_contract_code": ["BRQ4", "BRQ4", "BRN6"],
            "brent_contract_changed": [False, False, True],
            "brent_previous_contract_code": [None, "BRQ4", "BRQ4"],
            "brent_retrieved_at_utc": [
                "2026-10-02T10:00:02+00:00",
                "2026-10-02T10:00:03+00:00",
                "2026-10-02T10:00:04+00:00",
            ],
        }
    )
    rolls = pd.DataFrame(
        [
            {
                "target_or_future_information_used": False,
                "cross_contract_return_calculated": False,
            }
        ]
    )
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(phase07a, "EXPECTED_TARGET_COUNT", 3)
    try:
        gates = phase07a._validate_brent(identities, universe, candles, matrix, rolls)
    finally:
        monkeypatch.undo()
    assert all(item["passed"] for item in gates.values())


def test_non_utc_brent_provenance_fails_gate():
    identities = pd.DataFrame(
        [
            {
                "target_trade_date": "2024-08-05",
                "target_instrument_id": "forts.usdrubf",
                "prior_trade_date": "2024-08-02",
                "evaluation_segment": "discovery_reference",
            },
            {
                "target_trade_date": "2026-06-12",
                "target_instrument_id": "forts.usdrubf",
                "prior_trade_date": "2026-06-11",
                "evaluation_segment": "forward_extension",
            },
            {
                "target_trade_date": "2024-08-02",
                "target_instrument_id": "forts.usdrubf",
                "prior_trade_date": "2024-08-01",
                "evaluation_segment": "pre_discovery_extension",
            },
        ]
    ).iloc[[2, 0, 1]].reset_index(drop=True)
    universe = pd.DataFrame(
        {
            "source_id": [phase07a.phase84a.SOURCE_ID],
            "asset_code": [phase07a.phase84a.ASSET_CODE],
            "board_id": [phase07a.phase84a.BOARD_ID],
            "metadata_route": ["https://iss.moex.com/iss/securities/BRQ4.json"],
            "enumeration_route": ["https://iss.moex.com/iss/history/x"],
            "metadata_raw_payload_sha256": ["a" * 64],
            "enumeration_raw_payload_sha256": ["b" * 64],
            "metadata_retrieved_at_utc": ["2026-10-02T10:00:00"],
            "enumeration_retrieved_at_utc": ["2026-10-02T10:00:01+00:00"],
        }
    )
    candles = pd.DataFrame(
        {
            "source_id": [phase07a.phase84a.SOURCE_ID],
            "source_route": [
                "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/BRQ4/candles.json"
            ],
            "raw_payload_sha256": ["c" * 64],
            "retrieved_at_utc": ["2026-10-02T10:00:02+00:00"],
        }
    )
    matrix = pd.DataFrame(
        {
            "target_trade_date": identities["target_trade_date"],
            "target_instrument_id": identities["target_instrument_id"],
            "prior_trade_date": identities["prior_trade_date"],
            "brent_trade_date": identities["prior_trade_date"],
            "brent_days_to_expiration": [30, 30, 30],
            "brent_candle_end": [
                "2024-08-01T23:50:00+03:00",
                "2024-08-02T23:50:00+03:00",
                "2026-06-11T23:50:00+03:00",
            ],
            "brent_contract_code": ["BRQ4", "BRQ4", "BRN6"],
            "brent_contract_changed": [False, False, True],
            "brent_previous_contract_code": [None, "BRQ4", "BRQ4"],
            "brent_retrieved_at_utc": [
                "2026-10-02T10:00:02+00:00",
                "2026-10-02T10:00:03+00:00",
                "2026-10-02T10:00:04+00:00",
            ],
        }
    )
    rolls = pd.DataFrame(
        [{"target_or_future_information_used": False, "cross_contract_return_calculated": False}]
    )
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(phase07a, "EXPECTED_TARGET_COUNT", 3)
    try:
        gates = phase07a._validate_brent(identities, universe, candles, matrix, rolls)
    finally:
        monkeypatch.undo()
    assert gates["G3_brent_official_identity"]["passed"] is False
