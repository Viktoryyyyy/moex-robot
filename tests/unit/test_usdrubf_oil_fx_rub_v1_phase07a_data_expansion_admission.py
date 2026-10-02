from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

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


# Explicit regression inventory for the previously unchecked P2 boundaries.
UNVALIDATED_BOUNDARY_PATHS = [
    "usdrubf_frozen_prefix.prior_trade_date_rule",
    "brent_admission.official_host",
    "brent_admission.timezone",
    "brent_admission.contract_selection_rule",
    "evaluation_segmentation.pre_discovery_extension.rule",
    "evaluation_segmentation.discovery_reference.start_date",
    "evaluation_segmentation.discovery_reference.end_date",
    "evaluation_segmentation.forward_extension.rule",
    "evaluation_segmentation.phase07a_performance_evaluation_allowed",
    "authority_boundary.network_access_allowed",
    "authority_boundary.network_scope",
    "authority_boundary.source_mutation_allowed",
    "authority_boundary.model_fit_allowed",
    "authority_boundary.parameter_optimization_allowed",
    "authority_boundary.strategy_promotion_allowed",
    "authority_boundary.broker_action_allowed",
    "authority_boundary.trading_action_allowed",
]


@pytest.mark.parametrize("field", UNVALIDATED_BOUNDARY_PATHS)
@pytest.mark.parametrize("mutation", ["changed", "missing", "null", "wrong_type"])
def test_main_rejects_invalid_boundaries_before_io(tmp_path, monkeypatch, field, mutation):
    contract = json.loads(Path(
        "contracts/experiments/usdrubf_oil_fx_rub_v1_phase07a_data_expansion_admission.json"
    ).read_text(encoding="utf-8"))
    keys = field.split(".")
    parent = contract
    for key in keys[:-1]:
        parent = parent[key]
    old = parent[keys[-1]]
    if mutation == "missing":
        del parent[keys[-1]]
    elif mutation == "null":
        parent[keys[-1]] = None
    elif mutation == "wrong_type":
        parent[keys[-1]] = int(old) if type(old) is bool else []
    else:
        parent[keys[-1]] = not old if type(old) is bool else old + " changed"

    contract_path = tmp_path / "invalid_contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    data_root = tmp_path / "data"
    data_root.mkdir()
    output_dir = tmp_path / "outputs" / "admission"
    accepted = Mock(side_effect=AssertionError("accepted_current must not be read"))
    brent = Mock(side_effect=AssertionError("Brent must not be retrieved"))
    monkeypatch.setattr(phase07a.step7, "accepted_quote_history", accepted)
    monkeypatch.setattr(phase07a.phase84a, "build_brent_pit_matrix", brent)

    with pytest.raises(phase07a.Phase07AError, match=re.escape(field)):
        phase07a.main([
            "--contract-path", str(contract_path),
            "--data-root", str(data_root),
            "--output-dir", str(output_dir),
            "--run-id", "invalid-contract-test",
            "--git-commit-sha", "a" * 40,
        ])
    accepted.assert_not_called()
    brent.assert_not_called()
    assert not output_dir.parent.exists()
    assert list(data_root.iterdir()) == []
