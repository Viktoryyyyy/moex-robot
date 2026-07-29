from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from moex_research.external_data import moex_cnyrubf_algopack_history as source
from moex_research.runners import (
    usdrubf_phase8_6a_algopack_cnyrubf_source_validation as runner,
)


NOW = datetime(2026, 7, 29, 10, 0, tzinfo=timezone.utc)


def _identity() -> source.CnyrubfSecurityIdentity:
    return source.CnyrubfSecurityIdentity(
        source_id=source.SOURCE_ID,
        security_id=source.SECURITY_ID,
        asset_code=source.ASSET_CODE,
        board_id=source.BOARD_ID,
        engine=source.ENGINE,
        market=source.MARKET,
        primary_board=True,
        active_board=True,
        history_from=date(2022, 6, 1),
        history_till=None,
        metadata_route=source.build_security_metadata_url(),
        retrieved_at_utc=NOW,
        raw_payload_sha256="a" * 64,
        source_revision_status="official_iss_current_revision",
        historical_model_use_status=source.HISTORICAL_MODEL_USE_STATUS,
    )


def _candle() -> source.CnyrubfAlgoPackDailyCandle:
    return source.CnyrubfAlgoPackDailyCandle(
        source_id=source.SOURCE_ID,
        security_id=source.SECURITY_ID,
        asset_code=source.ASSET_CODE,
        board_id=source.BOARD_ID,
        engine=source.ENGINE,
        market=source.MARKET,
        trade_date=date(2026, 6, 10),
        open=12.0,
        high=12.4,
        low=11.9,
        close=12.3,
        volume=30.0,
        volume_buy=14.0,
        volume_sell=16.0,
        volume_imbalance=-2.0 / 30.0,
        value=366.0,
        value_buy=170.4,
        value_sell=195.6,
        trades=13,
        trades_buy=6,
        trades_sell=7,
        initial_margin_close=1100.0,
        open_interest_open=100.0,
        open_interest_high=120.0,
        open_interest_low=90.0,
        open_interest_close=115.0,
        candle_begin=datetime(2026, 6, 10, 9, 0, tzinfo=source.MOSCOW),
        candle_end=datetime(2026, 6, 10, 23, 50, tzinfo=source.MOSCOW),
        source_available_at=datetime(2026, 6, 10, 23, 50, 3, tzinfo=source.MOSCOW),
        source_route=source.build_tradestats_url(
            date(2026, 6, 10),
            date(2026, 6, 10),
        ),
        retrieved_at_utc=NOW,
        raw_payload_sha256="b" * 64,
        source_revision_status=source.SOURCE_REVISION_STATUS,
        historical_model_use_status=source.HISTORICAL_MODEL_USE_STATUS,
    )


def _eligible() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "target_trade_date": "2026-06-11",
                "target_instrument_id": "forts.usdrubf",
                "prior_trade_date": "2026-06-10",
            }
        ]
    )


def _route_validation() -> dict[str, object]:
    return {
        "official_hosts": ["apim.moex.com", "iss.moex.com"],
        "tradestats_route": source.ALGOPACK_TRADESTATS_ROUTE,
        "security_metadata_route": source.build_security_metadata_url(),
        "bucket_interval_minutes": 5,
        "tradetime_semantics": "completed_five_minute_interval_end",
        "pagination_complete": True,
        "schema_stable_within_run": True,
        "directional_fields_present": True,
        "open_interest_fields_present": True,
        "source_availability_present": True,
        "redirects_allowed": False,
        "fallback_used": False,
    }


def test_merged_correction_contract_is_accepted() -> None:
    contract_path = Path(
        "contracts/experiments/"
        "usdrubf_phase8_6a_algopack_cnyrubf_fo_source_correction_v1.json"
    )
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    runner._validate_experiment_contract(contract)


def test_metadata_columns_and_logical_keys_cannot_be_conflated() -> None:
    contract_path = Path(
        "contracts/experiments/"
        "usdrubf_phase8_6a_algopack_cnyrubf_fo_source_correction_v1.json"
    )
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract["metadata_identity_policy"]["required_description_columns"] = ["SECID"]
    with pytest.raises(runner.Phase86ACnyrubfSourceValidationError):
        runner._validate_experiment_contract(contract)


def test_acceptance_matrix_contains_only_source_and_identity_fields() -> None:
    matrix, diagnostics = runner.build_cnyrubf_pit_acceptance_matrix(
        _eligible(),
        [_candle()],
    )
    assert tuple(matrix.columns) == runner.ACCEPTANCE_MATRIX_COLUMNS
    assert not set(matrix.columns) & runner.FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS
    assert diagnostics.accepted.tolist() == [True]
    assert diagnostics.target_derived_field_used.tolist() == [False]
    assert matrix.cnyrubf_security_id.tolist() == ["CNYRUBF"]
    assert matrix.cnyrubf_asset_code.tolist() == ["CNYRUBTOM"]
    assert matrix.cnyrubf_open_interest_close.tolist() == [115.0]


def test_missing_exact_prior_date_is_not_filled() -> None:
    matrix, diagnostics = runner.build_cnyrubf_pit_acceptance_matrix(
        _eligible(),
        [],
    )
    assert matrix.cnyrubf_trade_date.isna().all()
    assert diagnostics.accepted.tolist() == [False]
    assert diagnostics.forward_fill_used.tolist() == [False]
    assert diagnostics.backward_fill_used.tolist() == [False]
    assert diagnostics.arbitrary_date_selection_used.tolist() == [False]
    assert diagnostics.source_substitution_used.tolist() == [False]


def test_g8_fails_closed_on_target_derived_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runner, "EXPECTED_ELIGIBLE_IDENTITIES", 1)
    monkeypatch.setattr(runner, "EXPECTED_VALIDATION_IDENTITIES", 1)
    eligible = _eligible()
    validation = eligible.loc[:, runner.IDENTITY_COLUMNS].copy()
    matrix, diagnostics = runner.build_cnyrubf_pit_acceptance_matrix(
        eligible,
        [_candle()],
    )
    matrix["y_true"] = "B"
    coverage = pd.DataFrame(
        [
            {
                "source_id": source.SOURCE_ID,
                "eligible_identity_count": 1,
                "eligible_covered_count": 1,
                "eligible_missing_count": 0,
                "eligible_coverage_pct": 100.0,
                "validation_identity_count": 1,
                "validation_covered_count": 1,
                "validation_missing_count": 0,
                "validation_coverage_pct": 100.0,
            }
        ]
    )
    gates = runner.evaluate_gates(
        immutable_inputs_verified=True,
        phase83_verified=True,
        eligible=eligible,
        validation=validation,
        identity=_identity(),
        candles=runner.normalized_candles([_candle()]),
        matrix=matrix,
        coverage=coverage,
        diagnostics=diagnostics,
        route_validation=_route_validation(),
    )
    assert gates["G8_no_fallback_or_target_leakage"]["passed"] is False
    assert gates["G8_no_fallback_or_target_leakage"]["forbidden_fields_present"] == [
        "y_true"
    ]
    assert (
        gates["G9_final_source_readiness"]["blocker_classification"]
        == "target_derived_field_leakage"
    )


@pytest.mark.parametrize(
    "run_id",
    [
        "phase8_6a_algopack_cnyrub_source_validation_20260729_v1",
        "phase8_6a_algopack_cnyrubf_source_validation_latest",
        "phase8_6a_algopack_cnyrubf_source_validation_20260729_v0",
    ],
)
def test_noncanonical_run_ids_are_rejected(
    tmp_path: Path,
    run_id: str,
) -> None:
    paths: list[Path] = []
    for index, suffix in enumerate(
        [".parquet", ".json", ".json", ".parquet", ".json", ".json", ".json"]
    ):
        path = tmp_path / f"input_{index}{suffix}"
        path.write_bytes(b"x")
        paths.append(path)
    request = runner.Phase86ARequest(
        modeling_dataset_path=paths[0],
        dataset_manifest_path=paths[1],
        feature_schema_path=paths[2],
        m0_validation_predictions_path=paths[3],
        phase83_aggregate_metrics_path=paths[4],
        phase83_gate_results_path=paths[5],
        experiment_contract_path=paths[6],
        output_dir=tmp_path / "out",
        run_id=run_id,
        git_commit_sha="a" * 40,
    )
    with pytest.raises(runner.Phase86ACnyrubfSourceValidationError):
        runner._validate_request(request)


def test_runtime_artifact_inventory_is_exact(tmp_path: Path) -> None:
    output = tmp_path / "artifacts"
    empty = pd.DataFrame()
    payloads: dict[str, object] = {
        "input_identity_verification.json": {},
        "official_route_validation.json": {},
        "cnyrubf_security_identity.json": {},
        "cnyrubf_daily_candles_normalized.parquet": empty,
        "cnyrubf_pit_acceptance_matrix.parquet": empty,
        "coverage_by_source.csv": empty,
        "session_alignment_diagnostics.csv": empty,
        "source_blocker_register.json": {},
        "gate_results.json": {},
    }
    runner._write_exact_artifacts(output, payloads)
    assert sorted(path.name for path in output.iterdir()) == sorted(
        runner.DECLARED_OUTPUT_ARTIFACTS
    )
