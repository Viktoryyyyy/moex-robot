from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from moex_research.external_data.moex_cnyrub_algopack_history import (
    CnyrubAlgoPackDailyCandle,
    CnyrubAlgoPackError,
    CnyrubSecurityIdentity,
)
from moex_research.runners import (
    usdrubf_phase8_6a_algopack_cnyrub_source_validation as runner,
)

RETRIEVED = datetime(2026, 7, 17, 9, 0, tzinfo=timezone.utc)
DIGEST = "a" * 64


def _dates() -> list[pd.Timestamp]:
    all_dates = list(pd.bdate_range("2024-08-05", "2026-06-11"))
    return [*all_dates[:471], all_dates[-1]]


def _eligible() -> pd.DataFrame:
    targets = _dates()
    return pd.DataFrame(
        {
            "target_trade_date": [
                item.strftime("%Y-%m-%d") for item in targets
            ],
            "target_instrument_id": ["forts.usdrubf"] * 472,
            "prior_trade_date": [
                (item - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                for item in targets
            ],
        }
    )


def _validation(eligible: pd.DataFrame) -> pd.DataFrame:
    return eligible.loc[
        :319,
        list(runner.IDENTITY_COLUMNS),
    ].reset_index(drop=True)


def _identity() -> CnyrubSecurityIdentity:
    return CnyrubSecurityIdentity(
        source_id="moex_cnyrub_tom_daily",
        security_id="CNYRUB_TOM",
        board_id="CETS",
        engine="currency",
        market="selt",
        primary_board=True,
        active_board=True,
        history_from=date(2010, 9, 27),
        history_till=None,
        metadata_route=runner.build_metadata_route(),
        retrieved_at_utc=RETRIEVED,
        raw_payload_sha256=DIGEST,
        source_revision_status="official_iss_current_revision",
        historical_model_use_status="source_validation_only",
    )


def _candle(trade_date: date) -> CnyrubAlgoPackDailyCandle:
    begin = datetime.combine(
        trade_date,
        datetime.min.time(),
        tzinfo=timezone(timedelta(hours=3)),
    ).replace(hour=10)
    return CnyrubAlgoPackDailyCandle(
        source_id=runner.SOURCE_ID,
        security_id="CNYRUB_TOM",
        board_id="CETS",
        engine="currency",
        market="selt",
        trade_date=trade_date,
        open=11.0,
        high=12.0,
        low=10.0,
        close=11.5,
        volume=100.0,
        volume_buy=60.0,
        volume_sell=40.0,
        volume_imbalance=0.2,
        value=1000.0,
        value_buy=600.0,
        value_sell=400.0,
        trades=20,
        trades_buy=12,
        trades_sell=8,
        candle_begin=begin,
        candle_end=begin.replace(hour=23, minute=55),
        source_route=(
            "https://apim.moex.com/iss/datashop/algopack/fx/tradestats/"
            "CNYRUB_TOM.json?from=2024-01-01&till=2026-01-01&start=0"
        ),
        retrieved_at_utc=RETRIEVED,
        raw_payload_sha256=DIGEST,
        source_revision_status=runner.SOURCE_REVISION_STATUS,
        historical_model_use_status="source_validation_only",
    )


def _candles(
    eligible: pd.DataFrame,
) -> list[CnyrubAlgoPackDailyCandle]:
    return [
        _candle(date.fromisoformat(item))
        for item in eligible["prior_trade_date"]
    ]


def _route_validation() -> dict[str, object]:
    return {
        "official_host": runner.ALGOPACK_HOST,
        "tradestats_route": runner.ALGOPACK_TRADESTATS_ROUTE,
        "bucket_interval_minutes": runner.ALGOPACK_BUCKET_MINUTES,
        "pagination_complete": True,
        "schema_stable_within_run": True,
        "directional_volume_fields_present": True,
    }


def test_exact_directional_acceptance_matrix_schema() -> None:
    eligible = _eligible().iloc[:1].reset_index(drop=True)
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(
        eligible,
        _candles(eligible),
    )
    assert tuple(matrix.columns) == runner.ACCEPTANCE_MATRIX_COLUMNS
    assert matrix.loc[0, "cnyrub_volume_buy"] == 60
    assert matrix.loc[0, "cnyrub_volume_sell"] == 40
    assert matrix.loc[0, "cnyrub_value_buy"] == 600
    assert matrix.loc[0, "cnyrub_value_sell"] == 400
    assert matrix.loc[0, "cnyrub_volume_imbalance"] == 0.2
    assert diagnostics.loc[0, "accepted"]
    assert not set(matrix.columns) & runner._FORBIDDEN_MATRIX_FIELDS


def test_exact_472_and_320_directional_coverage_passes() -> None:
    eligible = _eligible()
    validation = _validation(eligible)
    candle_list = _candles(eligible)
    candles = runner.normalized_candles(candle_list)
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(
        eligible,
        candle_list,
    )
    coverage = runner._coverage(matrix, validation)
    gates = runner.evaluate_gates(
        immutable_inputs_verified=True,
        phase83_verified=True,
        eligible=eligible,
        validation=validation,
        identity=_identity(),
        candles=candles,
        matrix=matrix,
        coverage=coverage,
        diagnostics=diagnostics,
        route_validation=_route_validation(),
    )
    assert coverage.iloc[0].eligible_covered_count == 472
    assert coverage.iloc[0].validation_covered_count == 320
    assert gates["G3_algopack_tradestats_route_and_schema"]["passed"]
    assert gates[
        "G6_directional_volume_and_numerical_integrity"
    ] == {
        "passed": True,
        "volume_equals_buy_plus_sell": True,
        "value_equals_buy_plus_sell": True,
        "trades_equal_buy_plus_sell": True,
    }
    assert gates["G9_final_source_readiness"] == {
        "passed": True,
        "requires": [f"G{index}" for index in range(1, 9)],
        "failed_gates": [],
        "status": "moex_algopack_cnyrub_source_candidate_for_phase8_6b",
        "historical_model_use_status": "source_validation_only",
        "blocker_classification": None,
    }


@pytest.mark.parametrize(
    "column,value",
    [
        ("volume_sell", 41.0),
        ("value_buy", 603.0),
        ("trades_sell", 9),
        ("volume_imbalance", 1.1),
    ],
)
def test_directional_integrity_failure(
    column: str,
    value: object,
) -> None:
    eligible = _eligible()
    validation = _validation(eligible)
    candles = runner.normalized_candles(_candles(eligible))
    candles.loc[0, column] = value
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(
        eligible,
        _candles(eligible),
    )
    gates = runner.evaluate_gates(
        immutable_inputs_verified=True,
        phase83_verified=True,
        eligible=eligible,
        validation=validation,
        identity=_identity(),
        candles=candles,
        matrix=matrix,
        coverage=runner._coverage(matrix, validation),
        diagnostics=diagnostics,
        route_validation=_route_validation(),
    )
    assert not gates[
        "G6_directional_volume_and_numerical_integrity"
    ]["passed"]
    assert (
        gates["G9_final_source_readiness"]["blocker_classification"]
        == "numerical_or_chronology_integrity_failure"
    )


def test_subscription_failure_is_structured() -> None:
    eligible = _eligible()
    validation = _validation(eligible)
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(
        eligible,
        [],
    )
    coverage = runner._coverage(matrix, validation)
    error = CnyrubAlgoPackError(
        "MOEX_ALGOPACK_TOKEN is required",
        blocker="algopack_subscription_not_available",
    )
    routes = runner._official_route_validation(
        _identity(),
        runner.normalized_candles([]),
        error,
    )
    gates = runner.evaluate_gates(
        immutable_inputs_verified=True,
        phase83_verified=True,
        eligible=eligible,
        validation=validation,
        identity=_identity(),
        candles=runner.normalized_candles([]),
        matrix=matrix,
        coverage=coverage,
        diagnostics=diagnostics,
        route_validation=routes,
        source_error=error,
    )
    final = gates["G9_final_source_readiness"]
    assert not final["passed"]
    assert (
        final["blocker_classification"]
        == "algopack_subscription_not_available"
    )


def test_pit_rejection_is_excluded_and_structured() -> None:
    eligible = _eligible().iloc[:1].reset_index(drop=True)
    candle = _candles(eligible)[0]
    bad = CnyrubAlgoPackDailyCandle(
        **{
            **candle.as_record(),
            "candle_end": datetime.combine(
                date.fromisoformat(eligible.loc[0, "target_trade_date"]),
                datetime.min.time(),
                tzinfo=timezone(timedelta(hours=3)),
            ).replace(hour=6),
        }
    )
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(
        eligible,
        [bad],
    )
    assert matrix["cnyrub_trade_date"].isna().all()
    assert (
        diagnostics.loc[0, "blocker_classification"]
        == "point_in_time_cutoff_not_provable"
    )


def test_contract_matches_runner() -> None:
    contract_path = (
        Path(__file__).parents[2]
        / "contracts/experiments/"
        "usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2.json"
    )
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    runner._validate_experiment_contract(contract)
