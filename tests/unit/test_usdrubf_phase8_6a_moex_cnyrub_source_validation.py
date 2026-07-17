from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from moex_research.external_data.moex_cnyrub_history import (
    CnyrubDailyCandle,
    CnyrubSecurityIdentity,
)
from moex_research.runners import (
    usdrubf_phase8_6a_moex_cnyrub_source_validation as runner,
)

RETRIEVED = datetime(2026, 7, 17, 9, 0, tzinfo=timezone.utc)
DIGEST = "a" * 64
MOSCOW = ZoneInfo("Europe/Moscow")


def _dates() -> list[pd.Timestamp]:
    all_dates = list(pd.bdate_range("2024-08-05", "2026-06-11"))
    return [*all_dates[:471], all_dates[-1]]


def _eligible() -> pd.DataFrame:
    targets = _dates()
    return pd.DataFrame(
        {
            "target_trade_date": [item.strftime("%Y-%m-%d") for item in targets],
            "target_instrument_id": ["forts.usdrubf"] * 472,
            "prior_trade_date": [
                (item - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                for item in targets
            ],
        }
    )


def _modeling_dataset(eligible: pd.DataFrame) -> pd.DataFrame:
    frame = eligible.copy()
    frame["target_phase_label"] = ["B", "S", "OUT", "B"] * 118
    frame["target_is_labeled"] = True
    frame["target_source"] = "manual_phase_labels_v1"
    return frame


def _validation(eligible: pd.DataFrame) -> pd.DataFrame:
    return eligible.loc[:319, list(runner.IDENTITY_COLUMNS)].reset_index(drop=True)


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


def _candle(trade_date: date) -> CnyrubDailyCandle:
    begin = datetime.combine(trade_date, datetime.min.time(), tzinfo=MOSCOW).replace(
        hour=7
    )
    end = begin.replace(hour=23, minute=49, second=59)
    return CnyrubDailyCandle(
        source_id="moex_cnyrub_tom_daily",
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
        value=1000.0,
        candle_begin=begin,
        candle_end=end,
        source_route=(
            runner.CANDLE_ROUTE
            + "?from=2024-01-01&till=2026-06-10&interval=24&start=0"
        ),
        retrieved_at_utc=RETRIEVED,
        raw_payload_sha256=DIGEST,
        source_revision_status="official_iss_current_revision",
        historical_model_use_status="source_validation_only",
    )


def _candles(eligible: pd.DataFrame) -> list[CnyrubDailyCandle]:
    return [
        _candle(date.fromisoformat(item))
        for item in eligible["prior_trade_date"]
    ]


def _route_validation() -> dict[str, object]:
    return {
        "pagination_complete": True,
        "schema_stable_within_run": True,
        "daily_interval": 24,
        "daily_candle_route": runner.CANDLE_ROUTE,
    }


def _evaluate(
    eligible: pd.DataFrame,
    candles_list: list[CnyrubDailyCandle],
    *,
    matrix_mutator=None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    validation = _validation(eligible)
    candles = runner.normalized_candles(candles_list)
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(
        eligible,
        candles_list,
    )
    if matrix_mutator is not None:
        matrix_mutator(matrix)
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
    return matrix, diagnostics, gates


def test_exact_prior_trade_date_join_has_no_same_day_or_fill() -> None:
    eligible = _eligible().iloc[:2].reset_index(drop=True)
    exact = _candle(date.fromisoformat(eligible.loc[0, "prior_trade_date"]))
    future = _candle(date.fromisoformat(eligible.loc[1, "target_trade_date"]))
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(
        eligible,
        [exact, future],
    )
    assert matrix.loc[0, "cnyrub_trade_date"] == eligible.loc[0, "prior_trade_date"]
    assert pd.isna(matrix.loc[1, "cnyrub_trade_date"])
    assert diagnostics.loc[1, "reason"] == "missing_exact_prior_trade_date_candle"
    assert not diagnostics[
        ["same_day_or_future_used", "forward_fill_used", "backward_fill_used"]
    ].any().any()


def test_forward_fill_and_backward_fill_are_refused() -> None:
    eligible = _eligible().iloc[:1].reset_index(drop=True)
    prior = date.fromisoformat(eligible.loc[0, "prior_trade_date"])
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(
        eligible,
        [_candle(prior - timedelta(days=1)), _candle(prior + timedelta(days=1))],
    )
    assert matrix["cnyrub_trade_date"].isna().all()
    assert diagnostics.loc[0, "reason"] == "missing_exact_prior_trade_date_candle"


def test_exact_472_and_320_coverage_passes_all_gates() -> None:
    eligible = _eligible()
    matrix, _, gates = _evaluate(eligible, _candles(eligible))
    assert runner._coverage(matrix, _validation(eligible)).iloc[0].to_dict()[
        "eligible_covered_count"
    ] == 472
    assert runner._coverage(matrix, _validation(eligible)).iloc[0].to_dict()[
        "validation_covered_count"
    ] == 320
    assert gates["G9_final_source_readiness"] == {
        "passed": True,
        "requires": [f"G{index}" for index in range(1, 9)],
        "failed_gates": [],
        "status": "moex_cnyrub_source_candidate_for_phase8_6b",
        "historical_model_use_status": "source_validation_only",
        "blocker_classification": None,
    }


def test_incomplete_coverage_is_structured_fail() -> None:
    eligible = _eligible()
    matrix, _, gates = _evaluate(eligible, _candles(eligible)[:-1])
    coverage = runner._coverage(matrix, _validation(eligible))
    final = gates["G9_final_source_readiness"]
    assert gates["G5_exact_coverage"]["passed"] is False
    assert coverage.iloc[0]["eligible_covered_count"] == 471
    assert final["status"] == "moex_cnyrub_source_not_ready"
    assert final["historical_model_use_status"] == "blocked"
    assert final["blocker_classification"] == "incomplete_identity_coverage"
    assert "G5" in final["failed_gates"]


def test_acceptance_matrix_has_exact_schema_revision_and_no_forbidden_fields() -> None:
    eligible = _eligible().iloc[:1].reset_index(drop=True)
    matrix, _ = runner.build_cnyrub_pit_acceptance_matrix(
        eligible,
        _candles(eligible),
    )
    assert tuple(matrix.columns) == runner.ACCEPTANCE_MATRIX_COLUMNS
    assert matrix.loc[0, "cnyrub_source_revision_status"] == (
        "official_iss_current_revision"
    )
    assert not (set(matrix.columns) & runner._FORBIDDEN_MATRIX_FIELDS)


def test_g7_fails_for_missing_or_wrong_source_revision_status() -> None:
    eligible = _eligible()
    for value in (None, "stale_or_unknown_revision"):
        _, _, gates = _evaluate(
            eligible,
            _candles(eligible),
            matrix_mutator=lambda matrix, value=value: matrix.__setitem__(
                "cnyrub_source_revision_status",
                [value, *(["official_iss_current_revision"] * 471)],
            ),
        )
        assert gates["G7_provenance"]["passed"] is False
        assert gates["G7_provenance"]["source_revision_status_valid"] is False
        assert "G7" in gates["G9_final_source_readiness"]["failed_gates"]


def _payloads() -> dict[str, object]:
    empty = pd.DataFrame()
    return {
        "input_identity_verification.json": {},
        "official_route_validation.json": {},
        "cnyrub_security_identity.json": {},
        "cnyrub_daily_candles_normalized.parquet": empty,
        "cnyrub_pit_acceptance_matrix.parquet": empty,
        "coverage_by_source.csv": empty,
        "session_alignment_diagnostics.csv": empty,
        "source_blocker_register.json": {},
        "gate_results.json": {},
    }


def test_exact_nine_artifact_inventory_and_no_outside_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: Path(path).write_bytes(b"PARQUET"),
    )
    output = tmp_path / "run"
    runner._write_exact_artifacts(output, _payloads())
    assert sorted(path.name for path in output.iterdir()) == sorted(
        runner.DECLARED_OUTPUT_ARTIFACTS
    )
    assert [path.name for path in tmp_path.iterdir()] == ["run"]


def test_output_directory_preexistence_is_rejected(tmp_path: Path) -> None:
    output = tmp_path / "existing"
    output.mkdir()
    with pytest.raises(
        runner.Phase86ACnyrubSourceValidationError,
        match="pre-exist",
    ):
        runner._write_exact_artifacts(output, _payloads())


def test_undeclared_artifact_is_rejected(tmp_path: Path) -> None:
    payloads = _payloads()
    payloads["extra.json"] = {}
    with pytest.raises(
        runner.Phase86ACnyrubSourceValidationError,
        match="inventory",
    ):
        runner._write_exact_artifacts(tmp_path / "run", payloads)


def test_no_model_fit_evaluation_serialization_or_promotion_contract() -> None:
    eligible = _eligible()
    _, _, gates = _evaluate(eligible, _candles(eligible))
    scope = gates["G8_leakage_and_scope"]
    assert scope["model_file_created"] is False
    assert scope["model_fit_or_evaluation_performed"] is False
    assert scope["promotion_performed"] is False
    assert scope["broker_or_trading_action_performed"] is False


def test_unit_path_uses_injected_objects_and_no_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner,
        "fetch_cnyrub_bytes_with_retry",
        lambda *_args, **_kwargs: pytest.fail("network must not run in unit test"),
    )
    eligible = _eligible().iloc[:1].reset_index(drop=True)
    matrix, _ = runner.build_cnyrub_pit_acceptance_matrix(
        eligible,
        _candles(eligible),
    )
    assert len(matrix) == 1


def test_contract_identity_and_retry_policy_match_json() -> None:
    contract_path = (
        Path(__file__).parents[2]
        / "contracts/experiments/usdrubf_phase8_6a_moex_cnyrub_source_validation_v1.json"
    )
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    runner._validate_experiment_contract(contract)


def test_pit_violation_produces_structured_exact_nine_artifact_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    eligible = _eligible()
    validation = _validation(eligible)
    modeling = _modeling_dataset(eligible)

    paths = {
        "modeling_dataset": tmp_path / "modeling.parquet",
        "dataset_manifest": tmp_path / "manifest.json",
        "feature_schema": tmp_path / "schema.json",
        "m0_validation_predictions": tmp_path / "predictions.parquet",
        "phase83_aggregate_metrics": tmp_path / "phase83_metrics.json",
        "phase83_gate_results": tmp_path / "phase83_gates.json",
        "experiment_contract": (
            Path(__file__).parents[2]
            / "contracts/experiments/usdrubf_phase8_6a_moex_cnyrub_source_validation_v1.json"
        ),
    }
    paths["modeling_dataset"].write_bytes(b"MODEL")
    paths["m0_validation_predictions"].write_bytes(b"PRED")
    paths["dataset_manifest"].write_text("{}\n", encoding="utf-8")
    paths["feature_schema"].write_text("{}\n", encoding="utf-8")
    paths["phase83_aggregate_metrics"].write_text(
        json.dumps({"final_status": runner.EXPECTED_PHASE83_STATUS}),
        encoding="utf-8",
    )
    paths["phase83_gate_results"].write_text(
        json.dumps(
            {
                "G12_final_acceptance": {
                    "status": runner.EXPECTED_PHASE83_STATUS,
                    "recommendation": runner.EXPECTED_PHASE83_RECOMMENDATION,
                }
            }
        ),
        encoding="utf-8",
    )

    request = runner.Phase86ARequest(
        modeling_dataset_path=paths["modeling_dataset"],
        dataset_manifest_path=paths["dataset_manifest"],
        feature_schema_path=paths["feature_schema"],
        m0_validation_predictions_path=paths["m0_validation_predictions"],
        phase83_aggregate_metrics_path=paths["phase83_aggregate_metrics"],
        phase83_gate_results_path=paths["phase83_gate_results"],
        experiment_contract_path=paths["experiment_contract"],
        output_dir=tmp_path / "run",
        run_id="phase8_6a_moex_cnyrub_source_validation_20260717_v1",
        git_commit_sha="b" * 40,
    )

    def read_parquet(path, *args, **kwargs):
        candidate = Path(path)
        if candidate == request.modeling_dataset_path:
            return modeling
        if candidate == request.m0_validation_predictions_path:
            return validation
        return pd.read_pickle(candidate)

    monkeypatch.setattr(pd, "read_parquet", read_parquet)
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: self.to_pickle(path),
    )
    monkeypatch.setattr(
        runner,
        "verify_immutable_inputs",
        lambda _request: dict(runner.EXPECTED_INPUT_SHA256),
    )

    candles = _candles(eligible)
    first_target = date.fromisoformat(eligible.loc[0, "target_trade_date"])
    offending = replace(
        candles[0],
        candle_end=datetime.combine(
            first_target,
            datetime.min.time(),
            tzinfo=MOSCOW,
        ).replace(hour=6),
    )
    candles[0] = offending

    result = runner.run_source_validation(
        request,
        transport=lambda _url: pytest.fail("transport must remain injected"),
        clock=lambda: RETRIEVED,
        identity_loader=lambda **_kwargs: _identity(),
        history_loader=lambda *_args, **_kwargs: candles,
    )

    assert result.final_status == "moex_cnyrub_source_not_ready"
    assert result.blocker_classification == "point_in_time_cutoff_not_provable"
    assert sorted(path.name for path in request.output_dir.iterdir()) == sorted(
        runner.DECLARED_OUTPUT_ARTIFACTS
    )

    gates = json.loads((request.output_dir / "gate_results.json").read_text())
    assert gates["G4_point_in_time_session_correctness"]["passed"] is False
    assert gates["G9_final_source_readiness"]["passed"] is False
    assert gates["G9_final_source_readiness"]["status"] == (
        "moex_cnyrub_source_not_ready"
    )
    assert gates["G9_final_source_readiness"]["blocker_classification"] == (
        "point_in_time_cutoff_not_provable"
    )

    blocker = json.loads(
        (request.output_dir / "source_blocker_register.json").read_text()
    )
    assert blocker["blocker_classification"] == "point_in_time_cutoff_not_provable"
    assert "prior-session forecast-anchor policy" in blocker["exact_blocker_reason"]
    assert blocker["offending_candle_used_in_acceptance_matrix"] is False
    assert blocker["fill_or_substitution_used"] is False

    matrix = pd.read_pickle(
        request.output_dir / "cnyrub_pit_acceptance_matrix.parquet"
    )
    first = matrix.iloc[0]
    assert first["target_trade_date"] == eligible.loc[0, "target_trade_date"]
    assert pd.isna(first["cnyrub_trade_date"])
    assert pd.isna(first["cnyrub_source_route"])
    assert pd.isna(first["cnyrub_source_revision_status"])
