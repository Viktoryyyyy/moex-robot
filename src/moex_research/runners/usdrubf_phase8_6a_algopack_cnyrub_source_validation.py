from __future__ import annotations

import argparse
import math
from collections.abc import Callable, Mapping
from dataclasses import asdict
from datetime import date, datetime
from typing import Any, Final

import pandas as pd

from moex_research.external_data.moex_cnyrub_algopack_history import (
    ALGOPACK_BUCKET_MINUTES,
    ALGOPACK_HOST,
    ALGOPACK_HTTP_MAX_ATTEMPTS,
    ALGOPACK_HTTP_RETRY_DELAYS_SECONDS,
    ALGOPACK_TOKEN_ENV,
    ALGOPACK_TRADESTATS_ROUTE,
    BOARD_ID,
    ENGINE,
    HISTORICAL_MODEL_USE_STATUS,
    MARKET,
    SECURITY_ID,
    SOURCE_ID,
    SOURCE_REVISION_STATUS,
    TRANSIENT_HTTP_ERROR_MESSAGE,
    AlgoPackTransport,
    CnyrubAlgoPackDailyCandle,
    CnyrubAlgoPackError,
    CnyrubSecurityIdentity,
    TokenLoader,
    UtcClock,
    build_security_metadata_url,
    fetch_algopack_bytes,
    load_algopack_token,
    load_daily_history,
    load_security_identity,
    utc_now,
    validate_prior_session_candle,
)
from moex_research.external_data.moex_cnyrub_history import CnyrubHistoryError
from moex_research.runners import (
    usdrubf_phase8_6a_moex_cnyrub_source_validation as base,
)

PROJECT: Final[str] = "MOEX Bot"
PHASE: Final[str] = "8.6A"
LANE: Final[str] = "ema_3_19_ai"
TASK_ID: Final[str] = (
    "ema_3_19_ai_phase_8_6a_algopack_cnyrub_source_validation_v2"
)
EXECUTION_MODE: Final[str] = "browser_chatgpt_github_direct"
CONTRACT_ID: Final[str] = (
    "usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2"
)
CONTRACT_VERSION: Final[str] = "2.0"
APPROVED_BRANCH: Final[str] = (
    "research/ema-3-19-ai/phase-8-6a-algopack-cnyrub-source-validation-v2"
)

EXPECTED_INPUT_SHA256 = base.EXPECTED_INPUT_SHA256
EXPECTED_ELIGIBLE_IDENTITIES = base.EXPECTED_ELIGIBLE_IDENTITIES
EXPECTED_VALIDATION_IDENTITIES = base.EXPECTED_VALIDATION_IDENTITIES
EXPECTED_FOLDS = base.EXPECTED_FOLDS
EXPECTED_VALIDATION_ROWS_PER_FOLD = base.EXPECTED_VALIDATION_ROWS_PER_FOLD
IDENTITY_COLUMNS = base.IDENTITY_COLUMNS
DIAGNOSTIC_COLUMNS = base.DIAGNOSTIC_COLUMNS
DECLARED_OUTPUT_ARTIFACTS = base.DECLARED_OUTPUT_ARTIFACTS
REQUIRED_CLI_ARGS = base.REQUIRED_CLI_ARGS
Phase86ARequest = base.Phase86ARequest
Phase86AResult = base.Phase86AResult

ACCEPTANCE_MATRIX_COLUMNS: Final[tuple[str, ...]] = (
    "target_trade_date",
    "target_instrument_id",
    "prior_trade_date",
    "cnyrub_security_id",
    "cnyrub_board_id",
    "cnyrub_trade_date",
    "cnyrub_open",
    "cnyrub_high",
    "cnyrub_low",
    "cnyrub_close",
    "cnyrub_volume",
    "cnyrub_volume_buy",
    "cnyrub_volume_sell",
    "cnyrub_volume_imbalance",
    "cnyrub_value",
    "cnyrub_value_buy",
    "cnyrub_value_sell",
    "cnyrub_trades",
    "cnyrub_trades_buy",
    "cnyrub_trades_sell",
    "cnyrub_candle_begin",
    "cnyrub_candle_end",
    "cnyrub_source_route",
    "cnyrub_payload_sha256",
    "cnyrub_retrieved_at_utc",
    "cnyrub_source_revision_status",
)
NORMALIZED_SOURCE_COLUMNS: Final[tuple[str, ...]] = (
    "source_id",
    "security_id",
    "board_id",
    "engine",
    "market",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "volume_buy",
    "volume_sell",
    "volume_imbalance",
    "value",
    "value_buy",
    "value_sell",
    "trades",
    "trades_buy",
    "trades_sell",
    "candle_begin",
    "candle_end",
    "source_route",
    "retrieved_at_utc",
    "raw_payload_sha256",
    "source_revision_status",
    "historical_model_use_status",
)
BLOCKER_CLASSIFICATIONS: Final[tuple[str, ...]] = (
    "security_identity_not_reproducible",
    "algopack_subscription_not_available",
    "algopack_authorization_failed",
    "algopack_tradestats_not_available",
    "algopack_schema_not_stable",
    "official_schema_not_stable",
    "point_in_time_cutoff_not_provable",
    "incomplete_identity_coverage",
    "numerical_or_chronology_integrity_failure",
    "provenance_not_sufficient",
    "other_fail_closed_with_exact_reason",
)
EXPECTED_TRANSIENT_HTTP_RETRY_POLICY: Final[dict[str, object]] = {
    "bounded_transient_retry_enabled": True,
    "enabled_for_source_id": SOURCE_ID,
    "phase_scope": "8.6A_algopack_v2_only",
    "maximum_total_attempts": ALGOPACK_HTTP_MAX_ATTEMPTS,
    "retry_delays_seconds": list(ALGOPACK_HTTP_RETRY_DELAYS_SECONDS),
    "random_jitter_allowed": False,
    "same_exact_official_route_only": True,
    "route_substitution_allowed": False,
    "fallback_source_allowed": False,
    "fallback_security_allowed": False,
    "fallback_board_allowed": False,
    "fallback_date_allowed": False,
    "retryable_exception": f"ExternalDataError({TRANSIENT_HTTP_ERROR_MESSAGE})",
    "semantic_failures_retried": False,
}
_FORBIDDEN_MATRIX_FIELDS = base._FORBIDDEN_MATRIX_FIELDS
_SHA256 = base._SHA256

IdentityLoader = Callable[..., CnyrubSecurityIdentity]
HistoryLoader = Callable[..., list[CnyrubAlgoPackDailyCandle]]
SourceError = CnyrubAlgoPackError | CnyrubHistoryError


class Phase86AAlgoPackSourceValidationError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        blocker: str = "other_fail_closed_with_exact_reason",
    ) -> None:
        super().__init__(message)
        self.blocker = (
            blocker
            if blocker in BLOCKER_CLASSIFICATIONS
            else "other_fail_closed_with_exact_reason"
        )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "python -m moex_research.runners."
            "usdrubf_phase8_6a_algopack_cnyrub_source_validation"
        )
    )
    for flag in REQUIRED_CLI_ARGS:
        parser.add_argument(flag, required=True)
    return parser


def request_from_args(arguments: argparse.Namespace) -> Phase86ARequest:
    return base.request_from_args(arguments)


def build_metadata_route() -> str:
    return build_security_metadata_url()


def _validate_experiment_contract(contract: Mapping[str, Any]) -> None:
    identity = {
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "project": PROJECT,
        "task_id": TASK_ID,
        "lane": LANE,
        "phase": PHASE,
        "execution_mode": EXECUTION_MODE,
        "status": "source_validation_only",
    }
    source = contract.get("source_identity", {})
    if (
        contract.get("contract_identity") != identity
        or contract.get("approved_branch") != APPROVED_BRANCH
    ):
        raise Phase86AAlgoPackSourceValidationError(
            "Phase 8.6A AlgoPack contract identity or branch mismatch"
        )
    if (
        tuple(contract.get("runtime_artifacts", ())) != DECLARED_OUTPUT_ARTIFACTS
        or contract.get("transient_http_retry_policy")
        != EXPECTED_TRANSIENT_HTTP_RETRY_POLICY
        or tuple(contract.get("acceptance_matrix_fields", ()))
        != ACCEPTANCE_MATRIX_COLUMNS
        or tuple(contract.get("normalized_source_required_fields", ()))
        != NORMALIZED_SOURCE_COLUMNS
        or contract.get("required_environment_variables")
        != [ALGOPACK_TOKEN_ENV]
    ):
        raise Phase86AAlgoPackSourceValidationError(
            "Phase 8.6A AlgoPack artifact, schema, environment, or retry "
            "contract mismatch"
        )
    if tuple(
        source.get(key)
        for key in ("security_id", "board_id", "engine", "market")
    ) != (SECURITY_ID, BOARD_ID, ENGINE, MARKET):
        raise Phase86AAlgoPackSourceValidationError(
            "Phase 8.6A AlgoPack source identity mismatch"
        )
    if (
        source.get("source_id") != SOURCE_ID
        or source.get("official_service") != "MOEX AlgoPack subscription"
        or source.get("tradestats_route") != ALGOPACK_TRADESTATS_ROUTE
    ):
        raise Phase86AAlgoPackSourceValidationError(
            "Phase 8.6A AlgoPack service or route mismatch"
        )


def normalized_candles(
    candles: list[CnyrubAlgoPackDailyCandle],
) -> pd.DataFrame:
    return pd.DataFrame(
        [candle.as_record() for candle in candles],
        columns=NORMALIZED_SOURCE_COLUMNS,
    )


def _empty_source_row() -> dict[str, None]:
    return {column: None for column in ACCEPTANCE_MATRIX_COLUMNS[3:]}


def build_cnyrub_pit_acceptance_matrix(
    eligible: pd.DataFrame,
    candles: list[CnyrubAlgoPackDailyCandle],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    keyed: dict[date, CnyrubAlgoPackDailyCandle] = {}
    for candle in candles:
        if candle.trade_date in keyed:
            raise Phase86AAlgoPackSourceValidationError(
                "duplicate AlgoPack CNYRUB date",
                blocker="numerical_or_chronology_integrity_failure",
            )
        keyed[candle.trade_date] = candle

    rows: list[dict[str, object]] = []
    diagnostics: list[dict[str, object]] = []
    for item in eligible.itertuples(index=False):
        target = date.fromisoformat(item.target_trade_date)
        prior = date.fromisoformat(item.prior_trade_date)
        candle = keyed.get(prior)
        accepted = False
        blocker: str | None = None
        candidate = None if candle is None else candle.trade_date.isoformat()

        if candle is None:
            source: dict[str, object] = _empty_source_row()
            reason = "missing_exact_prior_trade_date_algopack_aggregate"
        else:
            try:
                validate_prior_session_candle(
                    candle,
                    target_trade_date=target,
                    prior_trade_date=prior,
                )
            except CnyrubAlgoPackError as exc:
                if exc.blocker != "point_in_time_cutoff_not_provable":
                    raise Phase86AAlgoPackSourceValidationError(
                        str(exc), blocker=exc.blocker
                    ) from exc
                source = _empty_source_row()
                reason = str(exc)
                blocker = exc.blocker
            else:
                accepted = True
                reason = "accepted_exact_prior_trade_date_algopack_aggregate"
                source = {
                    "cnyrub_security_id": candle.security_id,
                    "cnyrub_board_id": candle.board_id,
                    "cnyrub_trade_date": candle.trade_date.isoformat(),
                    "cnyrub_open": candle.open,
                    "cnyrub_high": candle.high,
                    "cnyrub_low": candle.low,
                    "cnyrub_close": candle.close,
                    "cnyrub_volume": candle.volume,
                    "cnyrub_volume_buy": candle.volume_buy,
                    "cnyrub_volume_sell": candle.volume_sell,
                    "cnyrub_volume_imbalance": candle.volume_imbalance,
                    "cnyrub_value": candle.value,
                    "cnyrub_value_buy": candle.value_buy,
                    "cnyrub_value_sell": candle.value_sell,
                    "cnyrub_trades": candle.trades,
                    "cnyrub_trades_buy": candle.trades_buy,
                    "cnyrub_trades_sell": candle.trades_sell,
                    "cnyrub_candle_begin": candle.candle_begin.isoformat(),
                    "cnyrub_candle_end": candle.candle_end.isoformat(),
                    "cnyrub_source_route": candle.source_route,
                    "cnyrub_payload_sha256": candle.raw_payload_sha256,
                    "cnyrub_retrieved_at_utc": (
                        candle.retrieved_at_utc.isoformat().replace("+00:00", "Z")
                    ),
                    "cnyrub_source_revision_status": (
                        candle.source_revision_status
                    ),
                }

        rows.append(
            {
                "target_trade_date": item.target_trade_date,
                "target_instrument_id": item.target_instrument_id,
                "prior_trade_date": item.prior_trade_date,
                **source,
            }
        )
        diagnostics.append(
            {
                "target_trade_date": item.target_trade_date,
                "prior_trade_date": item.prior_trade_date,
                "candidate_trade_date": candidate,
                "accepted": accepted,
                "reason": reason,
                "blocker_classification": blocker,
                "same_day_or_future_used": False,
                "forward_fill_used": False,
                "backward_fill_used": False,
                "source_substitution_used": False,
            }
        )
    return (
        pd.DataFrame(rows, columns=ACCEPTANCE_MATRIX_COLUMNS),
        pd.DataFrame(diagnostics, columns=DIAGNOSTIC_COLUMNS),
    )


def _coverage(matrix: pd.DataFrame, validation: pd.DataFrame) -> pd.DataFrame:
    validation_mask = pd.MultiIndex.from_frame(
        matrix.loc[:, IDENTITY_COLUMNS]
    ).isin(pd.MultiIndex.from_frame(validation))
    complete = matrix.loc[:, ACCEPTANCE_MATRIX_COLUMNS[3:]].notna().all(axis=1)
    eligible_covered = int(complete.sum())
    validation_count = int(validation_mask.sum())
    validation_covered = int(complete.to_numpy()[validation_mask].sum())
    return pd.DataFrame(
        [
            {
                "source_id": SOURCE_ID,
                "eligible_identity_count": len(matrix),
                "eligible_covered_count": eligible_covered,
                "eligible_missing_count": len(matrix) - eligible_covered,
                "eligible_coverage_pct": (
                    eligible_covered / len(matrix) * 100 if len(matrix) else 0.0
                ),
                "validation_identity_count": validation_count,
                "validation_covered_count": validation_covered,
                "validation_missing_count": (
                    validation_count - validation_covered
                ),
                "validation_coverage_pct": (
                    validation_covered / validation_count * 100
                    if validation_count
                    else 0.0
                ),
            }
        ]
    )


def _identity_record(
    identity: CnyrubSecurityIdentity | None,
) -> dict[str, object]:
    if identity is not None:
        return {
            **asdict(identity),
            "identity_verified": True,
            "identity_service": "MOEX ISS metadata",
            "data_source_id": SOURCE_ID,
            "data_service": "MOEX AlgoPack subscription",
        }
    return {
        "source_id": SOURCE_ID,
        "security_id": SECURITY_ID,
        "board_id": BOARD_ID,
        "engine": ENGINE,
        "market": MARKET,
        "identity_verified": False,
        "historical_model_use_status": "blocked",
    }


def _official_route_validation(
    identity: CnyrubSecurityIdentity | None,
    candles: pd.DataFrame,
    error: SourceError | None = None,
) -> dict[str, object]:
    return {
        "official_service": "MOEX AlgoPack subscription",
        "official_host": ALGOPACK_HOST,
        "authorization_scheme": "Bearer",
        "token_environment_variable": ALGOPACK_TOKEN_ENV,
        "token_persisted_in_artifacts": False,
        "security_metadata_route": build_metadata_route(),
        "tradestats_route": ALGOPACK_TRADESTATS_ROUTE,
        "bucket_interval_minutes": ALGOPACK_BUCKET_MINUTES,
        "verified_security_id": identity.security_id if identity else None,
        "verified_board_id": identity.board_id if identity else None,
        "verified_engine": identity.engine if identity else None,
        "verified_market": identity.market if identity else None,
        "primary_board": bool(identity and identity.primary_board),
        "active_board": bool(identity and identity.active_board),
        "pagination_complete": error is None,
        "schema_stable_within_run": error is None,
        "earliest_available_date": (
            None if candles.empty else str(candles.trade_date.min())
        ),
        "latest_available_date": (
            None if candles.empty else str(candles.trade_date.max())
        ),
        "daily_aggregate_count": len(candles),
        "directional_volume_fields_present": bool(
            not candles.empty
            and {"volume_buy", "volume_sell"}.issubset(candles.columns)
        ),
        "fallback_used": False,
        "source_revision_status": SOURCE_REVISION_STATUS,
    }


def _finite(frame: pd.DataFrame, columns: tuple[str, ...]) -> bool:
    if frame.empty:
        return True
    values = frame.loc[:, columns].apply(pd.to_numeric, errors="coerce")
    return bool(values.map(math.isfinite).all().all())


def evaluate_gates(
    *,
    immutable_inputs_verified: bool,
    phase83_verified: bool,
    eligible: pd.DataFrame,
    validation: pd.DataFrame,
    identity: CnyrubSecurityIdentity | None,
    candles: pd.DataFrame,
    matrix: pd.DataFrame,
    coverage: pd.DataFrame,
    diagnostics: pd.DataFrame,
    route_validation: Mapping[str, Any],
    source_error: SourceError | None = None,
) -> dict[str, Any]:
    g1 = bool(
        immutable_inputs_verified
        and phase83_verified
        and len(eligible) == EXPECTED_ELIGIBLE_IDENTITIES
        and len(validation) == EXPECTED_VALIDATION_IDENTITIES
    )
    g2 = bool(
        identity
        and (
            identity.security_id,
            identity.board_id,
            identity.engine,
            identity.market,
        )
        == (SECURITY_ID, BOARD_ID, ENGINE, MARKET)
        and identity.primary_board
        and identity.active_board
    )
    g3 = bool(
        route_validation.get("official_host") == ALGOPACK_HOST
        and route_validation.get("tradestats_route")
        == ALGOPACK_TRADESTATS_ROUTE
        and route_validation.get("bucket_interval_minutes")
        == ALGOPACK_BUCKET_MINUTES
        and route_validation.get("pagination_complete")
        and route_validation.get("schema_stable_within_run")
        and route_validation.get("directional_volume_fields_present")
    )

    accepted = matrix.cnyrub_trade_date.notna()
    target = pd.to_datetime(matrix.loc[accepted, "target_trade_date"])
    prior = pd.to_datetime(matrix.loc[accepted, "prior_trade_date"])
    observed = pd.to_datetime(matrix.loc[accepted, "cnyrub_trade_date"])
    ends = pd.to_datetime(
        matrix.loc[accepted, "cnyrub_candle_end"], utc=True
    ).dt.tz_convert("Europe/Moscow")
    no_fill = not diagnostics[
        [
            "same_day_or_future_used",
            "forward_fill_used",
            "backward_fill_used",
            "source_substitution_used",
        ]
    ].any().any()
    pit_rejected = diagnostics.blocker_classification.eq(
        "point_in_time_cutoff_not_provable"
    ).any()
    g4 = bool(
        not pit_rejected
        and observed.eq(prior).all()
        and ends.lt(
            target.dt.tz_localize("Europe/Moscow") + pd.Timedelta(hours=6)
        ).all()
        and no_fill
    )

    coverage_row = coverage.iloc[0]
    g5 = bool(
        len(matrix) == EXPECTED_ELIGIBLE_IDENTITIES
        and int(coverage_row.eligible_covered_count)
        == EXPECTED_ELIGIBLE_IDENTITIES
        and int(coverage_row.validation_covered_count)
        == EXPECTED_VALIDATION_IDENTITIES
        and not matrix.duplicated(list(IDENTITY_COLUMNS)).any()
        and matrix.loc[:, IDENTITY_COLUMNS].equals(
            eligible.loc[:, IDENTITY_COLUMNS]
        )
        and tuple(matrix.columns) == ACCEPTANCE_MATRIX_COLUMNS
    )

    chronological = bool(
        candles.empty
        or (
            pd.to_datetime(candles.trade_date).is_monotonic_increasing
            and not candles.duplicated(["trade_date"]).any()
        )
    )
    ohlc = bool(
        candles.empty
        or (
            candles.high.ge(candles[["open", "close", "low"]].max(axis=1)).all()
            and candles.low.le(
                candles[["open", "close", "high"]].min(axis=1)
            ).all()
        )
    )
    volume_identity = bool(
        candles.empty
        or (
            candles.volume.sub(
                candles.volume_buy + candles.volume_sell
            ).abs()
            <= 1e-9
        ).all()
    )
    value_identity = bool(
        candles.empty
        or (
            candles.value.sub(
                candles.value_buy + candles.value_sell
            ).abs()
            <= candles.value.abs().mul(1e-6).clip(lower=1.0)
        ).all()
    )
    trade_identity = bool(
        candles.empty
        or candles.trades.eq(
            candles.trades_buy + candles.trades_sell
        ).all()
    )
    numeric_columns = (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "volume_buy",
        "volume_sell",
        "volume_imbalance",
        "value",
        "value_buy",
        "value_sell",
        "trades",
        "trades_buy",
        "trades_sell",
    )
    g6 = bool(
        _finite(candles, numeric_columns)
        and chronological
        and ohlc
        and volume_identity
        and value_identity
        and trade_identity
        and (
            candles.empty
            or candles[
                [
                    "volume",
                    "volume_buy",
                    "volume_sell",
                    "value",
                    "value_buy",
                    "value_sell",
                    "trades",
                    "trades_buy",
                    "trades_sell",
                ]
            ]
            .ge(0)
            .all()
            .all()
        )
        and (
            candles.empty
            or candles.volume_imbalance.between(-1.0, 1.0).all()
        )
        and (
            candles.empty
            or (
                pd.to_datetime(candles.candle_end, utc=True)
                >= pd.to_datetime(candles.candle_begin, utc=True)
            ).all()
        )
    )

    provenance = matrix.loc[
        accepted,
        [
            "cnyrub_security_id",
            "cnyrub_board_id",
            "cnyrub_source_route",
            "cnyrub_payload_sha256",
            "cnyrub_retrieved_at_utc",
            "cnyrub_source_revision_status",
        ],
    ]
    revision_valid = bool(
        matrix.cnyrub_source_revision_status.notna().all()
        and matrix.cnyrub_source_revision_status.eq(
            SOURCE_REVISION_STATUS
        ).all()
    )
    g7 = bool(
        provenance.notna().all().all()
        and provenance.cnyrub_security_id.eq(SECURITY_ID).all()
        and provenance.cnyrub_board_id.eq(BOARD_ID).all()
        and provenance.cnyrub_source_route.astype(str)
        .str.startswith(
            "https://apim.moex.com/iss/datashop/algopack/fx/tradestats/"
        )
        .all()
        and provenance.cnyrub_payload_sha256.astype(str)
        .map(lambda value: bool(_SHA256.fullmatch(value)))
        .all()
        and revision_valid
    )
    g8 = bool(
        not set(matrix.columns) & _FORBIDDEN_MATRIX_FIELDS
        and not diagnostics.source_substitution_used.any()
    )

    passed = (g1, g2, g3, g4, g5, g6, g7, g8)
    names = (
        "G1_immutable_inputs",
        "G2_official_security_identity",
        "G3_algopack_tradestats_route_and_schema",
        "G4_point_in_time_session_correctness",
        "G5_exact_coverage",
        "G6_directional_volume_and_numerical_integrity",
        "G7_provenance",
    )
    gates: dict[str, Any] = {
        name: {"passed": value}
        for name, value in zip(names, passed[:7], strict=True)
    }
    gates["G4_point_in_time_session_correctness"].update(
        {
            "pit_rejection_count": int(
                diagnostics.blocker_classification.eq(
                    "point_in_time_cutoff_not_provable"
                ).sum()
            ),
            "fill_or_substitution_used": not no_fill,
        }
    )
    gates["G6_directional_volume_and_numerical_integrity"].update(
        {
            "volume_equals_buy_plus_sell": volume_identity,
            "value_equals_buy_plus_sell": value_identity,
            "trades_equal_buy_plus_sell": trade_identity,
        }
    )
    gates["G7_provenance"].update(
        {
            "source_revision_status_required": SOURCE_REVISION_STATUS,
            "source_revision_status_valid": revision_valid,
            "token_persisted_in_artifacts": False,
        }
    )
    gates["G8_leakage_and_scope"] = {
        "passed": g8,
        "model_file_created": False,
        "model_fit_or_evaluation_performed": False,
        "target_prediction_or_probability_used": False,
        "source_fallback_used": False,
        "out_of_directory_write_performed": False,
        "promotion_performed": False,
        "broker_or_trading_action_performed": False,
    }
    failed = [
        f"G{index}" for index, value in enumerate(passed, 1) if not value
    ]
    blocker = None if not failed else _blocker_from_failure(
        failed, source_error
    )
    gates["G9_final_source_readiness"] = {
        "passed": not failed,
        "requires": [f"G{index}" for index in range(1, 9)],
        "failed_gates": failed,
        "status": (
            "moex_algopack_cnyrub_source_candidate_for_phase8_6b"
            if not failed
            else "moex_algopack_cnyrub_source_not_ready"
        ),
        "historical_model_use_status": (
            HISTORICAL_MODEL_USE_STATUS if not failed else "blocked"
        ),
        "blocker_classification": blocker,
    }
    return gates


def _blocker_from_failure(
    failed: list[str],
    error: SourceError | None,
) -> str:
    if "G4" in failed:
        return "point_in_time_cutoff_not_provable"
    if error and error.blocker in BLOCKER_CLASSIFICATIONS:
        return error.blocker
    mapping = {
        "G2": "security_identity_not_reproducible",
        "G3": "algopack_schema_not_stable",
        "G5": "incomplete_identity_coverage",
        "G6": "numerical_or_chronology_integrity_failure",
        "G7": "provenance_not_sufficient",
    }
    return next(
        (value for gate, value in mapping.items() if gate in failed),
        "other_fail_closed_with_exact_reason",
    )


def _structured_reason(
    blocker: str | None,
    diagnostics: pd.DataFrame,
    source_error: SourceError | None,
) -> str | None:
    if blocker is None:
        return None
    matching = diagnostics.loc[
        diagnostics.blocker_classification.eq(blocker), "reason"
    ]
    if not matching.empty:
        return str(matching.iloc[0])
    if source_error is not None:
        return str(source_error)
    return blocker


def run_source_validation(
    request: Phase86ARequest,
    *,
    algopack_transport: AlgoPackTransport = fetch_algopack_bytes,
    token_loader: TokenLoader = load_algopack_token,
    clock: UtcClock = utc_now,
    identity_loader: IdentityLoader = load_security_identity,
    history_loader: HistoryLoader = load_daily_history,
) -> Phase86AResult:
    base._validate_request(request)
    hashes = base.verify_immutable_inputs(request)
    aggregate = base._json(request.phase83_aggregate_metrics_path)
    phase83 = base._json(request.phase83_gate_results_path)
    base._validate_phase83_evidence(aggregate, phase83)
    _validate_experiment_contract(base._json(request.experiment_contract_path))
    base._json(request.dataset_manifest_path)
    base._json(request.feature_schema_path)

    eligible = base._eligible_identities(
        pd.read_parquet(request.modeling_dataset_path)
    )
    validation = base._validation_identities(
        pd.read_parquet(request.m0_validation_predictions_path),
        eligible,
    )

    identity: CnyrubSecurityIdentity | None = None
    source_error: SourceError | None = None
    candle_list: list[CnyrubAlgoPackDailyCandle] = []
    try:
        identity = identity_loader(clock=clock)
        first = min(map(date.fromisoformat, eligible.prior_trade_date))
        last = max(map(date.fromisoformat, eligible.prior_trade_date))
        candle_list = history_loader(
            identity,
            from_date=first,
            till_date=last,
            transport=algopack_transport,
            token_loader=token_loader,
            clock=clock,
        )
    except (CnyrubAlgoPackError, CnyrubHistoryError) as exc:
        if exc.blocker not in BLOCKER_CLASSIFICATIONS:
            raise Phase86AAlgoPackSourceValidationError(
                str(exc), blocker=exc.blocker
            ) from exc
        source_error = exc

    candles = normalized_candles(candle_list)
    matrix, diagnostics = build_cnyrub_pit_acceptance_matrix(
        eligible, candle_list
    )
    coverage = _coverage(matrix, validation)
    routes = _official_route_validation(identity, candles, source_error)
    gates = evaluate_gates(
        immutable_inputs_verified=True,
        phase83_verified=True,
        eligible=eligible,
        validation=validation,
        identity=identity,
        candles=candles,
        matrix=matrix,
        coverage=coverage,
        diagnostics=diagnostics,
        route_validation=routes,
        source_error=source_error,
    )
    final = gates["G9_final_source_readiness"]
    exact_reason = _structured_reason(
        final["blocker_classification"], diagnostics, source_error
    )

    inputs = {
        "project": PROJECT,
        "phase": PHASE,
        "task_id": TASK_ID,
        "run_id": request.run_id,
        "source_git_commit_sha": request.git_commit_sha,
        "eligible_identity_count": len(eligible),
        "validation_identity_count": len(validation),
        "expected_folds": EXPECTED_FOLDS,
        "expected_validation_rows_per_fold": (
            EXPECTED_VALIDATION_ROWS_PER_FOLD
        ),
        "frozen_target_interval": [
            eligible.target_trade_date.iloc[0],
            eligible.target_trade_date.iloc[-1],
        ],
        "immutable_inputs": {
            name: {
                "expected_sha256": expected,
                "observed_sha256": hashes[name],
                "matches": hashes[name] == expected,
            }
            for name, expected in EXPECTED_INPUT_SHA256.items()
        },
    }
    blocker_register = {
        "source_id": SOURCE_ID,
        "status": final["status"],
        "historical_model_use_status": final[
            "historical_model_use_status"
        ],
        "blocker_classification": final["blocker_classification"],
        "exact_blocker_reason": exact_reason,
        "failed_gates": final["failed_gates"],
        "offending_candle_used_in_acceptance_matrix": False,
        "fill_or_substitution_used": False,
        "source_fallback_used": False,
        "subscription_token_persisted": False,
        "model_fit_or_evaluation_performed": False,
        "promotion_authorized": False,
    }
    payloads: dict[str, object] = {
        "input_identity_verification.json": inputs,
        "official_route_validation.json": routes,
        "cnyrub_security_identity.json": _identity_record(identity),
        "cnyrub_daily_candles_normalized.parquet": candles,
        "cnyrub_pit_acceptance_matrix.parquet": matrix,
        "coverage_by_source.csv": coverage,
        "session_alignment_diagnostics.csv": diagnostics,
        "source_blocker_register.json": blocker_register,
        "gate_results.json": gates,
    }
    base._write_exact_artifacts(request.output_dir, payloads)
    return Phase86AResult(
        request.output_dir,
        DECLARED_OUTPUT_ARTIFACTS,
        len(eligible),
        len(validation),
        str(final["status"]),
        final["blocker_classification"],
    )


def run_from_args(arguments: argparse.Namespace) -> Phase86AResult:
    return run_source_validation(request_from_args(arguments))


def main(argv: list[str] | None = None) -> int:
    run_from_args(build_argument_parser().parse_args(argv))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
