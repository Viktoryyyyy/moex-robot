from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final

import pandas as pd

from moex_research.external_data.models import HttpTransport
from moex_research.external_data.moex_cnyrub_history import (
    BOARD_ID, CANDLE_ROUTE, CNYRUB_HTTP_MAX_ATTEMPTS,
    CNYRUB_HTTP_RETRY_DELAYS_SECONDS, ENGINE, HISTORICAL_MODEL_USE_STATUS,
    MARKET, MOEX_ISS_HOST, SECURITY_ID, SECURITY_METADATA_ROUTE, SOURCE_ID,
    SOURCE_REVISION_STATUS, TRANSIENT_HTTP_ERROR_MESSAGE, CnyrubDailyCandle,
    CnyrubHistoryError, CnyrubSecurityIdentity, UtcClock,
    fetch_cnyrub_bytes_with_retry, load_daily_history, load_security_identity,
    utc_now, validate_prior_session_candle,
)

PROJECT: Final = "MOEX Bot"
PHASE: Final = "8.6A"
LANE: Final = "ema_3_19_ai"
TASK_ID: Final = "ema_3_19_ai_phase_8_6a_moex_cnyrub_source_validation_v1"
EXECUTION_MODE: Final = "browser_chatgpt_github_direct"
CONTRACT_ID: Final = "usdrubf_phase8_6a_moex_cnyrub_source_validation_v1"
CONTRACT_VERSION: Final = "1.0"
APPROVED_BRANCH: Final = "research/ema-3-19-ai/phase-8-6a-moex-cnyrub-source-validation"
EXPECTED_INPUT_SHA256: Final = {
    "modeling_dataset": "fdd626f9e0522c6bbb653f9e17fbbbeef7ded77f57ff187b35246a2458d55d00",
    "dataset_manifest": "fcbbb5e5ed0549c5c6f397e34f203f01836271f6bf471f90cab5a2fd64ace082",
    "feature_schema": "8f08802c7fb0a4cc43ab4ba072ee22ff9edd92fe8d674ea0515545d20d143238",
    "m0_validation_predictions": "9769d00a49adeb54c016d965387774e46a3e09e09f895aa61d48a90bbf3568cf",
    "phase83_aggregate_metrics": "d6ad4f6587dadb32431bd7b8f3bd59c5393e04d742efb4af459b316b417f8756",
    "phase83_gate_results": "d3f7e24022e550e725eae7ec5bc214d6b95e0e9c66393574c575e5d6f593f33c",
}
EXPECTED_ELIGIBLE_IDENTITIES, EXPECTED_VALIDATION_IDENTITIES = 472, 320
EXPECTED_FOLDS, EXPECTED_VALIDATION_ROWS_PER_FOLD = 5, 64
EXPECTED_INSTRUMENT, TARGET_SOURCE = "forts.usdrubf", "manual_phase_labels_v1"
EXPECTED_FIRST_TARGET, EXPECTED_LAST_TARGET = date(2024, 8, 5), date(2026, 6, 11)
EXPECTED_PHASE83_STATUS = "external_factor_incremental_value_not_supported"
EXPECTED_PHASE83_RECOMMENDATION = "prioritize_blocked_oil_and_liquidity_sources"
CLASS_ORDER = ("B", "S", "OUT")
IDENTITY_COLUMNS = ("target_trade_date", "target_instrument_id")
ACCEPTANCE_MATRIX_COLUMNS = tuple("""target_trade_date target_instrument_id prior_trade_date
cnyrub_security_id cnyrub_board_id cnyrub_trade_date cnyrub_open cnyrub_high
cnyrub_low cnyrub_close cnyrub_volume cnyrub_value cnyrub_candle_begin
cnyrub_candle_end cnyrub_source_route cnyrub_payload_sha256 cnyrub_retrieved_at_utc""".split())
NORMALIZED_SOURCE_COLUMNS = tuple("""source_id security_id board_id engine market
trade_date open high low close volume value candle_begin candle_end source_route
retrieved_at_utc raw_payload_sha256 source_revision_status historical_model_use_status""".split())
DECLARED_OUTPUT_ARTIFACTS = tuple("""input_identity_verification.json
official_route_validation.json cnyrub_security_identity.json
cnyrub_daily_candles_normalized.parquet cnyrub_pit_acceptance_matrix.parquet
coverage_by_source.csv session_alignment_diagnostics.csv
source_blocker_register.json gate_results.json""".split())
BLOCKER_CLASSIFICATIONS = tuple("""security_identity_not_reproducible
official_daily_candles_not_available point_in_time_cutoff_not_provable
incomplete_identity_coverage official_schema_not_stable
numerical_or_chronology_integrity_failure provenance_not_sufficient
other_fail_closed_with_exact_reason""".split())
EXPECTED_TRANSIENT_HTTP_RETRY_POLICY = {
    "bounded_transient_retry_enabled": True, "enabled_for_source_id": SOURCE_ID,
    "phase_scope": "8.6A_only", "maximum_total_attempts": CNYRUB_HTTP_MAX_ATTEMPTS,
    "retry_delays_seconds": list(CNYRUB_HTTP_RETRY_DELAYS_SECONDS),
    "random_jitter_allowed": False, "same_exact_official_route_only": True,
    "route_substitution_allowed": False, "fallback_source_allowed": False,
    "fallback_security_allowed": False, "fallback_board_allowed": False,
    "fallback_date_allowed": False,
    "retryable_exception": f"ExternalDataError({TRANSIENT_HTTP_ERROR_MESSAGE})",
    "semantic_failures_retried": False,
}
REQUIRED_CLI_ARGS = tuple("""--modeling-dataset-path --dataset-manifest-path
--feature-schema-path --m0-validation-predictions-path
--phase8-3-aggregate-metrics-path --phase8-3-gate-results-path
--experiment-contract-path --output-dir --run-id --git-commit-sha""".split())
_FORBIDDEN_MATRIX_FIELDS = frozenset("""target_phase_label target_is_labeled
target_source fold_id y_true candidate_y_pred prediction probability_B probability_S
probability_OUT""".split())
_RUN_ID = re.compile(r"^phase8_6a_moex_cnyrub_source_validation_[0-9]{8}_v[1-9][0-9]*$")
_SHA40, _SHA256 = re.compile(r"^[0-9a-f]{40}$"), re.compile(r"^[0-9a-f]{64}$")
_ALIAS = re.compile(r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])", re.I)


class Phase86ACnyrubSourceValidationError(ValueError):
    def __init__(self, message: str, *, blocker: str = BLOCKER_CLASSIFICATIONS[-1]):
        super().__init__(message)
        self.blocker = blocker if blocker in BLOCKER_CLASSIFICATIONS else BLOCKER_CLASSIFICATIONS[-1]


@dataclass(frozen=True)
class Phase86ARequest:
    modeling_dataset_path: Path
    dataset_manifest_path: Path
    feature_schema_path: Path
    m0_validation_predictions_path: Path
    phase83_aggregate_metrics_path: Path
    phase83_gate_results_path: Path
    experiment_contract_path: Path
    output_dir: Path
    run_id: str
    git_commit_sha: str


@dataclass(frozen=True)
class Phase86AResult:
    output_dir: Path
    artifact_names: tuple[str, ...]
    eligible_identity_count: int
    validation_identity_count: int
    final_status: str
    blocker_classification: str | None


def build_argument_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m moex_research.runners.usdrubf_phase8_6a_moex_cnyrub_source_validation")
    for flag in REQUIRED_CLI_ARGS:
        p.add_argument(flag, required=True)
    return p


def _file(value: object, flag: str, suffix: str) -> Path:
    text = str(value).strip()
    if not text or any(c in text for c in "*?[]") or _ALIAS.search(text):
        raise Phase86ACnyrubSourceValidationError(f"{flag} must identify one immutable file")
    path = Path(text)
    if path.suffix.lower() != suffix or not path.is_file():
        raise Phase86ACnyrubSourceValidationError(f"{flag} file or suffix mismatch")
    return path


def request_from_args(a: argparse.Namespace) -> Phase86ARequest:
    r = Phase86ARequest(
        _file(a.modeling_dataset_path, REQUIRED_CLI_ARGS[0], ".parquet"),
        _file(a.dataset_manifest_path, REQUIRED_CLI_ARGS[1], ".json"),
        _file(a.feature_schema_path, REQUIRED_CLI_ARGS[2], ".json"),
        _file(a.m0_validation_predictions_path, REQUIRED_CLI_ARGS[3], ".parquet"),
        _file(getattr(a, "phase8_3_aggregate_metrics_path"), REQUIRED_CLI_ARGS[4], ".json"),
        _file(getattr(a, "phase8_3_gate_results_path"), REQUIRED_CLI_ARGS[5], ".json"),
        _file(a.experiment_contract_path, REQUIRED_CLI_ARGS[6], ".json"),
        Path(str(a.output_dir).strip()), str(a.run_id).strip(),
        str(a.git_commit_sha).strip().lower(),
    )
    _validate_request(r)
    return r


def _input_paths(r: Phase86ARequest) -> dict[str, Path]:
    return dict(zip((*EXPECTED_INPUT_SHA256, "experiment_contract"), (
        r.modeling_dataset_path, r.dataset_manifest_path, r.feature_schema_path,
        r.m0_validation_predictions_path, r.phase83_aggregate_metrics_path,
        r.phase83_gate_results_path, r.experiment_contract_path,
    ), strict=True))


def _validate_request(r: Phase86ARequest) -> None:
    if len({p.resolve() for p in _input_paths(r).values()}) != 7:
        raise Phase86ACnyrubSourceValidationError("all input files must be distinct")
    if not _RUN_ID.fullmatch(r.run_id) or not _SHA40.fullmatch(r.git_commit_sha):
        raise Phase86ACnyrubSourceValidationError("immutable run id or commit SHA mismatch")
    text = str(r.output_dir)
    if not text or any(c in text for c in "*?[]") or _ALIAS.search(text) or r.output_dir.exists():
        raise Phase86ACnyrubSourceValidationError("--output-dir must be explicit and must not pre-exist")


def _hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Phase86ACnyrubSourceValidationError(f"invalid JSON: {path.name}") from exc
    if not isinstance(value, dict):
        raise Phase86ACnyrubSourceValidationError(f"JSON must be object: {path.name}")
    return value


def verify_immutable_inputs(r: Phase86ARequest) -> dict[str, str]:
    paths = _input_paths(r)
    found = {name: _hash(paths[name]) for name in EXPECTED_INPUT_SHA256}
    bad = [name for name, expected in EXPECTED_INPUT_SHA256.items() if found[name] != expected]
    if bad:
        raise Phase86ACnyrubSourceValidationError("immutable input hash mismatch: " + ", ".join(bad))
    return found


def _validate_phase83_evidence(aggregate: Mapping[str, Any], gates: Mapping[str, Any]) -> None:
    final = gates.get("G12_final_acceptance", {})
    if not isinstance(final, Mapping) or aggregate.get("final_status") != EXPECTED_PHASE83_STATUS or final.get("status") != EXPECTED_PHASE83_STATUS:
        raise Phase86ACnyrubSourceValidationError("Phase 8.3 status mismatch")
    if final.get("recommendation") != EXPECTED_PHASE83_RECOMMENDATION:
        raise Phase86ACnyrubSourceValidationError("Phase 8.3 recommendation mismatch")


def _validate_experiment_contract(c: Mapping[str, Any]) -> None:
    identity = {
        "contract_id": CONTRACT_ID, "contract_version": CONTRACT_VERSION,
        "project": PROJECT, "task_id": TASK_ID, "lane": LANE, "phase": PHASE,
        "execution_mode": EXECUTION_MODE, "status": "source_validation_only",
    }
    source = c.get("source_identity", {})
    if c.get("contract_identity") != identity or c.get("approved_branch") != APPROVED_BRANCH:
        raise Phase86ACnyrubSourceValidationError("Phase 8.6A contract identity or branch mismatch")
    if tuple(c.get("runtime_artifacts", ())) != DECLARED_OUTPUT_ARTIFACTS or c.get("transient_http_retry_policy") != EXPECTED_TRANSIENT_HTTP_RETRY_POLICY:
        raise Phase86ACnyrubSourceValidationError("Phase 8.6A artifact or retry contract mismatch")
    if tuple(source.get(k) for k in ("security_id", "board_id", "engine", "market")) != (SECURITY_ID, BOARD_ID, ENGINE, MARKET):
        raise Phase86ACnyrubSourceValidationError("Phase 8.6A source identity mismatch")


def _eligible_identities(df: pd.DataFrame) -> pd.DataFrame:
    needed = (*IDENTITY_COLUMNS, "prior_trade_date", "target_phase_label", "target_is_labeled", "target_source")
    if any(c not in df for c in needed):
        raise Phase86ACnyrubSourceValidationError("modeling identity fields missing")
    target, prior = pd.to_datetime(df.target_trade_date, errors="coerce"), pd.to_datetime(df.prior_trade_date, errors="coerce")
    instrument = df.target_instrument_id.astype("string").str.strip()
    mask = df.target_source.eq(TARGET_SOURCE) & df.target_is_labeled.eq(True) & df.target_phase_label.isin(CLASS_ORDER) & target.notna() & prior.notna() & instrument.eq(EXPECTED_INSTRUMENT)
    out = df.loc[mask, [*IDENTITY_COLUMNS, "prior_trade_date"]].copy()
    out.target_trade_date, out.prior_trade_date = target[mask].dt.strftime("%Y-%m-%d"), prior[mask].dt.strftime("%Y-%m-%d")
    out.target_instrument_id = instrument[mask].astype(str)
    out = out.reset_index(drop=True)
    dates, priors = pd.to_datetime(out.target_trade_date), pd.to_datetime(out.prior_trade_date)
    if len(out) != 472 or out.duplicated(list(IDENTITY_COLUMNS)).any() or not dates.is_monotonic_increasing or dates.iloc[0].date() != EXPECTED_FIRST_TARGET or dates.iloc[-1].date() != EXPECTED_LAST_TARGET or not (priors < dates).all():
        raise Phase86ACnyrubSourceValidationError("frozen eligible identities mismatch")
    return out


def _validation_identities(df: pd.DataFrame, eligible: pd.DataFrame) -> pd.DataFrame:
    if any(c not in df for c in IDENTITY_COLUMNS):
        raise Phase86ACnyrubSourceValidationError("validation identities missing")
    out = df.loc[:, IDENTITY_COLUMNS].copy()
    out.target_trade_date = pd.to_datetime(out.target_trade_date, errors="raise").dt.strftime("%Y-%m-%d")
    out.target_instrument_id = out.target_instrument_id.astype("string").str.strip().astype(str)
    out = out.reset_index(drop=True)
    if len(out) != 320 or out.duplicated(list(IDENTITY_COLUMNS)).any() or not pd.MultiIndex.from_frame(out).isin(pd.MultiIndex.from_frame(eligible.loc[:, IDENTITY_COLUMNS])).all():
        raise Phase86ACnyrubSourceValidationError("frozen validation identities mismatch")
    return out


def normalized_candles(candles: list[CnyrubDailyCandle]) -> pd.DataFrame:
    return pd.DataFrame([c.as_record() for c in candles], columns=NORMALIZED_SOURCE_COLUMNS)


def build_cnyrub_pit_acceptance_matrix(eligible: pd.DataFrame, candles: list[CnyrubDailyCandle]) -> tuple[pd.DataFrame, pd.DataFrame]:
    keyed: dict[date, CnyrubDailyCandle] = {}
    for candle in candles:
        if candle.trade_date in keyed:
            raise Phase86ACnyrubSourceValidationError("duplicate CNYRUB date", blocker="numerical_or_chronology_integrity_failure")
        keyed[candle.trade_date] = candle
    rows, diag = [], []
    for item in eligible.itertuples(index=False):
        target, prior = date.fromisoformat(item.target_trade_date), date.fromisoformat(item.prior_trade_date)
        candle = keyed.get(prior)
        if candle:
            try:
                validate_prior_session_candle(candle, target_trade_date=target, prior_trade_date=prior)
            except CnyrubHistoryError as exc:
                raise Phase86ACnyrubSourceValidationError(str(exc), blocker=exc.blocker) from exc
            source = {
                "cnyrub_security_id": candle.security_id, "cnyrub_board_id": candle.board_id,
                "cnyrub_trade_date": candle.trade_date.isoformat(), "cnyrub_open": candle.open,
                "cnyrub_high": candle.high, "cnyrub_low": candle.low, "cnyrub_close": candle.close,
                "cnyrub_volume": candle.volume, "cnyrub_value": candle.value,
                "cnyrub_candle_begin": candle.candle_begin.isoformat(),
                "cnyrub_candle_end": candle.candle_end.isoformat(),
                "cnyrub_source_route": candle.source_route,
                "cnyrub_payload_sha256": candle.raw_payload_sha256,
                "cnyrub_retrieved_at_utc": candle.retrieved_at_utc.isoformat().replace("+00:00", "Z"),
            }
            reason = "accepted_exact_prior_trade_date"
        else:
            source = {column: None for column in ACCEPTANCE_MATRIX_COLUMNS[3:]}
            reason = "missing_exact_prior_trade_date_candle"
        rows.append({"target_trade_date": item.target_trade_date, "target_instrument_id": item.target_instrument_id, "prior_trade_date": item.prior_trade_date, **source})
        diag.append({"target_trade_date": item.target_trade_date, "prior_trade_date": item.prior_trade_date, "accepted": bool(candle), "reason": reason, "same_day_or_future_used": False, "forward_fill_used": False, "backward_fill_used": False, "source_substitution_used": False})
    return pd.DataFrame(rows, columns=ACCEPTANCE_MATRIX_COLUMNS), pd.DataFrame(diag)


def _coverage(matrix: pd.DataFrame, validation: pd.DataFrame) -> pd.DataFrame:
    valid = pd.MultiIndex.from_frame(matrix.loc[:, IDENTITY_COLUMNS]).isin(pd.MultiIndex.from_frame(validation))
    complete = matrix.loc[:, ACCEPTANCE_MATRIX_COLUMNS[3:]].notna().all(axis=1)
    ec, vc, vn = int(complete.sum()), int(complete.to_numpy()[valid].sum()), int(valid.sum())
    return pd.DataFrame([{"source_id": SOURCE_ID, "eligible_identity_count": len(matrix), "eligible_covered_count": ec, "eligible_missing_count": len(matrix)-ec, "eligible_coverage_pct": ec/len(matrix)*100 if len(matrix) else 0.0, "validation_identity_count": vn, "validation_covered_count": vc, "validation_missing_count": vn-vc, "validation_coverage_pct": vc/vn*100 if vn else 0.0}])


def build_metadata_route() -> str:
    return f"{SECURITY_METADATA_ROUTE}?iss.meta=off&iss.only=description%2Cboards"


def _identity_record(identity: CnyrubSecurityIdentity | None) -> dict[str, object]:
    return ({**asdict(identity), "identity_verified": True} if identity else {"source_id": SOURCE_ID, "security_id": SECURITY_ID, "board_id": BOARD_ID, "engine": ENGINE, "market": MARKET, "identity_verified": False, "historical_model_use_status": "blocked"})


def _official_route_validation(identity: CnyrubSecurityIdentity | None, candles: pd.DataFrame, error: CnyrubHistoryError | None) -> dict[str, object]:
    return {"official_service": "MOEX ISS", "official_host": MOEX_ISS_HOST, "security_metadata_route": build_metadata_route(), "daily_candle_route": CANDLE_ROUTE, "daily_interval": 24, "verified_security_id": identity.security_id if identity else None, "verified_board_id": identity.board_id if identity else None, "verified_engine": identity.engine if identity else None, "verified_market": identity.market if identity else None, "primary_board": bool(identity and identity.primary_board), "active_board": bool(identity and identity.active_board), "pagination_complete": error is None, "schema_stable_within_run": error is None, "earliest_available_date": None if candles.empty else str(candles.trade_date.min()), "latest_available_date": None if candles.empty else str(candles.trade_date.max()), "candle_count": len(candles), "fallback_used": False, "source_revision_status": SOURCE_REVISION_STATUS}


def _finite(frame: pd.DataFrame, columns: tuple[str, ...]) -> bool:
    return True if frame.empty else bool(frame.loc[:, columns].apply(pd.to_numeric, errors="coerce").map(math.isfinite).all().all())


def evaluate_gates(*, immutable_inputs_verified: bool, phase83_verified: bool, eligible: pd.DataFrame, validation: pd.DataFrame, identity: CnyrubSecurityIdentity | None, candles: pd.DataFrame, matrix: pd.DataFrame, coverage: pd.DataFrame, diagnostics: pd.DataFrame, route_validation: Mapping[str, Any], source_error: CnyrubHistoryError | None = None) -> dict[str, Any]:
    g1 = immutable_inputs_verified and phase83_verified and len(eligible) == 472 and len(validation) == 320
    g2 = bool(identity and (identity.security_id, identity.board_id, identity.engine, identity.market) == (SECURITY_ID, BOARD_ID, ENGINE, MARKET) and identity.primary_board and identity.active_board)
    g3 = bool(route_validation.get("pagination_complete") and route_validation.get("schema_stable_within_run") and route_validation.get("daily_interval") == 24 and route_validation.get("daily_candle_route") == CANDLE_ROUTE)
    accepted = matrix.cnyrub_trade_date.notna()
    target, prior, observed = pd.to_datetime(matrix.loc[accepted, "target_trade_date"]), pd.to_datetime(matrix.loc[accepted, "prior_trade_date"]), pd.to_datetime(matrix.loc[accepted, "cnyrub_trade_date"])
    ends = pd.to_datetime(matrix.loc[accepted, "cnyrub_candle_end"], utc=True).dt.tz_convert("Europe/Moscow")
    g4 = bool(observed.eq(prior).all() and ends.lt(target.dt.tz_localize("Europe/Moscow") + pd.Timedelta(hours=6)).all() and not diagnostics[["same_day_or_future_used", "forward_fill_used", "backward_fill_used", "source_substitution_used"]].any().any())
    cov = coverage.iloc[0]
    g5 = bool(len(matrix) == 472 and int(cov.eligible_covered_count) == 472 and int(cov.validation_covered_count) == 320 and not matrix.duplicated(list(IDENTITY_COLUMNS)).any() and matrix.loc[:, IDENTITY_COLUMNS].equals(eligible.loc[:, IDENTITY_COLUMNS]) and tuple(matrix.columns) == ACCEPTANCE_MATRIX_COLUMNS)
    chronological = True if candles.empty else pd.to_datetime(candles.trade_date).is_monotonic_increasing and not candles.duplicated(["trade_date"]).any()
    ohlc = True if candles.empty else candles.high.ge(candles[["open", "close", "low"]].max(axis=1)).all() and candles.low.le(candles[["open", "close", "high"]].min(axis=1)).all()
    g6 = bool(_finite(candles, ("open", "high", "low", "close", "volume", "value")) and chronological and ohlc and (candles.empty or candles[["volume", "value"]].ge(0).all().all()) and (candles.empty or (pd.to_datetime(candles.candle_end, utc=True) >= pd.to_datetime(candles.candle_begin, utc=True)).all()))
    provenance = matrix.loc[accepted, ["cnyrub_security_id", "cnyrub_board_id", "cnyrub_source_route", "cnyrub_payload_sha256", "cnyrub_retrieved_at_utc"]]
    g7 = bool(provenance.notna().all().all() and provenance.cnyrub_security_id.eq(SECURITY_ID).all() and provenance.cnyrub_board_id.eq(BOARD_ID).all() and provenance.cnyrub_source_route.astype(str).str.startswith("https://iss.moex.com/").all() and provenance.cnyrub_payload_sha256.astype(str).map(lambda v: bool(_SHA256.fullmatch(v))).all())
    g8 = bool(not set(matrix.columns) & _FORBIDDEN_MATRIX_FIELDS and not diagnostics.source_substitution_used.any())
    passed = (g1, g2, g3, g4, g5, g6, g7, g8)
    names = ("G1_immutable_inputs", "G2_official_security_identity", "G3_official_candle_route_and_schema", "G4_point_in_time_session_correctness", "G5_exact_coverage", "G6_numerical_and_chronology_integrity", "G7_provenance")
    gates: dict[str, Any] = {name: {"passed": value} for name, value in zip(names, passed[:7], strict=True)}
    gates["G8_leakage_and_scope"] = {"passed": g8, "model_file_created": False, "model_fit_or_evaluation_performed": False, "target_prediction_or_probability_used": False, "source_fallback_used": False, "out_of_directory_write_performed": False, "promotion_performed": False, "broker_or_trading_action_performed": False}
    failed = [f"G{i}" for i, value in enumerate(passed, 1) if not value]
    blocker = None if not failed else _blocker_from_failure(failed, source_error)
    gates["G9_final_source_readiness"] = {"passed": not failed, "requires": [f"G{i}" for i in range(1, 9)], "failed_gates": failed, "status": "moex_cnyrub_source_candidate_for_phase8_6b" if not failed else "moex_cnyrub_source_not_ready", "historical_model_use_status": HISTORICAL_MODEL_USE_STATUS if not failed else "blocked", "blocker_classification": blocker}
    return gates


def _blocker_from_failure(failed: list[str], error: CnyrubHistoryError | None) -> str:
    if error and error.blocker in BLOCKER_CLASSIFICATIONS:
        return error.blocker
    mapping = {"G2": BLOCKER_CLASSIFICATIONS[0], "G3": BLOCKER_CLASSIFICATIONS[4], "G4": BLOCKER_CLASSIFICATIONS[2], "G5": BLOCKER_CLASSIFICATIONS[3], "G6": BLOCKER_CLASSIFICATIONS[5], "G7": BLOCKER_CLASSIFICATIONS[6]}
    return next((value for gate, value in mapping.items() if gate in failed), BLOCKER_CLASSIFICATIONS[-1])


def _ready(value: Any) -> Any:
    if isinstance(value, dict): return {str(k): _ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)): return [_ready(v) for v in value]
    if isinstance(value, (date, datetime)): return value.isoformat()
    return value


def _write_exact_artifacts(output_dir: Path, payloads: Mapping[str, object]) -> None:
    if tuple(payloads) != DECLARED_OUTPUT_ARTIFACTS:
        raise Phase86ACnyrubSourceValidationError("undeclared runtime artifact inventory")
    if output_dir.exists():
        raise Phase86ACnyrubSourceValidationError("output directory must not pre-exist")
    output_dir.mkdir(parents=True, exist_ok=False)
    for name in DECLARED_OUTPUT_ARTIFACTS:
        path, payload = output_dir / name, payloads[name]
        if path.parent != output_dir:
            raise Phase86ACnyrubSourceValidationError("write outside output directory refused")
        if name.endswith(".json"): path.write_text(json.dumps(_ready(payload), ensure_ascii=False, indent=2, sort_keys=True)+"\n", encoding="utf-8")
        elif name.endswith(".csv") and isinstance(payload, pd.DataFrame): payload.to_csv(path, index=False)
        elif name.endswith(".parquet") and isinstance(payload, pd.DataFrame): payload.to_parquet(path, index=False)
        else: raise Phase86ACnyrubSourceValidationError("artifact payload or suffix mismatch")
    if sorted(p.name for p in output_dir.iterdir()) != sorted(DECLARED_OUTPUT_ARTIFACTS):
        raise Phase86ACnyrubSourceValidationError("runtime artifact inventory mismatch")


def run_source_validation(r: Phase86ARequest, *, transport: HttpTransport = fetch_cnyrub_bytes_with_retry, clock: UtcClock = utc_now) -> Phase86AResult:
    _validate_request(r)
    hashes = verify_immutable_inputs(r)
    aggregate, phase83 = _json(r.phase83_aggregate_metrics_path), _json(r.phase83_gate_results_path)
    _validate_phase83_evidence(aggregate, phase83)
    _validate_experiment_contract(_json(r.experiment_contract_path))
    _json(r.dataset_manifest_path); _json(r.feature_schema_path)
    eligible = _eligible_identities(pd.read_parquet(r.modeling_dataset_path))
    validation = _validation_identities(pd.read_parquet(r.m0_validation_predictions_path), eligible)
    identity, error, candle_list = None, None, []
    try:
        identity = load_security_identity(transport=transport, clock=clock)
        first, last = min(map(date.fromisoformat, eligible.prior_trade_date)), max(map(date.fromisoformat, eligible.prior_trade_date))
        candle_list = load_daily_history(identity, from_date=first, till_date=last, transport=transport, clock=clock)
    except CnyrubHistoryError as exc:
        if exc.blocker not in BLOCKER_CLASSIFICATIONS[:4]:
            raise Phase86ACnyrubSourceValidationError(str(exc), blocker=exc.blocker) from exc
        error = exc
    candles = normalized_candles(candle_list)
    matrix, diagnostics = build_cnyrub_pit_acceptance_matrix(eligible, candle_list)
    coverage, routes = _coverage(matrix, validation), _official_route_validation(identity, candles, error)
    gates = evaluate_gates(immutable_inputs_verified=True, phase83_verified=True, eligible=eligible, validation=validation, identity=identity, candles=candles, matrix=matrix, coverage=coverage, diagnostics=diagnostics, route_validation=routes, source_error=error)
    final = gates["G9_final_source_readiness"]
    inputs = {"project": PROJECT, "phase": PHASE, "task_id": TASK_ID, "run_id": r.run_id, "source_git_commit_sha": r.git_commit_sha, "eligible_identity_count": len(eligible), "validation_identity_count": len(validation), "expected_folds": EXPECTED_FOLDS, "expected_validation_rows_per_fold": EXPECTED_VALIDATION_ROWS_PER_FOLD, "frozen_target_interval": [eligible.target_trade_date.iloc[0], eligible.target_trade_date.iloc[-1]], "immutable_inputs": {name: {"expected_sha256": expected, "observed_sha256": hashes[name], "matches": hashes[name] == expected} for name, expected in EXPECTED_INPUT_SHA256.items()}}
    blocker = {"source_id": SOURCE_ID, "status": final["status"], "historical_model_use_status": final["historical_model_use_status"], "blocker_classification": final["blocker_classification"], "exact_blocker_reason": str(error) if error else None, "failed_gates": final["failed_gates"], "source_fallback_used": False, "model_fit_or_evaluation_performed": False, "promotion_authorized": False}
    payloads = {"input_identity_verification.json": inputs, "official_route_validation.json": routes, "cnyrub_security_identity.json": _identity_record(identity), "cnyrub_daily_candles_normalized.parquet": candles, "cnyrub_pit_acceptance_matrix.parquet": matrix, "coverage_by_source.csv": coverage, "session_alignment_diagnostics.csv": diagnostics, "source_blocker_register.json": blocker, "gate_results.json": gates}
    _write_exact_artifacts(r.output_dir, payloads)
    return Phase86AResult(r.output_dir, DECLARED_OUTPUT_ARTIFACTS, len(eligible), len(validation), str(final["status"]), final["blocker_classification"])


def main(argv: list[str] | None = None) -> int:
    run_source_validation(request_from_args(build_argument_parser().parse_args(argv)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
