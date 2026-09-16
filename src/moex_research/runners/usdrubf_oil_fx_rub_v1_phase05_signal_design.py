from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any, Final, Mapping

import numpy as np
import pandas as pd

from moex_research.features.oil_fx_rub_v1_features import prepare_identity_panel
from moex_research.runners import (
    usdrubf_oil_fx_rub_v1_phase04_regime_research as phase04,
)

PROJECT: Final[str] = "MOEX_Bot"
TASK_ID: Final[str] = "STRAT_OIL_FX_RUB_V1_05_SIGNAL_DESIGN"
CONTRACT_ID: Final[str] = "usdrubf_oil_fx_rub_v1_phase05_signal_design"
CONTRACT_VERSION: Final[str] = "1.0"
EXPECTED_IDENTITY_COUNT: Final[int] = 472
EXPECTED_INSTRUMENT: Final[str] = "forts.usdrubf"
MIN_ENTRY_SEPARATION: Final[int] = 21
EXIT_HORIZONS: Final[tuple[int, ...]] = (5, 10, 20)
COST_GRID_BPS: Final[tuple[int, ...]] = (0, 5, 10, 20)

DECLARED_OUTPUTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "signal_candidates.parquet",
    "signal_design_summary.json",
    "research_manifest.json",
    "gate_results.json",
)

_ALIAS_PATTERN = re.compile(r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])", re.I)
_SHA40_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_GLOB_CHARS = frozenset("*?[]")


class Phase05SignalDesignError(ValueError):
    """Fail-closed error for Oil/FX/RUB Phase 05 signal design."""


def _read_json(path: str | Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Phase05SignalDesignError(f"invalid JSON evidence: {path}") from exc
    if not isinstance(payload, dict):
        raise Phase05SignalDesignError(f"JSON evidence must be an object: {path}")
    return payload


def _explicit_file(raw: object, flag: str, suffix: str) -> Path:
    text = str(raw).strip()
    if not text or any(char in text for char in _GLOB_CHARS) or _ALIAS_PATTERN.search(text):
        raise Phase05SignalDesignError(f"{flag} must identify one explicit immutable file")
    path = Path(text)
    if path.suffix.lower() != suffix or not path.exists() or not path.is_file():
        raise Phase05SignalDesignError(f"{flag} file or suffix mismatch")
    return path


def _explicit_output_dir(raw: object) -> Path:
    text = str(raw).strip()
    if not text or any(char in text for char in _GLOB_CHARS) or _ALIAS_PATTERN.search(text):
        raise Phase05SignalDesignError("--output-dir must be explicit and immutable")
    path = Path(text)
    if path.exists():
        raise Phase05SignalDesignError("--output-dir must not pre-exist")
    return path


def _validate_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("contract_identity")
    if not isinstance(identity, Mapping):
        raise Phase05SignalDesignError("contract_identity is required")
    expected_identity = {
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "project": PROJECT,
        "task_id": TASK_ID,
    }
    for key, value in expected_identity.items():
        if identity.get(key) != value:
            raise Phase05SignalDesignError(f"contract identity mismatch: {key}")

    purpose = contract.get("purpose")
    if not isinstance(purpose, Mapping):
        raise Phase05SignalDesignError("purpose is required")
    if purpose.get("signal_design_allowed") is not True:
        raise Phase05SignalDesignError("signal design must be explicitly allowed")
    for key in (
        "performance_evaluation_allowed",
        "model_fit_allowed",
        "parameter_optimization_allowed",
        "strategy_promotion_allowed",
        "trading_allowed",
    ):
        if purpose.get(key) is not False:
            raise Phase05SignalDesignError(f"purpose boundary widened: {key}")

    signal = contract.get("signal_definition")
    if not isinstance(signal, Mapping):
        raise Phase05SignalDesignError("signal_definition is required")
    expected_signal = {
        "signal_observation_time": "prior_trade_date close",
        "raw_signal_condition": "brent_percentile_126 >= 0.75 AND usdrubf_percentile_126 >= 0.75",
        "direction": "short_usdrubf_long_rub",
        "execution_time": "target_trade_date open",
        "entry_price_field": "open",
        "minimum_entry_separation_source_sessions": MIN_ENTRY_SEPARATION,
        "pyramiding_allowed": False,
        "fixed_exit_horizons_sessions": list(EXIT_HORIZONS),
        "regime_exit_allowed": False,
        "stop_loss_allowed_in_phase05": False,
        "phase06_round_trip_cost_bps_sensitivity": list(COST_GRID_BPS),
    }
    for key, value in expected_signal.items():
        if signal.get(key) != value:
            raise Phase05SignalDesignError(f"signal definition mismatch: {key}")

    output_policy = contract.get("phase05_output_policy")
    if not isinstance(output_policy, Mapping):
        raise Phase05SignalDesignError("phase05_output_policy is required")
    for key in (
        "future_exit_prices_allowed",
        "future_returns_allowed",
        "pnl_allowed",
        "performance_ranking_allowed",
    ):
        if output_policy.get(key) is not False:
            raise Phase05SignalDesignError(f"outcome boundary widened: {key}")
    if output_policy.get("candidate_schedule_only") is not True:
        raise Phase05SignalDesignError("Phase 05 must remain candidate-schedule-only")

    if contract.get("runtime_artifacts") != list(DECLARED_OUTPUTS):
        raise Phase05SignalDesignError("runtime artifact inventory mismatch")

    authority = contract.get("authority_boundary")
    if not isinstance(authority, Mapping) or any(value is not False for value in authority.values()):
        raise Phase05SignalDesignError("authority boundary was widened")


def _validate_phase04_semantics(contract: Mapping[str, Any]) -> None:
    phase04._validate_contract(contract)
    method = contract.get("methodology", {})
    if method.get("rolling_window_sessions") != phase04.WINDOW:
        raise Phase05SignalDesignError("Phase 04 rolling window mismatch")
    if method.get("minimum_history_sessions") != phase04.MIN_HISTORY:
        raise Phase05SignalDesignError("Phase 04 minimum history mismatch")
    if method.get("high_threshold") != phase04.HIGH_THRESHOLD:
        raise Phase05SignalDesignError("Phase 04 high threshold mismatch")
    if phase04.WINDOW != 126 or phase04.MIN_HISTORY != 63 or phase04.HIGH_THRESHOLD != 0.75:
        raise Phase05SignalDesignError("Phase 04 repository semantics changed")


def _prepare_signal_observations(
    modeling_dataset: pd.DataFrame,
    source_panel: pd.DataFrame,
    brent_matrix: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    identities = prepare_identity_panel(modeling_dataset)
    phase04.phase02b.validate_phase6_source_panel_replay(modeling_dataset, source_panel)

    required_brent = {
        "target_trade_date",
        "target_instrument_id",
        "prior_trade_date",
        "brent_trade_date",
        "brent_close",
        "brent_contract_code",
    }
    if not required_brent.issubset(brent_matrix.columns):
        raise Phase05SignalDesignError("Brent PIT matrix schema mismatch")

    brent = brent_matrix.copy()
    for column in ("target_trade_date", "prior_trade_date", "brent_trade_date"):
        brent[column] = phase04._normalize_dates(brent[column], f"Brent {column}")
    brent["target_instrument_id"] = brent["target_instrument_id"].astype(str)
    if len(brent) != EXPECTED_IDENTITY_COUNT:
        raise Phase05SignalDesignError("Brent identity count mismatch")
    if not phase04._identity_values_equal(
        brent.loc[:, ["target_trade_date", "target_instrument_id"]],
        identities.loc[:, ["target_trade_date", "target_instrument_id"]],
    ):
        raise Phase05SignalDesignError("Brent identity/order mismatch")
    if not brent["prior_trade_date"].equals(identities["prior_trade_date"].astype("string")):
        raise Phase05SignalDesignError("Brent prior_trade_date mismatch")
    if not brent["brent_trade_date"].equals(brent["prior_trade_date"]):
        raise Phase05SignalDesignError("Brent close must be prior_trade_date close")

    panel = phase04.phase6_builder._prepare_internal_d1_panel(source_panel)
    panel_dates = phase04._normalize_dates(panel["trade_date"], "Phase6 source trade_date")
    if set(panel["instrument_id"].astype(str)) != {EXPECTED_INSTRUMENT}:
        raise Phase05SignalDesignError("Phase6 source instrument mismatch")
    panel_open = pd.to_numeric(panel["open"], errors="coerce").to_numpy(float)
    panel_close = pd.to_numeric(panel["close"], errors="coerce").to_numpy(float)
    if not np.isfinite(panel_open).all() or np.any(panel_open <= 0):
        raise Phase05SignalDesignError("Phase6 source open must be finite positive")
    if not np.isfinite(panel_close).all() or np.any(panel_close <= 0):
        raise Phase05SignalDesignError("Phase6 source close must be finite positive")
    date_to_index = {str(value): idx for idx, value in enumerate(panel_dates)}

    brent_close = pd.to_numeric(brent["brent_close"], errors="coerce").to_numpy(float)
    if not np.isfinite(brent_close).all() or np.any(brent_close <= 0):
        raise Phase05SignalDesignError("Brent close must be finite positive")

    usd_prior_close = np.full(EXPECTED_IDENTITY_COUNT, np.nan, dtype=float)
    entry_index = np.full(EXPECTED_IDENTITY_COUNT, -1, dtype=int)
    entry_open = np.full(EXPECTED_IDENTITY_COUNT, np.nan, dtype=float)
    for idx, (target_date, prior_date) in enumerate(
        zip(
            identities["target_trade_date"].astype(str),
            identities["prior_trade_date"].astype(str),
        )
    ):
        prior_idx = date_to_index.get(prior_date)
        target_idx = date_to_index.get(target_date)
        if prior_idx is None or target_idx is None:
            raise Phase05SignalDesignError("Phase6 source missing target/prior identity date")
        usd_prior_close[idx] = panel_close[prior_idx]
        entry_index[idx] = target_idx
        entry_open[idx] = panel_open[target_idx]

    oil_pct = phase04._rolling_percentile(brent_close)
    usd_pct = phase04._rolling_percentile(usd_prior_close)
    regimes = [phase04._classify_regime(a, b) for a, b in zip(oil_pct, usd_pct)]

    observations = identities.loc[
        :, ["target_trade_date", "target_instrument_id", "prior_trade_date"]
    ].copy()
    observations["brent_contract_code"] = brent["brent_contract_code"].astype(str).to_numpy()
    observations["brent_prior_close"] = brent_close
    observations["usdrubf_prior_close"] = usd_prior_close
    observations["brent_percentile_126"] = oil_pct
    observations["usdrubf_percentile_126"] = usd_pct
    observations["regime"] = regimes
    observations["entry_source_session_index"] = entry_index
    observations["entry_open"] = entry_open
    return observations, panel


def _execution_eligibility(indices: list[int]) -> list[bool]:
    accepted: list[bool] = []
    last_accepted: int | None = None
    for index in indices:
        eligible = last_accepted is None or index - last_accepted >= MIN_ENTRY_SEPARATION
        accepted.append(eligible)
        if eligible:
            last_accepted = index
    return accepted


def _build_signal_candidates(
    observations: pd.DataFrame,
    panel: pd.DataFrame,
) -> pd.DataFrame:
    raw = observations.loc[observations["regime"].eq("high_high")].copy()
    raw = raw.sort_values("entry_source_session_index", kind="mergesort").reset_index(drop=True)
    if raw.empty:
        raise Phase05SignalDesignError("no high_high raw signal observations")

    indices = raw["entry_source_session_index"].astype(int).tolist()
    eligible = _execution_eligibility(indices)
    panel_dates = panel["trade_date"].astype(str).tolist()
    terminal_index = len(panel_dates) - 1

    rows: list[dict[str, Any]] = []
    for ordinal, (_, row) in enumerate(raw.iterrows(), start=1):
        entry_idx = int(row["entry_source_session_index"])
        item: dict[str, Any] = {
            "signal_id": f"oil_fx_rub_v1_hh_{ordinal:03d}",
            "target_trade_date": str(row["target_trade_date"]),
            "target_instrument_id": str(row["target_instrument_id"]),
            "prior_trade_date": str(row["prior_trade_date"]),
            "direction": "short_usdrubf_long_rub",
            "signal_regime": "high_high",
            "brent_percentile_126": float(row["brent_percentile_126"]),
            "usdrubf_percentile_126": float(row["usdrubf_percentile_126"]),
            "entry_execution": "target_trade_date_open",
            "entry_source_session_index": entry_idx,
            "entry_open": float(row["entry_open"]),
            "execution_eligible": bool(eligible[ordinal - 1]),
            "suppression_reason": "" if eligible[ordinal - 1] else "overlap_cooldown_21_sessions",
        }
        for horizon in EXIT_HORIZONS:
            endpoint = entry_idx + horizon
            available = endpoint < terminal_index
            item[f"exit_{horizon}session_available"] = bool(available)
            item[f"exit_{horizon}session_trade_date"] = (
                panel_dates[endpoint] if available else None
            )
            item[f"exit_{horizon}session_execution"] = "close"
        rows.append(item)

    candidates = pd.DataFrame(rows)
    accepted_indices = candidates.loc[
        candidates["execution_eligible"], "entry_source_session_index"
    ].astype(int).tolist()
    if any(
        later - earlier < MIN_ENTRY_SEPARATION
        for earlier, later in zip(accepted_indices, accepted_indices[1:])
    ):
        raise Phase05SignalDesignError("overlap guard failed")

    forbidden_tokens = ("return", "pnl", "profit", "exit_price", "exit_close")
    forbidden_columns = [
        column
        for column in candidates.columns
        if any(token in column.lower() for token in forbidden_tokens)
    ]
    if forbidden_columns:
        raise Phase05SignalDesignError(
            "Phase 05 candidate output contains forbidden outcome columns: "
            + ", ".join(sorted(forbidden_columns))
        )
    return candidates


def _build_summary(candidates: pd.DataFrame) -> dict[str, Any]:
    eligible = candidates.loc[candidates["execution_eligible"]]
    return {
        "project": PROJECT,
        "task_id": TASK_ID,
        "signal_id": "oil_fx_rub_v1_high_high_short",
        "status": "signal_design_ready_for_phase06",
        "raw_high_high_observation_count": int(len(candidates)),
        "execution_eligible_entry_count": int(len(eligible)),
        "suppressed_overlap_count": int((~candidates["execution_eligible"]).sum()),
        "minimum_entry_separation_source_sessions": MIN_ENTRY_SEPARATION,
        "direction": "short_usdrubf_long_rub",
        "signal_observation_time": "prior_trade_date close",
        "entry_execution": "target_trade_date open",
        "fixed_exit_horizons_sessions": list(EXIT_HORIZONS),
        "phase06_round_trip_cost_bps_sensitivity": list(COST_GRID_BPS),
        "regime_exit_not_designed": True,
        "regime_exit_reason": (
            "Phase 04 regime is sampled on eligible research identities rather than a "
            "continuous daily Brent regime path."
        ),
        "future_exit_prices_used": False,
        "future_returns_used": False,
        "pnl_computed": False,
        "performance_ranking_performed": False,
        "model_fit_performed": False,
        "parameter_optimization_performed": False,
        "trading_action_performed": False,
        "eligible_entry_dates": eligible["target_trade_date"].astype(str).tolist(),
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m moex_research.runners.usdrubf_oil_fx_rub_v1_phase05_signal_design"
    )
    for flag in (
        "--contract-path",
        "--phase04-contract-path",
        "--phase6-modeling-dataset-path",
        "--phase6-dataset-manifest-path",
        "--phase6-source-panel-path",
        "--brent-matrix-path",
        "--brent-gate-results-path",
        "--brent-input-identity-path",
        "--output-dir",
        "--run-id",
        "--git-commit-sha",
    ):
        parser.add_argument(flag, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    contract_path = _explicit_file(args.contract_path, "--contract-path", ".json")
    phase04_contract_path = _explicit_file(
        args.phase04_contract_path, "--phase04-contract-path", ".json"
    )
    phase6_modeling = _explicit_file(
        args.phase6_modeling_dataset_path, "--phase6-modeling-dataset-path", ".parquet"
    )
    phase6_manifest = _explicit_file(
        args.phase6_dataset_manifest_path, "--phase6-dataset-manifest-path", ".json"
    )
    phase6_source = _explicit_file(
        args.phase6_source_panel_path, "--phase6-source-panel-path", ".parquet"
    )
    brent_matrix_path = _explicit_file(
        args.brent_matrix_path, "--brent-matrix-path", ".parquet"
    )
    brent_gates_path = _explicit_file(
        args.brent_gate_results_path, "--brent-gate-results-path", ".json"
    )
    brent_identity_path = _explicit_file(
        args.brent_input_identity_path, "--brent-input-identity-path", ".json"
    )
    output_dir = _explicit_output_dir(args.output_dir)
    run_id = str(args.run_id).strip()
    git_sha = str(args.git_commit_sha).strip().lower()
    if not run_id or _ALIAS_PATTERN.search(run_id):
        raise Phase05SignalDesignError("--run-id must be explicit and immutable")
    if not _SHA40_PATTERN.fullmatch(git_sha):
        raise Phase05SignalDesignError("--git-commit-sha must be exactly 40 lowercase hex characters")

    contract = _read_json(contract_path)
    phase04_contract = _read_json(phase04_contract_path)
    _validate_contract(contract)
    _validate_phase04_semantics(phase04_contract)

    immutable_paths = {
        "phase6_modeling_dataset": phase6_modeling,
        "phase6_dataset_manifest": phase6_manifest,
        "brent_pit_acceptance_matrix": brent_matrix_path,
        "phase84a_gate_results": brent_gates_path,
        "phase84a_input_identity": brent_identity_path,
    }
    observed_hashes = phase04._validate_immutable_hashes(immutable_paths)

    modeling = pd.read_parquet(phase6_modeling)
    source_panel = pd.read_parquet(phase6_source)
    brent_matrix = pd.read_parquet(brent_matrix_path)
    brent_gates = _read_json(brent_gates_path)
    brent_identity = _read_json(brent_identity_path)
    phase04._validate_brent_evidence(brent_gates)
    if not brent_identity:
        raise Phase05SignalDesignError("Brent input identity evidence must be non-empty")

    observations, panel = _prepare_signal_observations(modeling, source_panel, brent_matrix)
    candidates = _build_signal_candidates(observations, panel)
    summary = _build_summary(candidates)

    gates: dict[str, dict[str, Any]] = {
        "G1_lineage": {"passed": True},
        "G2_phase04_semantics": {
            "passed": True,
            "window_sessions": phase04.WINDOW,
            "minimum_history_sessions": phase04.MIN_HISTORY,
            "high_threshold": phase04.HIGH_THRESHOLD,
        },
        "G3_pit_signal": {
            "passed": True,
            "signal_source_time": "prior_trade_date_close",
            "earliest_execution": "target_trade_date_open",
        },
        "G4_signal_direction": {
            "passed": bool((candidates["signal_regime"] == "high_high").all()),
            "direction": "short_usdrubf_long_rub",
        },
        "G5_overlap": {
            "passed": True,
            "minimum_entry_separation_source_sessions": MIN_ENTRY_SEPARATION,
            "pyramiding": False,
        },
        "G6_exit_schedule": {
            "passed": True,
            "fixed_exit_horizons_sessions": list(EXIT_HORIZONS),
            "regime_exit_inferred": False,
        },
        "G7_no_outcome_use": {
            "passed": True,
            "future_exit_prices_used": False,
            "future_returns_used": False,
            "pnl_computed": False,
            "performance_ranking_performed": False,
            "model_fit_performed": False,
            "parameter_optimization_performed": False,
        },
        "G8_research_only": {
            "passed": True,
            "network_access_performed": False,
            "upstream_artifact_mutation_performed": False,
            "position_sizing_performed": False,
            "strategy_promotion_performed": False,
            "broker_action_performed": False,
            "trading_action_performed": False,
        },
    }
    failed = [key for key, value in gates.items() if value.get("passed") is not True]
    gates["G9_final"] = {
        "passed": not failed,
        "failed_gates": failed,
        "status": "oil_fx_rub_phase05_signal_design_complete" if not failed else "blocked",
    }

    manifest = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": run_id,
        "git_commit_sha": git_sha,
        "identity_count": EXPECTED_IDENTITY_COUNT,
        "input_sha256": observed_hashes,
        "raw_signal_count": int(len(candidates)),
        "execution_eligible_entry_count": int(candidates["execution_eligible"].sum()),
        "runtime_artifacts": list(DECLARED_OUTPUTS),
        "network_access_performed": False,
        "upstream_artifact_mutation_performed": False,
        "model_fit_performed": False,
        "parameter_optimization_performed": False,
        "performance_evaluation_performed": False,
        "trading_action_performed": False,
    }
    identity_verification = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "eligible_identity_count": EXPECTED_IDENTITY_COUNT,
        "instrument_id": EXPECTED_INSTRUMENT,
        "phase6_lineage_replayed": True,
        "brent_immutable_artifacts_verified": True,
        "phase04_semantics_verified": True,
    }

    output_dir.mkdir(parents=True, exist_ok=False)
    candidates.to_parquet(output_dir / "signal_candidates.parquet", index=False)
    phase04._write_json(output_dir / "input_identity_verification.json", identity_verification)
    phase04._write_json(output_dir / "signal_design_summary.json", summary)
    phase04._write_json(output_dir / "research_manifest.json", manifest)
    phase04._write_json(output_dir / "gate_results.json", gates)

    if tuple(sorted(path.name for path in output_dir.iterdir())) != tuple(sorted(DECLARED_OUTPUTS)):
        raise Phase05SignalDesignError("output artifact inventory mismatch")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
