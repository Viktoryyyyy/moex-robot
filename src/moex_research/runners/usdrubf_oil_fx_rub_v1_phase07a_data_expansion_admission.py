from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Final, Mapping
from zoneinfo import ZoneInfo

import pandas as pd

from moex_data.futures import freeze_step7_accepted_raw_5m as step7
from moex_research.runners import usdrubf_phase8_4a_moex_brent_source_validation as phase84a


PROJECT: Final[str] = "MOEX_Bot"
TASK_ID: Final[str] = "STRAT_OIL_FX_RUB_V1_07A_DATA_EXPANSION_ADMISSION"
CONTRACT_ID: Final[str] = "usdrubf_oil_fx_rub_v1_phase07a_data_expansion_admission"
CONTRACT_VERSION: Final[str] = "1.0"
SOURCE_INSTRUMENT: Final[str] = "usdrubf_futures_family"
TARGET_INSTRUMENT: Final[str] = "forts.usdrubf"
PREFIX_START: Final[str] = "2022-04-26"
PREFIX_END: Final[str] = "2026-10-01"
EXPECTED_DATE_COUNT: Final[int] = 1143
EXPECTED_RAW_ROW_COUNT: Final[int] = 188760
EXPECTED_TARGET_COUNT: Final[int] = 1142
EXPECTED_PREFIX_CONTENT_SHA256: Final[str] = "d29ca00d590609e09254f8c4298e335571b673ab0b0de534bfa3c51e478aa66e"
DISCOVERY_START: Final[str] = "2024-08-05"
DISCOVERY_END: Final[str] = "2026-06-11"
_SHA40 = re.compile(r"^[0-9a-f]{40}$")
_SHA64 = re.compile(r"^[0-9a-f]{64}$")

DECLARED_OUTPUTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "expanded_target_identities.parquet",
    "brent_contract_universe.parquet",
    "brent_daily_candles_normalized.parquet",
    "brent_pit_acceptance_matrix.parquet",
    "contract_roll_diagnostics.csv",
    "coverage_summary.json",
    "research_manifest.json",
    "gate_results.json",
)


class Phase07AError(ValueError):
    pass


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise Phase07AError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise Phase07AError("contract must be a JSON object")
    return value


def _validate_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("contract_identity")
    expected_identity = {
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "project": PROJECT,
        "task_id": TASK_ID,
    }
    if not isinstance(identity, Mapping):
        raise Phase07AError("contract_identity missing")
    for key, expected in expected_identity.items():
        if identity.get(key) != expected:
            raise Phase07AError(f"contract identity mismatch: {key}")

    prefix = contract.get("usdrubf_frozen_prefix")
    expected_prefix = {
        "source_mode": "accepted_current_prefix",
        "source_instrument_id": SOURCE_INSTRUMENT,
        "target_instrument_id": TARGET_INSTRUMENT,
        "prefix_start_date": PREFIX_START,
        "prefix_end_date": PREFIX_END,
        "accepted_date_count": EXPECTED_DATE_COUNT,
        "raw_row_count": EXPECTED_RAW_ROW_COUNT,
        "partition_content_set_sha256": EXPECTED_PREFIX_CONTENT_SHA256,
        "target_identity_count": EXPECTED_TARGET_COUNT,
        "first_target_trade_date": "2022-04-27",
        "last_target_trade_date": PREFIX_END,
        "mutable_current_tail_allowed_in_prefix": False,
    }
    if not isinstance(prefix, Mapping):
        raise Phase07AError("usdrubf_frozen_prefix missing")
    for key, expected in expected_prefix.items():
        if prefix.get(key) != expected:
            raise Phase07AError(f"frozen prefix mismatch: {key}")

    purpose = contract.get("purpose")
    if not isinstance(purpose, Mapping):
        raise Phase07AError("purpose missing")
    if purpose.get("data_expansion_admission_allowed") is not True or purpose.get("historical_brent_retrieval_allowed") is not True:
        raise Phase07AError("required admission permission missing")
    for key in (
        "signal_evaluation_allowed",
        "backtest_allowed",
        "parameter_optimization_allowed",
        "model_fit_allowed",
        "strategy_promotion_allowed",
        "broker_action_allowed",
        "trading_allowed",
    ):
        if purpose.get(key) is not False:
            raise Phase07AError(f"research boundary widened: {key}")

    brent = contract.get("brent_admission")
    if not isinstance(brent, Mapping):
        raise Phase07AError("brent_admission missing")
    if (
        brent.get("source_id") != phase84a.SOURCE_ID
        or brent.get("asset_code") != phase84a.ASSET_CODE
        or brent.get("board_id") != phase84a.BOARD_ID
        or brent.get("minimum_days_to_expiration") != 7
        or brent.get("candle_trade_date") != "exact prior_trade_date"
        or brent.get("decision_cutoff_local_time") != "08:45:00"
    ):
        raise Phase07AError("Brent admission semantics mismatch")
    for key in (
        "target_day_or_future_candle_allowed",
        "continuous_alias_allowed",
        "contract_code_inference_allowed",
        "volume_or_open_interest_roll_allowed",
        "cross_contract_return_allowed",
    ):
        if brent.get(key) is not False:
            raise Phase07AError(f"Brent boundary widened: {key}")

    if contract.get("runtime_artifacts") != list(DECLARED_OUTPUTS):
        raise Phase07AError("runtime artifact inventory mismatch")


def _prefix_records(scope: Any) -> tuple[tuple[dict[str, Any], ...], tuple[str, ...], int, str]:
    records = tuple(dict(row) for row in scope.records if str(row["trade_date"]) <= PREFIX_END)
    if not records:
        raise Phase07AError("frozen prefix is empty")
    dates = tuple(str(row["trade_date"]) for row in records)
    if dates[0] != PREFIX_START or dates[-1] != PREFIX_END:
        raise Phase07AError("frozen prefix boundary mismatch")
    if len(dates) != EXPECTED_DATE_COUNT or len(set(dates)) != len(dates) or list(dates) != sorted(dates):
        raise Phase07AError("frozen prefix date identity mismatch")
    rows = sum(int(row["row_count"]) for row in records)
    content = hashlib.sha256(
        "".join(f"{row['trade_date']}\t{row['sha256']}\n" for row in records).encode("utf-8")
    ).hexdigest()
    if rows != EXPECTED_RAW_ROW_COUNT:
        raise Phase07AError("frozen prefix raw row count mismatch")
    if content != EXPECTED_PREFIX_CONTENT_SHA256:
        raise Phase07AError("frozen prefix content SHA256 mismatch")
    return records, dates, rows, content


def _is_utc_timestamp(value: object) -> bool:
    try:
        parsed = pd.Timestamp(value)
    except Exception:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() == timedelta(0)


def _segment(target: str) -> str:
    if target < DISCOVERY_START:
        return "pre_discovery_extension"
    if target <= DISCOVERY_END:
        return "discovery_reference"
    return "forward_extension"


def _target_identities(dates: tuple[str, ...]) -> pd.DataFrame:
    rows = [
        {
            "target_trade_date": dates[index],
            "target_instrument_id": TARGET_INSTRUMENT,
            "prior_trade_date": dates[index - 1],
            "evaluation_segment": _segment(dates[index]),
        }
        for index in range(1, len(dates))
    ]
    frame = pd.DataFrame(rows)
    if len(frame) != EXPECTED_TARGET_COUNT:
        raise Phase07AError("target identity count mismatch")
    if frame.iloc[0]["target_trade_date"] != "2022-04-27" or frame.iloc[-1]["target_trade_date"] != PREFIX_END:
        raise Phase07AError("target identity boundary mismatch")
    if frame.duplicated(["target_trade_date", "target_instrument_id"]).any():
        raise Phase07AError("duplicate target identity")
    return frame


def _validate_brent(
    identities: pd.DataFrame,
    universe: pd.DataFrame,
    candles: pd.DataFrame,
    matrix: pd.DataFrame,
    rolls: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    id_cols = ["target_trade_date", "target_instrument_id"]
    g3 = bool(
        not universe.empty
        and not candles.empty
        and universe["source_id"].eq(phase84a.SOURCE_ID).all()
        and universe["asset_code"].eq(phase84a.ASSET_CODE).all()
        and universe["board_id"].eq(phase84a.BOARD_ID).all()
        and universe["metadata_route"].astype(str).str.startswith("https://iss.moex.com/iss/securities/").all()
        and universe["enumeration_route"].astype(str).str.startswith("https://iss.moex.com/iss/history/").all()
        and candles["source_id"].eq(phase84a.SOURCE_ID).all()
        and candles["source_route"].astype(str).str.startswith(
            "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/"
        ).all()
        and universe["metadata_raw_payload_sha256"].astype(str).map(lambda x: bool(_SHA64.fullmatch(x))).all()
        and universe["enumeration_raw_payload_sha256"].astype(str).map(lambda x: bool(_SHA64.fullmatch(x))).all()
        and candles["raw_payload_sha256"].astype(str).map(lambda x: bool(_SHA64.fullmatch(x))).all()
        and universe["metadata_retrieved_at_utc"].map(_is_utc_timestamp).all()
        and universe["enumeration_retrieved_at_utc"].map(_is_utc_timestamp).all()
        and candles["retrieved_at_utc"].map(_is_utc_timestamp).all()
        and matrix["brent_retrieved_at_utc"].map(_is_utc_timestamp).all()
    )

    target = pd.to_datetime(matrix["target_trade_date"])
    prior = pd.to_datetime(matrix["prior_trade_date"])
    candle_date = pd.to_datetime(matrix["brent_trade_date"])
    candle_end = pd.to_datetime(matrix["brent_candle_end"], utc=True).dt.tz_convert(ZoneInfo("Europe/Moscow"))
    cutoff = target.dt.tz_localize(ZoneInfo("Europe/Moscow")) + pd.Timedelta(hours=8, minutes=45)
    g4 = bool(
        matrix["brent_days_to_expiration"].ge(7).all()
        and candle_date.eq(prior).all()
        and candle_end.lt(cutoff).all()
    )

    expected = identities[id_cols].reset_index(drop=True)
    observed = matrix[id_cols].reset_index(drop=True)
    g5 = bool(
        len(matrix) == EXPECTED_TARGET_COUNT
        and not matrix.duplicated(id_cols).any()
        and observed.equals(expected)
    )

    changed = matrix["brent_contract_code"].ne(matrix["brent_contract_code"].shift())
    changed.iloc[0] = False
    previous = matrix["brent_contract_code"].shift()
    g6 = bool(
        matrix["brent_contract_changed"].eq(changed).all()
        and matrix["brent_previous_contract_code"].fillna("").eq(previous.fillna("")).all()
        and len(rolls) == int(changed.sum())
        and (rolls.empty or not rolls["target_or_future_information_used"].any())
        and (rolls.empty or not rolls["cross_contract_return_calculated"].any())
    )

    segments = identities["evaluation_segment"]
    valid_segments = {"pre_discovery_extension", "discovery_reference", "forward_extension"}
    g7 = bool(segments.isin(valid_segments).all() and set(segments.unique()) == valid_segments)

    return {
        "G3_brent_official_identity": {"passed": g3},
        "G4_brent_pit": {"passed": g4},
        "G5_exact_coverage": {"passed": g5, "covered_identity_count": int(len(matrix))},
        "G6_roll_integrity": {"passed": g6, "roll_count": int(len(rolls))},
        "G7_segmentation": {
            "passed": g7,
            "segment_counts": {str(k): int(v) for k, v in segments.value_counts().sort_index().items()},
        },
    }


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-path", required=True)
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--git-commit-sha", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    contract_path = Path(args.contract_path).resolve(strict=True)
    data_root = Path(args.data_root).resolve(strict=True)
    output_dir = Path(args.output_dir).resolve()
    if output_dir.exists():
        raise Phase07AError("output directory must not pre-exist")
    run_id = str(args.run_id).strip()
    git_sha = str(args.git_commit_sha).strip().lower()
    if not run_id or "/" in run_id or "\\" in run_id:
        raise Phase07AError("run-id must be an explicit safe token")
    if not _SHA40.fullmatch(git_sha):
        raise Phase07AError("git-commit-sha must be 40 lowercase hex")

    contract = _read_json(contract_path)
    _validate_contract(contract)

    scope = step7.accepted_quote_history(
        data_root,
        SOURCE_INSTRUMENT,
        repo_root=Path(__file__).resolve().parents[3],
        current=True,
    )
    records, dates, raw_rows, prefix_sha = _prefix_records(scope)
    identities = _target_identities(dates)

    brent_input = identities[["target_trade_date", "target_instrument_id", "prior_trade_date"]].copy()
    universe, candles, matrix, rolls = phase84a.build_brent_pit_matrix(brent_input)

    gates: dict[str, dict[str, Any]] = {
        "G1_usdrubf_prefix": {
            "passed": True,
            "prefix_start_date": dates[0],
            "prefix_end_date": dates[-1],
            "accepted_date_count": len(dates),
            "raw_row_count": raw_rows,
            "partition_content_set_sha256": prefix_sha,
        },
        "G2_target_identity": {
            "passed": True,
            "target_identity_count": len(identities),
            "first_target_trade_date": identities.iloc[0]["target_trade_date"],
            "last_target_trade_date": identities.iloc[-1]["target_trade_date"],
        },
    }
    gates.update(_validate_brent(identities, universe, candles, matrix, rolls))
    gates["G8_research_only"] = {
        "passed": True,
        "signal_evaluation_performed": False,
        "performance_calculation_performed": False,
        "model_fit_performed": False,
        "parameter_optimization_performed": False,
        "strategy_promotion_performed": False,
        "broker_action_performed": False,
        "trading_action_performed": False,
    }
    failed = [name for name, result in gates.items() if result.get("passed") is not True]
    gates["G9_final"] = {
        "passed": not failed,
        "failed_gates": failed,
        "status": "oil_fx_rub_phase07a_data_expansion_admitted" if not failed else "blocked",
    }

    coverage = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "frozen_prefix": {
            "start_date": dates[0],
            "end_date": dates[-1],
            "accepted_date_count": len(dates),
            "raw_row_count": raw_rows,
            "partition_content_set_sha256": prefix_sha,
        },
        "target_identity_count": len(identities),
        "brent_matrix_row_count": len(matrix),
        "unique_brent_contract_count": int(matrix["brent_contract_code"].nunique()),
        "roll_count": int(len(rolls)),
        "segment_counts": {
            str(k): int(v)
            for k, v in identities["evaluation_segment"].value_counts().sort_index().items()
        },
    }
    input_identity = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "accepted_current_observed_acceptance_run_id": str(scope.acceptance_run_id),
        "accepted_current_observed_last_date": str(scope.accepted_dates[-1]),
        "frozen_prefix_end_date": PREFIX_END,
        "frozen_prefix_partition_content_set_sha256": prefix_sha,
        "frozen_prefix_record_count": len(records),
        "frozen_prefix_raw_row_count": raw_rows,
    }
    manifest = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": run_id,
        "git_commit_sha": git_sha,
        "contract_id": CONTRACT_ID,
        "frozen_prefix_partition_content_set_sha256": prefix_sha,
        "target_identity_count": len(identities),
        "brent_matrix_row_count": len(matrix),
        "runtime_artifacts": list(DECLARED_OUTPUTS),
        "network_scope": "official MOEX ISS Brent routes via existing moex_brent_history primitive",
        "source_mutation_performed": False,
        "signal_evaluation_performed": False,
        "performance_calculation_performed": False,
        "trading_action_performed": False,
    }

    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "input_identity_verification.json", input_identity)
    identities.to_parquet(output_dir / "expanded_target_identities.parquet", index=False)
    universe.to_parquet(output_dir / "brent_contract_universe.parquet", index=False)
    candles.to_parquet(output_dir / "brent_daily_candles_normalized.parquet", index=False)
    matrix.to_parquet(output_dir / "brent_pit_acceptance_matrix.parquet", index=False)
    rolls.to_csv(output_dir / "contract_roll_diagnostics.csv", index=False)
    _write_json(output_dir / "coverage_summary.json", coverage)
    _write_json(output_dir / "research_manifest.json", manifest)
    _write_json(output_dir / "gate_results.json", gates)
    observed = tuple(sorted(path.name for path in output_dir.iterdir()))
    if observed != tuple(sorted(DECLARED_OUTPUTS)):
        raise Phase07AError("runtime artifact inventory mismatch")
    if failed:
        raise Phase07AError("Phase07A gates failed: " + ",".join(failed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
