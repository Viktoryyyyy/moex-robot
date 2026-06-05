from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final

from moex_core.calendars.moex_iss_calendar import build_futures_calendar_from_rows
from moex_data.futures.contract_io import expand_contract_path, load_simple_yaml_mapping
from moex_data.futures.manifests import futures_partition_manifest_to_values
from moex_data.futures.materialization import materialize_raw_5m_boundary
from moex_data.futures.resampling import resample_ohlcv_5m_partition
from moex_data.futures.validation import guard_text, validate_dataset_contract_values, validate_timeframe
from moex_data.quality.futures_ohlcv import futures_quality_report_to_values

CORE_CONFIG_REF: Final[str] = "configs/datasets/futures_historical_data_core.v1.yaml"
RAW_5M_CONTRACT_REF: Final[str] = "contracts/datasets/futures_ohlcv_5m.v1.yaml"
DERIVED_CONTRACT_REF: Final[str] = "contracts/datasets/futures_ohlcv_derived_timeframe.v1.yaml"
CALENDAR_CONTRACT_REF: Final[str] = "contracts/datasets/futures_calendar_session.v1.yaml"
RAW_SOURCE_CONTRACT_REF: Final[str] = "contracts/datasets/futures_source_contracts.v1.yaml"


class ControlledMaterializationProofError(ValueError):
    pass


@dataclass(frozen=True)
class DerivedProofOutput:
    timeframe: str
    storage_path: Path
    manifest_path: Path
    quality_report_path: Path
    row_count: int


@dataclass(frozen=True)
class ControlledMaterializationProofResult:
    run_id: str
    raw_storage_path: Path
    raw_manifest_path: Path
    raw_quality_report_path: Path
    derived_outputs: tuple[DerivedProofOutput, ...]
    proof_summary_path: Path
    output_files: tuple[Path, ...]
    proof_summary: Mapping[str, object]


class DeterministicRaw5mSourceAdapter:
    def __init__(self, rows: Sequence[Mapping[str, object]]) -> None:
        self._rows = tuple(rows)

    def read_rows(self, request: object) -> Sequence[Mapping[str, object]]:
        return self._rows


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError as exc:
        raise ControlledMaterializationProofError("python-dotenv is required for controlled proof execution") from exc
    load_dotenv()


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return value.as_posix()
    raise TypeError("unsupported json value: " + type(value).__name__)


def _write_json(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default) + "\n", encoding="utf-8")


def _write_parquet(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    try:
        import pandas as pd
    except ImportError as exc:
        raise ControlledMaterializationProofError("pandas/pyarrow parquet writer dependency is unavailable") from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        pd.DataFrame(list(rows)).to_parquet(path, index=False)
    except Exception as exc:
        raise ControlledMaterializationProofError("parquet write failed") from exc


def _safe_text(value: str, field_name: str) -> str:
    return guard_text(value.strip(), field_name)


def _require_path_root(value: str | None, field_name: str) -> Path:
    if value is None or not str(value).strip():
        raise ControlledMaterializationProofError(field_name + " is required")
    return Path(str(value).strip())


def _resolve_moex_data_root(cli_value: str | None) -> str:
    value = cli_value if cli_value is not None else os.environ.get("MOEX_DATA_ROOT")
    if value is None or not value.strip():
        raise ControlledMaterializationProofError("MOEX_DATA_ROOT or --moex-data-root is required")
    return value.strip()


def _dedupe_timeframes(values: Sequence[str]) -> tuple[str, ...]:
    result: list[str] = []
    for raw_value in values:
        timeframe = validate_timeframe(_safe_text(raw_value, "timeframe"), derived_only=True)
        if timeframe in result:
            raise ControlledMaterializationProofError("derived timeframe is duplicated")
        result.append(timeframe)
    if not result:
        raise ControlledMaterializationProofError("at least one derived timeframe is required")
    return tuple(result)


def _contract_values(repo_root: Path, repo_relative_path: str) -> Mapping[str, object]:
    values = load_simple_yaml_mapping(repo_root, repo_relative_path)
    validate_dataset_contract_values(values)
    return values


def _source_contract_values(board: str, market: str) -> dict[str, object]:
    return {
        "source_id": "moex_iss_forts_candles_5m",
        "source_system": "MOEX_ISS",
        "market": market,
        "board": board,
        "native_timeframe": "5m",
        "output_contract_ref": RAW_5M_CONTRACT_REF,
    }


def _universe_values(family: str, secid: str, board: str, market: str, series_type: str) -> dict[str, object]:
    return {
        "universe_id": "controlled_materialization_proof_universe.v1",
        "dynamic_scan_allowed": False,
        "instruments": [
            {
                "FAMILY": family,
                "SECID": secid,
                "BOARD": board,
                "MARKET": market,
                "SERIES_TYPE": series_type,
            }
        ],
    }


def _raw_rows(family: str, secid: str, board: str, market: str, series_type: str, trade_date: date) -> tuple[dict[str, object], ...]:
    return (
        {
            "ts": datetime.combine(trade_date, datetime.strptime("10:00", "%H:%M").time()),
            "trade_date": trade_date,
            "session_date": trade_date,
            "FAMILY": family,
            "SECID": secid,
            "BOARD": board,
            "MARKET": market,
            "SERIES_TYPE": series_type,
            "open": 100.0,
            "high": 101.0,
            "low": 99.5,
            "close": 100.5,
            "volume": 10,
            "value": 1005.0,
            "trades": 2,
        },
        {
            "ts": datetime.combine(trade_date, datetime.strptime("10:05", "%H:%M").time()),
            "trade_date": trade_date,
            "session_date": trade_date,
            "FAMILY": family,
            "SECID": secid,
            "BOARD": board,
            "MARKET": market,
            "SERIES_TYPE": series_type,
            "open": 100.5,
            "high": 102.0,
            "low": 100.0,
            "close": 101.0,
            "volume": 11,
            "value": 1111.0,
            "trades": 3,
        },
    )


def _placeholders(
    *, run_id: str, timeframe: str | None, family: str, secid: str, board: str, market: str, series_type: str, trade_date: date
) -> dict[str, str | None]:
    return {
        "RUN_ID": run_id,
        "TIMEFRAME": timeframe,
        "FAMILY": family,
        "SECID": secid,
        "BOARD": board,
        "MARKET": market,
        "SERIES_TYPE": series_type,
        "TRADE_DATE": trade_date.isoformat(),
        "YYYY-MM-DD": trade_date.isoformat(),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Controlled futures data-core materialization proof runner")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--moex-data-root")
    parser.add_argument("--artifact-bundle-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--family", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--board", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--series-type", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--derived-timeframes", nargs="+", required=True)
    parser.add_argument("--raw-manifest-ref", required=True)
    parser.add_argument("--raw-quality-report-ref", required=True)
    parser.add_argument("--derived-manifest-ref", required=True)
    parser.add_argument("--derived-quality-report-ref", required=True)
    return parser


def _execute(args: argparse.Namespace) -> ControlledMaterializationProofResult:
    repo_root = Path(args.repo_root)
    moex_data_root = _resolve_moex_data_root(args.moex_data_root)
    artifact_bundle_root = _require_path_root(args.artifact_bundle_root, "artifact_bundle_root")
    run_id = _safe_text(args.run_id, "run_id")
    family = _safe_text(args.family, "FAMILY")
    secid = _safe_text(args.secid, "SECID")
    board = _safe_text(args.board, "BOARD")
    market = _safe_text(args.market, "MARKET")
    series_type = _safe_text(args.series_type, "SERIES_TYPE")
    trade_date = date.fromisoformat(_safe_text(args.trade_date, "trade_date"))
    derived_timeframes = _dedupe_timeframes(args.derived_timeframes)
    raw_manifest_ref = _safe_text(args.raw_manifest_ref, "raw_manifest_ref")
    raw_quality_report_ref = _safe_text(args.raw_quality_report_ref, "raw_quality_report_ref")
    derived_manifest_ref = _safe_text(args.derived_manifest_ref, "derived_manifest_ref")
    derived_quality_report_ref = _safe_text(args.derived_quality_report_ref, "derived_quality_report_ref")

    core_config_values = load_simple_yaml_mapping(repo_root, CORE_CONFIG_REF)
    raw_contract_values = _contract_values(repo_root, RAW_5M_CONTRACT_REF)
    derived_contract_values = _contract_values(repo_root, DERIVED_CONTRACT_REF)
    raw_storage_ref = _safe_text(str(raw_contract_values["path_pattern"]), "raw_storage_ref")
    derived_storage_ref = _safe_text(str(derived_contract_values["path_pattern"]), "derived_storage_ref")

    base_placeholders = _placeholders(
        run_id=run_id,
        timeframe=None,
        family=family,
        secid=secid,
        board=board,
        market=market,
        series_type=series_type,
        trade_date=trade_date,
    )
    raw_storage_path = expand_contract_path(raw_storage_ref, moex_data_root, base_placeholders)
    raw_manifest_path = expand_contract_path(raw_manifest_ref, moex_data_root, base_placeholders)
    raw_quality_report_path = expand_contract_path(raw_quality_report_ref, moex_data_root, base_placeholders)

    raw_request_values = {
        "dataset_id": "futures_ohlcv_5m",
        "contract_id": "futures_ohlcv_5m.v1",
        "timeframe": "5m",
        "FAMILY": family,
        "SECID": secid,
        "BOARD": board,
        "MARKET": market,
        "SERIES_TYPE": series_type,
        "partition_key": trade_date.isoformat(),
        "storage_ref": raw_storage_ref,
        "calendar_contract_ref": CALENDAR_CONTRACT_REF,
        "manifest_ref": raw_manifest_ref,
        "quality_report_ref": raw_quality_report_ref,
        "source_contract_ref": RAW_SOURCE_CONTRACT_REF,
    }
    calendar = build_futures_calendar_from_rows(
        [{"trade_date": trade_date.isoformat(), "is_trading_day": True, "reason": "controlled_proof"}],
        calendar_contract_ref=CALENDAR_CONTRACT_REF,
    )
    rows = _raw_rows(family, secid, board, market, series_type, trade_date)
    raw_result = materialize_raw_5m_boundary(
        raw_request_values,
        universe_values=_universe_values(family, secid, board, market, series_type),
        source_contract_values=_source_contract_values(board, market),
        calendar=calendar,
        source_adapter=DeterministicRaw5mSourceAdapter(rows),
    )
    raw_manifest_values = futures_partition_manifest_to_values(raw_result.partition_validation.manifest)
    raw_quality_values = futures_quality_report_to_values(raw_result.partition_validation.quality_report)
    _write_parquet(raw_storage_path, rows)
    _write_json(raw_manifest_path, raw_manifest_values)
    _write_json(raw_quality_report_path, raw_quality_values)

    derived_outputs: list[DerivedProofOutput] = []
    for timeframe in derived_timeframes:
        placeholders = _placeholders(
            run_id=run_id,
            timeframe=timeframe,
            family=family,
            secid=secid,
            board=board,
            market=market,
            series_type=series_type,
            trade_date=trade_date,
        )
        derived_storage_path = expand_contract_path(derived_storage_ref, moex_data_root, placeholders)
        derived_manifest_path = expand_contract_path(derived_manifest_ref, moex_data_root, placeholders)
        derived_quality_path = expand_contract_path(derived_quality_report_ref, moex_data_root, placeholders)
        derived_request_values = {
            "dataset_id": "futures_ohlcv_derived_timeframe",
            "contract_id": "futures_ohlcv_derived_timeframe.v1",
            "timeframe": timeframe,
            "FAMILY": family,
            "SECID": secid,
            "BOARD": board,
            "MARKET": market,
            "SERIES_TYPE": series_type,
            "partition_key": trade_date.isoformat(),
            "storage_ref": derived_storage_ref,
            "parent_manifest_ref": raw_manifest_ref,
            "calendar_contract_ref": CALENDAR_CONTRACT_REF,
            "manifest_ref": derived_manifest_ref,
            "quality_report_ref": derived_quality_report_ref,
        }
        derived_result = resample_ohlcv_5m_partition(
            rows,
            derived_request_values,
            parent_manifest_values=raw_manifest_values,
            core_config_values=core_config_values,
            calendar=calendar,
        )
        derived_manifest_values = futures_partition_manifest_to_values(derived_result.manifest)
        derived_quality_values = futures_quality_report_to_values(derived_result.quality_report)
        _write_parquet(derived_storage_path, derived_result.rows)
        _write_json(derived_manifest_path, derived_manifest_values)
        _write_json(derived_quality_path, derived_quality_values)
        derived_outputs.append(
            DerivedProofOutput(
                timeframe=timeframe,
                storage_path=derived_storage_path,
                manifest_path=derived_manifest_path,
                quality_report_path=derived_quality_path,
                row_count=len(derived_result.rows),
            )
        )

    proof_summary_path = artifact_bundle_root / run_id / "controlled_materialization_proof_summary.json"
    output_files: tuple[Path, ...] = (
        raw_storage_path,
        raw_manifest_path,
        raw_quality_report_path,
        *(path for item in derived_outputs for path in (item.storage_path, item.manifest_path, item.quality_report_path)),
        proof_summary_path,
    )
    proof_summary = {
        "run_id": run_id,
        "status": "succeeded",
        "proof_type": "controlled_data_core_materialization",
        "source_adapter": "deterministic_fixture_input",
        "real_iss_fetch_performed": False,
        "strategy_execution_performed": False,
        "runtime_live_performed": False,
        "raw_5m": {
            "storage_path": raw_storage_path.as_posix(),
            "manifest_path": raw_manifest_path.as_posix(),
            "quality_report_path": raw_quality_report_path.as_posix(),
            "row_count": raw_result.partition_validation.row_count,
        },
        "derived": [
            {
                "timeframe": item.timeframe,
                "storage_path": item.storage_path.as_posix(),
                "manifest_path": item.manifest_path.as_posix(),
                "quality_report_path": item.quality_report_path.as_posix(),
                "row_count": item.row_count,
            }
            for item in derived_outputs
        ],
    }
    _write_json(proof_summary_path, proof_summary)
    return ControlledMaterializationProofResult(
        run_id=run_id,
        raw_storage_path=raw_storage_path,
        raw_manifest_path=raw_manifest_path,
        raw_quality_report_path=raw_quality_report_path,
        derived_outputs=tuple(derived_outputs),
        proof_summary_path=proof_summary_path,
        output_files=output_files,
        proof_summary=proof_summary,
    )


def run_controlled_materialization_proof(argv: Sequence[str] | None = None) -> ControlledMaterializationProofResult:
    _load_dotenv()
    args = _build_parser().parse_args(argv)
    try:
        return _execute(args)
    except ControlledMaterializationProofError:
        raise
    except ValueError as exc:
        raise ControlledMaterializationProofError(str(exc)) from exc


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = run_controlled_materialization_proof(argv)
    except ControlledMaterializationProofError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result.proof_summary, ensure_ascii=False, sort_keys=True, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
