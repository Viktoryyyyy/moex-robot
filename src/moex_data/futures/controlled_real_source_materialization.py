from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Final

from moex_core.calendars.moex_iss_calendar import build_futures_calendar_from_rows
from moex_data.futures.apim_tradestats_5m import (
    MOEX_APIM_FO_TRADESTATS_SOURCE_ID,
    MoexApimFoTradestats5mAdapter,
)
from moex_data.futures.contract_io import expand_contract_path, load_simple_yaml_mapping
from moex_data.futures.iss_forts_5m import MOEX_ISS_FORTS_CANDLES_SOURCE_ID, MoexIssFortsCandles5mAdapter
from moex_data.futures.manifests import futures_partition_manifest_to_values
from moex_data.futures.materialization import Raw5mSourceAdapter, materialize_raw_5m_boundary
from moex_data.futures.raw_ohlcv_5m import Raw5mMaterializationRequest
from moex_data.futures.validation import guard_text, validate_dataset_contract_values
from moex_data.quality.futures_ohlcv import futures_quality_report_to_values

RAW_5M_CONTRACT_REF: Final[str] = "contracts/datasets/futures_ohlcv_5m.v1.yaml"
CALENDAR_CONTRACT_REF: Final[str] = "contracts/datasets/futures_calendar_session.v1.yaml"
RAW_SOURCE_CONTRACT_REF: Final[str] = "contracts/datasets/futures_source_contracts.v1.yaml"
FUTURES_UNIVERSE_REF: Final[str] = "configs/instruments/futures_universe.v1.yaml"
IDENTITY_FIELDS: Final[tuple[str, ...]] = ("FAMILY", "SECID", "BOARD", "MARKET", "SERIES_TYPE")
SOURCE_FIELDS: Final[tuple[str, ...]] = ("source_id", "source_system", "market", "board", "native_timeframe", "output_contract_ref")


class ControlledRealSourceMaterializationError(ValueError):
    pass


@dataclass(frozen=True)
class ControlledRealSourceMaterializationResult:
    run_id: str
    raw_storage_path: Path
    raw_manifest_path: Path
    raw_quality_report_path: Path
    output_files: tuple[Path, ...]
    proof_summary: Mapping[str, object]


class _RecordingAdapter:
    def __init__(self, wrapped: Raw5mSourceAdapter) -> None:
        self._wrapped = wrapped
        self.rows: tuple[Mapping[str, object], ...] | None = None

    def read_rows(self, request: Raw5mMaterializationRequest) -> Sequence[Mapping[str, object]]:
        if self.rows is not None:
            raise ControlledRealSourceMaterializationError("source adapter was read more than once")
        self.rows = tuple(self._wrapped.read_rows(request))
        return self.rows


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError as exc:
        raise ControlledRealSourceMaterializationError("python-dotenv is required") from exc
    load_dotenv()


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime, Path)):
        return value.isoformat() if not isinstance(value, Path) else value.as_posix()
    raise TypeError("unsupported json value: " + type(value).__name__)


def _write_json(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default) + "\n", encoding="utf-8")


def _write_parquet(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    try:
        import pandas as pd
    except ImportError as exc:
        raise ControlledRealSourceMaterializationError("pandas/pyarrow parquet writer dependency is unavailable") from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        pd.DataFrame(list(rows)).to_parquet(path, index=False)
    except Exception as exc:
        raise ControlledRealSourceMaterializationError("parquet write failed") from exc


def _safe_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ControlledRealSourceMaterializationError(field_name + " is required")
    return guard_text(value.strip(), field_name)


def _resolve_root(value: str | None) -> str:
    result = value if value is not None else os.environ.get("MOEX_DATA_ROOT")
    if result is None or not result.strip():
        raise ControlledRealSourceMaterializationError("MOEX_DATA_ROOT or --moex-data-root is required")
    return result.strip()


def _key_value(text: str, field_name: str) -> tuple[str, str]:
    key, sep, value = text.partition(":")
    if sep != ":":
        raise ControlledRealSourceMaterializationError(field_name + " must be key: value")
    return _safe_text(key, field_name + " key"), _safe_text(value, field_name + " value")


def _complete_identity(values: Mapping[str, object]) -> dict[str, object]:
    missing = tuple(field for field in IDENTITY_FIELDS if field not in values)
    if missing:
        raise ControlledRealSourceMaterializationError("missing configured instrument field: " + missing[0])
    return {field: _safe_text(values[field], field) for field in IDENTITY_FIELDS}


def _complete_source(values: object) -> dict[str, object]:
    if not isinstance(values, Mapping):
        raise ControlledRealSourceMaterializationError("source contract entry must be a mapping")
    missing = tuple(field for field in SOURCE_FIELDS if field not in values)
    if missing:
        raise ControlledRealSourceMaterializationError("missing source contract field: " + missing[0])
    return {field: _safe_text(values[field], field) for field in SOURCE_FIELDS}


def _load_universe(repo_root: Path) -> Mapping[str, object]:
    lines = (repo_root / FUTURES_UNIVERSE_REF).read_text(encoding="utf-8").splitlines()
    instruments: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    dynamic_scan_allowed_false = False
    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped == "dynamic_scan_allowed: false":
            dynamic_scan_allowed_false = True
            continue
        if stripped.startswith("- "):
            if current is not None:
                instruments.append(_complete_identity(current))
            current = {}
            key, value = _key_value(stripped.removeprefix("- ").strip(), "instrument")
            current[key] = value
            continue
        if current is not None and ":" in stripped:
            key, value = _key_value(stripped, "instrument")
            current[key] = value
    if current is not None:
        instruments.append(_complete_identity(current))
    if not dynamic_scan_allowed_false:
        raise ControlledRealSourceMaterializationError("futures universe dynamic_scan_allowed must be false")
    if not instruments:
        raise ControlledRealSourceMaterializationError("futures universe instruments must be non-empty")
    return {"universe_id": "futures_universe.v1", "dynamic_scan_allowed": False, "instruments": tuple(instruments)}


def _load_source_contract_entries(repo_root: Path) -> tuple[Mapping[str, object], ...]:
    values = load_simple_yaml_mapping(repo_root, RAW_SOURCE_CONTRACT_REF)
    path_rules = values.get("path_rules")
    if not isinstance(path_rules, Mapping):
        raise ControlledRealSourceMaterializationError("source contract path_rules must be a mapping")
    if path_rules.get("implicit_file_selection_allowed") is not False or path_rules.get("dynamic_scan_allowed") is not False:
        raise ControlledRealSourceMaterializationError("source contract must reject implicit selection and dynamic scan")
    raw_sources = values.get("sources")
    if isinstance(raw_sources, (str, bytes)) or not isinstance(raw_sources, Sequence):
        raise ControlledRealSourceMaterializationError("source contract sources must be a sequence")
    sources = tuple(_complete_source(item) for item in raw_sources)
    if not sources:
        raise ControlledRealSourceMaterializationError("source contract set must be non-empty")
    source_ids = tuple(source["source_id"] for source in sources)
    if len(set(source_ids)) != len(source_ids):
        raise ControlledRealSourceMaterializationError("duplicate source_id")
    return sources


def _contract_values(repo_root: Path, repo_relative_path: str) -> Mapping[str, object]:
    values = load_simple_yaml_mapping(repo_root, repo_relative_path)
    validate_dataset_contract_values(values)
    return values


def _source_contract_values(repo_root: Path, source_id: str, board: str, market: str) -> Mapping[str, object]:
    matches = tuple(source for source in _load_source_contract_entries(repo_root) if source["source_id"] == source_id)
    if len(matches) != 1:
        raise ControlledRealSourceMaterializationError("selected source_id is not uniquely declared")
    source = matches[0]
    if source["board"] != board or source["market"] != market:
        raise ControlledRealSourceMaterializationError("selected source contract does not match requested board/market")
    if source["native_timeframe"] != "5m":
        raise ControlledRealSourceMaterializationError("selected source contract is not native 5m")
    if source["output_contract_ref"] != RAW_5M_CONTRACT_REF:
        raise ControlledRealSourceMaterializationError("selected source contract does not bind raw 5m output")
    return source


def _default_source_adapter(source_id: str, iss_base_url: str, apim_base_url: str) -> Raw5mSourceAdapter:
    if source_id == MOEX_ISS_FORTS_CANDLES_SOURCE_ID:
        return MoexIssFortsCandles5mAdapter(base_url=iss_base_url)
    if source_id == MOEX_APIM_FO_TRADESTATS_SOURCE_ID:
        return MoexApimFoTradestats5mAdapter(base_url=apim_base_url)
    raise ControlledRealSourceMaterializationError("unsupported source_id")


def _placeholders(run_id: str, family: str, secid: str, board: str, market: str, series_type: str, trade_date: date) -> dict[str, str | None]:
    return {
        "RUN_ID": run_id,
        "TIMEFRAME": None,
        "FAMILY": family,
        "SECID": secid,
        "BOARD": board,
        "MARKET": market,
        "SERIES_TYPE": series_type,
        "TRADE_DATE": trade_date.isoformat(),
        "YYYY-MM-DD": trade_date.isoformat(),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Controlled FORTS native 5m real-source materialization runner")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--moex-data-root")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--family", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--board", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--series-type", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--raw-manifest-ref", required=True)
    parser.add_argument("--raw-quality-report-ref", required=True)
    parser.add_argument("--source-id", default=MOEX_ISS_FORTS_CANDLES_SOURCE_ID)
    parser.add_argument("--iss-base-url", default="https://iss.moex.com")
    parser.add_argument("--apim-base-url", default="https://iss.moex.com")
    return parser


def _execute(args: argparse.Namespace, *, source_adapter: Raw5mSourceAdapter | None = None) -> ControlledRealSourceMaterializationResult:
    repo_root = Path(args.repo_root)
    moex_data_root = _resolve_root(args.moex_data_root)
    run_id = _safe_text(args.run_id, "run_id")
    family = _safe_text(args.family, "FAMILY")
    secid = _safe_text(args.secid, "SECID")
    board = _safe_text(args.board, "BOARD")
    market = _safe_text(args.market, "MARKET")
    series_type = _safe_text(args.series_type, "SERIES_TYPE")
    raw_manifest_ref = _safe_text(args.raw_manifest_ref, "raw_manifest_ref")
    raw_quality_ref = _safe_text(args.raw_quality_report_ref, "raw_quality_report_ref")
    source_id = _safe_text(args.source_id, "source_id")
    iss_base_url = _safe_text(args.iss_base_url, "iss_base_url")
    apim_base_url = _safe_text(args.apim_base_url, "apim_base_url")
    try:
        trade_date = date.fromisoformat(_safe_text(args.trade_date, "trade_date"))
    except ValueError as exc:
        raise ControlledRealSourceMaterializationError("trade_date must be ISO date") from exc
    raw_contract = _contract_values(repo_root, RAW_5M_CONTRACT_REF)
    raw_storage_ref = _safe_text(raw_contract.get("path_pattern"), "raw_storage_ref")
    source_contract = _source_contract_values(repo_root, source_id, board, market)
    placeholders = _placeholders(run_id, family, secid, board, market, series_type, trade_date)
    raw_storage_path = expand_contract_path(raw_storage_ref, moex_data_root, placeholders)
    raw_manifest_path = expand_contract_path(raw_manifest_ref, moex_data_root, placeholders)
    raw_quality_path = expand_contract_path(raw_quality_ref, moex_data_root, placeholders)
    request_values = {
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
        "quality_report_ref": raw_quality_ref,
        "source_contract_ref": RAW_SOURCE_CONTRACT_REF,
    }
    calendar = build_futures_calendar_from_rows(
        [{"trade_date": trade_date.isoformat(), "is_trading_day": True, "reason": "controlled_real_source_materialization"}],
        calendar_contract_ref=CALENDAR_CONTRACT_REF,
    )
    adapter = source_adapter if source_adapter is not None else _default_source_adapter(source_id, iss_base_url, apim_base_url)
    recording = _RecordingAdapter(adapter)
    raw_result = materialize_raw_5m_boundary(
        request_values,
        universe_values=_load_universe(repo_root),
        source_contract_values=source_contract,
        calendar=calendar,
        source_adapter=recording,
    )
    if recording.rows is None:
        raise ControlledRealSourceMaterializationError("source adapter returned no recorded rows")
    _write_parquet(raw_storage_path, recording.rows)
    _write_json(raw_manifest_path, futures_partition_manifest_to_values(raw_result.partition_validation.manifest))
    _write_json(raw_quality_path, futures_quality_report_to_values(raw_result.partition_validation.quality_report))
    real_fetch = source_adapter is None
    proof_summary = {
        "run_id": run_id,
        "status": "succeeded",
        "proof_type": "controlled_real_source_materialization",
        "source_adapter": source_id,
        "real_source_fetch_performed": real_fetch,
        "real_iss_fetch_performed": real_fetch and source_id == MOEX_ISS_FORTS_CANDLES_SOURCE_ID,
        "real_apim_fetch_performed": real_fetch and source_id == MOEX_APIM_FO_TRADESTATS_SOURCE_ID,
        "strategy_execution_performed": False,
        "backtest_performed": False,
        "runtime_live_performed": False,
        "raw_5m": {
            "storage_path": raw_storage_path.as_posix(),
            "manifest_path": raw_manifest_path.as_posix(),
            "quality_report_path": raw_quality_path.as_posix(),
            "row_count": raw_result.partition_validation.row_count,
        },
    }
    return ControlledRealSourceMaterializationResult(
        run_id=run_id,
        raw_storage_path=raw_storage_path,
        raw_manifest_path=raw_manifest_path,
        raw_quality_report_path=raw_quality_path,
        output_files=(raw_storage_path, raw_manifest_path, raw_quality_path),
        proof_summary=proof_summary,
    )


def run_controlled_real_source_materialization(argv: Sequence[str] | None = None, *, source_adapter: Raw5mSourceAdapter | None = None) -> ControlledRealSourceMaterializationResult:
    _load_dotenv()
    args = _build_parser().parse_args(argv)
    try:
        return _execute(args, source_adapter=source_adapter)
    except ControlledRealSourceMaterializationError:
        raise
    except ValueError as exc:
        raise ControlledRealSourceMaterializationError(str(exc)) from exc


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = run_controlled_real_source_materialization(argv)
    except ControlledRealSourceMaterializationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result.proof_summary, ensure_ascii=False, sort_keys=True, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
