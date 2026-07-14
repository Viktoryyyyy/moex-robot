from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
import os
from pathlib import Path
from typing import Final

from .contract_io import (
    FuturesContractIoError,
    FuturesContractPackage,
    expand_contract_path,
    load_futures_data_lake_contract_package,
    reject_dynamic_markers,
)
from .schemas import FuturesDatasetContract


class FuturesControlledReadError(ValueError):
    pass


_MAX_CONTROLLED_RANGE_DAYS: Final[int] = 7
_BLOCKED_IMPLEMENTATION_STATUS: Final[str] = "blocked_placeholder"


@dataclass(frozen=True)
class ControlledReadPlan:
    dataset_id: str
    contract_id: str
    family: str
    secid: str | None = None
    trade_date: str | None = None
    from_trade_date: str | None = None
    till_trade_date: str | None = None
    series_type: str | None = "raw"
    roll_policy: str | None = None
    run_id: str | None = None
    instrument_id: str | None = None
    source_id: str | None = None
    board: str | None = None
    market: str | None = None
    engine: str | None = None
    max_days: int = _MAX_CONTROLLED_RANGE_DAYS


@dataclass(frozen=True)
class ControlledReadPathProbe:
    path: Path
    exists: bool


@dataclass(frozen=True)
class ControlledReadEvidence:
    status: str
    dataset_id: str
    contract_id: str
    paths: tuple[ControlledReadPathProbe, ...]
    message: str


def _as_read_error(exc: Exception) -> FuturesControlledReadError:
    return FuturesControlledReadError(str(exc))


def _require_text(value: str | None, field_name: str) -> str:
    try:
        return reject_dynamic_markers(value or "", field_name)
    except FuturesContractIoError as exc:
        raise _as_read_error(exc) from exc


def _optional_text(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _parse_trade_date(value: str, field_name: str) -> date:
    guarded = _require_text(value, field_name)
    try:
        parsed = date.fromisoformat(guarded)
    except ValueError as exc:
        raise FuturesControlledReadError(f"{field_name} must use YYYY-MM-DD") from exc
    if parsed.isoformat() != guarded:
        raise FuturesControlledReadError(f"{field_name} must use YYYY-MM-DD")
    return parsed


def _date_values(plan: ControlledReadPlan) -> tuple[str, ...]:
    if plan.max_days < 1 or plan.max_days > _MAX_CONTROLLED_RANGE_DAYS:
        raise FuturesControlledReadError("max_days exceeds controlled read hard limit")
    has_single = plan.trade_date is not None
    has_range_start = plan.from_trade_date is not None
    has_range_end = plan.till_trade_date is not None
    if has_single and (has_range_start or has_range_end):
        raise FuturesControlledReadError("trade_date and date range are mutually exclusive")
    if has_single:
        return (_parse_trade_date(plan.trade_date or "", "trade_date").isoformat(),)
    if has_range_start != has_range_end:
        raise FuturesControlledReadError("date range must include both from_trade_date and till_trade_date")
    if not has_range_start:
        raise FuturesControlledReadError("controlled read requires trade_date or bounded date range")
    start = _parse_trade_date(plan.from_trade_date or "", "from_trade_date")
    end = _parse_trade_date(plan.till_trade_date or "", "till_trade_date")
    if end < start:
        raise FuturesControlledReadError("till_trade_date must be greater than or equal to from_trade_date")
    count = (end - start).days + 1
    if count > plan.max_days:
        raise FuturesControlledReadError("date range exceeds controlled read hard limit")
    return tuple((start + timedelta(days=offset)).isoformat() for offset in range(count))


def _require_env_root(env: Mapping[str, str]) -> str:
    root = env.get("MOEX_DATA_ROOT")
    if not root or not root.strip():
        raise FuturesControlledReadError("MOEX_DATA_ROOT is required")
    return root


def _contract_for_plan(package: FuturesContractPackage, plan: ControlledReadPlan) -> FuturesDatasetContract:
    dataset_id = _require_text(plan.dataset_id, "dataset_id")
    contract_id = _require_text(plan.contract_id, "contract_id")
    contract = package.contracts_by_dataset_id.get(dataset_id)
    if contract is None:
        raise FuturesControlledReadError("unsupported dataset_id")
    if contract.contract_id != contract_id:
        raise FuturesControlledReadError("contract_id does not match dataset_id")
    if contract.implementation_status == _BLOCKED_IMPLEMENTATION_STATUS:
        raise FuturesControlledReadError("blocked placeholder contract cannot be read")
    return contract


def _placeholders_for_plan(plan: ControlledReadPlan, trade_date: str) -> dict[str, str | None]:
    family = _require_text(plan.family, "family")
    secid = _optional_text(plan.secid, "secid")
    return {
        "YYYY-MM-DD": trade_date,
        "FAMILY": family,
        "SECID": secid,
        "SERIES_TYPE": _optional_text(plan.series_type, "series_type"),
        "ROLL_POLICY": _optional_text(plan.roll_policy, "roll_policy"),
        "RUN_ID": _optional_text(plan.run_id, "run_id"),
        "INSTRUMENT_ID": _optional_text(plan.instrument_id, "instrument_id") or family,
        "SOURCE_ID": _optional_text(plan.source_id, "source_id") or secid,
        "BOARD": _optional_text(plan.board, "board"),
        "MARKET": _optional_text(plan.market, "market"),
        "ENGINE": _optional_text(plan.engine, "engine"),
    }


def controlled_read_paths(
    package: FuturesContractPackage,
    plan: ControlledReadPlan,
    env: Mapping[str, str] | None = None,
) -> tuple[Path, ...]:
    active_env = os.environ if env is None else env
    env_root = _require_env_root(active_env)
    contract = _contract_for_plan(package, plan)
    dates = _date_values(plan)
    try:
        return tuple(
            expand_contract_path(contract.path_pattern, env_root, _placeholders_for_plan(plan, trade_date))
            for trade_date in dates
        )
    except FuturesContractIoError as exc:
        raise _as_read_error(exc) from exc


def controlled_read_probe(
    repo_root: str | Path,
    plan: ControlledReadPlan,
    env: Mapping[str, str] | None = None,
) -> ControlledReadEvidence:
    package = load_futures_data_lake_contract_package(repo_root)
    paths = controlled_read_paths(package, plan, env)
    probes = tuple(ControlledReadPathProbe(path=path, exists=path.exists()) for path in paths)
    if all(probe.exists for probe in probes):
        return ControlledReadEvidence(
            status="available",
            dataset_id=plan.dataset_id,
            contract_id=plan.contract_id,
            paths=probes,
            message="controlled file existence probe succeeded",
        )
    return ControlledReadEvidence(
        status="blocked_no_server_artifact",
        dataset_id=plan.dataset_id,
        contract_id=plan.contract_id,
        paths=probes,
        message="controlled contract path expanded, but required server artifact is absent",
    )
