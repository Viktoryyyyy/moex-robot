from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from moex_data import step8_position_risk_state as step8


SCHEMA_VERSION = "rub_analysis_bundle.v1"
SUPPORTED_SCOPES = {"daily", "weekly"}
ROOT_REF_PREFIX = "${MOEX_DATA_ROOT}/"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class Step9AnalysisBundleError(ValueError):
    pass


def _fail(message: str) -> None:
    raise Step9AnalysisBundleError(message)


def _reject_duplicate_json_members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail("duplicate JSON object member: " + key)
        result[key] = value
    return result


def _reject_json_constant(token: str) -> None:
    _fail("JSON numeric constant must be finite: " + token)


def _load_json(path: Path, field: str, *, decimal_tokens: bool = False) -> Mapping[str, Any]:
    if path.is_symlink() or not path.is_file():
        _fail(field + " must be a regular non-symlink file")
    try:
        kwargs: dict[str, Any] = {
            "object_pairs_hook": _reject_duplicate_json_members,
            "parse_constant": _reject_json_constant,
        }
        if decimal_tokens:
            kwargs["parse_float"] = Decimal
        value = json.loads(path.read_text(encoding="utf-8"), **kwargs)
    except Step9AnalysisBundleError:
        raise
    except Exception as exc:
        raise Step9AnalysisBundleError(field + " is not valid JSON: " + str(exc)) from exc
    if not isinstance(value, Mapping):
        _fail(field + " must contain a JSON object")
    return value


def _data_root() -> Path:
    raw = os.environ.get("MOEX_DATA_ROOT")
    if not isinstance(raw, str) or not raw.strip():
        _fail("MOEX_DATA_ROOT is required")
    if raw != raw.strip():
        _fail("MOEX_DATA_ROOT must not contain surrounding whitespace")
    root = Path(raw)
    if not root.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    if root.is_symlink() or not root.is_dir():
        _fail("MOEX_DATA_ROOT must be an existing non-symlink directory")
    return root.resolve(strict=True)


def _parse_as_of(value: str) -> datetime:
    if not isinstance(value, str) or not value or value != value.strip():
        _fail("as_of must be a non-empty timestamp without surrounding whitespace")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise Step9AnalysisBundleError("as_of must be ISO-8601 timezone-aware timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail("as_of must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_root_ref(value: object, field: str, root: Path) -> Path:
    if not isinstance(value, str) or not value.startswith(ROOT_REF_PREFIX):
        _fail(field + " must be an explicit ${MOEX_DATA_ROOT}/ reference")
    relative_text = value[len(ROOT_REF_PREFIX):]
    if not relative_text or relative_text.startswith("/") or "\\" in relative_text:
        _fail(field + " has invalid rooted reference")
    relative = Path(relative_text)
    if relative.is_absolute() or any(part in ("", ".", "..") for part in relative.parts):
        _fail(field + " contains invalid path traversal")
    candidate = root.joinpath(relative)
    if candidate.is_symlink():
        _fail(field + " must not reference a symlink")
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise Step9AnalysisBundleError(field + " must resolve inside MOEX_DATA_ROOT") from exc
    if not resolved.is_file():
        _fail(field + " must reference a regular file")
    return resolved


def _verify_required_sha(pointer: Mapping[str, Any], field: str, path: Path) -> str:
    sha_field = field + "_sha256"
    expected = pointer.get(sha_field)
    if expected is None:
        _fail(sha_field + " is required trusted integrity evidence")
    if not isinstance(expected, str) or _SHA256_RE.fullmatch(expected) is None:
        _fail(sha_field + " must be lowercase SHA256")
    observed = _sha256_file(path)
    if observed != expected:
        _fail(sha_field + " mismatch")
    return observed


@dataclass(frozen=True)
class PointerSpec:
    block_id: str
    stage: int
    dataset_id: str
    instrument_id: str
    causal_field: str
    timeframe: str | None = None
    event_tiebreak_fields: tuple[str, ...] = ()


def _stage3_specs() -> tuple[PointerSpec, ...]:
    quote_instruments = ("si_front_contract", "si_next_contract", "cr_front_contract", "cr_next_contract")
    specs = [
        PointerSpec("stage3.quote." + instrument, 3, "futures_raw_5m", instrument, "ts", event_tiebreak_fields=("ts",))
        for instrument in quote_instruments
    ]
    specs.extend(
        PointerSpec("stage3.oi." + instrument, 3, "futures_open_interest_raw_5m", instrument, "availability_ts_utc", event_tiebreak_fields=("ts",))
        for instrument in quote_instruments
    )
    specs.extend(
        (
            PointerSpec("stage3.spot.usd_tom", 3, "fx_spot_raw_5m", "usd_tom", "ts", event_tiebreak_fields=("ts",)),
            PointerSpec("stage3.spot.cny_tom", 3, "fx_spot_raw_5m", "cny_tom", "ts", event_tiebreak_fields=("ts",)),
        )
    )
    return tuple(specs)


def _stage4_specs() -> tuple[PointerSpec, ...]:
    return (
        PointerSpec("stage4.basis.usd_rub", 4, "rub_basis_carry_5m", "usd_rub_basis_carry", "ts", event_tiebreak_fields=("ts",)),
        PointerSpec("stage4.basis.cny_rub", 4, "rub_basis_carry_5m", "cny_rub_basis_carry", "ts", event_tiebreak_fields=("ts",)),
    )


def _stage5_specs() -> tuple[PointerSpec, ...]:
    result: list[PointerSpec] = []
    for instrument in ("si_futures_family", "cr_futures_family"):
        result.append(
            PointerSpec(
                "stage5.futoi_eod." + instrument,
                5,
                "futures_futoi_eod",
                instrument,
                "availability_ts_utc",
                event_tiebreak_fields=("snapshot_ts_utc", "trade_date"),
            )
        )
        result.append(
            PointerSpec(
                "stage5.positioning." + instrument,
                5,
                "futures_futoi_positioning_features_d1",
                instrument,
                "availability_ts_utc",
                event_tiebreak_fields=("snapshot_ts_utc", "trade_date"),
            )
        )
    return tuple(result)


def _stage7_specs(scope: str) -> tuple[PointerSpec, ...]:
    timeframes = ("1D",) if scope == "daily" else ("1D", "1W")
    result: list[PointerSpec] = []
    for timeframe in timeframes:
        for instrument in ("usdrubf_futures_family", "cnyrubf_futures_family"):
            for dataset_id, name in (
                ("rub_native_ohlcv_htf", "ohlcv"),
                ("rub_technical_features_htf", "technical"),
            ):
                result.append(
                    PointerSpec(
                        f"stage7.{name}.{timeframe}.{instrument}",
                        7,
                        dataset_id,
                        instrument,
                        "availability_ts_utc",
                        timeframe=timeframe,
                        event_tiebreak_fields=("period_end_date", "period_start_date"),
                    )
                )
    return tuple(result)


def pointer_specs(scope: str) -> tuple[PointerSpec, ...]:
    if scope not in SUPPORTED_SCOPES:
        _fail("scope must be daily or weekly")
    return _stage3_specs() + _stage4_specs() + _stage5_specs() + _stage7_specs(scope)


def _pointer_path(root: Path, spec: PointerSpec) -> Path:
    base = root / "state" / "datasets" / ("dataset_id=" + spec.dataset_id)
    if spec.timeframe is not None:
        base = base / ("timeframe=" + spec.timeframe)
    return base / ("instrument_id=" + spec.instrument_id) / "current_accepted_manifest.json"


def _validate_support_identity(
    values: Mapping[str, Any],
    spec: PointerSpec,
    label: str,
    *,
    quality_required: bool,
    support_kind: str = "manifest",
    producer_run_id: str | None = None,
) -> None:
    if producer_run_id is not None and values.get("run_id") != producer_run_id:
        _fail(label + " run_id mismatch")
    if spec.stage == 3 and spec.dataset_id == "futures_raw_5m":
        if support_kind == "manifest":
            scope = values.get("instrument_scope")
            if isinstance(scope, (str, bytes)) or not isinstance(scope, Sequence) or list(scope) != [spec.instrument_id]:
                _fail(label + " instrument_scope mismatch")
            source_contract = values.get("source_contract")
            if not isinstance(source_contract, Mapping):
                _fail(label + " source_contract must be an object")
            if source_contract.get("instrument_id") != spec.instrument_id:
                _fail(label + " source_contract instrument_id mismatch")
            if source_contract.get("source_id") != "moex_algopack_fo_tradestats_5m":
                _fail(label + " source_contract source_id mismatch")
            if values.get("refresh_status") != "succeeded":
                _fail(label + " refresh_status must be succeeded")
            return
        rows = values.get("rows")
        if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence) or len(rows) != 1 or not isinstance(rows[0], Mapping):
            _fail(label + " rows must contain exactly one identity object")
        row = rows[0]
        if row.get("run_id") != producer_run_id:
            _fail(label + " rows[0].run_id mismatch")
        if row.get("dataset_id") != spec.dataset_id:
            _fail(label + " rows[0].dataset_id mismatch")
        if row.get("instrument_id") != spec.instrument_id:
            _fail(label + " rows[0].instrument_id mismatch")
        if row.get("source_id") != "moex_algopack_fo_tradestats_5m":
            _fail(label + " rows[0].source_id mismatch")
        if row.get("quality_status") != "pass":
            _fail(label + " rows[0].quality_status must be pass")
        return
    if "dataset_id" not in values:
        _fail(label + " missing dataset_id")
    if values["dataset_id"] != spec.dataset_id:
        _fail(label + " dataset_id mismatch")
    if "instrument_id" not in values:
        _fail(label + " missing instrument_id")
    if values["instrument_id"] != spec.instrument_id:
        _fail(label + " instrument_id mismatch")
    if spec.timeframe is not None:
        if "timeframe" not in values:
            _fail(label + " missing timeframe")
        if values["timeframe"] != spec.timeframe:
            _fail(label + " timeframe mismatch")
    if quality_required:
        status_field = "status" if spec.stage == 3 and support_kind == "manifest" else "quality_status"
        expected_status = "succeeded" if status_field == "status" else "pass"
        if status_field not in values:
            _fail(label + " missing " + status_field)
        if values[status_field] != expected_status:
            _fail(label + " " + status_field + " must be " + expected_status)


def _required_pointer_token(pointer: Mapping[str, Any], field: str, spec: PointerSpec) -> str:
    value = pointer.get(field)
    if not isinstance(value, str) or not value or value != value.strip():
        _fail(spec.block_id + " pointer " + field + " must be non-empty without surrounding whitespace")
    return value


def _validate_pointer_provenance(pointer: Mapping[str, Any], spec: PointerSpec) -> tuple[str, str, str]:
    run_id = _required_pointer_token(pointer, "run_id", spec)
    acceptance_run_id = _required_pointer_token(pointer, "acceptance_run_id", spec)
    acceptance_contract_id = _required_pointer_token(pointer, "acceptance_contract_id", spec)
    expected_contracts = {
        3: "step3_canonical_raw_acceptance.v1",
        4: "step4_rub_basis_carry_acceptance.v1",
        5: "step5_futoi_positioning_acceptance.v1",
        7: "step7_rub_native_d1_w1_technical_acceptance.v1",
    }
    expected = expected_contracts.get(spec.stage)
    if expected is not None and acceptance_contract_id != expected:
        _fail(spec.block_id + " pointer acceptance_contract_id mismatch")
    if expected is None:
        _fail(spec.block_id + " has no declared acceptance contract")
    return run_id, acceptance_run_id, acceptance_contract_id


def _to_utc_series(frame: pd.DataFrame, spec: PointerSpec) -> pd.Series:
    field = spec.causal_field
    block_id = spec.block_id
    if field not in frame.columns:
        _fail(block_id + " missing causal field: " + field)
    try:
        converted = pd.to_datetime(frame[field], errors="raise")
        if converted.dt.tz is None:
            timezone_name = "Europe/Moscow" if spec.stage == 3 and field == "ts" else "UTC"
            converted = converted.dt.tz_localize(timezone_name, ambiguous="raise", nonexistent="raise")
        converted = converted.dt.tz_convert("UTC")
    except Exception as exc:
        raise Step9AnalysisBundleError(block_id + " causal field is not timestamp-compatible") from exc
    if converted.isna().any():
        _fail(block_id + " causal field contains null")
    return converted


def _selected_row(frame: pd.DataFrame, spec: PointerSpec, as_of: datetime) -> tuple[dict[str, Any], int]:
    if frame.empty:
        _fail(spec.block_id + " accepted partition is empty")
    if "instrument_id" not in frame.columns:
        _fail(spec.block_id + " missing instrument_id column")
    identities = set(str(value) for value in frame["instrument_id"].dropna().tolist())
    if identities != {spec.instrument_id}:
        _fail(spec.block_id + " partition instrument identity mismatch")
    if spec.timeframe is not None:
        if "timeframe" not in frame.columns:
            _fail(spec.block_id + " missing timeframe column")
        timeframes = set(str(value) for value in frame["timeframe"].dropna().tolist())
        if timeframes != {spec.timeframe}:
            _fail(spec.block_id + " partition timeframe mismatch")

    causal = _to_utc_series(frame, spec)
    cutoff = pd.Timestamp(as_of)
    eligible_mask = causal <= cutoff
    eligible = frame.loc[eligible_mask].copy()
    eligible_causal = causal.loc[eligible_mask]
    if eligible.empty:
        _fail(spec.block_id + " has no causal observation at or before as_of")

    eligible["_step9_causal"] = eligible_causal
    sort_fields = ["_step9_causal"]
    for field in spec.event_tiebreak_fields:
        if field not in eligible.columns:
            continue
        try:
            converted = pd.to_datetime(eligible[field], utc=True, errors="raise")
            eligible["_step9_sort_" + field] = converted
        except Exception:
            eligible["_step9_sort_" + field] = eligible[field].astype(str)
        sort_fields.append("_step9_sort_" + field)
    eligible = eligible.sort_values(sort_fields, kind="mergesort")
    row = eligible.iloc[-1].drop(labels=[column for column in eligible.columns if column.startswith("_step9_")])
    selected = {str(key): _json_value(value, spec.block_id + "." + str(key)) for key, value in row.to_dict().items()}
    return selected, int(eligible_mask.sum())


def _json_value(value: Any, field: str) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.isoformat()
        return value.tz_convert("UTC").isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat()
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            _fail(field + " contains non-finite Decimal")
        return str(value)
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if not math.isfinite(value):
            _fail(field + " contains non-finite float")
        return value
    if isinstance(value, (str, int, bool)):
        return value
    if pd.isna(value):
        return None
    return str(value)


def _read_pointer_block(root: Path, spec: PointerSpec, as_of: datetime) -> dict[str, Any]:
    pointer_path = _pointer_path(root, spec)
    try:
        resolved_pointer = pointer_path.resolve(strict=True)
        resolved_pointer.relative_to(root)
    except (OSError, ValueError) as exc:
        raise Step9AnalysisBundleError(spec.block_id + " accepted pointer missing or escaped data root") from exc
    if pointer_path.is_symlink() or not resolved_pointer.is_file():
        _fail(spec.block_id + " accepted pointer must be regular non-symlink file")
    pointer = _load_json(resolved_pointer, spec.block_id + ".pointer")
    if pointer.get("dataset_id") != spec.dataset_id:
        _fail(spec.block_id + " pointer dataset_id mismatch")
    if pointer.get("instrument_id") != spec.instrument_id:
        _fail(spec.block_id + " pointer instrument_id mismatch")
    if spec.timeframe is not None and pointer.get("timeframe") != spec.timeframe:
        _fail(spec.block_id + " pointer timeframe mismatch")
    if pointer.get("quality_status") != "pass":
        _fail(spec.block_id + " pointer quality_status must be pass")
    if "refresh_status" in pointer and pointer.get("refresh_status") != "succeeded":
        _fail(spec.block_id + " pointer refresh_status must be succeeded")
    run_id, acceptance_run_id, acceptance_contract_id = _validate_pointer_provenance(pointer, spec)

    resolved: dict[str, Path] = {}
    observed_hashes: dict[str, str] = {}
    for field in ("manifest_ref", "quality_report_ref", "partition_ref"):
        if field not in pointer:
            _fail(spec.block_id + " pointer missing " + field)
        path = _resolve_root_ref(pointer[field], spec.block_id + "." + field, root)
        resolved[field] = path
        base = {
            "manifest_ref": "manifest",
            "quality_report_ref": "quality_report",
            "partition_ref": "partition",
        }[field]
        observed_hashes[base + "_sha256"] = _verify_required_sha(pointer, base, path)

    manifest = _load_json(resolved["manifest_ref"], spec.block_id + ".manifest")
    quality = _load_json(resolved["quality_report_ref"], spec.block_id + ".quality_report")
    _validate_support_identity(
        manifest, spec, spec.block_id + ".manifest",
        support_kind="manifest", producer_run_id=run_id, quality_required=True,
    )
    _validate_support_identity(
        quality, spec, spec.block_id + ".quality_report",
        support_kind="quality_report", producer_run_id=run_id, quality_required=True,
    )

    try:
        frame = pd.read_parquet(resolved["partition_ref"])
    except Exception as exc:
        raise Step9AnalysisBundleError(spec.block_id + " partition read failed: " + str(exc)) from exc
    if not isinstance(frame, pd.DataFrame):
        _fail(spec.block_id + " partition did not produce a dataframe")

    selected, causal_row_count = _selected_row(frame, spec, as_of)
    selected_causal_value = selected.get(spec.causal_field)
    if selected_causal_value is None:
        _fail(spec.block_id + " selected observation lost causal field")

    return {
        "block_id": spec.block_id,
        "stage": spec.stage,
        "dataset_id": spec.dataset_id,
        "instrument_id": spec.instrument_id,
        "timeframe": spec.timeframe,
        "status": "ready",
        "causal_field": spec.causal_field,
        "selected_observation": selected,
        "causal_rows_at_or_before_as_of": causal_row_count,
        "provenance": {
            "pointer_ref": ROOT_REF_PREFIX + pointer_path.relative_to(root).as_posix(),
            "run_id": run_id,
            "acceptance_run_id": acceptance_run_id,
            "acceptance_contract_id": acceptance_contract_id,
            "manifest_ref": pointer["manifest_ref"],
            "quality_report_ref": pointer["quality_report_ref"],
            "partition_ref": pointer["partition_ref"],
            **observed_hashes,
        },
    }


def _load_position_risk(path_value: str | None, as_of: datetime) -> dict[str, Any]:
    if path_value is None:
        return {
            "status": "not_supplied",
            "reason": "explicit Stage 8 manual/read-only risk-state input was not supplied",
        }
    if not isinstance(path_value, str) or not path_value or path_value != path_value.strip():
        _fail("position_risk_input must be a non-empty path without surrounding whitespace")
    path = Path(path_value)
    if path.is_symlink() or not path.is_file():
        _fail("position_risk_input must be an existing regular non-symlink file")
    payload = _load_json(path, "position_risk_input", decimal_tokens=True)
    try:
        state = step8.build_position_risk_state(payload)
    except Exception as exc:
        raise Step9AnalysisBundleError("position_risk_input failed Stage 8 validation: " + str(exc)) from exc
    state_as_of_raw = state.get("as_of_ts_utc")
    if not isinstance(state_as_of_raw, str):
        _fail("validated Stage 8 state missing as_of_ts_utc")
    state_as_of = _parse_as_of(state_as_of_raw)
    if state_as_of > as_of:
        _fail("Stage 8 risk state is later than bundle as_of")
    return {
        "status": "ready",
        "input_mode": "explicit_manual_or_read_only_file",
        "state": state,
    }


def _external_context_required() -> list[dict[str, str]]:
    return [
        {"block_id": "macro_ru", "status": "external_context_required"},
        {"block_id": "government_fx_operations_and_tax_cycle", "status": "external_context_required"},
        {"block_id": "official_event_calendar_and_surprises", "status": "external_context_required"},
        {"block_id": "fresh_news_and_geopolitics", "status": "external_context_required"},
        {"block_id": "oil_brent_urals", "status": "external_context_required"},
        {"block_id": "global_usd_usd_cny_cnh_dxy_ust", "status": "external_context_required"},
    ]


def _policy_gaps(scope: str) -> list[dict[str, str]]:
    if scope != "weekly":
        return []
    return [
        {"block_id": "si_cr_continuous_weekly", "status": "not_ready_policy_gap"},
        {"block_id": "weekly_open_interest", "status": "not_ready_policy_gap"},
        {"block_id": "ema_filter", "status": "not_ready_policy_gap"},
        {"block_id": "realized_volatility", "status": "not_ready_policy_gap"},
        {"block_id": "range_percentile", "status": "not_ready_policy_gap"},
        {"block_id": "swing_high_low", "status": "not_ready_policy_gap"},
        {"block_id": "break_of_structure", "status": "not_ready_policy_gap"},
    ]


def build_analysis_bundle(
    *,
    scope: str,
    as_of: str,
    position_risk_input: str | None = None,
) -> dict[str, Any]:
    if scope not in SUPPORTED_SCOPES:
        _fail("scope must be daily or weekly")
    as_of_dt = _parse_as_of(as_of)
    root = _data_root()
    blocks = [_read_pointer_block(root, spec, as_of_dt) for spec in pointer_specs(scope)]
    if len({block["block_id"] for block in blocks}) != len(blocks):
        _fail("duplicate logical server-core block")
    acceptance_runs_by_stage: dict[int, set[str]] = {}
    for block in blocks:
        acceptance_runs_by_stage.setdefault(int(block["stage"]), set()).add(str(block["provenance"]["acceptance_run_id"]))
    mixed_stages = sorted(stage for stage, run_ids in acceptance_runs_by_stage.items() if len(run_ids) != 1)
    if mixed_stages:
        _fail("mixed acceptance_run_id within stage(s): " + ",".join(str(stage) for stage in mixed_stages))
    position_risk = _load_position_risk(position_risk_input, as_of_dt)
    external = _external_context_required()
    gaps = _policy_gaps(scope)

    if gaps:
        bundle_status = "partial_external_context_and_policy_gaps"
    else:
        bundle_status = "partial_external_context_required"
    if position_risk["status"] != "ready":
        bundle_status += "_position_risk_not_supplied"

    return {
        "schema_version": SCHEMA_VERSION,
        "identity": {
            "project": "MOEX_Bot",
            "scope": scope,
            "as_of": _iso_utc(as_of_dt),
        },
        "server_core": {
            "status": "ready",
            "block_count": len(blocks),
            "blocks": blocks,
        },
        "position_risk": position_risk,
        "external_context_required": external,
        "readiness": {
            "bundle_status": bundle_status,
            "server_core": "ready",
            "position_risk": position_risk["status"],
            "external_context": "external_context_required",
            "policy_gaps": gaps,
            "analysis_bundle_complete": False,
        },
        "quality_gates": {
            "bundle_generates_trade_recommendation": False,
            "bundle_generates_scenario_probabilities": False,
            "bundle_generates_market_regime": False,
            "bundle_generates_position_size": False,
            "bundle_generates_stop_or_target": False,
            "no_front_next_mix_without_explicit_stage3_binding": True,
            "futoi_participant_groups_are_descriptive_not_smart_money_labels": True,
            "missing_external_context_requires_downstream_completion": True,
            "missing_position_risk_blocks_downstream_size_or_add_recommendation": position_risk["status"] != "ready",
            "weekly_declared_policy_gaps_preserved": scope != "weekly" or bool(gaps),
        },
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build deterministic Stage 9 RUB daily/weekly analysis bundle.")
    parser.add_argument("--scope", required=True, choices=sorted(SUPPORTED_SCOPES))
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--position-risk-input")
    parser.add_argument("--output-json")
    return parser.parse_args(argv)


def _write_output(path_value: str, bundle: Mapping[str, Any]) -> None:
    path = Path(path_value)
    if path.exists() and path.is_symlink():
        _fail("output_json must not be a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(bundle, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    path.write_text(payload, encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        bundle = build_analysis_bundle(
            scope=args.scope,
            as_of=args.as_of,
            position_risk_input=args.position_risk_input,
        )
        payload = json.dumps(bundle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if args.output_json:
            _write_output(args.output_json, bundle)
        print(payload)
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {"project": "MOEX_Bot", "stage": 9, "status": "bundle_failed", "error": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

