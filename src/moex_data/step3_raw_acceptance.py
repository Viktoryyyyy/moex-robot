from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final
from zoneinfo import ZoneInfo

import pandas as pd

CONTRACT_ID: Final[str] = "step3_canonical_raw_acceptance.v1"
CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
RUNS_SUBPATH: Final[tuple[str, ...]] = ("runs", "step3_canonical_raw")
MOSCOW_TZ: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")
EXPECTED_COUNTS: Final[dict[str, int]] = {
    "bindings": 4,
    "quote_partitions": 4,
    "open_interest_partitions": 4,
    "tom_partitions": 2,
}
EXPECTED_POINTER_COUNTS: Final[dict[str, int]] = {
    "futures_raw_5m": 4,
    "futures_open_interest_raw_5m": 4,
    "fx_spot_raw_5m": 2,
}
EXPECTED_SOURCE_IDS: Final[dict[str, str]] = {
    "futures_raw_5m": "moex_algopack_fo_tradestats_5m",
    "futures_open_interest_raw_5m": "moex_algopack_fo_open_interest_5m",
    "fx_spot_raw_5m": "moex_iss_cets_tom_1m",
}
EXPECTED_BINDINGS: Final[dict[tuple[str, str], str]] = {
    ("Si", "front"): "si_front_contract",
    ("Si", "next"): "si_next_contract",
    ("CR", "front"): "cr_front_contract",
    ("CR", "next"): "cr_next_contract",
}
SECID_PATTERNS: Final[dict[str, re.Pattern[str]]] = {
    "Si": re.compile(r"^Si[HMUZ][0-9]$"),
    "CR": re.compile(r"^CR[HMUZ][0-9]$"),
}
EXPECTED_TOM_IDENTITIES: Final[dict[str, str]] = {
    "usd_tom": "USD000UTSTOM",
    "cny_tom": "CNYRUB_TOM",
}
BINDING_SOURCE_ID: Final[str] = "moex_iss_forts_securities_reference"


class Step3AcceptanceError(ValueError):
    pass


@dataclass(frozen=True)
class PointerSpec:
    dataset_id: str
    instrument_id: str
    source_id: str
    secid: str
    trade_date: str
    row_count: int
    manifest_path: Path
    quality_path: Path
    partition_path: Path
    manifest_run_id: str


def _fail(message: str) -> None:
    raise Step3AcceptanceError(message)


def _require_token(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    if text in (".", "..") or "/" in text or "\\" in text or any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        _fail(field_name + " must be an explicit safe token")
    return text


def _require_date(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise Step3AcceptanceError(field_name + " must be explicit YYYY-MM-DD") from exc


def _require_utc_datetime(value: object, field_name: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Step3AcceptanceError(field_name + " must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        _fail(field_name + " must be timezone-aware")
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def _require_utc_timestamp(value: object, field_name: str) -> str:
    return _require_utc_datetime(value, field_name).isoformat()


def load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.exists() or not env_path.is_file():
        _fail("env_file does not exist")
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        _fail("MOEX_DATA_ROOT is required")
    path = Path(value)
    if not path.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    return path


def _evidence_dir(run_id: str) -> Path:
    return _data_root() / "state" / "acceptance" / "step3_canonical_raw" / ("run_id=" + run_id)


def pilot_evidence_path(run_id: str) -> Path:
    return _evidence_dir(_require_token(run_id, "run_id")) / "pilot_evidence.json"


def acceptance_evidence_path(run_id: str) -> Path:
    return _evidence_dir(_require_token(run_id, "run_id")) / "accepted_pointers.json"


def _load_json(path: Path, field_name: str) -> Mapping[str, object]:
    if not path.exists() or not path.is_file():
        _fail(field_name + " does not exist")
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise Step3AcceptanceError(field_name + " is not valid JSON: " + str(exc)) from exc
    if not isinstance(values, Mapping):
        _fail(field_name + " must be a JSON object")
    return values


def _require_under_root(value: object, field_name: str) -> Path:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    candidate = Path(text)
    if not candidate.is_absolute():
        _fail(field_name + " must be an absolute producer output path")
    try:
        resolved = candidate.resolve(strict=True)
        root = _data_root().resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise Step3AcceptanceError(field_name + " must exist under MOEX_DATA_ROOT") from exc
    if not resolved.is_file():
        _fail(field_name + " must identify a regular file")
    return resolved


def _rooted_ref(path: Path) -> str:
    try:
        relative = path.resolve(strict=True).relative_to(_data_root().resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step3AcceptanceError("artifact path must be rooted at MOEX_DATA_ROOT") from exc
    return "${MOEX_DATA_ROOT}/" + relative.as_posix()


def _expected_run_root(run_id: str) -> Path:
    return _data_root().joinpath(*RUNS_SUBPATH, "run_id=" + _require_token(run_id, "run_id"))


def _expected_run_root_ref(run_id: str) -> str:
    return "${MOEX_DATA_ROOT}/" + "/".join((*RUNS_SUBPATH, "run_id=" + _require_token(run_id, "run_id")))


def _validate_materialization_root(values: Mapping[str, object], run_id: str) -> Path:
    if values.get("run_artifacts_immutable") is not True:
        _fail("pilot evidence must prove run_artifacts_immutable=true")
    if values.get("run_id_reuse_allowed") is not False:
        _fail("pilot evidence must prove run_id_reuse_allowed=false")
    if values.get("materialization_root_ref") != _expected_run_root_ref(run_id):
        _fail("pilot materialization_root_ref mismatch")
    raw = str(values.get("materialization_root") or "").strip()
    if not raw:
        _fail("pilot materialization_root is required")
    candidate = Path(raw)
    if not candidate.is_absolute():
        _fail("pilot materialization_root must be absolute")
    try:
        resolved = candidate.resolve(strict=True)
        expected = _expected_run_root(run_id).resolve(strict=True)
        resolved.relative_to(_data_root().resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step3AcceptanceError("pilot materialization_root must exist under canonical MOEX_DATA_ROOT") from exc
    if resolved != expected or not resolved.is_dir():
        _fail("pilot materialization_root must equal the immutable run-scoped root")
    return resolved


def _require_path_in_run_root(path: Path, run_root: Path, field_name: str) -> None:
    try:
        path.resolve(strict=True).relative_to(run_root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step3AcceptanceError(field_name + " must be inside the declared immutable run root") from exc


def _single_scope(value: object, field_name: str) -> str:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or len(value) != 1:
        _fail(field_name + " must contain exactly one value")
    return _require_token(value[0], field_name)


def _positive_int(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        _fail(field_name + " must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise Step3AcceptanceError(field_name + " must be a positive integer") from exc
    if result <= 0:
        _fail(field_name + " must be positive")
    return result


def _positive_row_count(values: Mapping[str, object], context: str) -> int:
    return _positive_int(values.get("row_count"), context + " row_count")


def _same_path(value: object, expected: Path, field_name: str) -> None:
    actual = _require_under_root(value, field_name)
    if actual != expected.resolve(strict=True):
        _fail(field_name + " mismatch")


def _manifest_run_id(path: Path) -> str:
    values = _load_json(path, "manifest")
    run_id = _require_token(values.get("run_id"), "manifest.run_id")
    status = values.get("refresh_status", values.get("status"))
    if status != "succeeded":
        _fail("manifest status must be succeeded")
    return run_id


def _require_canonical_source(dataset_id: str, source_id: str, context: str) -> str:
    expected = EXPECTED_SOURCE_IDS.get(dataset_id)
    if expected is None or source_id != expected:
        _fail(context + " source_id does not match canonical Step 3 source")
    return source_id


def _quote_spec(values: Mapping[str, object], *, trade_date: str) -> PointerSpec:
    if values.get("dataset_id") != "futures_raw_5m":
        _fail("quote dataset_id mismatch")
    if values.get("quality_status") != "pass":
        _fail("quote quality_status must be pass")
    instrument_id = _single_scope(values.get("instrument_id_scope"), "quote.instrument_id_scope")
    secid = _single_scope(values.get("secid_scope"), "quote.secid_scope")
    source_id = _require_canonical_source("futures_raw_5m", _require_token(values.get("source_id"), "quote.source_id"), "quote")
    manifest_path = _require_under_root(values.get("manifest_reference"), "quote.manifest_reference")
    quality_path = _require_under_root(values.get("quality_report_reference"), "quote.quality_report_reference")
    partition_path = _require_under_root(values.get("storage_partition_path"), "quote.storage_partition_path")
    return PointerSpec(
        "futures_raw_5m", instrument_id, source_id, secid, trade_date,
        _positive_row_count(values, "quote"), manifest_path, quality_path, partition_path,
        _manifest_run_id(manifest_path),
    )


def _supplementary_spec(values: Mapping[str, object], *, dataset_id: str, context: str, trade_date: str) -> PointerSpec:
    if values.get("dataset_id") != dataset_id:
        _fail(context + " dataset_id mismatch")
    if values.get("trade_date") != trade_date:
        _fail(context + " trade_date mismatch")
    if values.get("quality_status") != "pass":
        _fail(context + " quality_status must be pass")
    instrument_id = _require_token(values.get("instrument_id"), context + ".instrument_id")
    secid = _require_token(values.get("secid"), context + ".secid")
    source_id = _require_canonical_source(dataset_id, _require_token(values.get("source_id"), context + ".source_id"), context)
    manifest_path = _require_under_root(values.get("manifest_path"), context + ".manifest_path")
    quality_path = _require_under_root(values.get("quality_report_path"), context + ".quality_report_path")
    partition_path = _require_under_root(values.get("partition_path"), context + ".partition_path")
    return PointerSpec(
        dataset_id, instrument_id, source_id, secid, trade_date,
        _positive_row_count(values, context), manifest_path, quality_path, partition_path,
        _manifest_run_id(manifest_path),
    )


def _require_list(value: object, field_name: str, expected_count: int) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        _fail(field_name + " must be a sequence")
    if len(value) != expected_count:
        _fail(field_name + " count mismatch")
    result: list[Mapping[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            _fail(field_name + " items must be objects")
        result.append(item)
    return tuple(result)


def _validate_bindings(values: Mapping[str, object], *, trade_date: str, as_of_date: str) -> dict[str, str]:
    rows = _require_list(values.get("bindings"), "bindings", EXPECTED_COUNTS["bindings"])
    reference_dt = _require_utc_datetime(values.get("reference_observed_at_utc"), "reference_observed_at_utc")
    reference_observed_at = reference_dt.isoformat()
    if reference_dt.astimezone(MOSCOW_TZ).date().isoformat() != as_of_date:
        _fail("reference_observed_at_utc Europe/Moscow date must equal as_of_date")

    observed: dict[tuple[str, str], tuple[str, str]] = {}
    by_instrument: dict[str, str] = {}
    secids: set[str] = set()
    expiries: dict[tuple[str, str], date] = {}
    for row in rows:
        root = _require_token(row.get("root"), "binding.root")
        role = _require_token(row.get("role"), "binding.role")
        key = (root, role)
        expected_instrument = EXPECTED_BINDINGS.get(key)
        if expected_instrument is None:
            _fail("binding root/role identity is not canonical")
        if key in observed:
            _fail("binding root/role identities must be unique")
        instrument_id = _require_token(row.get("instrument_id"), "binding.instrument_id")
        if instrument_id != expected_instrument:
            _fail("binding instrument_id does not match canonical root/role")
        secid = _require_token(row.get("secid"), "binding.secid")
        pattern = SECID_PATTERNS.get(root)
        if pattern is None or pattern.fullmatch(secid) is None:
            _fail("binding SECID does not match canonical root/month pattern")
        if secid in secids:
            _fail("all four binding SECIDs must be distinct")
        secids.add(secid)
        if row.get("source_id") != BINDING_SOURCE_ID:
            _fail("binding source_id mismatch")
        if _require_date(row.get("as_of_date"), "binding.as_of_date") != as_of_date:
            _fail("binding as_of_date mismatch")
        last_trade_date = _require_date(row.get("last_trade_date"), "binding.last_trade_date")
        if last_trade_date < trade_date:
            _fail("binding last_trade_date precedes trade_date")
        expiries[key] = date.fromisoformat(last_trade_date)
        mapping_fixed = _require_utc_timestamp(row.get("mapping_fixed_ts_utc"), "binding.mapping_fixed_ts_utc")
        availability = _require_utc_timestamp(row.get("availability_ts_utc"), "binding.availability_ts_utc")
        if mapping_fixed != availability or availability != reference_observed_at:
            _fail("binding causal timestamps must equal the controlled reference observation timestamp")
        observed[key] = (instrument_id, secid)
        by_instrument[instrument_id] = secid

    if set(observed) != set(EXPECTED_BINDINGS):
        _fail("bindings must contain the exact four canonical root/role identities")
    for root in ("Si", "CR"):
        if expiries[(root, "front")] >= expiries[(root, "next")]:
            _fail(root + " front last_trade_date must be strictly earlier than next")
    return by_instrument


def _validate_quote_support(spec: PointerSpec) -> None:
    manifest = _load_json(spec.manifest_path, "quote.manifest")
    if _require_token(manifest.get("run_id"), "quote.manifest.run_id") != spec.manifest_run_id:
        _fail("quote manifest run_id mismatch")
    if manifest.get("refresh_status") != "succeeded":
        _fail("quote manifest refresh_status must be succeeded")
    if _single_scope(manifest.get("instrument_scope"), "quote.manifest.instrument_scope") != spec.instrument_id:
        _fail("quote manifest instrument mismatch")
    if _single_scope(manifest.get("source_scope"), "quote.manifest.source_scope") != spec.source_id:
        _fail("quote manifest source mismatch")
    partitions = manifest.get("partitions_written")
    if isinstance(partitions, (str, bytes)) or not isinstance(partitions, Sequence) or len(partitions) != 1:
        _fail("quote manifest must reference exactly one written partition")
    _same_path(partitions[0], spec.partition_path, "quote.manifest.partitions_written")
    _same_path(manifest.get("quality_report_ref"), spec.quality_path, "quote.manifest.quality_report_ref")
    source_contract = manifest.get("source_contract")
    if not isinstance(source_contract, Mapping):
        _fail("quote manifest source_contract must be an object")
    for field_name, expected in {
        "instrument_id": spec.instrument_id,
        "source_id": spec.source_id,
        "secid": spec.secid,
        "trade_date": spec.trade_date,
    }.items():
        if source_contract.get(field_name) != expected:
            _fail("quote manifest source_contract mismatch: " + field_name)

    quality = _load_json(spec.quality_path, "quote.quality_report")
    if _require_token(quality.get("run_id"), "quote.quality_report.run_id") != spec.manifest_run_id:
        _fail("quote quality report run_id mismatch")
    row = _require_list(quality.get("rows"), "quote.quality_report.rows", 1)[0]
    for field_name, expected in {
        "run_id": spec.manifest_run_id,
        "dataset_id": spec.dataset_id,
        "instrument_id": spec.instrument_id,
        "source_id": spec.source_id,
        "secid": spec.secid,
        "trade_date": spec.trade_date,
        "quality_status": "pass",
    }.items():
        if row.get(field_name) != expected:
            _fail("quote quality report mismatch: " + field_name)
    if _positive_int(row.get("rows"), "quote.quality_report.rows[0].rows") != spec.row_count:
        _fail("quote quality report row count mismatch")


def _validate_supplementary_support(spec: PointerSpec, *, context: str) -> tuple[Mapping[str, object], Mapping[str, object]]:
    manifest = _load_json(spec.manifest_path, context + ".manifest")
    for field_name, expected in {
        "dataset_id": spec.dataset_id,
        "run_id": spec.manifest_run_id,
        "instrument_id": spec.instrument_id,
        "source_id": spec.source_id,
        "secid": spec.secid,
        "trade_date": spec.trade_date,
        "status": "succeeded",
    }.items():
        if manifest.get(field_name) != expected:
            _fail(context + " manifest mismatch: " + field_name)
    if _positive_int(manifest.get("row_count"), context + ".manifest.row_count") != spec.row_count:
        _fail(context + " manifest row count mismatch")
    _same_path(manifest.get("partition_path"), spec.partition_path, context + ".manifest.partition_path")
    _same_path(manifest.get("quality_report_path"), spec.quality_path, context + ".manifest.quality_report_path")

    quality = _load_json(spec.quality_path, context + ".quality_report")
    for field_name, expected in {
        "dataset_id": spec.dataset_id,
        "run_id": spec.manifest_run_id,
        "instrument_id": spec.instrument_id,
        "source_id": spec.source_id,
        "secid": spec.secid,
        "trade_date": spec.trade_date,
        "quality_status": "pass",
    }.items():
        if quality.get(field_name) != expected:
            _fail(context + " quality report mismatch: " + field_name)
    _same_path(quality.get("partition_path"), spec.partition_path, context + ".quality_report.partition_path")
    if _positive_int(quality.get("rows"), context + ".quality_report.rows") != spec.row_count:
        _fail(context + " quality report row count mismatch")
    return manifest, quality


def _validate_oi_causal_partition(spec: PointerSpec, manifest: Mapping[str, object], quality: Mapping[str, object]) -> None:
    manifest_min = _require_utc_datetime(manifest.get("min_availability_ts_utc"), "open_interest.manifest.min_availability_ts_utc")
    manifest_max = _require_utc_datetime(manifest.get("max_availability_ts_utc"), "open_interest.manifest.max_availability_ts_utc")
    quality_min = _require_utc_datetime(quality.get("min_availability_ts_utc"), "open_interest.quality_report.min_availability_ts_utc")
    quality_max = _require_utc_datetime(quality.get("max_availability_ts_utc"), "open_interest.quality_report.max_availability_ts_utc")
    if manifest_min != quality_min or manifest_max != quality_max:
        _fail("open_interest availability bounds mismatch between manifest and quality report")
    if quality_min > quality_max:
        _fail("open_interest availability bounds are not ordered")

    try:
        frame = pd.read_parquet(spec.partition_path, columns=["availability_ts_utc", "systime_source"])
    except Exception as exc:
        raise Step3AcceptanceError("open_interest partition causal Parquet validation failed: " + str(exc)) from exc
    if len(frame.index) != spec.row_count:
        _fail("open_interest Parquet row count mismatch")
    if frame["availability_ts_utc"].isna().any() or frame["systime_source"].isna().any():
        _fail("open_interest Parquet causal columns contain null values")
    if frame["systime_source"].astype(str).str.strip().eq("").any():
        _fail("open_interest Parquet systime_source contains empty values")

    parsed: list[datetime] = []
    for value in frame["availability_ts_utc"].tolist():
        try:
            timestamp = pd.Timestamp(value)
        except Exception as exc:
            raise Step3AcceptanceError("open_interest Parquet availability_ts_utc is invalid") from exc
        if timestamp.tzinfo is None:
            _fail("open_interest Parquet availability_ts_utc must be timezone-aware")
        parsed.append(timestamp.tz_convert("UTC").to_pydatetime().astimezone(timezone.utc).replace(microsecond=0))
    if not parsed:
        _fail("open_interest Parquet must contain availability evidence")
    actual_min = min(parsed)
    actual_max = max(parsed)
    if actual_min != quality_min or actual_max != quality_max:
        _fail("open_interest Parquet availability bounds mismatch declared evidence")


def validate_pilot_evidence(values: Mapping[str, object], *, run_id: str) -> tuple[PointerSpec, ...]:
    checked_run = _require_token(run_id, "run_id")
    if values.get("project") != "MOEX_Bot" or values.get("step") != 3:
        _fail("pilot evidence project/step mismatch")
    if values.get("status") != "pilot_passed":
        _fail("pilot evidence status must be pilot_passed")
    if values.get("artifact_version") != checked_run:
        _fail("pilot evidence artifact_version mismatch")
    if values.get("latest_autodetect_used") is not False:
        _fail("pilot evidence must prove latest_autodetect_used=false")
    if values.get("historical_backdating_used") is not False:
        _fail("pilot evidence must prove historical_backdating_used=false")
    if values.get("continuous_series_created") is not False:
        _fail("pilot evidence must prove continuous_series_created=false")

    trade_date = _require_date(values.get("trade_date"), "pilot.trade_date")
    as_of_date = _require_date(values.get("as_of_date"), "pilot.as_of_date")
    if trade_date != as_of_date:
        _fail("pilot trade_date must equal as_of_date for the unversioned current FORTS binding")
    run_root = _validate_materialization_root(values, checked_run)

    counts = values.get("counts")
    if not isinstance(counts, Mapping):
        _fail("pilot evidence counts must be an object")
    for field_name, expected in EXPECTED_COUNTS.items():
        if counts.get(field_name) != expected:
            _fail("pilot evidence count mismatch: " + field_name)

    binding_by_instrument = _validate_bindings(values, trade_date=trade_date, as_of_date=as_of_date)
    quote_rows = _require_list(values.get("quote_partitions"), "quote_partitions", EXPECTED_COUNTS["quote_partitions"])
    oi_rows = _require_list(values.get("open_interest_partitions"), "open_interest_partitions", EXPECTED_COUNTS["open_interest_partitions"])
    tom_rows = _require_list(values.get("tom_partitions"), "tom_partitions", EXPECTED_COUNTS["tom_partitions"])

    specs: list[PointerSpec] = [_quote_spec(item, trade_date=trade_date) for item in quote_rows]
    specs.extend(_supplementary_spec(item, dataset_id="futures_open_interest_raw_5m", context="open_interest", trade_date=trade_date) for item in oi_rows)
    specs.extend(_supplementary_spec(item, dataset_id="fx_spot_raw_5m", context="tom", trade_date=trade_date) for item in tom_rows)

    keys = {(spec.dataset_id, spec.instrument_id) for spec in specs}
    if len(keys) != sum(EXPECTED_POINTER_COUNTS.values()):
        _fail("accepted pointer dataset/instrument identities must be unique")
    for dataset_id, expected in EXPECTED_POINTER_COUNTS.items():
        if sum(1 for spec in specs if spec.dataset_id == dataset_id) != expected:
            _fail("accepted pointer count mismatch for " + dataset_id)
    for spec in specs:
        _require_path_in_run_root(spec.partition_path, run_root, spec.dataset_id + ".partition_path")
        _require_path_in_run_root(spec.manifest_path, run_root, spec.dataset_id + ".manifest_path")
        _require_path_in_run_root(spec.quality_path, run_root, spec.dataset_id + ".quality_path")

    quote_by_instrument = {spec.instrument_id: spec for spec in specs if spec.dataset_id == "futures_raw_5m"}
    oi_by_instrument = {spec.instrument_id: spec for spec in specs if spec.dataset_id == "futures_open_interest_raw_5m"}
    if set(quote_by_instrument) != set(binding_by_instrument) or set(oi_by_instrument) != set(binding_by_instrument):
        _fail("quote/OI instrument identities must exactly match causal bindings")
    for instrument_id, bound_secid in binding_by_instrument.items():
        if quote_by_instrument[instrument_id].secid != bound_secid:
            _fail("quote SECID does not match causal binding: " + instrument_id)
        if oi_by_instrument[instrument_id].secid != bound_secid:
            _fail("OI SECID does not match causal binding: " + instrument_id)

    tom_by_instrument = {spec.instrument_id: spec for spec in specs if spec.dataset_id == "fx_spot_raw_5m"}
    if set(tom_by_instrument) != set(EXPECTED_TOM_IDENTITIES):
        _fail("TOM instrument identities mismatch")
    for instrument_id, expected_secid in EXPECTED_TOM_IDENTITIES.items():
        if tom_by_instrument[instrument_id].secid != expected_secid:
            _fail("TOM SECID mismatch: " + instrument_id)

    for spec in specs:
        if spec.dataset_id == "futures_raw_5m":
            _validate_quote_support(spec)
        elif spec.dataset_id == "futures_open_interest_raw_5m":
            manifest, quality = _validate_supplementary_support(spec, context="open_interest")
            _validate_oi_causal_partition(spec, manifest, quality)
        elif spec.dataset_id == "fx_spot_raw_5m":
            _validate_supplementary_support(spec, context="tom")
        else:
            _fail("unsupported Step 3 dataset_id")
    return tuple(specs)


def _pointer_path(spec: PointerSpec) -> Path:
    return _data_root() / "state" / "datasets" / ("dataset_id=" + spec.dataset_id) / ("instrument_id=" + spec.instrument_id) / "current_accepted_manifest.json"


def _pointer_values(spec: PointerSpec, *, acceptance_run_id: str) -> dict[str, object]:
    return {
        "dataset_id": spec.dataset_id,
        "instrument_id": spec.instrument_id,
        "source_id": spec.source_id,
        "secid": spec.secid,
        "run_id": spec.manifest_run_id,
        "acceptance_run_id": acceptance_run_id,
        "manifest_ref": _rooted_ref(spec.manifest_path),
        "quality_report_ref": _rooted_ref(spec.quality_path),
        "partition_ref": _rooted_ref(spec.partition_path),
        "quality_status": "pass",
        "refresh_status": "succeeded",
        "acceptance_contract_id": CONTRACT_ID,
    }


def _stage_json(path: Path, values: Mapping[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".stage") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        return Path(handle.name)


def _replace_staged(staged_path: Path, final_path: Path) -> None:
    staged_path.replace(final_path)


def _restore_bytes(path: Path, previous: bytes | None) -> None:
    if previous is None:
        if path.exists():
            path.unlink()
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".rollback") as handle:
        handle.write(previous)
        temp_name = handle.name
    Path(temp_name).replace(path)


def _transactional_json_replace(records: Sequence[tuple[Path, Mapping[str, object]]]) -> None:
    final_paths = [path for path, _ in records]
    if len(set(final_paths)) != len(final_paths):
        _fail("transaction target paths must be unique")
    previous = {path: path.read_bytes() if path.exists() else None for path in final_paths}
    staged: list[tuple[Path, Path]] = []
    applied: list[Path] = []
    try:
        for final_path, values in records:
            staged.append((_stage_json(final_path, values), final_path))
        for staged_path, final_path in staged:
            _replace_staged(staged_path, final_path)
            applied.append(final_path)
    except Exception as exc:
        rollback_errors: list[str] = []
        for final_path in reversed(applied):
            try:
                _restore_bytes(final_path, previous[final_path])
            except Exception as rollback_exc:
                rollback_errors.append(str(rollback_exc))
        for staged_path, _ in staged:
            if staged_path.exists():
                try:
                    staged_path.unlink()
                except OSError:
                    pass
        if rollback_errors:
            raise Step3AcceptanceError("pointer promotion failed and rollback was incomplete: " + "; ".join(rollback_errors)) from exc
        raise Step3AcceptanceError("pointer promotion transaction failed: " + str(exc)) from exc
    finally:
        for staged_path, _ in staged:
            if staged_path.exists():
                try:
                    staged_path.unlink()
                except OSError:
                    pass


def promote_step3_pilot(*, run_id: str) -> dict[str, object]:
    checked_run = _require_token(run_id, "run_id")
    evidence_path = pilot_evidence_path(checked_run)
    specs = validate_pilot_evidence(_load_json(evidence_path, "pilot_evidence"), run_id=checked_run)

    pointer_records: list[dict[str, object]] = []
    pointer_writes: list[tuple[Path, Mapping[str, object]]] = []
    for spec in specs:
        path = _pointer_path(spec)
        pointer_values = _pointer_values(spec, acceptance_run_id=checked_run)
        pointer_writes.append((path, pointer_values))
        pointer_records.append({
            "dataset_id": spec.dataset_id,
            "instrument_id": spec.instrument_id,
            "pointer_path": path.as_posix(),
            "pointer_ref": "${MOEX_DATA_ROOT}/" + path.relative_to(_data_root()).as_posix(),
            "manifest_ref": pointer_values["manifest_ref"],
            "quality_report_ref": pointer_values["quality_report_ref"],
        })

    result: dict[str, object] = {
        "project": "MOEX_Bot",
        "step": 3,
        "status": "accepted",
        "acceptance_contract_id": CONTRACT_ID,
        "run_id": checked_run,
        "pilot_evidence_ref": _rooted_ref(evidence_path),
        "accepted_pointer_count": len(pointer_records),
        "expected_pointer_count": sum(EXPECTED_POINTER_COUNTS.values()),
        "pointers": pointer_records,
        "latest_autodetect_used": False,
        "historical_backdating_used": False,
        "continuous_series_created": False,
        "promotion_semantics": "transactional_with_rollback",
        "artifact_semantics": "immutable_run_scoped",
    }
    if result["accepted_pointer_count"] != result["expected_pointer_count"]:
        _fail("accepted pointer total count mismatch")
    marker_path = acceptance_evidence_path(checked_run)
    _transactional_json_replace([*pointer_writes, (marker_path, result)])
    result["acceptance_evidence_path"] = marker_path.as_posix()
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote a fully passed Step 3 physical pilot to canonical accepted pointers.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_env_file(args.env_file)
        result = promote_step3_pilot(run_id=args.run_id)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 3, "status": "acceptance_failed", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
