from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import tempfile
from typing import Final

from . import materialize_raw_5m as legacy
from .materialize_raw_5m_full_session import _fetch_apim_tradestats_full_session_frame

ARTIFACT_ID: Final[str] = "dataset.forts.raw_5m.tradestats.v1"
SOURCE_ARTIFACT_ID: Final[str] = "external.apim.fo.tradestats.v1"
SCHEMA_VERSION: Final[str] = ARTIFACT_ID
MANIFEST_ARTIFACT_ID: Final[str] = "manifest.data_asset.v1"
QUALITY_REPORT_ARTIFACT_ID: Final[str] = "reports.data_asset.quality.v1"
SOURCE_ENDPOINT: Final[str] = "/iss/datashop/algopack/fo/tradestats.json"
PRODUCER_ID: Final[str] = "moex_data.futures.materialize_forts_raw_5m_tradestats.v2"
STORAGE_PATTERN: Final[str] = (
    "${MOEX_DATA_ROOT}/forts/raw_5m/tradestats/"
    "trade_date={YYYY-MM-DD}/family={FAMILY}/secid={SECID}/part.parquet"
)


class FortsRaw5mTradestatsError(ValueError):
    pass


@dataclass(frozen=True)
class TargetPaths:
    partition_path: Path
    manifest_path: Path
    quality_report_path: Path


@dataclass(frozen=True)
class MaterializationResult:
    partition_path: Path
    manifest_path: Path
    quality_report_path: Path
    row_count: int
    quality_status: str
    data_start: str
    data_end: str
    last_valid_trade_date: str
    family: str
    secid: str
    content_hash: str


def _fail(message: str) -> None:
    raise FortsRaw5mTradestatsError(message)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.exists():
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


def _require_text(value: str | None, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        _fail(name + " is required")
    if any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        _fail(name + " contains a dynamic marker")
    return text


def _require_date(value: str | None, name: str) -> str:
    text = _require_text(value, name)
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise FortsRaw5mTradestatsError(name + " must be YYYY-MM-DD") from exc


def _require_token(value: str | None, name: str) -> str:
    text = _require_text(value, name)
    if text in (".", "..") or "/" in text or "\\" in text:
        _fail(name + " must be a single safe token")
    return text


def _data_root(env: Mapping[str, str] | None = None) -> Path:
    active_env = os.environ if env is None else env
    root = str(active_env.get("MOEX_DATA_ROOT", "")).strip()
    if not root:
        _fail("MOEX_DATA_ROOT is required")
    return Path(root)


def build_target_paths(
    trade_date: str,
    family: str,
    secid: str,
    artifact_version: str,
    env: Mapping[str, str] | None = None,
) -> TargetPaths:
    checked_date = _require_date(trade_date, "trade_date")
    checked_family = _require_token(family, "family")
    checked_secid = _require_token(secid, "secid")
    checked_version = _require_token(artifact_version, "artifact_version")
    root = _data_root(env)
    partition_path = (
        root
        / "forts"
        / "raw_5m"
        / "tradestats"
        / ("trade_date=" + checked_date)
        / ("family=" + checked_family)
        / ("secid=" + checked_secid)
        / "part.parquet"
    )
    manifest_path = (
        root
        / "manifests"
        / ("artifact_id=" + ARTIFACT_ID)
        / ("artifact_version=" + checked_version)
        / "manifest.json"
    )
    quality_path = (
        root
        / "quality_reports"
        / ("artifact_id=" + ARTIFACT_ID)
        / ("artifact_version=" + checked_version)
        / "quality_report.json"
    )
    return TargetPaths(partition_path, manifest_path, quality_path)


def build_legacy_request(trade_date: str, family: str, secid: str, artifact_version: str) -> legacy.Raw5mMaterializationRequest:
    checked_date = _require_date(trade_date, "trade_date")
    checked_family = _require_token(family, "family")
    checked_secid = _require_token(secid, "secid")
    checked_version = _require_token(artifact_version, "artifact_version")
    return legacy.Raw5mMaterializationRequest(
        repo_root=Path("."),
        dataset_id=ARTIFACT_ID,
        contract_id=ARTIFACT_ID,
        trade_date=checked_date,
        family=checked_family,
        secid=checked_secid,
        source_path=None,
        run_id=checked_version,
        source_candidate=SOURCE_ARTIFACT_ID,
        source_endpoint=SOURCE_ENDPOINT,
        market="FORTS",
        board="RFUD",
        series_type="native",
        granularity="5m",
    )


def _write_json_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        tmp_name = handle.name
    Path(tmp_name).replace(path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest(
    paths: TargetPaths,
    artifact_version: str,
    metrics: Mapping[str, object],
    content_hash: str,
    family: str,
    secid: str,
    build_started_at: str,
    build_finished_at: str,
) -> dict[str, object]:
    return {
        "artifact_id": ARTIFACT_ID,
        "artifact_class": "raw_native_dataset",
        "artifact_version": artifact_version,
        "schema_version": SCHEMA_VERSION,
        "path_contract_type": "external_pattern",
        "artifact_uri": paths.partition_path.as_posix(),
        "input_references": [SOURCE_ARTIFACT_ID],
        "deterministic_builder_config_version": PRODUCER_ID,
        "build_started_at": build_started_at,
        "build_finished_at": build_finished_at,
        "producer": PRODUCER_ID,
        "content_hash": content_hash,
        "source_artifact_id": SOURCE_ARTIFACT_ID,
        "data_start": metrics["data_start"],
        "data_end": metrics["data_end"],
        "last_valid_trade_date": metrics["last_valid_trade_date"],
        "row_count": metrics["rows"],
        "family_scope": [family],
        "secid_scope": [secid],
        "date_selection_rule": "explicit_trade_date_session",
        "session_binding": "explicit_trade_date_session",
        "storage_pattern": STORAGE_PATTERN,
        "partition_hashes": {paths.partition_path.as_posix(): content_hash},
        "quality_report_reference": paths.quality_report_path.as_posix(),
    }


def _quality_report(artifact_version: str, metrics: Mapping[str, object], family: str, secid: str) -> dict[str, object]:
    checks = [
        {"check_id": "row_count_positive", "status": "passed", "value": int(metrics["rows"])},
        {"check_id": "duplicate_ts_secid_count", "status": "passed", "value": int(metrics["duplicate_key_count"])},
        {"check_id": "gap_count", "status": "passed", "value": int(metrics["gap_count"])},
        {"check_id": "null_ohlc_count", "status": "passed", "value": int(metrics["null_ohlc_count"])},
        {"check_id": "invalid_ohlc_count", "status": "passed", "value": int(metrics["invalid_ohlc_count"])},
    ]
    return {
        "artifact_id": QUALITY_REPORT_ARTIFACT_ID,
        "artifact_class": "quality_report",
        "artifact_version": artifact_version,
        "schema_version": QUALITY_REPORT_ARTIFACT_ID,
        "input_references": [ARTIFACT_ID],
        "deterministic_builder_config_version": PRODUCER_ID,
        "quality_status": "passed",
        "checks": checks,
        "failure_reasons": [],
        "checked_at": _utc_now(),
        "target_artifact_id": ARTIFACT_ID,
        "source_artifact_id": SOURCE_ARTIFACT_ID,
        "data_start": metrics["data_start"],
        "data_end": metrics["data_end"],
        "last_valid_trade_date": metrics["last_valid_trade_date"],
        "row_count": metrics["rows"],
        "family_scope": [family],
        "secid_scope": [secid],
        "date_selection_rule": "explicit_trade_date_session",
        "session_binding": "explicit_trade_date_session",
    }


def materialize_partition(
    trade_date: str,
    family: str,
    secid: str,
    artifact_version: str,
    *,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
) -> MaterializationResult:
    request = build_legacy_request(trade_date, family, secid, artifact_version)
    paths = build_target_paths(request.trade_date, request.family, request.secid, request.run_id)
    build_started_at = _utc_now()
    raw_frame, source_url = _fetch_apim_tradestats_full_session_frame(
        request=request,
        timeout=timeout,
        apim_base_url=apim_base_url,
        env=os.environ,
    )
    normalized = legacy._normalize_apim_tradestats(raw_frame, request, source_url, _utc_now())
    output, metrics = legacy._validate_source_table(normalized, request.trade_date, request.family, request.secid)
    metrics = dict(metrics)
    metrics["data_start"] = request.trade_date
    metrics["data_end"] = request.trade_date
    metrics["last_valid_trade_date"] = request.trade_date
    legacy._write_parquet_atomic(paths.partition_path, output, request.run_id)
    content_hash = _sha256(paths.partition_path)
    build_finished_at = _utc_now()
    _write_json_atomic(paths.quality_report_path, _quality_report(request.run_id, metrics, request.family, request.secid))
    _write_json_atomic(
        paths.manifest_path,
        _manifest(paths, request.run_id, metrics, content_hash, request.family, request.secid, build_started_at, build_finished_at),
    )
    return MaterializationResult(
        partition_path=paths.partition_path,
        manifest_path=paths.manifest_path,
        quality_report_path=paths.quality_report_path,
        row_count=int(metrics["rows"]),
        quality_status="passed",
        data_start=request.trade_date,
        data_end=request.trade_date,
        last_valid_trade_date=request.trade_date,
        family=request.family,
        secid=request.secid,
        content_hash=content_hash,
    )


def result_payload(result: MaterializationResult) -> dict[str, object]:
    return {
        "status": "succeeded",
        "artifact_id": ARTIFACT_ID,
        "source_artifact_id": SOURCE_ARTIFACT_ID,
        "storage_partition_path": result.partition_path.as_posix(),
        "manifest_reference": result.manifest_path.as_posix(),
        "quality_report_reference": result.quality_report_path.as_posix(),
        "quality_status": result.quality_status,
        "data_start": result.data_start,
        "data_end": result.data_end,
        "last_valid_trade_date": result.last_valid_trade_date,
        "row_count": result.row_count,
        "family_scope": [result.family],
        "secid_scope": [result.secid],
        "schema_version": SCHEMA_VERSION,
        "date_selection_rule": "explicit_trade_date_session",
        "session_binding": "explicit_trade_date_session",
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "content_hash": result.content_hash,
    }


def error_payload(exc: Exception) -> dict[str, object]:
    return {
        "status": "failed",
        "artifact_id": ARTIFACT_ID,
        "source_artifact_id": SOURCE_ARTIFACT_ID,
        "error": str(exc),
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize one canonical FORTS raw 5m tradestats pilot partition.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--family", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--artifact-version", required=True)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--apim-base-url", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_env_file(args.env_file)
        result = materialize_partition(
            trade_date=args.trade_date,
            family=args.family,
            secid=args.secid,
            artifact_version=args.artifact_version,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
        )
    except Exception as exc:
        print(json.dumps(error_payload(exc), ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result_payload(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
