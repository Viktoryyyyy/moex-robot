from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
import tempfile
from typing import Final

import pandas as pd

from . import materialize_forts_raw_5m_tradestats as pilot

PRODUCER_ID: Final[str] = "moex_data.futures.materialize_forts_raw_5m_instrument.v1"
STORAGE_PATTERN: Final[str] = (
    "${MOEX_DATA_ROOT}/forts/raw_5m/tradestats/"
    "trade_date={YYYY-MM-DD}/instrument_id={INSTRUMENT_ID}/secid={SECID}/part.parquet"
)


@dataclass(frozen=True)
class InstrumentResult:
    payload: dict[str, object]


def _require_token(value: str | None, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(field_name + " is required")
    if text in (".", "..") or "/" in text or "\\" in text or any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        raise ValueError(field_name + " must be an explicit safe token")
    return text


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        raise ValueError("MOEX_DATA_ROOT is required")
    return Path(value)


def load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.exists():
        raise ValueError("env_file does not exist")
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def target_paths(trade_date: str, instrument_id: str, secid: str, artifact_version: str) -> pilot.TargetPaths:
    checked_date = pilot._require_date(trade_date, "trade_date")
    checked_instrument_id = _require_token(instrument_id, "instrument_id")
    checked_secid = _require_token(secid, "secid")
    checked_version = _require_token(artifact_version, "artifact_version")
    root = _data_root()
    return pilot.TargetPaths(
        partition_path=(
            root
            / "forts"
            / "raw_5m"
            / "tradestats"
            / ("trade_date=" + checked_date)
            / ("instrument_id=" + checked_instrument_id)
            / ("secid=" + checked_secid)
            / "part.parquet"
        ),
        manifest_path=(
            root
            / "manifests"
            / ("artifact_id=" + pilot.ARTIFACT_ID)
            / ("artifact_version=" + checked_version)
            / "manifest.json"
        ),
        quality_report_path=(
            root
            / "quality_reports"
            / ("artifact_id=" + pilot.ARTIFACT_ID)
            / ("artifact_version=" + checked_version)
            / "quality_report.json"
        ),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary_name = handle.name
    Path(temporary_name).replace(path)


def _adjust_outputs(paths: pilot.TargetPaths, instrument_id: str, secid: str) -> dict[str, object]:
    frame = pd.read_parquet(paths.partition_path)
    if "family" in frame.columns:
        frame = frame.rename(columns={"family": "instrument_id"})
    frame["instrument_id"] = instrument_id
    columns = [
        "trade_date",
        "ts",
        "session_date",
        "secid",
        "instrument_id",
        "board",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "num_trades",
        "source",
        "ingest_ts",
    ]
    frame = frame.loc[:, columns]
    frame.to_parquet(paths.partition_path, index=False)
    content_hash = _sha256(paths.partition_path)

    manifest = json.loads(paths.manifest_path.read_text(encoding="utf-8"))
    manifest.pop("family_scope", None)
    manifest["producer"] = PRODUCER_ID
    manifest["deterministic_builder_config_version"] = PRODUCER_ID
    manifest["instrument_id_scope"] = [instrument_id]
    manifest["secid_scope"] = [secid]
    manifest["storage_pattern"] = STORAGE_PATTERN
    manifest["content_hash"] = content_hash
    manifest["partition_hashes"] = {paths.partition_path.as_posix(): content_hash}
    _write_json_atomic(paths.manifest_path, manifest)

    quality = json.loads(paths.quality_report_path.read_text(encoding="utf-8"))
    quality.pop("family_scope", None)
    quality["deterministic_builder_config_version"] = PRODUCER_ID
    quality["instrument_id_scope"] = [instrument_id]
    quality["secid_scope"] = [secid]
    _write_json_atomic(paths.quality_report_path, quality)

    return {
        "status": "succeeded",
        "artifact_id": pilot.ARTIFACT_ID,
        "source_artifact_id": pilot.SOURCE_ARTIFACT_ID,
        "storage_partition_path": paths.partition_path.as_posix(),
        "manifest_reference": paths.manifest_path.as_posix(),
        "quality_report_reference": paths.quality_report_path.as_posix(),
        "quality_status": "passed",
        "data_start": manifest["data_start"],
        "data_end": manifest["data_end"],
        "last_valid_trade_date": manifest["last_valid_trade_date"],
        "row_count": int(manifest["row_count"]),
        "instrument_id_scope": [instrument_id],
        "secid_scope": [secid],
        "schema_version": pilot.SCHEMA_VERSION,
        "calendar_session_binding": "moex_iss_futures_calendar/explicit_trade_date_session",
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "content_hash": content_hash,
    }


def materialize_instrument_partition(
    trade_date: str,
    instrument_id: str,
    secid: str,
    artifact_version: str,
    *,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
) -> InstrumentResult:
    checked_instrument_id = _require_token(instrument_id, "instrument_id")
    checked_secid = _require_token(secid, "secid")
    checked_version = _require_token(artifact_version, "artifact_version")
    original_paths_builder = pilot.build_target_paths

    def patched_paths(trade_date_arg: str, instrument_arg: str, secid_arg: str, version_arg: str, env: Mapping[str, str] | None = None) -> pilot.TargetPaths:
        return target_paths(trade_date_arg, checked_instrument_id, secid_arg, version_arg)

    try:
        pilot.build_target_paths = patched_paths
        result = pilot.materialize_partition(
            trade_date=trade_date,
            family=checked_instrument_id,
            secid=checked_secid,
            artifact_version=checked_version,
            timeout=timeout,
            apim_base_url=apim_base_url,
        )
    finally:
        pilot.build_target_paths = original_paths_builder
    paths = target_paths(trade_date, checked_instrument_id, checked_secid, checked_version)
    if result.partition_path != paths.partition_path:
        raise ValueError("instrument target path mismatch")
    return InstrumentResult(payload=_adjust_outputs(paths, checked_instrument_id, checked_secid))


def error_payload(exc: Exception) -> dict[str, object]:
    return {
        "status": "failed",
        "artifact_id": pilot.ARTIFACT_ID,
        "source_artifact_id": pilot.SOURCE_ARTIFACT_ID,
        "error": str(exc),
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize one FORTS raw 5m partition by instrument registry identity.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--instrument-id", required=True)
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
        result = materialize_instrument_partition(
            trade_date=args.trade_date,
            instrument_id=args.instrument_id,
            secid=args.secid,
            artifact_version=args.artifact_version,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
        )
    except Exception as exc:
        print(json.dumps(error_payload(exc), ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result.payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
