from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from . import materialize_raw_5m as core
from . import materialize_raw_5m_full_session as full_session

PRODUCER_ID: Final[str] = "moex_data.futures.materialize_forts_raw_5m_instrument.v2"
DATASET_ID: Final[str] = "futures_raw_5m"
CONTRACT_ID: Final[str] = "futures_raw_5m.v1"
SOURCE_ID: Final[str] = "moex_algopack_fo_tradestats_5m"
SOURCE_CONTRACT_REF: Final[str] = "contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml"
SOURCE_CANDIDATE: Final[str] = core.SOURCE_CANDIDATE_APIM_TRADESTATS
SOURCE_ENDPOINT: Final[str] = core.SOURCE_ENDPOINT_APIM_FO_TRADESTATS
STORAGE_PATTERN: Final[str] = (
    "${MOEX_DATA_ROOT}/market/raw/timeframe=5m/"
    "instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet"
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


def _auth_headers_with_bearer(env: Mapping[str, str] | None) -> dict[str, str]:
    active_env = os.environ if env is None else env
    headers = dict(core._auth_headers(env))
    token = str(active_env.get("MOEX_API_KEY", "")).strip()
    if not token:
        raise ValueError("MOEX_API_KEY is required for canonical FORTS AlgoPack source")
    headers["Authorization"] = "Bearer " + token
    return headers


def target_paths(
    trade_date: str,
    instrument_id: str,
    secid: str,
    artifact_version: str,
    source_id: str = SOURCE_ID,
) -> core.Raw5mMaterializationPaths:
    return core.materialization_target_paths(
        repo_root=Path.cwd(),
        dataset_id=DATASET_ID,
        contract_id=CONTRACT_ID,
        trade_date=trade_date,
        secid=_require_token(secid, "secid"),
        run_id=_require_token(artifact_version, "artifact_version"),
        instrument_id=_require_token(instrument_id, "instrument_id"),
        source_id=_require_token(source_id, "source_id"),
    )


def materialize_instrument_partition(
    trade_date: str,
    instrument_id: str,
    secid: str,
    artifact_version: str,
    *,
    source_id: str = SOURCE_ID,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
) -> InstrumentResult:
    checked_instrument_id = _require_token(instrument_id, "instrument_id")
    checked_secid = _require_token(secid, "secid")
    checked_version = _require_token(artifact_version, "artifact_version")
    checked_source_id = _require_token(source_id, "source_id")
    if checked_source_id != SOURCE_ID:
        raise ValueError("source_id does not match canonical FORTS tradestats source contract")

    original_headers = core._auth_headers
    original_fetcher = core._fetch_apim_tradestats_frame
    try:
        core._auth_headers = _auth_headers_with_bearer
        core._fetch_apim_tradestats_frame = full_session._fetch_apim_tradestats_full_session_frame
        result = core.materialize_single_raw_5m_partition(
            repo_root=Path.cwd(),
            dataset_id=DATASET_ID,
            contract_id=CONTRACT_ID,
            trade_date=trade_date,
            family=None,
            secid=checked_secid,
            source_path=None,
            run_id=checked_version,
            instrument_id=checked_instrument_id,
            source_id=checked_source_id,
            source_candidate=SOURCE_CANDIDATE,
            source_endpoint=SOURCE_ENDPOINT,
            market="FORTS",
            board="RFUD",
            engine="futures",
            series_type="native",
            granularity="5m",
            timeout=timeout,
            apim_base_url=apim_base_url,
        )
    finally:
        core._auth_headers = original_headers
        core._fetch_apim_tradestats_frame = original_fetcher

    return InstrumentResult(
        payload={
            "status": result.status,
            "dataset_id": DATASET_ID,
            "source_id": checked_source_id,
            "source_contract_ref": SOURCE_CONTRACT_REF,
            "storage_partition_path": result.partition_path.as_posix(),
            "manifest_reference": result.manifest_path.as_posix(),
            "quality_report_reference": result.quality_report_path.as_posix(),
            "quality_status": result.quality_status,
            "row_count": result.rows,
            "instrument_id_scope": [checked_instrument_id],
            "secid_scope": [checked_secid],
            "storage_pattern": STORAGE_PATTERN,
            "latest_autodetect_used": False,
            "hardcoded_server_path_used": False,
        }
    )


def error_payload(exc: Exception) -> dict[str, object]:
    return {
        "status": "failed",
        "dataset_id": DATASET_ID,
        "source_id": SOURCE_ID,
        "source_contract_ref": SOURCE_CONTRACT_REF,
        "error": str(exc),
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize one canonical FORTS raw 5m partition by instrument registry identity.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--source-id", default=SOURCE_ID)
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
            source_id=args.source_id,
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
