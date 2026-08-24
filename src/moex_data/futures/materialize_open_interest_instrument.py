from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import pandas as pd

from . import materialize_forts_raw_5m_instrument as forts_quotes
from . import materialize_raw_5m as raw_core
from . import materialize_raw_5m_full_session as full_session

DATASET_ID: Final[str] = "futures_open_interest_raw_5m"
SOURCE_ID: Final[str] = "moex_algopack_fo_open_interest_5m"
SOURCE_CONTRACT_REF: Final[str] = "contracts/sources/futures/moex_algopack_fo_open_interest_5m.v1.yaml"
PRODUCER_ID: Final[str] = "moex_data.futures.materialize_open_interest_instrument.v1"
ENDPOINT_PATH: Final[str] = "/iss/datashop/algopack/fo/tradestats.json"
CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"


class OpenInterestMaterializationError(ValueError):
    pass


def _fail(message: str) -> None:
    raise OpenInterestMaterializationError(message)


def _require_token(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    if text in (".", "..") or "/" in text or "\\" in text or any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        _fail(field_name + " must be an explicit safe token")
    return text


def _require_date(value: object) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise OpenInterestMaterializationError("trade_date must be explicit YYYY-MM-DD") from exc


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        _fail("MOEX_DATA_ROOT is required")
    return Path(value)


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


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _partition_path(trade_date: str, instrument_id: str, source_id: str) -> Path:
    return (
        _data_root()
        / "market"
        / "supplementary"
        / ("dataset_id=" + DATASET_ID)
        / ("instrument_id=" + instrument_id)
        / ("trade_date=" + trade_date)
        / ("source=" + source_id)
        / "part.parquet"
    )


def _state_path(kind: str, trade_date: str, run_id: str, filename: str) -> Path:
    return _data_root() / "state" / kind / ("dataset_id=" + DATASET_ID) / ("run_date=" + trade_date) / ("run_id=" + run_id) / filename


def _write_json_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        temp_name = handle.name
    Path(temp_name).replace(path)


def _write_parquet_atomic(path: Path, frame: pd.DataFrame, run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name("." + path.name + "." + run_id + ".tmp")
    try:
        frame.to_parquet(temp_path, index=False)
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _fetch_tradestats(trade_date: str, secid: str, timeout: float, apim_base_url: str | None) -> tuple[pd.DataFrame, str]:
    request = raw_core.Raw5mMaterializationRequest(
        repo_root=Path.cwd(),
        dataset_id="futures_raw_5m",
        contract_id="futures_raw_5m.v1",
        trade_date=trade_date,
        secid=secid,
        run_id="oi_fetch",
        source_candidate=raw_core.SOURCE_CANDIDATE_APIM_TRADESTATS,
        source_endpoint=ENDPOINT_PATH,
        market="FORTS",
        board="RFUD",
        series_type="native",
        granularity="5m",
        instrument_id="oi_fetch",
        source_id="oi_fetch",
        engine="futures",
    )
    original_headers = raw_core._auth_headers
    try:
        raw_core._auth_headers = forts_quotes._auth_headers_with_bearer
        return full_session._fetch_apim_tradestats_full_session_frame(request, timeout, apim_base_url, None)
    finally:
        raw_core._auth_headers = original_headers


def _find_column(frame: pd.DataFrame, name: str) -> object:
    by_upper = {str(column).upper(): column for column in frame.columns}
    column = by_upper.get(name.upper())
    if column is None:
        _fail("tradestats response missing required column: " + name)
    return column


def normalize_open_interest(frame: pd.DataFrame, *, trade_date: str, instrument_id: str, secid: str, source_url: str) -> pd.DataFrame:
    checked_date = _require_date(trade_date)
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_secid = _require_token(secid, "secid")
    columns = {name: _find_column(frame, name) for name in ("SECID", "TRADEDATE", "TRADETIME", "OI_OPEN", "OI_HIGH", "OI_LOW", "OI_CLOSE")}
    work = frame.loc[frame[columns["SECID"]].astype(str).str.upper().eq(checked_secid.upper())].copy()
    if work.empty:
        _fail("tradestats response contains no rows for requested secid")
    source_dates = pd.to_datetime(work[columns["TRADEDATE"]], errors="coerce")
    if source_dates.isna().any():
        _fail("tradestats response contains invalid TRADEDATE")
    work = work.loc[source_dates.dt.date.astype(str).eq(checked_date)].copy()
    if work.empty:
        _fail("tradestats response contains no rows for requested trade_date")
    timestamps = pd.to_datetime(
        work[columns["TRADEDATE"]].astype(str).str.strip() + " " + work[columns["TRADETIME"]].astype(str).str.strip(),
        errors="coerce",
    )
    if timestamps.isna().any():
        _fail("tradestats response contains invalid TRADEDATE/TRADETIME")
    output = pd.DataFrame(
        {
            "instrument_id": checked_instrument,
            "trade_date": checked_date,
            "ts": timestamps,
            "secid": checked_secid,
            "board": "RFUD",
            "market": "FORTS",
            "engine": "futures",
            "source_id": SOURCE_ID,
            "oi_open": pd.to_numeric(work[columns["OI_OPEN"]], errors="coerce"),
            "oi_high": pd.to_numeric(work[columns["OI_HIGH"]], errors="coerce"),
            "oi_low": pd.to_numeric(work[columns["OI_LOW"]], errors="coerce"),
            "oi_close": pd.to_numeric(work[columns["OI_CLOSE"]], errors="coerce"),
            "source": source_url,
            "ingest_ts": _utc_now(),
        }
    ).sort_values("ts").reset_index(drop=True)
    oi_columns = ["oi_open", "oi_high", "oi_low", "oi_close"]
    if output[oi_columns].isna().any(axis=None):
        _fail("tradestats response contains null OI values")
    if (output[oi_columns] < 0).any(axis=None):
        _fail("tradestats response contains negative OI values")
    if (output["oi_high"] < output["oi_low"]).any():
        _fail("OI high is lower than OI low")
    if ((output["oi_open"] < output["oi_low"]) | (output["oi_open"] > output["oi_high"]) | (output["oi_close"] < output["oi_low"]) | (output["oi_close"] > output["oi_high"])).any():
        _fail("OI open/close is outside OI high-low range")
    if output.duplicated(subset=["instrument_id", "ts", "source_id"]).any():
        _fail("duplicate OI instrument_id/ts/source_id keys")
    if not output["ts"].is_monotonic_increasing:
        _fail("OI timestamps are not monotonic")
    return output


def materialize_open_interest_partition(*, trade_date: str, instrument_id: str, secid: str, artifact_version: str, timeout: float = 60.0, apim_base_url: str | None = None) -> dict[str, object]:
    checked_date = _require_date(trade_date)
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_secid = _require_token(secid, "secid")
    run_id = _require_token(artifact_version, "artifact_version")
    frame, source_url = _fetch_tradestats(checked_date, checked_secid, timeout, apim_base_url)
    normalized = normalize_open_interest(frame, trade_date=checked_date, instrument_id=checked_instrument, secid=checked_secid, source_url=source_url)
    partition_path = _partition_path(checked_date, checked_instrument, SOURCE_ID)
    quality_path = _state_path("quality", checked_date, run_id, "quality_report.json")
    manifest_path = _state_path("refresh", checked_date, run_id, "manifest.json")
    _write_parquet_atomic(partition_path, normalized, run_id)
    quality = {
        "dataset_id": DATASET_ID,
        "run_id": run_id,
        "instrument_id": checked_instrument,
        "secid": checked_secid,
        "trade_date": checked_date,
        "rows": int(len(normalized.index)),
        "min_ts": str(normalized["ts"].min()),
        "max_ts": str(normalized["ts"].max()),
        "quality_status": "pass",
    }
    manifest = {
        "dataset_id": DATASET_ID,
        "producer": PRODUCER_ID,
        "source_id": SOURCE_ID,
        "source_contract_ref": SOURCE_CONTRACT_REF,
        "run_id": run_id,
        "trade_date": checked_date,
        "instrument_id": checked_instrument,
        "secid": checked_secid,
        "partition_path": partition_path.as_posix(),
        "quality_report_path": quality_path.as_posix(),
        "row_count": int(len(normalized.index)),
        "status": "succeeded",
        "latest_autodetect_used": False,
    }
    _write_json_atomic(quality_path, quality)
    _write_json_atomic(manifest_path, manifest)
    return {**manifest, "manifest_path": manifest_path.as_posix(), "quality_status": "pass"}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize canonical 5m futures open interest from AlgoPack FO tradestats.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--artifact-version", required=True)
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--apim-base-url", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_env_file(args.env_file)
        payload = materialize_open_interest_partition(
            trade_date=args.trade_date,
            instrument_id=args.instrument_id,
            secid=args.secid,
            artifact_version=args.artifact_version,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "dataset_id": DATASET_ID, "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
