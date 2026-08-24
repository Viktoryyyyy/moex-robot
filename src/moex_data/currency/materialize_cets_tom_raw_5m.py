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
import requests

DATASET_ID: Final[str] = "fx_spot_raw_5m"
SOURCE_ID: Final[str] = "moex_iss_cets_tom_1m"
SOURCE_CONTRACT_REF: Final[str] = "contracts/sources/currency/moex_iss_cets_tom_1m.v1.yaml"
PRODUCER_ID: Final[str] = "moex_data.currency.materialize_cets_tom_raw_5m.v1"
DEFAULT_BASE_URL: Final[str] = "https://iss.moex.com"
CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
MAX_PAGES: Final[int] = 100
INSTRUMENTS: Final[dict[str, str]] = {"usd_tom": "USD000UTSTOM", "cny_tom": "CNYRUB_TOM"}


class CetsTomMaterializationError(ValueError):
    pass


def _fail(message: str) -> None:
    raise CetsTomMaterializationError(message)


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
        raise CetsTomMaterializationError("trade_date must be explicit YYYY-MM-DD") from exc


def _validate_identity(instrument_id: str, secid: str) -> tuple[str, str]:
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_secid = _require_token(secid, "secid")
    expected = INSTRUMENTS.get(checked_instrument)
    if expected is None:
        _fail("instrument_id is not in canonical CETS TOM registry")
    if expected.upper() != checked_secid.upper():
        _fail("secid does not match canonical CETS TOM registry binding")
    return checked_instrument, expected


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


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        _fail("MOEX_DATA_ROOT is required")
    return Path(value)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _block_to_frame(payload: Mapping[str, object]) -> pd.DataFrame:
    block = payload.get("candles")
    if not isinstance(block, Mapping):
        _fail("MOEX ISS response missing candles block")
    columns = block.get("columns")
    rows = block.get("data")
    if not isinstance(columns, list) or not isinstance(rows, list):
        _fail("MOEX ISS candles block has invalid shape")
    return pd.DataFrame(rows, columns=columns)


def _page_signature(frame: pd.DataFrame) -> tuple[object, ...]:
    if frame.empty:
        return (0,)
    return (len(frame.index), tuple(str(v) for v in frame.iloc[0].tolist()), tuple(str(v) for v in frame.iloc[-1].tolist()))


def fetch_1m_candles(*, trade_date: str, secid: str, timeout: float = 30.0, base_url: str | None = None) -> tuple[pd.DataFrame, str]:
    checked_date = _require_date(trade_date)
    checked_secid = _require_token(secid, "secid")
    base = str(base_url or os.environ.get("MOEX_ISS_URL", DEFAULT_BASE_URL)).strip().rstrip("/")
    if not base:
        _fail("MOEX ISS base URL is required")
    path = f"/iss/engines/currency/markets/selt/boards/CETS/securities/{checked_secid}/candles.json"
    url = base + path
    frames: list[pd.DataFrame] = []
    seen: set[tuple[object, ...]] = set()
    start = 0
    source_url = url
    for _ in range(MAX_PAGES):
        response = requests.get(url, params={"from": checked_date, "till": checked_date, "interval": 1, "start": start, "iss.meta": "off"}, timeout=timeout, headers={"User-Agent": "moex_bot_step3_cets_tom/1.0"})
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, Mapping):
            _fail("MOEX ISS response root is not an object")
        frame = _block_to_frame(payload)
        source_url = str(getattr(response, "url", url))
        if frame.empty:
            if not frames:
                _fail("MOEX ISS returned no 1m candles for explicit secid/date")
            break
        signature = _page_signature(frame)
        if signature in seen:
            _fail("MOEX ISS candle pagination did not advance")
        seen.add(signature)
        frames.append(frame)
        start += len(frame.index)
    else:
        _fail("MOEX ISS candle pagination exceeded max-pages guard")
    return pd.concat(frames, ignore_index=True), source_url


def _sum_preserve_all_null(values: pd.Series) -> float:
    return values.sum(min_count=1)


def normalize_to_5m(frame: pd.DataFrame, *, trade_date: str, instrument_id: str, secid: str, source_url: str) -> pd.DataFrame:
    checked_date = _require_date(trade_date)
    checked_instrument, checked_secid = _validate_identity(instrument_id, secid)
    by_lower = {str(column).lower(): column for column in frame.columns}
    required = ("open", "high", "low", "close", "volume", "begin", "end")
    missing = [name for name in required if name not in by_lower]
    if missing:
        _fail("MOEX ISS candles schema missing: " + ",".join(missing))
    work = frame.copy()
    for name in ("open", "high", "low", "close", "volume", "value"):
        column = by_lower.get(name)
        if column is not None:
            original = work[column]
            converted = pd.to_numeric(original, errors="coerce")
            if (original.notna() & converted.isna()).any():
                _fail("MOEX ISS candles contain nonnumeric " + name)
            work[name] = converted
    work["_end"] = pd.to_datetime(work[by_lower["end"]], errors="coerce")
    if work["_end"].isna().any():
        _fail("MOEX ISS candles contain invalid end timestamps")
    work = work.loc[work["_end"].dt.date.astype(str).eq(checked_date)].copy()
    if work.empty:
        _fail("MOEX ISS candles contain no rows for explicit trade_date")
    if work[["open", "high", "low", "close"]].isna().any(axis=None):
        _fail("MOEX ISS candles contain null OHLC")
    work = work.set_index("_end").sort_index()
    aggregation: dict[str, object] = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": _sum_preserve_all_null}
    if "value" in work.columns:
        aggregation["value"] = _sum_preserve_all_null
    bars = work.resample("5min", label="right", closed="right").agg(aggregation).dropna(subset=["open", "high", "low", "close"]).reset_index()
    if bars.empty:
        _fail("5m resample produced no rows")
    if "value" not in bars.columns:
        bars["value"] = pd.NA
    output = pd.DataFrame({"instrument_id": checked_instrument, "trade_date": checked_date, "ts": bars["_end"], "session_date": checked_date, "secid": checked_secid, "board": "CETS", "market": "selt", "engine": "currency", "source_id": SOURCE_ID, "open": bars["open"], "high": bars["high"], "low": bars["low"], "close": bars["close"], "volume": bars["volume"], "value": bars["value"], "source": source_url, "ingest_ts": _utc_now()}).sort_values("ts").reset_index(drop=True)
    if (output["high"] < output["low"]).any():
        _fail("5m bars contain high lower than low")
    if ((output["open"] < output["low"]) | (output["open"] > output["high"]) | (output["close"] < output["low"]) | (output["close"] > output["high"])).any():
        _fail("5m bars contain open/close outside high-low range")
    if output.duplicated(subset=["instrument_id", "ts", "source_id"]).any():
        _fail("duplicate 5m instrument_id/ts/source_id keys")
    if not output["ts"].is_monotonic_increasing:
        _fail("5m timestamps are not monotonic")
    return output


def _partition_path(trade_date: str, instrument_id: str) -> Path:
    return _data_root() / "market" / "raw" / "timeframe=5m" / ("instrument_id=" + instrument_id) / ("trade_date=" + trade_date) / ("source=" + SOURCE_ID) / "part.parquet"


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


def materialize_cets_tom_partition(*, trade_date: str, instrument_id: str, secid: str, artifact_version: str, timeout: float = 30.0, base_url: str | None = None) -> dict[str, object]:
    checked_date = _require_date(trade_date)
    checked_instrument, checked_secid = _validate_identity(instrument_id, secid)
    run_id = _require_token(artifact_version, "artifact_version")
    frame, source_url = fetch_1m_candles(trade_date=checked_date, secid=checked_secid, timeout=timeout, base_url=base_url)
    normalized = normalize_to_5m(frame, trade_date=checked_date, instrument_id=checked_instrument, secid=checked_secid, source_url=source_url)
    partition_path = _partition_path(checked_date, checked_instrument)
    quality_path = _state_path("quality", checked_date, run_id, "quality_report.json")
    manifest_path = _state_path("refresh", checked_date, run_id, "manifest.json")
    _write_parquet_atomic(partition_path, normalized, run_id)
    quality = {"dataset_id": DATASET_ID, "run_id": run_id, "instrument_id": checked_instrument, "source_id": SOURCE_ID, "secid": checked_secid, "trade_date": checked_date, "partition_path": partition_path.as_posix(), "rows": int(len(normalized.index)), "min_ts": str(normalized["ts"].min()), "max_ts": str(normalized["ts"].max()), "volume_null_rows": int(normalized["volume"].isna().sum()), "value_null_rows": int(normalized["value"].isna().sum()), "quality_status": "pass"}
    manifest = {"dataset_id": DATASET_ID, "producer": PRODUCER_ID, "source_id": SOURCE_ID, "source_contract_ref": SOURCE_CONTRACT_REF, "run_id": run_id, "trade_date": checked_date, "instrument_id": checked_instrument, "secid": checked_secid, "partition_path": partition_path.as_posix(), "quality_report_path": quality_path.as_posix(), "row_count": int(len(normalized.index)), "status": "succeeded", "latest_autodetect_used": False}
    _write_json_atomic(quality_path, quality)
    _write_json_atomic(manifest_path, manifest)
    return {**manifest, "manifest_path": manifest_path.as_posix(), "quality_status": "pass"}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize canonical CETS TOM 5m bars from explicit 1m ISS candles.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--instrument-id", required=True, choices=tuple(INSTRUMENTS))
    parser.add_argument("--secid", required=True)
    parser.add_argument("--artifact-version", required=True)
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--iss-base-url", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_env_file(args.env_file)
        payload = materialize_cets_tom_partition(trade_date=args.trade_date, instrument_id=args.instrument_id, secid=args.secid, artifact_version=args.artifact_version, timeout=args.timeout, base_url=args.iss_base_url)
    except Exception as exc:
        print(json.dumps({"status": "failed", "dataset_id": DATASET_ID, "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
