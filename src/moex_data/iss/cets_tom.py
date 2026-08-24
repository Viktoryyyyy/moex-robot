from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Mapping

import pandas as pd
import requests

SOURCE_ID = "moex_iss_cets_tom_candles_1m"
BOARD = "CETS"
MARKET = "selt"
ENGINE = "currency"
DEFAULT_BASE_URL = "https://iss.moex.com"
BINDINGS = {
    "usd_tom": "USD000UTSTOM",
    "cny_tom": "CNYRUB_TOM",
}


class CetsTomError(ValueError):
    pass


def validate_identity(instrument_id: str, secid: str) -> tuple[str, str]:
    checked_id = str(instrument_id).strip()
    checked_secid = str(secid).strip()
    expected = BINDINGS.get(checked_id)
    if expected is None:
        raise CetsTomError("unsupported TOM instrument_id")
    if checked_secid != expected:
        raise CetsTomError("secid does not match canonical TOM binding")
    return checked_id, checked_secid


def candles_url(secid: str, base_url: str = DEFAULT_BASE_URL) -> str:
    return base_url.rstrip("/") + f"/iss/engines/currency/markets/selt/boards/CETS/securities/{secid}/candles.json"


def fetch_1m(*, instrument_id: str, secid: str, trade_date: str, timeout: float = 30.0, base_url: str = DEFAULT_BASE_URL) -> pd.DataFrame:
    validate_identity(instrument_id, secid)
    try:
        pd.Timestamp(trade_date)
    except Exception as exc:
        raise CetsTomError("trade_date must be an explicit date") from exc
    response = requests.get(
        candles_url(secid, base_url),
        params={"from": trade_date, "till": trade_date, "interval": 1, "iss.meta": "off"},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    block = payload.get("candles") if isinstance(payload, Mapping) else None
    if not isinstance(block, Mapping):
        raise CetsTomError("ISS candles response missing candles block")
    columns = block.get("columns") or []
    rows = block.get("data") or []
    frame = pd.DataFrame(rows, columns=columns)
    if frame.empty:
        raise CetsTomError("ISS candles response returned no rows")
    return frame


def normalize_1m_to_5m(frame: pd.DataFrame, *, instrument_id: str, secid: str, trade_date: str) -> pd.DataFrame:
    validate_identity(instrument_id, secid)
    by_lower = {str(column).lower(): column for column in frame.columns}
    required = ("open", "high", "low", "close", "volume")
    if any(name not in by_lower for name in required):
        raise CetsTomError("ISS candles response missing OHLCV columns")
    ts_col = by_lower.get("end") or by_lower.get("begin")
    if ts_col is None:
        raise CetsTomError("ISS candles response missing timestamp column")
    work = frame.copy()
    work["_ts"] = pd.to_datetime(work[ts_col], errors="coerce")
    if work["_ts"].isna().any():
        raise CetsTomError("ISS candles response contains invalid timestamps")
    for name in required:
        work[name] = pd.to_numeric(work[by_lower[name]], errors="coerce")
    if work[list(required[:4])].isna().any().any():
        raise CetsTomError("ISS candles response contains null OHLC values")
    work = work.set_index("_ts").sort_index()
    aggregation = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    if "value" in by_lower:
        work["value"] = pd.to_numeric(work[by_lower["value"]], errors="coerce")
        aggregation["value"] = "sum"
    bars = work.resample("5min", label="right", closed="right").agg(aggregation).dropna(subset=["close"]).reset_index()
    if bars.empty:
        raise CetsTomError("5m resample returned no rows")
    output = pd.DataFrame()
    output["instrument_id"] = instrument_id
    output["trade_date"] = trade_date
    output["ts"] = bars["_ts"]
    output["session_date"] = trade_date
    output["secid"] = secid
    output["board"] = BOARD
    output["market"] = MARKET
    output["engine"] = ENGINE
    output["source_id"] = SOURCE_ID
    for name in ("open", "high", "low", "close", "volume"):
        output[name] = bars[name]
    output["value"] = bars["value"] if "value" in bars.columns else pd.NA
    output["source"] = "MOEX_ISS_CETS_CANDLES"
    output["ingest_ts"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return output


def target_path(*, data_root: str | Path, instrument_id: str, trade_date: str) -> Path:
    return Path(data_root) / "market" / "raw" / "timeframe=5m" / f"instrument_id={instrument_id}" / f"trade_date={trade_date}" / f"source={SOURCE_ID}" / "part.parquet"


def materialize(*, instrument_id: str, secid: str, trade_date: str, data_root: str | Path | None = None, timeout: float = 30.0, base_url: str = DEFAULT_BASE_URL) -> Path:
    frame = fetch_1m(instrument_id=instrument_id, secid=secid, trade_date=trade_date, timeout=timeout, base_url=base_url)
    bars = normalize_1m_to_5m(frame, instrument_id=instrument_id, secid=secid, trade_date=trade_date)
    root = data_root or os.environ.get("MOEX_DATA_ROOT")
    if not root:
        raise CetsTomError("MOEX_DATA_ROOT is required")
    path = target_path(data_root=root, instrument_id=instrument_id, trade_date=trade_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    bars.to_parquet(path, index=False)
    return path
