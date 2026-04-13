from __future__ import annotations

from pathlib import Path

import pandas as pd


def _resolve_ohlc_cols(df_cols: list[str]) -> dict[str, str]:
    required = ["end", "open", "high", "low", "close"]
    if all(column in df_cols for column in required):
        return {column: column for column in required}

    candidates = [
        {"end": "end", "open": "open_fo", "high": "high_fo", "low": "low_fo", "close": "close_fo"},
        {"end": "end", "open": "OPEN", "high": "HIGH", "low": "LOW", "close": "CLOSE"},
    ]
    for candidate in candidates:
        if all(candidate[key] in df_cols for key in required):
            return candidate
    raise ValueError("cannot resolve OHLC columns")


def materialize_feature_frame(*, dataset_artifact_path: str | Path, instrument_id: str, timezone_name: str) -> pd.DataFrame:
    path = Path(dataset_artifact_path)
    df = pd.read_csv(path)

    colmap = _resolve_ohlc_cols(list(df.columns))
    columns = [colmap["end"], colmap["open"], colmap["high"], colmap["low"], colmap["close"]]
    work = df[columns].copy()
    work.columns = ["end", "open", "high", "low", "close"]

    if "volume" in df.columns:
        work["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    elif "vol" in df.columns:
        work["volume"] = pd.to_numeric(df["vol"], errors="coerce")

    work["end"] = pd.to_datetime(work["end"], errors="coerce")
    if getattr(work["end"].dt, "tz", None) is None:
        work["end"] = work["end"].dt.tz_localize(timezone_name, nonexistent="NaT", ambiguous="NaT")
    else:
        work["end"] = work["end"].dt.tz_convert(timezone_name)

    for column in ["open", "high", "low", "close"]:
        work[column] = pd.to_numeric(work[column], errors="coerce")

    work = work.dropna(subset=["end", "open", "high", "low", "close"]).sort_values("end").reset_index(drop=True)

    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    }
    if "volume" in work.columns:
        agg["volume"] = "sum"

    out = (
        work.set_index("end")
        .resample("15min", label="right", closed="right")
        .agg(agg)
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )
    if out.empty:
        raise ValueError("15m materialization produced zero rows")

    out.insert(0, "instrument_id", instrument_id)
    return out
