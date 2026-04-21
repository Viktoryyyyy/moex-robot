from __future__ import annotations

from pathlib import Path

import pandas as pd


_REQUIRED_COLS = ("end", "open", "high", "low", "close")


def _sign(value: float) -> int:
    if value > 0.0:
        return 1
    if value < 0.0:
        return -1
    return 0


def _load_intraday(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = [column for column in _REQUIRED_COLS if column not in df.columns]
    if missing:
        raise ValueError("missing required columns: " + ", ".join(missing))

    work = df[list(_REQUIRED_COLS)].copy()
    work["end"] = pd.to_datetime(work["end"], errors="coerce")
    for column in ("open", "high", "low", "close"):
        work[column] = pd.to_numeric(work[column], errors="coerce")

    if work["end"].isna().any():
        raise ValueError("invalid timestamp values in column end")
    if work[["open", "high", "low", "close"]].isna().any().any():
        raise ValueError("non-numeric or missing OHLC values found")
    if work["end"].duplicated().any():
        raise ValueError("duplicate intraday timestamps found")

    work = work.sort_values("end", ascending=True).reset_index(drop=True)
    if work.empty:
        raise ValueError("input csv has zero valid rows")

    work["trade_date"] = work["end"].dt.normalize()
    return work


def _build_daily(work: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for trade_date, group in work.groupby("trade_date", sort=True):
        day = group.sort_values("end", ascending=True).reset_index(drop=True)
        rows.append(
            {
                "trade_date": pd.Timestamp(trade_date),
                "open": float(day.iloc[0]["open"]),
                "high": float(day["high"].max()),
                "low": float(day["low"].min()),
                "close": float(day.iloc[-1]["close"]),
            }
        )

    daily = pd.DataFrame(rows).sort_values("trade_date", ascending=True).reset_index(drop=True)
    if len(daily) < 2:
        raise ValueError("need at least 2 completed trading days after aggregation")
    if daily["trade_date"].duplicated().any():
        raise ValueError("duplicate aggregated trade_date rows found")
    return daily


def _build_pairs(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for index in range(1, len(daily)):
        source = daily.iloc[index - 1]
        outcome = daily.iloc[index]

        source_trade_date = pd.Timestamp(source["trade_date"]).date().isoformat()
        trade_date = pd.Timestamp(outcome["trade_date"]).date().isoformat()
        if source_trade_date >= trade_date:
            raise ValueError("source_trade_date must be strictly earlier than date")

        prior_open = float(source["open"])
        prior_high = float(source["high"])
        prior_low = float(source["low"])
        prior_close = float(source["close"])
        outcome_open = float(outcome["open"])
        outcome_close = float(outcome["close"])

        prior_body_points = prior_close - prior_open
        prior_abs_body_points = abs(prior_body_points)
        prior_range_points = prior_high - prior_low
        if prior_range_points <= 0.0:
            raise ValueError("prior_range_points must be > 0 for source_trade_date=" + source_trade_date)
        if prior_close == 0.0:
            raise ValueError("prior_close must be non-zero for source_trade_date=" + source_trade_date)

        prior_rel_range = prior_range_points / prior_close
        prior_dir = _sign(prior_body_points)
        outcome_oc_points = outcome_close - outcome_open
        mr_outcome_points = -1.0 * float(_sign(prior_body_points)) * outcome_oc_points
        mr_edge_day = int(mr_outcome_points > 0.0)

        rows.append(
            {
                "date": trade_date,
                "source_trade_date": source_trade_date,
                "prior_open": prior_open,
                "prior_high": prior_high,
                "prior_low": prior_low,
                "prior_close": prior_close,
                "prior_body_points": prior_body_points,
                "prior_abs_body_points": prior_abs_body_points,
                "prior_range_points": prior_range_points,
                "prior_rel_range": prior_rel_range,
                "prior_dir": prior_dir,
                "outcome_open": outcome_open,
                "outcome_close": outcome_close,
                "outcome_oc_points": outcome_oc_points,
                "mr_outcome_points": mr_outcome_points,
                "MR_EDGE_DAY": mr_edge_day,
            }
        )

    out = pd.DataFrame(rows).sort_values("date", ascending=True).reset_index(drop=True)
    if out.empty:
        raise ValueError("pair output is empty")
    if out["date"].duplicated().any():
        raise ValueError("duplicate outcome date rows found")
    if (out["source_trade_date"] >= out["date"]).any():
        raise ValueError("source_trade_date must be strictly earlier than date for all rows")
    return out


def materialize_feature_frame(*, dataset_artifact_path: str | Path, instrument_id: str, timezone_name: str) -> pd.DataFrame:
    del timezone_name
    if instrument_id != "usdrubf":
        raise ValueError("instrument_id must equal 'usdrubf'")

    path = Path(dataset_artifact_path)
    work = _load_intraday(path)
    daily = _build_daily(work)
    return _build_pairs(daily)
