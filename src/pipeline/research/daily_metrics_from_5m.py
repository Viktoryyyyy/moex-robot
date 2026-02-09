import sys
import pandas as pd


def compute_daily_metrics(df: pd.DataFrame) -> pd.DataFrame:
    need = ["end", "open", "high", "low", "close"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"missing column: {c}")

    x = df.copy()
    x["end"] = pd.to_datetime(x["end"], errors="coerce")
    x = x.sort_values("end").reset_index(drop=True)

    dates = x["end"].dt.date.unique()
    if len(dates) != 1:
        raise ValueError("input must contain exactly one trading day (D-1)")

    day_open = float(x.iloc[0]["open"])
    day_close = float(x.iloc[-1]["close"])
    day_high = float(x["high"].max())
    day_low = float(x["low"].min())

    day_range = day_high - day_low

    rel_range = day_range / day_close if day_close != 0 else 0.0
    trend_ratio = abs(day_close - day_open) / day_range if day_range > 0 else 0.0

    out = pd.DataFrame([{
        "date": str(dates[0]),
        "rel_range": rel_range,
        "trend_ratio": trend_ratio,
    }])

    return out


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python daily_metrics_from_5m.py <5m_csv_path>")

    path = sys.argv[1]
    df = pd.read_csv(path)
    out = compute_daily_metrics(df)
    print(out.to_csv(index=False).strip())
