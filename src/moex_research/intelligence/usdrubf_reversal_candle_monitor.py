from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo

import pandas as pd


PROJECT: Final[str] = "MOEX_Bot"
SCHEMA_VERSION: Final[str] = "usdrubf_reversal_candle_monitor.v2"
RULES_SCHEMA_VERSION: Final[str] = "usdrubf_reversal_candle_rules.v2"
MARKET_TZ: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")
APIM_BASE_URL: Final[str] = "https://apim.moex.com"
TRADESTATS_PATH: Final[str] = "/iss/datashop/algopack/fo/tradestats/USDRUBF.json"
FUTOI_PATH: Final[str] = "/iss/analyticalproducts/futoi/securities/usdrubf.json"
DEFAULT_RULES_PATH: Final[str] = "configs/monitoring/usdrubf_reversal_candle_v2.json"
DEFAULT_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"


class ReversalMonitorError(ValueError):
    pass


def _load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.is_file() or env_path.is_symlink():
        raise ReversalMonitorError("env_file must be a regular non-symlink file")
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip('"').strip("'")


def _iso_date(value: object, field: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ReversalMonitorError(field + " must be YYYY-MM-DD") from exc
    if parsed.isoformat() != text:
        raise ReversalMonitorError(field + " must be YYYY-MM-DD")
    return text


def _aware_utc(value: datetime | None = None) -> datetime:
    now = value or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ReversalMonitorError("now_utc must be timezone-aware")
    return now.astimezone(timezone.utc)


def _read_rules(path: str | Path) -> dict[str, Any]:
    rules_path = Path(path).resolve(strict=True)
    if rules_path.is_symlink() or not rules_path.is_file():
        raise ReversalMonitorError("rules_path must be a regular non-symlink file")
    rules = json.loads(rules_path.read_text(encoding="utf-8"))
    if not isinstance(rules, dict) or rules.get("schema_version") != RULES_SCHEMA_VERSION:
        raise ReversalMonitorError("rules schema_version mismatch")
    if rules.get("instrument") != "USDRUBF" or rules.get("timeframe") != "5m":
        raise ReversalMonitorError("rules instrument/timeframe mismatch")
    return rules


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, suffix=".tmp", delete=False
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        staged = Path(handle.name)
    try:
        staged.replace(path)
    finally:
        staged.unlink(missing_ok=True)


def _lower_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result.columns = [str(column).strip().lower() for column in result.columns]
    return result


def normalize_tradestats(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "tradedate", "tradetime", "systime", "secid", "pr_open", "pr_high",
        "pr_low", "pr_close", "pr_vwap", "vol", "vol_b", "vol_s", "oi_open",
        "oi_close",
    }
    work = _lower_columns(frame)
    missing = sorted(required - set(work.columns))
    if missing:
        raise ReversalMonitorError("TradeStats missing columns: " + ",".join(missing))
    work = work.loc[work["secid"].astype(str).str.upper() == "USDRUBF"].copy()
    if work.empty:
        raise ReversalMonitorError("TradeStats has no USDRUBF rows")
    work["trade_date"] = pd.to_datetime(work["tradedate"], errors="raise").dt.date.astype(str)
    work["ts"] = pd.to_datetime(
        work["trade_date"] + " " + work["tradetime"].astype(str), errors="raise"
    )
    work["publication_ts"] = pd.to_datetime(work["systime"], errors="raise")
    numeric = [
        "pr_open", "pr_high", "pr_low", "pr_close", "pr_vwap", "vol", "vol_b",
        "vol_s", "oi_open", "oi_close",
    ]
    for column in numeric:
        work[column] = pd.to_numeric(work[column], errors="raise")
    if bool(work.duplicated(["trade_date", "ts"]).any()):
        raise ReversalMonitorError("TradeStats contains duplicate five-minute timestamps")
    if bool((work["vol_b"] + work["vol_s"] != work["vol"]).any()):
        raise ReversalMonitorError("TradeStats volume identity failed")
    if bool((work[["pr_open", "pr_high", "pr_low", "pr_close"]] <= 0).any().any()):
        raise ReversalMonitorError("TradeStats contains non-positive price")
    work["delta"] = work["vol_b"] - work["vol_s"]
    return work.sort_values(["trade_date", "ts"]).reset_index(drop=True)


def build_daily(tradestats: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    previous_close: float | None = None
    for trade_date, group in tradestats.groupby("trade_date", sort=True):
        part = group.sort_values("ts")
        high = float(part["pr_high"].max())
        low = float(part["pr_low"].min())
        close = float(part.iloc[-1]["pr_close"])
        volume = float(part["vol"].sum())
        if high <= low or volume <= 0:
            raise ReversalMonitorError("daily range and volume must be positive")
        true_range = high - low
        if previous_close is not None:
            true_range = max(true_range, abs(high - previous_close), abs(low - previous_close))
        rows.append(
            {
                "trade_date": str(trade_date),
                "open": float(part.iloc[0]["pr_open"]),
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "vol_b": float(part["vol_b"].sum()),
                "vol_s": float(part["vol_s"].sum()),
                "oi_close": float(part.iloc[-1]["oi_close"]),
                "vwap": float((part["pr_vwap"] * part["vol"]).sum() / volume),
                "row_count": int(len(part.index)),
                "first_ts": part.iloc[0]["ts"],
                "last_ts": part.iloc[-1]["ts"],
                "true_range": float(true_range),
            }
        )
        previous_close = close
    return pd.DataFrame(rows)


def _complete_window(frame: pd.DataFrame, start_exclusive: pd.Timestamp, end_inclusive: pd.Timestamp, count: int) -> pd.DataFrame | None:
    selected = frame.loc[(frame["ts"] > start_exclusive) & (frame["ts"] <= end_inclusive)].copy()
    if len(selected.index) != count:
        return None
    if count > 1 and not bool(selected["ts"].diff().dropna().eq(pd.Timedelta(minutes=5)).all()):
        return None
    return selected


def intraday_features(day: pd.DataFrame) -> dict[str, object]:
    part = day.sort_values("ts").reset_index(drop=True)
    peak_index = int(part.index[part["pr_high"].eq(part["pr_high"].max())][0])
    peak = part.iloc[peak_index]
    peak_ts = pd.Timestamp(peak["ts"])
    trough_index = int(part.index[part["pr_low"].eq(part["pr_low"].min())][0])
    trough = part.iloc[trough_index]
    trough_ts = pd.Timestamp(trough["ts"])

    def transition(
        extreme_index: int,
        extreme_ts: pd.Timestamp,
        *,
        direction: str,
    ) -> dict[str, object] | None:
        for index in range(max(extreme_index + 1, 6), len(part.index)):
            row = part.iloc[index]
            if pd.Timestamp(row["ts"]) > extreme_ts + pd.Timedelta(minutes=60):
                break
            previous = part.iloc[index - 6:index]
            timestamps = part.iloc[index - 6:index + 1]["ts"]
            if not bool(timestamps.diff().dropna().eq(pd.Timedelta(minutes=5)).all()):
                continue
            bearish = (
                float(row["pr_close"]) < float(previous["pr_low"].min())
                and float(row["delta"]) < 0
            )
            bullish = (
                float(row["pr_close"]) > float(previous["pr_high"].max())
                and float(row["delta"]) > 0
            )
            if (direction == "bearish" and bearish) or (direction == "bullish" and bullish):
                return {
                    "time": pd.Timestamp(row["ts"]).time().isoformat(),
                    "delay_minutes": float(
                        (pd.Timestamp(row["ts"]) - extreme_ts).total_seconds() / 60
                    ),
                    "close": float(row["pr_close"]),
                    "publication_ts": pd.Timestamp(row["publication_ts"]).isoformat(),
                }
        return None

    post_peak_60 = _complete_window(part, peak_ts, peak_ts + pd.Timedelta(minutes=60), 12)
    post_trough_60 = _complete_window(part, trough_ts, trough_ts + pd.Timedelta(minutes=60), 12)
    bearish_break = transition(peak_index, peak_ts, direction="bearish")
    bullish_break = transition(trough_index, trough_ts, direction="bullish")

    def flow(window: pd.DataFrame | None) -> float | None:
        if window is None or float(window["vol"].sum()) <= 0:
            return None
        return float(100 * window["delta"].sum() / window["vol"].sum())

    return {
        "peak_time": peak_ts.time().isoformat(),
        "peak_publication_ts": pd.Timestamp(peak["publication_ts"]).isoformat(),
        "post_peak_60m_complete": post_peak_60 is not None,
        "post_peak_60m_flow_pct": flow(post_peak_60),
        "break_previous_6_bar_low_with_negative_delta_within_60m": bearish_break is not None,
        "first_break": bearish_break,
        "trough_time": trough_ts.time().isoformat(),
        "trough_publication_ts": pd.Timestamp(trough["publication_ts"]).isoformat(),
        "post_trough_60m_complete": post_trough_60 is not None,
        "post_trough_60m_flow_pct": flow(post_trough_60),
        "break_previous_6_bar_high_with_positive_delta_within_60m": bullish_break is not None,
        "first_break_high": bullish_break,
    }


def session_metrics(
    tradestats: pd.DataFrame,
    target_date: str,
    *,
    minimum_rows: int,
    new_extreme_lookback_sessions: int,
) -> dict[str, object]:
    daily = build_daily(tradestats)
    matches = daily.index[daily["trade_date"].eq(target_date)].tolist()
    if not matches:
        raise ReversalMonitorError("target session is absent")
    index = int(matches[0])
    if index < max(21, new_extreme_lookback_sessions):
        raise ReversalMonitorError("insufficient completed-session history")
    current = daily.iloc[index]
    previous = daily.iloc[index - 1]
    if int(current["row_count"]) < minimum_rows:
        raise ReversalMonitorError("target session has too few five-minute rows")
    prior = daily.iloc[:index]
    target_weekday = pd.Timestamp(target_date).dayofweek < 5
    same_kind = prior.loc[
        pd.to_datetime(prior["trade_date"]).dt.dayofweek.lt(5).eq(target_weekday)
    ].tail(20)
    if len(same_kind.index) < 5:
        raise ReversalMonitorError("insufficient same-session-type history for relative volume")
    atr_source = prior.tail(20)
    atr20 = float(atr_source["true_range"].mean())
    if atr20 <= 0:
        raise ReversalMonitorError("ATR20 is not positive")
    range_value = float(current["high"] - current["low"])
    previous_range = float(previous["high"] - previous["low"])
    target = tradestats.loc[tradestats["trade_date"].eq(target_date)].copy()
    metrics: dict[str, object] = {
        "trade_date": target_date,
        "previous_trade_date": str(previous["trade_date"]),
        "open": float(current["open"]),
        "high": float(current["high"]),
        "low": float(current["low"]),
        "close": float(current["close"]),
        "vwap": float(current["vwap"]),
        "row_count": int(current["row_count"]),
        "first_bar_ts": pd.Timestamp(current["first_ts"]).isoformat(),
        "last_bar_ts": pd.Timestamp(current["last_ts"]).isoformat(),
        "relative_volume_20": float(current["volume"] / same_kind["volume"].median()),
        "volume_vs_previous": float(current["volume"] / previous["volume"]),
        "daily_return_pct": float(100 * (current["close"] / previous["close"] - 1)),
        "daily_flow_pct": float(100 * (current["vol_b"] - current["vol_s"]) / current["volume"]),
        "daily_oi_pct": float(100 * (current["oi_close"] / previous["oi_close"] - 1)),
        "close_location": float((current["close"] - current["low"]) / range_value),
        "upper_wick_fraction": float((current["high"] - max(current["open"], current["close"])) / range_value),
        "lower_wick_fraction": float((min(current["open"], current["close"]) - current["low"]) / range_value),
        "close_below_session_vwap": bool(current["close"] < current["vwap"]),
        "close_above_session_vwap": bool(current["close"] > current["vwap"]),
        "atr20": atr20,
        "peak_to_close_atr": float((current["high"] - current["close"]) / atr20),
        "low_to_close_atr": float((current["close"] - current["low"]) / atr20),
        "new_5_session_high": bool(
            current["high"] >= prior.tail(new_extreme_lookback_sessions)["high"].max()
        ),
        "new_5_session_low": bool(
            current["low"] <= prior.tail(new_extreme_lookback_sessions)["low"].min()
        ),
        "previous_session_up": bool(previous["close"] > daily.iloc[index - 2]["close"]),
        "previous_session_down": bool(previous["close"] < daily.iloc[index - 2]["close"]),
        "previous_session_close_location": float(
            (previous["close"] - previous["low"]) / previous_range
        ),
    }
    metrics.update(intraday_features(target))
    return metrics


def futoi_change(
    frame: pd.DataFrame,
    *,
    previous_date: str,
    target_date: str,
    previous_end: pd.Timestamp,
    target_end: pd.Timestamp,
    maximum_age_minutes: int,
) -> tuple[float | None, dict[str, object]]:
    if frame.empty:
        return None, {"status": "UNAVAILABLE", "reason": "empty_response"}
    work = _lower_columns(frame)
    required = {"tradedate", "tradetime", "systime", "clgroup", "pos", "pos_long", "pos_short"}
    missing = sorted(required - set(work.columns))
    if missing:
        return None, {"status": "UNAVAILABLE", "reason": "missing_columns", "columns": missing}
    work["trade_date"] = pd.to_datetime(work["tradedate"], errors="coerce").dt.date.astype(str)
    work["ts"] = pd.to_datetime(work["trade_date"] + " " + work["tradetime"].astype(str), errors="coerce")
    work["publication_ts"] = pd.to_datetime(work["systime"], errors="coerce")
    for column in ("pos", "pos_long", "pos_short"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work = work.dropna(subset=["ts", "publication_ts", "pos", "pos_long", "pos_short"])
    work["clgroup"] = work["clgroup"].astype(str).str.upper()
    work = work.loc[work["clgroup"].isin(["FIZ", "YUR"])].sort_values("publication_ts")
    work = work.drop_duplicates(["ts", "clgroup"], keep="first")
    pairs: dict[pd.Timestamp, dict[str, pd.Series]] = {}
    for ts, group in work.groupby("ts"):
        by_group = {str(row["clgroup"]): row for _, row in group.iterrows()}
        if set(by_group) != {"FIZ", "YUR"}:
            continue
        if int(by_group["FIZ"]["pos"] + by_group["YUR"]["pos"]) != 0:
            continue
        pairs[pd.Timestamp(ts)] = by_group

    def snapshot(trade_date: str, endpoint: pd.Timestamp) -> tuple[pd.Timestamp, dict[str, pd.Series]] | None:
        candidates = [(ts, pair) for ts, pair in pairs.items() if str(ts.date()) == trade_date and ts <= endpoint]
        if not candidates:
            return None
        selected = max(candidates, key=lambda value: value[0])
        if endpoint - selected[0] > pd.Timedelta(minutes=maximum_age_minutes):
            return None
        return selected

    first = snapshot(previous_date, previous_end)
    second = snapshot(target_date, target_end)
    if first is None or second is None:
        return None, {"status": "UNAVAILABLE", "reason": "no_balanced_endpoint_within_age_limit"}
    first_ts, first_pair = first
    second_ts, second_pair = second
    denominator = float(first_pair["FIZ"]["pos_long"] + first_pair["YUR"]["pos_long"])
    if denominator <= 0:
        return None, {"status": "UNAVAILABLE", "reason": "non_positive_one_sided_oi"}
    value = float(
        100 * (second_pair["FIZ"]["pos_long"] - first_pair["FIZ"]["pos_long"]) / denominator
    )
    return value, {
        "status": "READY",
        "pair_balance": "exact_zero",
        "previous_endpoint": first_ts.isoformat(),
        "target_endpoint": second_ts.isoformat(),
        "maximum_endpoint_age_minutes": maximum_age_minutes,
    }


def evaluate(
    metrics: Mapping[str, object],
    rules: Mapping[str, object],
    direction: str = "bearish",
) -> dict[str, object]:
    direction_rules = rules.get("direction_rules", {}).get(direction)
    if not isinstance(direction_rules, Mapping):
        raise ReversalMonitorError("direction rules are missing: " + direction)
    score_values = direction_rules["score"]
    if not isinstance(score_values, Mapping):
        raise ReversalMonitorError("score configuration must be an object")
    if direction == "bearish":
        checks = {
            "upper_wick_fraction_gte_0_4": float(metrics["upper_wick_fraction"]) >= 0.4,
            "close_location_lte_0_35": float(metrics["close_location"]) <= 0.35,
            "close_below_session_vwap": bool(metrics["close_below_session_vwap"]),
            "break_previous_6_bar_low_with_negative_delta_within_60m": bool(
                metrics["break_previous_6_bar_low_with_negative_delta_within_60m"]
            ),
            "peak_to_close_atr_gte_0_5": float(metrics["peak_to_close_atr"]) >= 0.5,
            "relative_volume_gte_1_3": float(metrics["relative_volume_20"]) >= 1.3,
            "volume_vs_previous_gte_1_25": float(metrics["volume_vs_previous"]) >= 1.25,
            "daily_flow_pct_lte_minus_2": float(metrics["daily_flow_pct"]) <= -2.0,
            "daily_oi_pct_lte_minus_1": float(metrics["daily_oi_pct"]) <= -1.0,
            "post_peak_60m_flow_pct_lte_minus_5": metrics.get("post_peak_60m_flow_pct") is not None
            and float(metrics["post_peak_60m_flow_pct"]) <= -5.0,
            "fiz_long_change_pp_lte_minus_1": metrics.get("fiz_long_change_pp") is not None
            and float(metrics["fiz_long_change_pp"]) <= -1.0,
            "previous_session_up_and_close_location_gte_0_75": bool(metrics["previous_session_up"])
            and float(metrics["previous_session_close_location"]) >= 0.75,
        }
        new_extreme = bool(metrics["new_5_session_high"])
        price_rejection = (
            float(metrics["upper_wick_fraction"]) >= 0.4
            and float(metrics["close_location"]) <= 0.5
        )
        intraday_confirmation = (
            bool(metrics["break_previous_6_bar_low_with_negative_delta_within_60m"])
            or (
                bool(metrics["close_below_session_vwap"])
                and float(metrics["peak_to_close_atr"]) >= 0.5
            )
        )
        extreme_name = "new_5_session_high"
    elif direction == "bullish":
        checks = {
            "lower_wick_fraction_gte_0_4": float(metrics["lower_wick_fraction"]) >= 0.4,
            "close_location_gte_0_65": float(metrics["close_location"]) >= 0.65,
            "close_above_session_vwap": bool(metrics["close_above_session_vwap"]),
            "break_previous_6_bar_high_with_positive_delta_within_60m": bool(
                metrics["break_previous_6_bar_high_with_positive_delta_within_60m"]
            ),
            "low_to_close_atr_gte_0_5": float(metrics["low_to_close_atr"]) >= 0.5,
            "relative_volume_gte_1_3": float(metrics["relative_volume_20"]) >= 1.3,
            "volume_vs_previous_gte_1_25": float(metrics["volume_vs_previous"]) >= 1.25,
            "daily_flow_pct_lte_minus_2": float(metrics["daily_flow_pct"]) <= -2.0,
            "daily_oi_pct_lte_minus_1": float(metrics["daily_oi_pct"]) <= -1.0,
            "previous_session_down_and_close_location_lte_0_25": bool(metrics["previous_session_down"])
            and float(metrics["previous_session_close_location"]) <= 0.25,
        }
        new_extreme = bool(metrics["new_5_session_low"])
        price_rejection = (
            float(metrics["lower_wick_fraction"]) >= 0.4
            and float(metrics["close_location"]) >= 0.5
        )
        intraday_confirmation = (
            bool(metrics["break_previous_6_bar_high_with_positive_delta_within_60m"])
            or (
                bool(metrics["close_above_session_vwap"])
                and float(metrics["low_to_close_atr"]) >= 0.5
            )
        )
        extreme_name = "new_5_session_low"
    else:
        raise ReversalMonitorError("unsupported direction: " + direction)
    score = sum(int(score_values[name]) for name, passed in checks.items() if passed)
    mandatory = {
        extreme_name: new_extreme,
        "price_rejection": price_rejection,
        "intraday_confirmation": intraday_confirmation,
    }
    alert_threshold = int(direction_rules["alert_score_threshold"])
    watch_threshold = int(direction_rules["watch_score_threshold"])
    if all(mandatory.values()) and score >= alert_threshold:
        status = "ALERT"
    elif new_extreme and price_rejection and score >= watch_threshold:
        status = "WATCH"
    else:
        status = "NO_ALERT"
    return {
        "status": status,
        "score": score,
        "alert_score_threshold": alert_threshold,
        "watch_score_threshold": watch_threshold,
        "direction": direction,
        "mandatory_gates": mandatory,
        "score_checks": checks,
    }


def render_notification(result: Mapping[str, object]) -> str | None:
    if result.get("status") != "ALERT":
        return None
    metrics = result["metrics"]
    if not isinstance(metrics, Mapping):
        raise ReversalMonitorError("result metrics must be an object")
    decisions = result.get("decisions")
    if not isinstance(decisions, Mapping):
        raise ReversalMonitorError("result decisions must be an object")
    messages: list[str] = []
    for direction in ("bearish", "bullish"):
        decision = decisions.get(direction)
        if not isinstance(decision, Mapping) or decision.get("status") != "ALERT":
            continue
        checks = decision.get("score_checks")
        if not isinstance(checks, Mapping):
            raise ReversalMonitorError("decision score_checks must be an object")
        reasons = [name for name, passed in checks.items() if passed]
        if direction == "bearish":
            label = "кандидат на медвежий разворот после максимума"
            extreme = "high=" + format(float(metrics["high"]), ".4f")
            wick = float(metrics["upper_wick_fraction"])
        else:
            label = "кандидат на бычий разворот после минимума"
            extreme = "low=" + format(float(metrics["low"]), ".4f")
            wick = float(metrics["lower_wick_fraction"])
        messages.append(
            "USDRUBF: "
            + label
            + " по завершённой сессии "
            + str(metrics["trade_date"])
            + ". Score="
            + str(decision["score"])
            + "; "
            + extreme
            + "; close="
            + format(float(metrics["close"]), ".4f")
            + "; RVOL="
            + format(float(metrics["relative_volume_20"]), ".2f")
            + "x; wick="
            + format(100 * wick, ".1f")
            + "%; close_location="
            + format(100 * float(metrics["close_location"]), ".1f")
            + "%. Подтверждения: "
            + ", ".join(reasons)
            + ". Исследовательское уведомление, без полномочий на сделку."
        )
    return "\n".join(messages) if messages else None


def _fetch_frames(
    from_date: str,
    target_date: str,
    timeout: float,
    *,
    fetch_fn=None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if fetch_fn is None:
        from moex_data.futures import liquidity_history_metrics_probe as source

        fetch_fn = source.fetch_paged_frame

    tradestats = fetch_fn(
        APIM_BASE_URL,
        TRADESTATS_PATH,
        {"from": from_date, "till": target_date},
        "tradestats",
        timeout,
        True,
    )
    try:
        futoi = fetch_fn(
            APIM_BASE_URL,
            FUTOI_PATH,
            {"from": from_date, "till": target_date, "latest": 0},
            "futoi",
            timeout,
            True,
        )
    except Exception:
        # FUTOI is an optional one-point confirmation. Its transport failure must
        # never be converted to zero or suppress an otherwise valid price signal.
        futoi = pd.DataFrame()
    return tradestats, futoi


def run_monitor(
    *,
    trade_date: str,
    repo_root: str | Path,
    state_root: str | Path,
    env_file: str | None = DEFAULT_ENV_PATH,
    timeout: float = 35.0,
    now_utc: datetime | None = None,
    tradestats_frame: pd.DataFrame | None = None,
    futoi_frame: pd.DataFrame | None = None,
) -> dict[str, object]:
    target_date = _iso_date(trade_date, "trade_date")
    now = _aware_utc(now_utc)
    if now.astimezone(MARKET_TZ).date() <= date.fromisoformat(target_date):
        raise ReversalMonitorError("only a completed prior Moscow session may be evaluated")
    repo = Path(repo_root).resolve(strict=True)
    rules_path = repo / DEFAULT_RULES_PATH
    rules = _read_rules(rules_path)
    root = Path(state_root).expanduser()
    if not root.is_absolute():
        raise ReversalMonitorError("state_root must be absolute")
    _load_env_file(env_file)
    fetched = tradestats_frame is None
    try:
        if fetched:
            start = (date.fromisoformat(target_date) - timedelta(days=int(rules["lookback_calendar_days"]))).isoformat()
            tradestats_frame, futoi_frame = _fetch_frames(start, target_date, timeout)
        if tradestats_frame is None:
            raise ReversalMonitorError("TradeStats source returned no frame")
        normalized = normalize_tradestats(tradestats_frame)
        if target_date not in set(normalized["trade_date"]):
            result: dict[str, object] = {
                "schema_version": SCHEMA_VERSION,
                "project": PROJECT,
                "instrument": "USDRUBF",
                "timeframe": "5m",
                "generated_at_utc": now.isoformat(),
                "trade_date": target_date,
                "status": "NO_SESSION",
                "notify": False,
                "notification": None,
                "execution_authority": False,
            }
        else:
            metrics = session_metrics(
                normalized,
                target_date,
                minimum_rows=int(rules["minimum_intraday_rows"]),
                new_extreme_lookback_sessions=int(rules["new_extreme_lookback_sessions"]),
            )
            target_end = pd.Timestamp(metrics["last_bar_ts"])
            previous_rows = normalized.loc[normalized["trade_date"].eq(metrics["previous_trade_date"])]
            previous_end = pd.Timestamp(previous_rows["ts"].max())
            fiz_change, futoi_quality = futoi_change(
                futoi_frame if futoi_frame is not None else pd.DataFrame(),
                previous_date=str(metrics["previous_trade_date"]),
                target_date=target_date,
                previous_end=previous_end,
                target_end=target_end,
                maximum_age_minutes=int(rules["futoi_policy"]["maximum_endpoint_age_minutes"]),
            )
            metrics["fiz_long_change_pp"] = fiz_change
            decisions = {
                direction: evaluate(metrics, rules, direction)
                for direction in ("bearish", "bullish")
            }
            alert_directions = [
                direction for direction, decision in decisions.items()
                if decision["status"] == "ALERT"
            ]
            watch_directions = [
                direction for direction, decision in decisions.items()
                if decision["status"] == "WATCH"
            ]
            status = "ALERT" if alert_directions else ("WATCH" if watch_directions else "NO_ALERT")
            result = {
                "schema_version": SCHEMA_VERSION,
                "rules_schema_version": rules["schema_version"],
                "rules_ref": DEFAULT_RULES_PATH,
                "project": PROJECT,
                "instrument": "USDRUBF",
                "timeframe": "5m",
                "generated_at_utc": now.isoformat(),
                "trade_date": target_date,
                "status": status,
                "notify": bool(alert_directions),
                "alert_directions": alert_directions,
                "watch_directions": watch_directions,
                "metrics": metrics,
                "futoi_quality": futoi_quality,
                "decisions": decisions,
                "decision": decisions["bearish"],
                "data_quality": {
                    "status": "PASS",
                    "source": "MOEX_ALGOPACK_APIM" if fetched else "SUPPLIED_TEST_FRAME",
                    "target_publication_is_historical_archive": True,
                    "target_session_is_prior_moscow_date": True,
                },
                "execution_authority": False,
            }
            result["notification"] = render_notification(result)
    except Exception as exc:
        result = {
            "schema_version": SCHEMA_VERSION,
            "project": PROJECT,
            "instrument": "USDRUBF",
            "timeframe": "5m",
            "generated_at_utc": now.isoformat(),
            "trade_date": target_date,
            "status": "DATA_BLOCKED",
            "notify": False,
            "notification": None,
            "data_quality": {
                "status": "BLOCKED",
                "error_class": exc.__class__.__name__,
                "error": str(exc)[:500],
            },
            "execution_authority": False,
        }
    history_path = root / "history" / (target_date + ".json")
    current_path = root / "current.json"
    _atomic_json(history_path, result)
    _atomic_json(current_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate a completed USDRUBF session for bearish and bullish reversal-candle alerts."
    )
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--state-root", default=None)
    parser.add_argument("--env-file", default=DEFAULT_ENV_PATH)
    parser.add_argument("--timeout", type=float, default=35.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    state_root = args.state_root
    if not state_root:
        data_root = os.environ.get("MOEX_DATA_ROOT", "").strip()
        if not data_root:
            _load_env_file(args.env_file)
            data_root = os.environ.get("MOEX_DATA_ROOT", "").strip()
        if not data_root:
            raise ReversalMonitorError("MOEX_DATA_ROOT or --state-root is required")
        state_root = str(Path(data_root) / "state" / "usdrubf_reversal_candle")
    result = run_monitor(
        trade_date=args.trade_date,
        repo_root=args.repo_root,
        state_root=state_root,
        env_file=args.env_file,
        timeout=args.timeout,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False, default=str))
    return 0 if result["status"] != "DATA_BLOCKED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
