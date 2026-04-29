#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests

TZ_MSK = ZoneInfo("Europe/Moscow")
DEFAULT_ISS_BASE_URL = "https://iss.moex.com"
DEFAULT_APIM_BASE_URL = "https://apim.moex.com"
SCHEMA_LIQUIDITY_SCREEN = "futures_liquidity_screen.v1"
SCHEMA_HISTORY_DEPTH_SCREEN = "futures_history_depth_screen.v1"

REQUIRED_CONTRACTS = [
    "contracts/datasets/futures_normalized_instrument_registry_contract.md",
    "contracts/datasets/futures_algopack_tradestats_availability_report_contract.md",
    "contracts/datasets/futures_liquidity_screen_contract.md",
    "contracts/datasets/futures_history_depth_screen_contract.md",
]

REQUIRED_CONFIGS = [
    "configs/datasets/futures_slice1_universe_config.json",
    "configs/datasets/futures_liquidity_screen_thresholds_config.json",
    "configs/datasets/futures_history_depth_thresholds_config.json",
]

CONTRACT_BY_ID = {
    "normalized_registry": "contracts/datasets/futures_normalized_instrument_registry_contract.md",
    "tradestats_availability": "contracts/datasets/futures_algopack_tradestats_availability_report_contract.md",
    "liquidity_screen": "contracts/datasets/futures_liquidity_screen_contract.md",
    "history_depth_screen": "contracts/datasets/futures_history_depth_screen_contract.md",
}


def repo_root() -> Path:
    return Path.cwd().resolve()


def today_msk() -> str:
    return datetime.now(TZ_MSK).date().isoformat()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise RuntimeError("JSON root is not object: " + str(path))
    return data


def assert_files_exist(root: Path, rel_paths: Iterable[str]) -> None:
    missing = []
    for rel in rel_paths:
        if not (root / rel).exists():
            missing.append(rel)
    if missing:
        raise FileNotFoundError("Missing required SoT files: " + ", ".join(missing))


def extract_contract_value(text: str, key: str) -> str:
    prefix = key + ":"
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return ""


def load_contract_values(root: Path) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for rel in REQUIRED_CONTRACTS:
        text = read_text(root / rel)
        out[rel] = {
            "path_pattern": extract_contract_value(text, "path_pattern"),
            "schema_version": extract_contract_value(text, "schema_version"),
            "format": extract_contract_value(text, "format"),
        }
    return out


def resolve_data_root(args: argparse.Namespace) -> Path:
    raw = str(args.data_root or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        raise RuntimeError("MOEX_DATA_ROOT is required for external_pattern inputs and outputs")
    return Path(raw).expanduser().resolve()


def resolve_contract_path(data_root: Path, contracts: Dict[str, Dict[str, str]], contract_rel: str, snapshot_date: str) -> Path:
    pattern = str(contracts.get(contract_rel, {}).get("path_pattern") or "").strip()
    if not pattern:
        raise RuntimeError("Contract path_pattern is missing: " + contract_rel)
    prefix = "${MOEX_DATA_ROOT}"
    if not pattern.startswith(prefix):
        raise RuntimeError("Unsupported non-MOEX_DATA_ROOT path_pattern in " + contract_rel)
    rel = pattern[len(prefix):].lstrip("/")
    rel = rel.replace("{snapshot_date}", snapshot_date)
    rel = rel.replace("YYYY-MM-DD", snapshot_date)
    return data_root / rel


def stable_id(parts: Iterable[Any]) -> str:
    raw = "|".join([str(x) for x in parts])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def auth_headers(use_apim: bool) -> Dict[str, str]:
    user_agent = os.getenv("MOEX_UA", "moex_bot_futures_liquidity_history_metrics_probe/1.0").strip()
    headers = {"User-Agent": user_agent}
    token = os.getenv("MOEX_API_KEY", "").strip()
    if use_apim and token:
        headers["Authorization"] = "Bearer " + token
    return headers


def url_join(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def request_json(base_url: str, path: str, params: Dict[str, Any], timeout: float, use_apim: bool) -> Dict[str, Any]:
    url = url_join(base_url, path)
    response = requests.get(url, params=params, headers=auth_headers(use_apim), timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError("MOEX response JSON root is not object")
    return data


def block_to_frame(data: Dict[str, Any], preferred_blocks: Iterable[str]) -> pd.DataFrame:
    for block in preferred_blocks:
        raw = data.get(block)
        if isinstance(raw, dict):
            cols = raw.get("columns") or []
            rows = raw.get("data") or []
            if isinstance(cols, list) and isinstance(rows, list) and cols:
                return pd.DataFrame(rows, columns=cols)
    for raw in data.values():
        if isinstance(raw, dict) and isinstance(raw.get("columns"), list) and isinstance(raw.get("data"), list):
            cols = raw.get("columns") or []
            rows = raw.get("data") or []
            if cols:
                return pd.DataFrame(rows, columns=cols)
    return pd.DataFrame()


def fetch_paged_frame(base_url: str, path: str, params: Dict[str, Any], block: str, timeout: float, use_apim: bool) -> pd.DataFrame:
    frames = []
    start = 0
    while True:
        query = dict(params)
        query["start"] = start
        data = request_json(base_url, path, query, timeout, use_apim)
        frame = block_to_frame(data, [block, "data", "tradestats", "off_days", "securities"])
        if frame.empty:
            break
        frames.append(frame)
        cursor = data.get(block + ".cursor") or data.get("data.cursor") or data.get("tradestats.cursor") or data.get("off_days.cursor") or {}
        if not isinstance(cursor, dict):
            break
        ccols = cursor.get("columns") or []
        crows = cursor.get("data") or []
        if not ccols or not crows:
            break
        cdf = pd.DataFrame(crows, columns=ccols)
        cols = {str(c).upper(): c for c in cdf.columns}
        if "TOTAL" not in cols or "PAGESIZE" not in cols or "INDEX" not in cols:
            break
        total = int(cdf.iloc[0][cols["TOTAL"]])
        page_size = int(cdf.iloc[0][cols["PAGESIZE"]])
        index = int(cdf.iloc[0][cols["INDEX"]])
        next_start = index + page_size
        if next_start >= total:
            break
        if next_start <= start:
            raise RuntimeError("Non-increasing MOEX cursor for " + path)
        start = next_start
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def canonical_column(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    by_upper = {str(c).upper(): c for c in df.columns}
    for name in candidates:
        found = by_upper.get(name.upper())
        if found is not None:
            return found
    return None


def parse_iso_date(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return pd.to_datetime(text).date().isoformat()
    except Exception:
        return text[:10] if len(text) >= 10 else None


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def threshold_profile(config: Dict[str, Any]) -> Dict[str, Any]:
    profiles = config.get("threshold_profiles") or []
    if not isinstance(profiles, list) or not profiles:
        raise RuntimeError("No threshold_profiles in config")
    first = profiles[0]
    if not isinstance(first, dict):
        raise RuntimeError("threshold_profile is not object")
    return first


def date_range_defaults(snapshot_date: str, args: argparse.Namespace) -> Tuple[str, str]:
    till = str(args.till or snapshot_date).strip()
    if args.from_date:
        return str(args.from_date).strip(), till
    parsed = datetime.strptime(till, "%Y-%m-%d").date()
    start = parsed - timedelta(days=int(args.history_lookback_days))
    return start.isoformat(), till


def selected_instruments_from_artifacts(normalized: pd.DataFrame, availability: pd.DataFrame) -> pd.DataFrame:
    if availability.empty:
        raise RuntimeError("Tradestats availability artifact is empty")
    required = ["secid", "board", "family_code", "availability_status"]
    missing = [x for x in required if x not in availability.columns]
    if missing:
        raise RuntimeError("Tradestats availability artifact missing fields: " + ", ".join(missing))
    selected = availability.loc[availability["availability_status"].astype(str) == "available"].copy()
    if selected.empty:
        raise RuntimeError("No available instruments in tradestats availability artifact")
    selected = selected[["board", "secid", "family_code", "availability_status"]].drop_duplicates().copy()
    normalized_fields = [x for x in ["secid", "family_code", "asset_class", "instrument_kind", "expiration_date", "last_trade_date"] if x in normalized.columns]
    if normalized_fields:
        norm = normalized[normalized_fields].drop_duplicates(subset=["secid"], keep="last").copy()
        selected = selected.merge(norm, on=["secid", "family_code"], how="left")
    if "asset_class" not in selected.columns:
        selected["asset_class"] = None
    selected = selected.sort_values(["family_code", "secid"]).reset_index(drop=True)
    return selected


def fetch_tradestats(secid: str, screen_from: str, screen_till: str, timeout: float, apim_base_url: str, iss_base_url: str) -> Tuple[pd.DataFrame, str, str, str]:
    candidates = [
        (apim_base_url, "/iss/datashop/algopack/fo/tradestats/" + secid + ".json", {}, True),
        (iss_base_url, "/iss/datashop/algopack/fo/tradestats/" + secid + ".json", {}, False),
        (apim_base_url, "/iss/datashop/algopack/fo/tradestats.json", {"secid": secid}, True),
        (iss_base_url, "/iss/datashop/algopack/fo/tradestats.json", {"secid": secid}, False),
    ]
    last_error = ""
    last_url = ""
    for base_url, path, extra, use_apim in candidates:
        params = {"from": screen_from, "till": screen_till}
        params.update(extra)
        last_url = url_join(base_url, path)
        try:
            frame = fetch_paged_frame(base_url, path, params, "data", timeout, use_apim)
            if not frame.empty:
                return frame, last_url, "completed", ""
        except Exception as exc:
            last_error = exc.__class__.__name__ + ": " + str(exc)[:500]
    return pd.DataFrame(), last_url, "failed", last_error or "empty_response"


def duplicate_intraday_rows(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    date_col = canonical_column(frame, ["tradedate", "TRADEDATE", "date", "DATE"])
    time_col = canonical_column(frame, ["tradetime", "TRADETIME", "time", "TIME", "moment", "MOMENT"])
    secid_col = canonical_column(frame, ["secid", "SECID", "ticker", "TICKER"])
    keys = [x for x in [date_col, time_col, secid_col] if x]
    if len(keys) < 2:
        return 0
    return int(frame.duplicated(subset=keys).sum())


def aggregate_daily_tradestats(frame: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if frame.empty:
        return pd.DataFrame(), {"metric_columns_found": {}, "normalization_error": "empty_frame"}
    date_col = canonical_column(frame, ["tradedate", "TRADEDATE", "date", "DATE"])
    volume_col = canonical_column(frame, ["vol", "VOL", "volume", "VOLUME", "qty", "QTY"])
    value_col = canonical_column(frame, ["val", "VAL", "value", "VALUE", "turnover", "TURNOVER"])
    trades_col = canonical_column(frame, ["trades", "TRADES", "num_trades", "NUM_TRADES", "numtrades", "NUMTRADES"])
    if not date_col:
        return pd.DataFrame(), {"metric_columns_found": {}, "normalization_error": "missing_trade_date_column"}
    work = pd.DataFrame()
    work["trade_date"] = frame[date_col].map(parse_iso_date)
    if volume_col:
        work["volume"] = coerce_numeric(frame[volume_col]).fillna(0.0)
    else:
        work["volume"] = None
    if value_col:
        work["value"] = coerce_numeric(frame[value_col]).fillna(0.0)
    else:
        work["value"] = None
    if trades_col:
        work["trades"] = coerce_numeric(frame[trades_col]).fillna(0.0)
    else:
        work["trades"] = None
    work = work.loc[work["trade_date"].notna()].copy()
    if work.empty:
        return pd.DataFrame(), {"metric_columns_found": {}, "normalization_error": "no_valid_trade_dates"}
    agg_map: Dict[str, str] = {}
    if volume_col:
        agg_map["volume"] = "sum"
    if value_col:
        agg_map["value"] = "sum"
    if trades_col:
        agg_map["trades"] = "sum"
    if not agg_map:
        daily = work[["trade_date"]].drop_duplicates().copy()
    else:
        daily = work.groupby("trade_date", as_index=False).agg(agg_map)
    meta = {
        "metric_columns_found": {
            "trade_date": str(date_col),
            "volume": str(volume_col) if volume_col else None,
            "value": str(value_col) if value_col else None,
            "trades": str(trades_col) if trades_col else None,
        },
        "normalization_error": "",
    }
    return daily.sort_values("trade_date").reset_index(drop=True), meta


def year_chunks(screen_from: str, screen_till: str) -> List[Tuple[str, str]]:
    start = datetime.strptime(screen_from, "%Y-%m-%d").date()
    end = datetime.strptime(screen_till, "%Y-%m-%d").date()
    chunks = []
    current = start
    while current <= end:
        year_end = date(current.year, 12, 31)
        chunk_end = min(year_end, end)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)
    return chunks


def fetch_futures_calendar(screen_from: str, screen_till: str, timeout: float, iss_base_url: str) -> Tuple[Optional[Set[str]], str]:
    frames = []
    try:
        for chunk_from, chunk_till in year_chunks(screen_from, screen_till):
            params = {"from": chunk_from, "till": chunk_till, "iss.only": "off_days", "show_all_days": "1"}
            frame = fetch_paged_frame(iss_base_url, "/iss/calendars.json", params, "off_days", timeout, False)
            if not frame.empty:
                frames.append(frame)
    except Exception as exc:
        return None, exc.__class__.__name__ + ": " + str(exc)[:300]
    if not frames:
        return None, "calendar_empty_response"
    calendar = pd.concat(frames, ignore_index=True)
    date_col = canonical_column(calendar, ["date", "DATE", "tradedate", "TRADEDATE"])
    status_col = canonical_column(calendar, ["futures_workday", "FUTURES_WORKDAY", "futures", "FUTURES", "futures_", "FUTURES_", "is_traded", "IS_TRADED", "workday", "WORKDAY", "is_workday", "IS_WORKDAY"])
    if not date_col or not status_col:
        return None, "calendar_required_columns_not_found"
    out: Set[str] = set()
    for _, row in calendar.iterrows():
        dt = parse_iso_date(row.get(date_col))
        if not dt:
            continue
        value = row.get(status_col)
        if pd.isna(value):
            continue
        try:
            is_open = int(float(value)) == 1
        except Exception:
            is_open = str(value).strip().lower() in ["1", "true", "t", "yes", "y"]
        if is_open:
            out.add(dt)
    if not out:
        return None, "calendar_no_trading_days_detected"
    return out, ""


def median_or_none(series: pd.Series) -> Optional[float]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.median())


def int_or_none(value: Any) -> Optional[int]:
    if value is None or pd.isna(value):
        return None
    return int(value)


def compute_one_metrics(
    instrument: pd.Series,
    screen_from: str,
    screen_till: str,
    expected_calendar: Optional[Set[str]],
    calendar_note: str,
    liquidity_profile: Dict[str, Any],
    history_profile: Dict[str, Any],
    recent_gap_days: int,
    full_history_proven: bool,
    allow_bounded_pass: bool,
    timeout: float,
    apim_base_url: str,
    iss_base_url: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    secid = str(instrument.get("secid", "")).strip()
    board = str(instrument.get("board", "rfud") or "rfud").strip()
    family = str(instrument.get("family_code", "")).strip()
    asset_class = instrument.get("asset_class", None)
    frame, source_url, fetch_status, fetch_error = fetch_tradestats(secid, screen_from, screen_till, timeout, apim_base_url, iss_base_url)
    duplicate_rows = duplicate_intraday_rows(frame)
    daily, meta = aggregate_daily_tradestats(frame)

    observed_dates = set(daily["trade_date"].astype(str).tolist()) if not daily.empty and "trade_date" in daily.columns else set()
    first_available_date = min(observed_dates) if observed_dates else None
    last_available_date = max(observed_dates) if observed_dates else None
    available_trading_days = len(observed_dates)
    active_days: Optional[int] = None
    zero_volume_days: Optional[int] = None
    median_daily_volume: Optional[float] = None
    median_daily_value: Optional[float] = None
    median_daily_trades: Optional[float] = None

    if not daily.empty and "volume" in daily.columns:
        volume_values = pd.to_numeric(daily["volume"], errors="coerce").fillna(0.0)
        active_days = int((volume_values > 0).sum())
        zero_volume_days = int((volume_values <= 0).sum())
        median_daily_volume = median_or_none(volume_values)
    if not daily.empty and "value" in daily.columns:
        median_daily_value = median_or_none(daily["value"])
    if not daily.empty and "trades" in daily.columns:
        median_daily_trades = median_or_none(daily["trades"])

    expected_trading_days: Optional[int] = None
    coverage_ratio: Optional[float] = None
    missing_days: Optional[int] = None
    recent_gap_count: Optional[int] = None
    missing_day_diagnostics: Optional[str] = None
    calendar_status = "unavailable"

    if expected_calendar is not None and first_available_date:
        expected_since_first = sorted([x for x in expected_calendar if first_available_date <= x <= screen_till])
        expected_set = set(expected_since_first)
        expected_trading_days = len(expected_set)
        missing_set = sorted(expected_set - observed_dates)
        missing_days = len(missing_set)
        if expected_trading_days > 0:
            coverage_ratio = float(available_trading_days) / float(expected_trading_days)
        recent_expected = expected_since_first[-int(recent_gap_days):] if recent_gap_days > 0 else []
        recent_gap_count = len([x for x in recent_expected if x not in observed_dates])
        missing_day_diagnostics = json.dumps({"first_20_missing_days": missing_set[:20], "missing_days_total": len(missing_set)}, ensure_ascii=False, sort_keys=True)
        calendar_status = "computed"
    elif expected_calendar is None:
        missing_day_diagnostics = json.dumps({"calendar_status": "not_computed", "reason": calendar_note}, ensure_ascii=False, sort_keys=True)
    else:
        missing_day_diagnostics = json.dumps({"calendar_status": "not_computed", "reason": "no_first_available_date"}, ensure_ascii=False, sort_keys=True)

    min_active_days = liquidity_profile.get("min_active_days")
    min_available_days = history_profile.get("min_available_trading_days")
    profile_id_liq = str(liquidity_profile.get("threshold_profile_id", "")).strip()
    profile_id_hist = str(history_profile.get("threshold_profile_id", "")).strip()
    metrics_error = str(meta.get("normalization_error") or fetch_error or "").strip()
    metric_columns = meta.get("metric_columns_found") or {}
    volume_col_missing = not metric_columns.get("volume")
    value_col_missing = not metric_columns.get("value")
    trades_col_missing = not metric_columns.get("trades")

    if fetch_status != "completed" or frame.empty or daily.empty:
        liquidity_status = "fail"
        liquidity_review = "TradeStats fetch failed or returned no metric rows: " + (fetch_error or metrics_error or "empty")
    elif duplicate_rows > 0:
        liquidity_status = "fail"
        liquidity_review = "duplicate intraday TradeStats rows detected"
    elif volume_col_missing:
        liquidity_status = "fail"
        liquidity_review = "volume column missing in TradeStats rows"
    elif min_active_days is not None and active_days is not None and active_days < int(min_active_days):
        liquidity_status = "fail"
        liquidity_review = "active_days below threshold"
    elif value_col_missing or trades_col_missing:
        liquidity_status = "review_required"
        liquidity_review = "TradeStats rows lack value or trades column required for enriched review"
    else:
        liquidity_status = "pass"
        liquidity_review = "metrics computed from actual TradeStats rows"

    if fetch_status != "completed" or frame.empty or daily.empty:
        history_status = "fail"
        history_review = "TradeStats fetch failed or returned no metric rows: " + (fetch_error or metrics_error or "empty")
    elif duplicate_rows > 0:
        history_status = "fail"
        history_review = "duplicate intraday TradeStats rows detected"
    elif min_available_days is not None and available_trading_days < int(min_available_days):
        history_status = "fail"
        history_review = "available_trading_days below threshold"
    elif expected_calendar is None:
        history_status = "review_required"
        history_review = "calendar denominator not safely computed; " + calendar_note
    elif coverage_ratio is None or coverage_ratio < 0.95:
        history_status = "review_required"
        history_review = "coverage ratio below review threshold or unavailable"
    elif recent_gap_count is not None and recent_gap_count > 0:
        history_status = "review_required"
        history_review = "recent expected trading-day gaps detected"
    elif not full_history_proven and not allow_bounded_pass:
        history_status = "review_required"
        history_review = "bounded history probe only; full historical depth not proven"
    else:
        history_status = "pass"
        history_review = "history metrics computed from actual TradeStats rows and calendar denominator"

    common = {
        "snapshot_date": None,
        "board": board,
        "secid": secid,
        "family_code": family,
        "screen_from": screen_from,
        "screen_till": screen_till,
        "source_endpoint_url": source_url,
        "trade_stats_rows": int(len(frame)),
        "daily_rows": int(len(daily)),
        "duplicate_intraday_rows": int(duplicate_rows),
        "first_available_date": first_available_date,
        "last_available_date": last_available_date,
        "available_trading_days": int(available_trading_days),
        "expected_trading_days": int_or_none(expected_trading_days),
        "coverage_ratio": coverage_ratio,
        "recent_gap_count": int_or_none(recent_gap_count),
        "missing_day_diagnostics": missing_day_diagnostics,
        "calendar_status": calendar_status,
        "calendar_note": calendar_note or None,
        "full_history_proven": bool(full_history_proven),
        "history_proof_scope": "operator_declared_full_history" if full_history_proven else "bounded_probe",
        "metric_columns_json": json.dumps(metric_columns, ensure_ascii=False, sort_keys=True),
        "fetch_status": fetch_status,
        "fetch_error": fetch_error or None,
    }

    liquidity = dict(common)
    liquidity.update({
        "liquidity_screen_id": stable_id(["liquidity", screen_from, screen_till, board, secid]),
        "asset_class": asset_class,
        "liquidity_status": liquidity_status,
        "schema_version": SCHEMA_LIQUIDITY_SCREEN,
        "median_daily_volume": median_daily_volume,
        "median_daily_value": median_daily_value,
        "median_daily_trades": median_daily_trades,
        "active_days": int_or_none(active_days),
        "zero_volume_days": int_or_none(zero_volume_days),
        "missing_days": int_or_none(missing_days),
        "threshold_profile_id": profile_id_liq,
        "review_notes": liquidity_review,
        "validation_status": "metrics_computed" if liquidity_status != "fail" else "failed",
        "review_status": "ready_for_pm_review" if liquidity_status != "fail" else "blocked",
    })

    history = dict(common)
    history.update({
        "history_depth_screen_id": stable_id(["history", screen_from, screen_till, board, secid]),
        "history_depth_status": history_status,
        "schema_version": SCHEMA_HISTORY_DEPTH_SCREEN,
        "required_min_trading_days": int_or_none(min_available_days),
        "missing_days": int_or_none(missing_days),
        "threshold_profile_id": profile_id_hist,
        "review_notes": history_review,
        "validation_status": "metrics_computed" if history_status != "fail" else "failed",
        "review_status": "ready_for_pm_review" if history_status != "fail" else "blocked",
    })
    return liquidity, history


def validate_primary_key(df: pd.DataFrame, keys: List[str], artifact_name: str) -> None:
    missing = [x for x in keys if x not in df.columns]
    if missing:
        raise RuntimeError(artifact_name + " missing primary key fields: " + ", ".join(missing))
    duplicates = int(df.duplicated(subset=keys).sum())
    if duplicates > 0:
        raise RuntimeError(artifact_name + " duplicate primary key rows: " + str(duplicates))


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
    except Exception as exc:
        raise RuntimeError("Cannot write parquet " + str(path) + ": " + exc.__class__.__name__ + ": " + str(exc)) from exc


def summarize_screen(df: pd.DataFrame, status_col: str) -> Dict[str, Any]:
    counts = df[status_col].astype(str).value_counts(dropna=False).to_dict() if status_col in df.columns else {}
    instruments = []
    cols = [x for x in ["secid", "family_code", status_col, "first_available_date", "last_available_date", "available_trading_days", "active_days", "median_daily_volume", "median_daily_value", "median_daily_trades", "coverage_ratio", "recent_gap_count", "review_notes"] if x in df.columns]
    for _, row in df[cols].iterrows():
        item = {str(k): (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        instruments.append(item)
    return {"rows": int(len(df)), "status_counts": {str(k): int(v) for k, v in counts.items()}, "instruments": instruments}


def print_json_line(key: str, value: Any) -> None:
    print(key + ": " + json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--history-lookback-days", type=int, default=365)
    parser.add_argument("--recent-gap-days", type=int, default=10)
    parser.add_argument("--data-root", default="")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", DEFAULT_ISS_BASE_URL))
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=45.0)
    parser.add_argument("--full-history-proven", action="store_true")
    parser.add_argument("--allow-bounded-pass", action="store_true")
    args = parser.parse_args()

    root = repo_root()
    snapshot_date = str(args.snapshot_date).strip()
    data_root = resolve_data_root(args)
    screen_from, screen_till = date_range_defaults(snapshot_date, args)

    assert_files_exist(root, REQUIRED_CONTRACTS + REQUIRED_CONFIGS)
    contracts = load_contract_values(root)
    liquidity_profile = threshold_profile(read_json(root / "configs/datasets/futures_liquidity_screen_thresholds_config.json"))
    history_profile = threshold_profile(read_json(root / "configs/datasets/futures_history_depth_thresholds_config.json"))

    normalized_path = resolve_contract_path(data_root, contracts, CONTRACT_BY_ID["normalized_registry"], snapshot_date)
    availability_path = resolve_contract_path(data_root, contracts, CONTRACT_BY_ID["tradestats_availability"], snapshot_date)
    liquidity_path = resolve_contract_path(data_root, contracts, CONTRACT_BY_ID["liquidity_screen"], snapshot_date)
    history_path = resolve_contract_path(data_root, contracts, CONTRACT_BY_ID["history_depth_screen"], snapshot_date)

    if not normalized_path.exists():
        raise FileNotFoundError("Missing normalized registry artifact: " + str(normalized_path))
    if not availability_path.exists():
        raise FileNotFoundError("Missing tradestats availability artifact: " + str(availability_path))

    normalized = pd.read_parquet(normalized_path)
    availability = pd.read_parquet(availability_path)
    instruments = selected_instruments_from_artifacts(normalized, availability)

    expected_calendar, calendar_note = fetch_futures_calendar(screen_from, screen_till, float(args.timeout), str(args.iss_base_url))
    liquidity_rows = []
    history_rows = []
    for _, instrument in instruments.iterrows():
        liquidity, history = compute_one_metrics(
            instrument,
            screen_from,
            screen_till,
            expected_calendar,
            calendar_note,
            liquidity_profile,
            history_profile,
            int(args.recent_gap_days),
            bool(args.full_history_proven),
            bool(args.allow_bounded_pass),
            float(args.timeout),
            str(args.apim_base_url),
            str(args.iss_base_url),
        )
        liquidity["snapshot_date"] = snapshot_date
        history["snapshot_date"] = snapshot_date
        liquidity_rows.append(liquidity)
        history_rows.append(history)

    liquidity_df = pd.DataFrame(liquidity_rows)
    history_df = pd.DataFrame(history_rows)
    validate_primary_key(liquidity_df, ["liquidity_screen_id", "snapshot_date", "board", "secid"], "futures_liquidity_screen")
    validate_primary_key(history_df, ["history_depth_screen_id", "snapshot_date", "board", "secid"], "futures_history_depth_screen")
    write_parquet(liquidity_df, liquidity_path)
    write_parquet(history_df, history_path)

    output_paths = {
        "futures_liquidity_screen": str(liquidity_path),
        "futures_history_depth_screen": str(history_path),
    }
    selected = instruments[["board", "secid", "family_code"]].to_dict("records")
    history_window = {
        "screen_from": screen_from,
        "screen_till": screen_till,
        "history_lookback_days": int(args.history_lookback_days),
        "full_history_proven": bool(args.full_history_proven),
        "calendar_status": "computed" if expected_calendar is not None else "unavailable",
        "calendar_note": calendar_note or None,
    }

    print_json_line("output_artifacts_created", output_paths)
    print_json_line("selected_instruments_covered", selected)
    print_json_line("history_window_checked", history_window)
    print_json_line("liquidity_metrics_summary", summarize_screen(liquidity_df, "liquidity_status"))
    print_json_line("history_depth_metrics_summary", summarize_screen(history_df, "history_depth_status"))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
