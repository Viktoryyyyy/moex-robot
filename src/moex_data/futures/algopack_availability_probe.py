#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

TZ_MSK = ZoneInfo("Europe/Moscow")
DEFAULT_ISS_BASE_URL = "https://iss.moex.com"
DEFAULT_APIM_BASE_URL = "https://apim.moex.com"
SCHEMA_REGISTRY_SNAPSHOT = "futures_registry_snapshot.v1"
SCHEMA_NORMALIZED_REGISTRY = "futures_normalized_instrument_registry.v1"
REPORT_SCHEMA_BY_ENDPOINT = {
    "algopack_fo_tradestats": "futures_algopack_tradestats_availability_report.v1",
    "moex_futoi": "futures_futoi_availability_report.v1",
    "algopack_fo_obstats": "futures_obstats_availability_report.v1",
    "algopack_fo_hi2": "futures_hi2_availability_report.v1",
}
REPORT_FILE_BY_ENDPOINT = {
    "algopack_fo_tradestats": "futures_algopack_tradestats_availability_report.parquet",
    "moex_futoi": "futures_futoi_availability_report.parquet",
    "algopack_fo_obstats": "futures_obstats_availability_report.parquet",
    "algopack_fo_hi2": "futures_hi2_availability_report.parquet",
}
CONTRACT_BY_ENDPOINT = {
    "algopack_fo_tradestats": "contracts/datasets/futures_algopack_tradestats_availability_report_contract.md",
    "moex_futoi": "contracts/datasets/futures_futoi_availability_report_contract.md",
    "algopack_fo_obstats": "contracts/datasets/futures_obstats_availability_report_contract.md",
    "algopack_fo_hi2": "contracts/datasets/futures_hi2_availability_report_contract.md",
}
REQUIRED_CONTRACTS = [
    "contracts/datasets/futures_registry_snapshot_contract.md",
    "contracts/datasets/futures_normalized_instrument_registry_contract.md",
    "contracts/datasets/futures_algopack_tradestats_availability_report_contract.md",
    "contracts/datasets/futures_futoi_availability_report_contract.md",
    "contracts/datasets/futures_obstats_availability_report_contract.md",
    "contracts/datasets/futures_hi2_availability_report_contract.md",
    "contracts/datasets/futures_rfud_candidates_evidence_contract.md",
]
REQUIRED_CONFIGS = [
    "configs/datasets/futures_algopack_availability_sources_config.json",
    "configs/datasets/futures_slice1_universe_config.json",
    "configs/datasets/futures_evidence_universe_scope_config.json",
]
MONTH_CODES = set("FGHJKMNQUVXZ")


def repo_root() -> Path:
    return Path.cwd().resolve()


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("JSON root is not object: " + str(path))
    return data


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


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


def load_contracts(root: Path) -> Dict[str, Dict[str, str]]:
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
        raise RuntimeError("MOEX_DATA_ROOT is required for external_pattern outputs")
    return Path(raw).expanduser().resolve()


def today_msk() -> str:
    return datetime.now(TZ_MSK).date().isoformat()


def date_range_defaults(snapshot_date: str, args: argparse.Namespace) -> Tuple[str, str]:
    till = str(args.till or snapshot_date).strip()
    if args.from_date:
        return str(args.from_date).strip(), till
    dt = datetime.strptime(till, "%Y-%m-%d").date() - timedelta(days=int(args.lookback_days))
    return dt.isoformat(), till


def auth_headers(use_apim: bool) -> Dict[str, str]:
    ua = os.getenv("MOEX_UA", "moex_bot_futures_availability_probe/1.0").strip()
    headers = {"User-Agent": ua}
    token = os.getenv("MOEX_API_KEY", "").strip()
    if use_apim and token:
        headers["Authorization"] = "Bearer " + token
    return headers


def url_join(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def request_json(base_url: str, path: str, params: Dict[str, Any], timeout: float, use_apim: bool) -> Dict[str, Any]:
    url = url_join(base_url, path)
    resp = requests.get(url, params=params, headers=auth_headers(use_apim), timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
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
        frame = block_to_frame(data, [block, "data", "securities"])
        if frame.empty:
            break
        frames.append(frame)
        cursor = data.get(block + ".cursor") or data.get("data.cursor") or data.get("securities.cursor") or {}
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
        c = by_upper.get(name.upper())
        if c is not None:
            return c
    return None


def string_value(row: pd.Series, column: Optional[str]) -> str:
    if not column or column not in row.index:
        return ""
    value = row[column]
    if pd.isna(value):
        return ""
    return str(value).strip()


def numeric_value(row: pd.Series, column: Optional[str]) -> Any:
    if not column or column not in row.index:
        return None
    value = row[column]
    if pd.isna(value):
        return None
    return value


def normalize_date(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return ""
    return value[:10]


def family_from_secid(secid: str) -> str:
    secid = str(secid or "").strip()
    if not secid:
        return "UNKNOWN"
    if secid.upper() == "USDRUBF":
        return "USDRUBF"
    match = re.match(r"^([A-Za-z]+)([FGHJKMNQUVXZ][0-9]{1,2})$", secid)
    if match:
        return match.group(1)
    match = re.match(r"^([A-Za-z]+)", secid)
    if match:
        return match.group(1)
    return secid


def contract_code_from_secid(secid: str, family: str) -> str:
    if not secid:
        return ""
    if family and secid.startswith(family):
        return secid[len(family):]
    return ""


def instrument_kind(secid: str, expiration_date: str, last_trade_date: str) -> str:
    if not secid:
        return "unknown"
    if secid.upper() == "USDRUBF":
        return "perpetual_future_candidate"
    if expiration_date or last_trade_date:
        return "expiring_future"
    return "unknown"


def raw_json_from_row(row: pd.Series) -> str:
    payload: Dict[str, Any] = {}
    for key, value in row.items():
        if pd.isna(value):
            payload[str(key)] = None
        else:
            payload[str(key)] = value
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def stable_id(parts: Iterable[Any]) -> str:
    raw = "|".join([str(x) for x in parts])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def build_registry_snapshot(df: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    secid_col = canonical_column(df, ["SECID", "secid"])
    short_col = canonical_column(df, ["SHORTNAME", "shortname"])
    secname_col = canonical_column(df, ["SECNAME", "secname"])
    board_col = canonical_column(df, ["BOARDID", "BOARD", "board"])
    last_trade_col = canonical_column(df, ["LASTTRADEDATE", "LASTTRADE", "LASTDELDATE"])
    exp_col = canonical_column(df, ["EXPIRATIONDATE", "MATDATE", "LASTTRADEDATE", "LASTDELDATE"])
    asset_col = canonical_column(df, ["ASSETCODE", "ASSET_CODE", "ASSETID"])
    lot_col = canonical_column(df, ["LOTSIZE", "LOTVOLUME", "LOTVALUE"])
    step_col = canonical_column(df, ["MINSTEP", "STEPPRICE", "SEC_PRICE_STEP"])
    step_value_col = canonical_column(df, ["STEPPRICE", "MINSTEPPRICE"])
    currency_col = canonical_column(df, ["CURRENCYID", "CURRENCY", "FACEUNIT"])
    if not secid_col:
        raise RuntimeError("Registry response has no SECID column")
    rows = []
    snapshot_id = "futures_registry_snapshot_" + snapshot_date
    for _, row in df.iterrows():
        secid = string_value(row, secid_col)
        if not secid:
            continue
        board = string_value(row, board_col) or "rfud"
        rows.append({
            "snapshot_id": snapshot_id,
            "snapshot_date": snapshot_date,
            "source_system": "MOEX_ISS",
            "source_endpoint_id": "iss_futures_forts_rfud_securities",
            "engine": "futures",
            "market": "forts",
            "board": board,
            "secid": secid,
            "shortname": string_value(row, short_col),
            "secname": string_value(row, secname_col),
            "raw_payload_json": raw_json_from_row(row),
            "last_trade_date": normalize_date(string_value(row, last_trade_col)),
            "expiration_date": normalize_date(string_value(row, exp_col)),
            "asset_code": string_value(row, asset_col),
            "lot_size": numeric_value(row, lot_col),
            "price_step": numeric_value(row, step_col),
            "price_step_value": numeric_value(row, step_value_col),
            "currency": string_value(row, currency_col),
            "registry_status": "observed",
            "ingest_status": "completed",
            "validation_status": "not_validated",
        })
    return pd.DataFrame(rows)


def build_normalized_registry(registry: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in registry.iterrows():
        secid = str(row.get("secid", "")).strip()
        family = family_from_secid(secid)
        contract_code = contract_code_from_secid(secid, family)
        expiration_date = str(row.get("expiration_date", "") or "").strip()
        last_trade_date = str(row.get("last_trade_date", "") or "").strip()
        kind = instrument_kind(secid, expiration_date, last_trade_date)
        rows.append({
            "snapshot_id": row.get("snapshot_id", ""),
            "snapshot_date": row.get("snapshot_date", ""),
            "secid": secid,
            "board": row.get("board", ""),
            "engine": row.get("engine", "futures"),
            "market": row.get("market", "forts"),
            "shortname": row.get("shortname", ""),
            "secname": row.get("secname", ""),
            "family_code": family,
            "contract_code": contract_code,
            "instrument_kind": kind,
            "is_perpetual_candidate": bool(kind == "perpetual_future_candidate"),
            "source_snapshot_id": row.get("snapshot_id", ""),
            "schema_version": SCHEMA_NORMALIZED_REGISTRY,
            "expiration_date": expiration_date or None,
            "last_trade_date": last_trade_date or None,
            "asset_code": row.get("asset_code", None),
            "asset_class": "currency" if family in ["Si", "USDRUBF"] else None,
            "underlying": None,
            "lot_size": row.get("lot_size", None),
            "price_step": row.get("price_step", None),
            "price_step_value": row.get("price_step_value", None),
            "currency": row.get("currency", None),
            "notes": None,
            "normalization_status": "completed",
            "mapping_status": "draft",
            "validation_status": "not_validated",
        })
    return pd.DataFrame(rows)


def included_family_config(universe_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    families = universe_config.get("included_families") or []
    if not isinstance(families, list):
        raise RuntimeError("included_families is not a list")
    out = []
    for item in families:
        if isinstance(item, dict) and item.get("family_code"):
            out.append(item)
    if not out:
        raise RuntimeError("No included families in Slice 1 universe config")
    return out


def select_probe_instruments(normalized: pd.DataFrame, universe_config: Dict[str, Any], universe_scope: str = "slice1") -> pd.DataFrame:
    if universe_scope == "rfud_candidates":
        required = ["board", "secid", "family_code"]
        missing = [x for x in required if x not in normalized.columns]
        if missing:
            raise RuntimeError("Normalized registry missing required columns for rfud_candidates scope: " + ", ".join(missing))
        selected = normalized.loc[normalized["board"].astype(str).str.strip().str.lower() == "rfud"].copy()
        if selected.empty:
            raise RuntimeError("rfud_candidates scope selected zero instruments from normalized registry")
        selected["selection_status"] = "selected_from_rfud_candidates"
        return selected.drop_duplicates(["board", "secid"]).sort_values(["family_code", "secid", "board"]).reset_index(drop=True)
    if universe_scope != "slice1":
        raise ValueError("Unsupported universe_scope: " + str(universe_scope))
    selected = []
    seen = set()
    for item in included_family_config(universe_config):
        family = str(item.get("family_code", "")).strip()
        preferred = str(item.get("preferred_secid", "")).strip()
        subset = normalized.loc[normalized["family_code"].astype(str) == family].copy()
        if preferred:
            exact = subset.loc[subset["secid"].astype(str).str.upper() == preferred.upper()].copy()
            if not exact.empty:
                subset = exact
        if subset.empty:
            selected.append({
                "snapshot_id": "",
                "snapshot_date": "",
                "secid": preferred or family,
                "board": "rfud",
                "family_code": family,
                "instrument_kind": "unknown",
                "selection_status": "missing_in_registry",
            })
            continue
        for _, row in subset.iterrows():
            key = (str(row.get("board", "rfud")), str(row.get("secid", "")))
            if key in seen:
                continue
            seen.add(key)
            item_row = row.to_dict()
            item_row["selection_status"] = "selected_from_config"
            selected.append(item_row)
    return pd.DataFrame(selected)


def rows_stats(frame: pd.DataFrame) -> Dict[str, Any]:
    if frame.empty:
        return {"rows": 0, "min_ts": None, "max_ts": None}
    ts_col = canonical_column(frame, ["ts", "tradedate", "TRADEDATE", "moment", "MOMENT", "SYSTIME", "systime", "date", "DATE"])
    if not ts_col:
        return {"rows": int(len(frame)), "min_ts": None, "max_ts": None}
    values = frame[ts_col].dropna().astype(str)
    if values.empty:
        return {"rows": int(len(frame)), "min_ts": None, "max_ts": None}
    return {"rows": int(len(frame)), "min_ts": values.min(), "max_ts": values.max()}


def probe_one_path(base_url: str, path: str, params: Dict[str, Any], timeout: float, use_apim: bool) -> Tuple[str, pd.DataFrame, str, str, str]:
    url = url_join(base_url, path)
    try:
        data = request_json(base_url, path, params, timeout, use_apim)
        frame = block_to_frame(data, ["data", "securities", "tradestats", "obstats", "hi2"])
        status = "available" if not frame.empty else "unavailable"
        return status, frame, url, "", ""
    except Exception as exc:
        return "error", pd.DataFrame(), url, exc.__class__.__name__, str(exc)[:500]


def endpoint_probe_candidates(endpoint_id: str, secid: str, family: str, config_path: str) -> List[Tuple[str, Dict[str, Any], bool]]:
    if endpoint_id == "algopack_fo_tradestats":
        return [
            ("/iss/datashop/algopack/fo/tradestats/" + secid + ".json", {}, True),
            (config_path, {"secid": secid}, True),
        ]
    if endpoint_id == "algopack_fo_obstats":
        return [
            ("/iss/datashop/algopack/fo/obstats/" + secid + ".json", {}, True),
            (config_path, {"secid": secid}, True),
        ]
    if endpoint_id == "algopack_fo_hi2":
        return [
            ("/iss/datashop/algopack/fo/hi2/" + secid + ".json", {}, True),
            (config_path, {"secid": secid}, True),
        ]
    if endpoint_id == "moex_futoi":
        ticker = family or secid
        return [
            ("/iss/analyticalproducts/futoi/securities/" + ticker.lower() + ".json", {}, False),
            ("/iss/analyticalproducts/futoi/securities/" + secid.lower() + ".json", {}, False),
            (config_path, {"ticker": ticker}, False),
            (config_path, {"secid": secid}, False),
        ]
    return [(config_path, {"secid": secid}, True)]


def probe_endpoint_for_instrument(
    endpoint_id: str,
    config_path: str,
    secid: str,
    family: str,
    probe_from: str,
    probe_till: str,
    timeout: float,
    apim_base_url: str,
    iss_base_url: str,
) -> Dict[str, Any]:
    base_params = {"from": probe_from, "till": probe_till}
    best_status = "error"
    best_frame = pd.DataFrame()
    best_url = ""
    error_code = ""
    error_message = ""
    candidates = endpoint_probe_candidates(endpoint_id, secid, family, config_path)
    for path, extra_params, prefer_apim in candidates:
        params = dict(base_params)
        params.update(extra_params)
        base_url = apim_base_url if prefer_apim else iss_base_url
        status, frame, url, code, msg = probe_one_path(base_url, path, params, timeout, prefer_apim)
        best_url = url
        if status == "available":
            best_status = status
            best_frame = frame
            error_code = ""
            error_message = ""
            break
        if status == "unavailable" and best_status != "available":
            best_status = "unavailable"
            best_frame = frame
            error_code = ""
            error_message = ""
        elif status == "error" and best_status == "error":
            error_code = code
            error_message = msg
    stats = rows_stats(best_frame)
    availability_status = best_status
    if best_status == "available" and stats["rows"] == 0:
        availability_status = "unavailable"
    return {
        "source_endpoint_url": best_url,
        "availability_status": availability_status,
        "observed_rows": stats["rows"],
        "observed_min_ts": stats["min_ts"],
        "observed_max_ts": stats["max_ts"],
        "error_code": error_code or None,
        "error_message": error_message or None,
    }


def build_availability_report(
    endpoint_id: str,
    config_path: str,
    instruments: pd.DataFrame,
    snapshot_date: str,
    probe_from: str,
    probe_till: str,
    timeout: float,
    apim_base_url: str,
    iss_base_url: str,
) -> pd.DataFrame:
    rows = []
    for _, row in instruments.iterrows():
        secid = str(row.get("secid", "")).strip()
        family = str(row.get("family_code", "")).strip()
        board = str(row.get("board", "rfud") or "rfud").strip()
        result = probe_endpoint_for_instrument(endpoint_id, config_path, secid, family, probe_from, probe_till, timeout, apim_base_url, iss_base_url)
        rows.append({
            "availability_report_id": stable_id([endpoint_id, snapshot_date, board, secid, probe_from, probe_till]),
            "snapshot_date": snapshot_date,
            "board": board,
            "secid": secid,
            "family_code": family,
            "endpoint_id": endpoint_id,
            "source_endpoint_url": result["source_endpoint_url"],
            "probe_from": probe_from,
            "probe_till": probe_till,
            "availability_status": result["availability_status"],
            "schema_version": REPORT_SCHEMA_BY_ENDPOINT.get(endpoint_id, "unknown"),
            "first_available_date": None,
            "last_available_date": None,
            "observed_rows": result["observed_rows"],
            "observed_min_ts": result["observed_min_ts"],
            "observed_max_ts": result["observed_max_ts"],
            "error_code": result["error_code"],
            "error_message": result["error_message"],
            "review_notes": None,
            "probe_status": "completed",
            "validation_status": "not_validated",
        })
    return pd.DataFrame(rows)


def contract_output_path(data_root: Path, relative_contract: str, snapshot_date: str, universe_scope: str = "slice1") -> Path:
    if universe_scope == "rfud_candidates":
        if relative_contract == "contracts/datasets/futures_registry_snapshot_contract.md":
            return data_root / "futures" / "registry" / "universe_scope=rfud_candidates" / ("snapshot_date=" + snapshot_date) / "futures_registry_snapshot.parquet"
        if relative_contract == "contracts/datasets/futures_normalized_instrument_registry_contract.md":
            return data_root / "futures" / "registry" / "universe_scope=rfud_candidates" / ("snapshot_date=" + snapshot_date) / "futures_normalized_instrument_registry.parquet"
        for endpoint_id, rel in CONTRACT_BY_ENDPOINT.items():
            if rel == relative_contract:
                return data_root / "futures" / "availability" / "universe_scope=rfud_candidates" / ("snapshot_date=" + snapshot_date) / REPORT_FILE_BY_ENDPOINT[endpoint_id]
        raise RuntimeError("Unknown contract output path for rfud_candidates: " + relative_contract)
    if universe_scope != "slice1":
        raise ValueError("Unsupported universe_scope: " + str(universe_scope))
    if relative_contract == "contracts/datasets/futures_registry_snapshot_contract.md":
        return data_root / "futures" / "registry" / ("snapshot_date=" + snapshot_date) / "futures_registry_snapshot.parquet"
    if relative_contract == "contracts/datasets/futures_normalized_instrument_registry_contract.md":
        return data_root / "futures" / "registry" / ("snapshot_date=" + snapshot_date) / "futures_normalized_instrument_registry.parquet"
    for endpoint_id, rel in CONTRACT_BY_ENDPOINT.items():
        if rel == relative_contract:
            return data_root / "futures" / "availability" / ("snapshot_date=" + snapshot_date) / REPORT_FILE_BY_ENDPOINT[endpoint_id]
    raise RuntimeError("Unknown contract output path: " + relative_contract)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
    except Exception as exc:
        raise RuntimeError("Cannot write parquet " + str(path) + ": " + exc.__class__.__name__ + ": " + str(exc)) from exc


def summarize_status(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty or "availability_status" not in df.columns:
        return {"rows": int(len(df))}
    counts = df["availability_status"].astype(str).value_counts(dropna=False).to_dict()
    by_family: Dict[str, Dict[str, int]] = {}
    if "family_code" in df.columns:
        for family, sub in df.groupby("family_code"):
            by_family[str(family)] = {str(k): int(v) for k, v in sub["availability_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    return {"rows": int(len(df)), "status_counts": {str(k): int(v) for k, v in counts.items()}, "by_family": by_family}


def print_json_line(key: str, value: Any) -> None:
    print(key + ": " + json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--data-root", default="")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", DEFAULT_ISS_BASE_URL))
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=35.0)
    parser.add_argument("--universe-scope", choices=["slice1", "rfud_candidates"], default="slice1")
    args = parser.parse_args()

    root = repo_root()
    snapshot_date = str(args.snapshot_date).strip()
    data_root = resolve_data_root(args)
    probe_from, probe_till = date_range_defaults(snapshot_date, args)

    assert_files_exist(root, REQUIRED_CONTRACTS + REQUIRED_CONFIGS)
    load_contracts(root)
    sources_config = read_json(root / "configs/datasets/futures_algopack_availability_sources_config.json")
    universe_config = read_json(root / "configs/datasets/futures_slice1_universe_config.json")

    registry_raw = fetch_paged_frame(
        args.iss_base_url,
        "/iss/engines/futures/markets/forts/boards/rfud/securities.json",
        {},
        "securities",
        args.timeout,
        False,
    )
    if registry_raw.empty:
        raise RuntimeError("MOEX futures registry returned zero rows")

    registry = build_registry_snapshot(registry_raw, snapshot_date)
    if registry.empty:
        raise RuntimeError("Registry snapshot normalization produced zero rows")
    registry["raw_payload_json"] = registry["raw_payload_json"].astype(str)

    normalized = build_normalized_registry(registry)
    instruments = select_probe_instruments(normalized, universe_config, str(args.universe_scope))
    rfud_registry_count = int((normalized["board"].astype(str).str.strip().str.lower() == "rfud").sum())
    if str(args.universe_scope) == "rfud_candidates" and rfud_registry_count > 7 and len(instruments) <= 7:
        raise RuntimeError("rfud_candidates scope selected <=7 instruments while registry has more than 7 RFUD instruments")

    registry_path = contract_output_path(data_root, "contracts/datasets/futures_registry_snapshot_contract.md", snapshot_date, str(args.universe_scope))
    normalized_path = contract_output_path(data_root, "contracts/datasets/futures_normalized_instrument_registry_contract.md", snapshot_date, str(args.universe_scope))
    write_parquet(registry, registry_path)
    write_parquet(normalized, normalized_path)

    source_items = sources_config.get("sources") or []
    if not isinstance(source_items, list):
        raise RuntimeError("sources config is not a list")

    report_paths: Dict[str, str] = {}
    report_summaries: Dict[str, Any] = {}
    for source in source_items:
        if not isinstance(source, dict):
            continue
        endpoint_id = str(source.get("endpoint_id", "")).strip()
        if endpoint_id not in REPORT_SCHEMA_BY_ENDPOINT:
            continue
        contract_rel = str(source.get("dataset_contract") or CONTRACT_BY_ENDPOINT.get(endpoint_id) or "").strip()
        endpoint_path = str(source.get("endpoint_path", "")).strip()
        if not contract_rel or not endpoint_path:
            raise RuntimeError("Unresolved endpoint contract/path for " + endpoint_id)
        report = build_availability_report(
            endpoint_id,
            endpoint_path,
            instruments,
            snapshot_date,
            probe_from,
            probe_till,
            float(args.timeout),
            str(args.apim_base_url),
            str(args.iss_base_url),
        )
        out_path = contract_output_path(data_root, contract_rel, snapshot_date, str(args.universe_scope))
        write_parquet(report, out_path)
        report_paths[endpoint_id] = str(out_path)
        report_summaries[endpoint_id] = summarize_status(report)

    output_paths = {
        "registry_snapshot": str(registry_path),
        "normalized_registry": str(normalized_path),
    }
    output_paths.update(report_paths)

    registry_summary = {
        "rows": int(len(registry)),
        "unique_secid": int(registry["secid"].nunique()),
        "boards": sorted([str(x) for x in registry["board"].dropna().unique().tolist()]),
        "snapshot_date": snapshot_date,
        "universe_scope": str(args.universe_scope),
    }
    normalized_summary = {
        "rows": int(len(normalized)),
        "unique_family_code": int(normalized["family_code"].nunique()),
        "selected_probe_instruments": instruments[["family_code", "secid", "board", "selection_status"]].to_dict("records"),
        "universe_scope": str(args.universe_scope),
        "rfud_registry_count": rfud_registry_count,
    }

    print_json_line("output_artifacts_created", output_paths)
    print_json_line("registry_snapshot_summary", registry_summary)
    print_json_line("normalized_registry_summary", normalized_summary)
    for endpoint_id in ["algopack_fo_tradestats", "moex_futoi", "algopack_fo_obstats", "algopack_fo_hi2"]:
        print_json_line(endpoint_id + "_availability_summary", report_summaries.get(endpoint_id, {"rows": 0, "status_counts": {}}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
