#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures.slice1_common import DEFAULT_EXCLUDED
from moex_data.futures.slice1_common import parse_list
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

SCHEMA_CONTINUOUS_5M = "futures_continuous_5m.v1"
SCHEMA_CONTINUOUS_D1 = "futures_continuous_d1.v1"

CONTRACT_CONTINUOUS_5M = "contracts/datasets/futures_continuous_5m_contract.md"
CONTRACT_CONTINUOUS_D1 = "contracts/datasets/futures_continuous_d1_contract.md"
REQUIRED_CONTRACTS = [
    CONTRACT_CONTINUOUS_5M,
    CONTRACT_CONTINUOUS_D1,
]

ROLL_POLICY_ID = "expiration_minus_1_trading_session_v1"
ADJUSTMENT_POLICY_ID = "unadjusted_v1"
ADJUSTMENT_FACTOR = 1.0


def repo_root() -> Path:
    return Path.cwd().resolve()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def contract_value(root: Path, rel_path: str, key: str) -> str:
    prefix = key + ":"
    for raw in read_text(root / rel_path).splitlines():
        line = raw.strip()
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return ""


def contract_path_pattern(root: Path, rel_path: str) -> str:
    pattern = contract_value(root, rel_path, "path_pattern")
    if not pattern:
        raise RuntimeError("Contract path_pattern is missing: " + rel_path)
    prefix = "${MOEX_DATA_ROOT}"
    if not pattern.startswith(prefix):
        raise RuntimeError("Unsupported non-MOEX_DATA_ROOT path_pattern: " + rel_path)
    return pattern


def contract_tail(root: Path, rel_path: str) -> str:
    pattern = contract_path_pattern(root, rel_path)
    return pattern[len("${MOEX_DATA_ROOT}"):].lstrip("/")


def resolve_contract_path(root: Path, data_root: Path, rel_path: str, replacements: Dict[str, str]) -> Path:
    tail = contract_tail(root, rel_path)
    for key, value in replacements.items():
        tail = tail.replace("{" + key + "}", str(value))
    tail = tail.replace("YYYY-MM-DD", str(replacements.get("trade_date", "")))
    unresolved = [part for part in tail.split("/") if "{" in part or "}" in part]
    if unresolved:
        raise RuntimeError("Unresolved path pattern tokens in " + rel_path + ": " + ",".join(unresolved))
    return data_root / tail


def continuous_5m_glob_tail(root: Path, roll_policy_id: str, adjustment_policy_id: str) -> str:
    tail = contract_tail(root, CONTRACT_CONTINUOUS_5M)
    tail = tail.replace("{roll_policy_id}", roll_policy_id)
    tail = tail.replace("{adjustment_policy_id}", adjustment_policy_id)
    tail = tail.replace("{family_code}", "*")
    tail = tail.replace("{trade_date}", "*")
    tail = tail.replace("YYYY-MM-DD", "*")
    unresolved = [part for part in tail.split("/") if "{" in part or "}" in part]
    if unresolved:
        raise RuntimeError("Unresolved continuous 5m glob tokens: " + ",".join(unresolved))
    return tail


def partition_value(path: Path, key: str) -> str:
    prefix = key + "="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix):]
    return ""


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none", "null"}:
        return None
    return text


def clean_date(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    try:
        return pd.to_datetime(text, errors="raise").date().isoformat()
    except Exception:
        return text[:10] if len(text) >= 10 else None


def bool_value(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def ordered_distinct(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        text = clean_text(value)
        if not text:
            continue
        if text not in seen:
            seen.add(text)
            out.append(text)
    return out


def discover_continuous_5m_paths(root: Path, data_root: Path, roll_policy_id: str, adjustment_policy_id: str, from_date: str, till: str) -> List[Path]:
    glob_tail = continuous_5m_glob_tail(root, roll_policy_id, adjustment_policy_id)
    paths: List[Path] = []
    for path in sorted(data_root.glob(glob_tail)):
        trade_date = partition_value(path, "trade_date")
        if from_date and trade_date < from_date:
            continue
        if till and trade_date > till:
            continue
        paths.append(path)
    if not paths:
        raise FileNotFoundError("No continuous 5m partitions found for roll_policy_id=" + roll_policy_id + " adjustment_policy_id=" + adjustment_policy_id)
    return paths


def read_partitions(paths: List[Path]) -> pd.DataFrame:
    frames = []
    for path in paths:
        part = pd.read_parquet(path)
        part["_source_partition_path"] = str(path)
        frames.append(part)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def validate_continuous_5m(frame: pd.DataFrame, excluded: List[str], roll_policy_id: str, adjustment_policy_id: str) -> List[str]:
    blockers: List[str] = []
    required = [
        "trade_date",
        "end",
        "session_date",
        "continuous_symbol",
        "family_code",
        "source_secid",
        "source_contract",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "roll_policy_id",
        "adjustment_policy_id",
        "adjustment_factor",
        "is_roll_boundary",
        "roll_map_id",
        "schema_version",
        "ingest_ts",
    ]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        return ["continuous_5m_missing_required_fields:" + ",".join(missing)]
    if frame.empty:
        return ["continuous_5m_empty"]
    required_nulls = [x for x in required if int(frame[x].isna().sum()) > 0]
    if required_nulls:
        blockers.append("continuous_5m_null_required_fields:" + ",".join(required_nulls))
    bad_schema = frame.loc[frame["schema_version"].astype(str) != SCHEMA_CONTINUOUS_5M]
    if not bad_schema.empty:
        blockers.append("invalid_continuous_5m_schema_rows:" + str(len(bad_schema)))
    bad_roll_policy = frame.loc[frame["roll_policy_id"].astype(str) != roll_policy_id]
    if not bad_roll_policy.empty:
        blockers.append("invalid_roll_policy_rows:" + str(len(bad_roll_policy)))
    bad_adjustment_policy = frame.loc[frame["adjustment_policy_id"].astype(str) != adjustment_policy_id]
    if not bad_adjustment_policy.empty:
        blockers.append("invalid_adjustment_policy_rows:" + str(len(bad_adjustment_policy)))
    bad_adjustment_factor = frame.loc[pd.to_numeric(frame["adjustment_factor"], errors="coerce") != ADJUSTMENT_FACTOR]
    if not bad_adjustment_factor.empty:
        blockers.append("invalid_adjustment_factor_rows:" + str(len(bad_adjustment_factor)))
    null_ohlc = int(frame[["open", "high", "low", "close"]].isna().any(axis=1).sum())
    if null_ohlc > 0:
        blockers.append("continuous_5m_null_ohlc_rows:" + str(null_ohlc))
    invalid = (frame["high"] < frame["low"]) | (frame["open"] > frame["high"]) | (frame["open"] < frame["low"]) | (frame["close"] > frame["high"]) | (frame["close"] < frame["low"])
    invalid_count = int(invalid.fillna(True).sum())
    if invalid_count > 0:
        blockers.append("continuous_5m_invalid_ohlc_rows:" + str(invalid_count))
    duplicate_pk = int(frame.duplicated(subset=["continuous_symbol", "trade_date", "end"]).sum())
    if duplicate_pk > 0:
        blockers.append("continuous_5m_duplicate_primary_key_rows:" + str(duplicate_pk))
    excluded_upper = {x.upper() for x in excluded}
    source_hits = sorted({str(x).upper() for x in frame["source_secid"].dropna().astype(str).tolist()}.intersection(excluded_upper))
    contract_hits = sorted({str(x).upper() for x in frame["source_contract"].dropna().astype(str).tolist()}.intersection(excluded_upper))
    if source_hits:
        blockers.append("excluded_source_secid_present:" + ",".join(source_hits))
    if contract_hits:
        blockers.append("excluded_source_contract_present:" + ",".join(contract_hits))
    usdrubf = frame.loc[frame["continuous_symbol"].astype(str).str.upper() == "USDRUBF"].copy()
    if not usdrubf.empty:
        if sorted(usdrubf["source_secid"].astype(str).str.upper().unique().tolist()) != ["USDRUBF"]:
            blockers.append("usdrubf_invalid_source_secid")
        if sorted(usdrubf["source_contract"].astype(str).str.upper().unique().tolist()) != ["USDRUBF"]:
            blockers.append("usdrubf_invalid_source_contract")
        if int(usdrubf["is_roll_boundary"].map(bool_value).sum()) != 0:
            blockers.append("usdrubf_roll_boundary_true_rows")
    return blockers


def normalize_continuous_5m(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["trade_date"] = out["trade_date"].astype(str)
    out["session_date"] = out["session_date"].map(clean_date)
    out["continuous_symbol"] = out["continuous_symbol"].astype(str)
    out["family_code"] = out["family_code"].astype(str)
    out["source_secid"] = out["source_secid"].astype(str)
    out["source_contract"] = out["source_contract"].astype(str)
    out["roll_map_id"] = out["roll_map_id"].astype(str)
    out["roll_policy_id"] = out["roll_policy_id"].astype(str)
    out["adjustment_policy_id"] = out["adjustment_policy_id"].astype(str)
    out["end"] = pd.to_datetime(out["end"], errors="coerce")
    out["is_roll_boundary"] = out["is_roll_boundary"].map(bool_value)
    for col in ["open", "high", "low", "close", "volume", "adjustment_factor"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.loc[out["session_date"].notna() & out["end"].notna()].copy()
    return out.sort_values(["continuous_symbol", "trade_date", "end", "source_contract"]).reset_index(drop=True)


def aggregate_d1(continuous_5m: pd.DataFrame, ingest_ts: str) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    session_conflicts: List[str] = []
    policy_conflicts: List[str] = []
    work = continuous_5m.sort_values(["continuous_symbol", "trade_date", "end", "source_contract"]).copy()
    for (continuous_symbol, family_code, trade_date), part in work.groupby(["continuous_symbol", "family_code", "trade_date"], sort=True):
        session_dates = ordered_distinct(part["session_date"])
        roll_policy_ids = ordered_distinct(part["roll_policy_id"])
        adjustment_policy_ids = ordered_distinct(part["adjustment_policy_id"])
        adjustment_factors = sorted(set([float(x) for x in pd.to_numeric(part["adjustment_factor"], errors="coerce").dropna().tolist()]))
        if len(session_dates) != 1:
            session_conflicts.append(str(continuous_symbol) + ":" + str(trade_date))
            continue
        if roll_policy_ids != [ROLL_POLICY_ID]:
            policy_conflicts.append("roll_policy_id:" + str(continuous_symbol) + ":" + str(trade_date))
            continue
        if adjustment_policy_ids != [ADJUSTMENT_POLICY_ID]:
            policy_conflicts.append("adjustment_policy_id:" + str(continuous_symbol) + ":" + str(trade_date))
            continue
        if adjustment_factors != [ADJUSTMENT_FACTOR]:
            policy_conflicts.append("adjustment_factor:" + str(continuous_symbol) + ":" + str(trade_date))
            continue
        source_contracts = ordered_distinct(part["source_contract"])
        roll_map_ids = ordered_distinct(part["roll_map_id"])
        rows.append({
            "trade_date": str(trade_date),
            "session_date": session_dates[0],
            "continuous_symbol": str(continuous_symbol),
            "family_code": str(family_code),
            "source_contracts": source_contracts,
            "open": part["open"].iloc[0],
            "high": part["high"].max(),
            "low": part["low"].min(),
            "close": part["close"].iloc[-1],
            "volume": part["volume"].sum(),
            "roll_policy_id": ROLL_POLICY_ID,
            "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
            "adjustment_factor": ADJUSTMENT_FACTOR,
            "has_roll_boundary": bool(part["is_roll_boundary"].any()),
            "roll_map_id": ",".join(roll_map_ids),
            "schema_version": SCHEMA_CONTINUOUS_D1,
            "ingest_ts": ingest_ts,
            "_source_partition_count": int(part["_source_partition_path"].nunique()),
            "_source_rows": int(len(part)),
        })
    if session_conflicts:
        raise RuntimeError("Multiple session_date values inside D1 primary key groups: " + json.dumps(session_conflicts[:50], ensure_ascii=False))
    if policy_conflicts:
        raise RuntimeError("Non-unique or invalid policy fields inside D1 groups: " + json.dumps(policy_conflicts[:50], ensure_ascii=False))
    if not rows:
        raise RuntimeError("Continuous D1 aggregation produced zero rows")
    return pd.DataFrame(rows).sort_values(["trade_date", "family_code", "continuous_symbol"]).reset_index(drop=True)


def source_contract_hits(source_contracts: Any, excluded_upper: set) -> List[str]:
    values: List[str] = []
    if isinstance(source_contracts, list):
        values = [str(x) for x in source_contracts]
    elif isinstance(source_contracts, tuple):
        values = [str(x) for x in source_contracts]
    else:
        text = clean_text(source_contracts)
        if text:
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    values = [str(x) for x in parsed]
                else:
                    values = [text]
            except Exception:
                values = [x.strip() for x in text.split(",") if x.strip()]
    return sorted({x.upper() for x in values}.intersection(excluded_upper))


def validate_d1(d1: pd.DataFrame, continuous_5m: pd.DataFrame, excluded: List[str], roll_policy_id: str, adjustment_policy_id: str) -> List[str]:
    blockers: List[str] = []
    required = [
        "trade_date",
        "session_date",
        "continuous_symbol",
        "family_code",
        "source_contracts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "roll_policy_id",
        "adjustment_policy_id",
        "adjustment_factor",
        "has_roll_boundary",
        "roll_map_id",
        "schema_version",
        "ingest_ts",
    ]
    missing = [x for x in required if x not in d1.columns]
    if missing:
        return ["continuous_d1_missing_required_fields:" + ",".join(missing)]
    if d1.empty:
        return ["continuous_d1_empty"]
    null_required = [x for x in required if x != "source_contracts" and int(d1[x].isna().sum()) > 0]
    if null_required:
        blockers.append("continuous_d1_null_required_fields:" + ",".join(null_required))
    empty_source_contracts = int(d1["source_contracts"].map(lambda x: not isinstance(x, list) or len(x) == 0).sum())
    if empty_source_contracts > 0:
        blockers.append("empty_source_contracts_rows:" + str(empty_source_contracts))
    bad_schema = d1.loc[d1["schema_version"].astype(str) != SCHEMA_CONTINUOUS_D1]
    if not bad_schema.empty:
        blockers.append("invalid_continuous_d1_schema_rows:" + str(len(bad_schema)))
    bad_roll_policy = d1.loc[d1["roll_policy_id"].astype(str) != roll_policy_id]
    if not bad_roll_policy.empty:
        blockers.append("invalid_roll_policy_rows:" + str(len(bad_roll_policy)))
    bad_adjustment_policy = d1.loc[d1["adjustment_policy_id"].astype(str) != adjustment_policy_id]
    if not bad_adjustment_policy.empty:
        blockers.append("invalid_adjustment_policy_rows:" + str(len(bad_adjustment_policy)))
    bad_adjustment_factor = d1.loc[pd.to_numeric(d1["adjustment_factor"], errors="coerce") != ADJUSTMENT_FACTOR]
    if not bad_adjustment_factor.empty:
        blockers.append("invalid_adjustment_factor_rows:" + str(len(bad_adjustment_factor)))
    duplicate_pk = int(d1.duplicated(subset=["continuous_symbol", "trade_date"]).sum())
    if duplicate_pk > 0:
        blockers.append("duplicate_d1_primary_key_rows:" + str(duplicate_pk))
    null_ohlc = int(d1[["open", "high", "low", "close"]].isna().any(axis=1).sum())
    if null_ohlc > 0:
        blockers.append("continuous_d1_null_ohlc_rows:" + str(null_ohlc))
    invalid = (d1["high"] < d1["low"]) | (d1["open"] > d1["high"]) | (d1["open"] < d1["low"]) | (d1["close"] > d1["high"]) | (d1["close"] < d1["low"])
    invalid_count = int(invalid.fillna(True).sum())
    if invalid_count > 0:
        blockers.append("continuous_d1_invalid_ohlc_rows:" + str(invalid_count))
    raw_keys = continuous_5m[["continuous_symbol", "trade_date"]].drop_duplicates().copy()
    d1_keys = d1[["continuous_symbol", "trade_date"]].drop_duplicates().copy()
    merged = raw_keys.merge(d1_keys, on=["continuous_symbol", "trade_date"], how="left", indicator=True)
    missing_keys = merged.loc[merged["_merge"] == "left_only"]
    if not missing_keys.empty:
        blockers.append("missing_d1_keys_from_continuous_5m:" + json.dumps(missing_keys.head(50).to_dict("records"), ensure_ascii=False, sort_keys=True))
    excluded_upper = {x.upper() for x in excluded}
    excluded_hits = sorted({hit for values in d1["source_contracts"].tolist() for hit in source_contract_hits(values, excluded_upper)})
    if excluded_hits:
        blockers.append("excluded_source_contracts_present:" + ",".join(excluded_hits))
    usdrubf = d1.loc[d1["continuous_symbol"].astype(str).str.upper() == "USDRUBF"].copy()
    if not usdrubf.empty:
        invalid_source_contracts = int(usdrubf["source_contracts"].map(lambda x: x != ["USDRUBF"]).sum())
        if invalid_source_contracts > 0:
            blockers.append("usdrubf_invalid_source_contracts_rows:" + str(invalid_source_contracts))
        if int(usdrubf["has_roll_boundary"].map(bool_value).sum()) != 0:
            blockers.append("usdrubf_has_roll_boundary_true_rows")
        if int((pd.to_numeric(usdrubf["adjustment_factor"], errors="coerce") != ADJUSTMENT_FACTOR).sum()) != 0:
            blockers.append("usdrubf_invalid_adjustment_factor_rows")
    return blockers


def output_partition_path(root: Path, data_root: Path, roll_policy_id: str, adjustment_policy_id: str, family_code: str, trade_date: str) -> Path:
    return resolve_contract_path(
        root,
        data_root,
        CONTRACT_CONTINUOUS_D1,
        {
            "roll_policy_id": roll_policy_id,
            "adjustment_policy_id": adjustment_policy_id,
            "family_code": family_code,
            "trade_date": trade_date,
        },
    )


def write_partitions(root: Path, data_root: Path, d1: pd.DataFrame, roll_policy_id: str, adjustment_policy_id: str) -> List[str]:
    paths: List[str] = []
    clean = d1.drop(columns=[x for x in ["_source_partition_count", "_source_rows"] if x in d1.columns]).copy()
    for (family_code, trade_date), part in clean.groupby(["family_code", "trade_date"], sort=True):
        path = output_partition_path(root, data_root, roll_policy_id, adjustment_policy_id, str(family_code), str(trade_date))
        path.parent.mkdir(parents=True, exist_ok=True)
        part.sort_values(["continuous_symbol"]).to_parquet(path, index=False)
        paths.append(str(path))
    return paths


def summarize(d1: pd.DataFrame, continuous_5m: pd.DataFrame, partition_paths: List[str], input_paths: List[Path]) -> Dict[str, Any]:
    roll_boundary_counts = {str(k): int(v) for k, v in d1.loc[d1["has_roll_boundary"].map(bool_value), "continuous_symbol"].astype(str).value_counts(dropna=False).to_dict().items()}
    rows_by_symbol = {str(k): int(v) for k, v in d1["continuous_symbol"].astype(str).value_counts(dropna=False).to_dict().items()}
    source_contracts_by_symbol = {}
    for symbol, part in d1.groupby("continuous_symbol", sort=True):
        values = []
        for item in part["source_contracts"].tolist():
            if isinstance(item, list):
                values.extend([str(x) for x in item])
        source_contracts_by_symbol[str(symbol)] = ordered_distinct(values)
    return {
        "continuous_5m_rows": int(len(continuous_5m)),
        "continuous_d1_rows": int(len(d1)),
        "continuous_symbols": sorted([str(x) for x in d1["continuous_symbol"].dropna().unique().tolist()]),
        "families": sorted([str(x) for x in d1["family_code"].dropna().unique().tolist()]),
        "rows_by_continuous_symbol": rows_by_symbol,
        "source_contracts_by_symbol": source_contracts_by_symbol,
        "roll_boundary_rows_by_symbol": roll_boundary_counts,
        "input_partition_count": int(len(input_paths)),
        "output_partition_count": int(len(partition_paths)),
        "min_trade_date": str(d1["trade_date"].min()),
        "max_trade_date": str(d1["trade_date"].max()),
    }


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--roll-policy-id", default=ROLL_POLICY_ID)
    parser.add_argument("--adjustment-policy-id", default=ADJUSTMENT_POLICY_ID)
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
    args = parser.parse_args()

    root = repo_root()
    data_root = base.resolve_data_root(args)
    run_date = str(args.run_date).strip()
    from_date = base.parse_iso_date(str(args.from_date or "")) if str(args.from_date or "").strip() else ""
    till = base.parse_iso_date(str(args.till or "")) if str(args.till or "").strip() else ""
    roll_policy_id = str(args.roll_policy_id).strip()
    adjustment_policy_id = str(args.adjustment_policy_id).strip()
    excluded = parse_list(args.excluded, DEFAULT_EXCLUDED)
    ingest_ts = utc_now_iso()
    run_id = "futures_continuous_d1_builder_" + run_date + "_" + stable_id([ingest_ts, roll_policy_id, adjustment_policy_id, from_date, till])

    if roll_policy_id != ROLL_POLICY_ID:
        raise RuntimeError("Unsupported roll_policy_id: " + roll_policy_id)
    if adjustment_policy_id != ADJUSTMENT_POLICY_ID:
        raise RuntimeError("Unsupported adjustment_policy_id: " + adjustment_policy_id)

    base.assert_files_exist(root, REQUIRED_CONTRACTS)

    input_paths = discover_continuous_5m_paths(root, data_root, roll_policy_id, adjustment_policy_id, from_date, till)
    continuous_5m_raw = read_partitions(input_paths)
    input_blockers = validate_continuous_5m(continuous_5m_raw, excluded, roll_policy_id, adjustment_policy_id)
    if input_blockers:
        print_json_line("blockers", input_blockers)
        return 1

    continuous_5m = normalize_continuous_5m(continuous_5m_raw)
    d1 = aggregate_d1(continuous_5m, ingest_ts)
    output_blockers = validate_d1(d1, continuous_5m, excluded, roll_policy_id, adjustment_policy_id)
    if output_blockers:
        print_json_line("blockers", output_blockers)
        return 1

    partition_paths = write_partitions(root, data_root, d1, roll_policy_id, adjustment_policy_id)
    summary = summarize(d1, continuous_5m, partition_paths, input_paths)

    print_json_line("run_id", run_id)
    print_json_line("input_artifact_contract", SCHEMA_CONTINUOUS_5M)
    print_json_line("output_artifact_contract", SCHEMA_CONTINUOUS_D1)
    print_json_line("input_artifacts", {"continuous_5m_partitions_read": [str(x) for x in input_paths]})
    print_json_line("output_artifacts_created", {"continuous_d1_partitions_created": partition_paths})
    print_json_line("continuous_d1_summary", summary)
    print_json_line("builder_result_verdict", "pass")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
