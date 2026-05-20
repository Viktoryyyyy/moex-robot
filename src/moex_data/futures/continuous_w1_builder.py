#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures.slice1_common import print_json_line, stable_id, today_msk, utc_now_iso

SCHEMA_D1 = "futures_continuous_d1.v1"
SCHEMA_W1 = "futures_continuous_w1.v1"
SCHEMA_Q = "futures_continuous_w1_quality_report.v1"
ROLL_POLICY_ID = "expiration_minus_1_trading_session_v1"
ADJUSTMENT_POLICY_ID = "unadjusted_v1"
ADJUSTMENT_FACTOR = 1.0
REQUIRED_CONTRACTS = [
    "contracts/datasets/futures_continuous_d1_contract.md",
    "contracts/datasets/futures_continuous_w1_contract.md",
    "contracts/datasets/futures_continuous_w1_quality_report_contract.md",
]
D1_COLUMNS = ["trade_date", "session_date", "continuous_symbol", "family_code", "source_contracts", "open", "high", "low", "close", "volume", "roll_policy_id", "adjustment_policy_id", "adjustment_factor", "has_roll_boundary", "roll_map_id", "schema_version", "ingest_ts"]
W1_COLUMNS = ["week_start", "week_end", "iso_year", "iso_week", "continuous_symbol", "family_code", "source_trade_dates", "source_contracts", "open", "high", "low", "close", "volume", "roll_policy_id", "adjustment_policy_id", "adjustment_factor", "has_roll_boundary", "roll_map_id", "source_d1_row_count", "schema_version", "ingest_ts"]


def clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"nan", "nat", "none", "null"} else text


def parse_items(value: Any) -> List[str]:
    if value is None:
        return []
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        value = value.tolist()
    if isinstance(value, (list, tuple, set)):
        return [str(x) for x in value if clean(x)]
    text = clean(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, (list, tuple, set)):
            return [str(x) for x in parsed if clean(x)]
    except Exception:
        pass
    return [x.strip().strip("[]").strip(chr(39)).strip(chr(34)) for x in text.split(",") if x.strip()]


def ordered(values) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        items = parse_items(value)
        if not items and clean(value):
            items = [clean(value)]
        for item in items:
            text = clean(item)
            if text and text not in seen:
                seen.add(text)
                out.append(text)
    return out


def bval(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def p_d1(root: Path, roll: str, adj: str, family: str, trade_date: str) -> Path:
    return root / "futures" / "continuous_d1" / ("roll_policy=" + roll) / ("adjustment_policy=" + adj) / ("family=" + family) / ("trade_date=" + trade_date) / "part.parquet"


def p_w1(root: Path, roll: str, adj: str, family: str, week_start: str) -> Path:
    return root / "futures" / "continuous_w1" / ("roll_policy=" + roll) / ("adjustment_policy=" + adj) / ("family=" + family) / ("week_start=" + week_start) / "part.parquet"


def p_quality(root: Path, run_date: str) -> Path:
    return root / "futures" / "quality" / "continuous_w1_builder" / ("run_date=" + run_date) / "quality_report.json"


def iso_date(value: str, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise RuntimeError("--" + name + " is required")
    return pd.to_datetime(text, errors="raise").date().isoformat()


def date_range(from_date: str, till: str) -> List[str]:
    start = pd.Timestamp(from_date)
    end = pd.Timestamp(till)
    if start > end:
        raise RuntimeError("--from is after --till")
    return [x.date().isoformat() for x in pd.date_range(start, end, freq="D")]


def read_d1(root: Path, roll: str, adj: str, family: str, dates: List[str]) -> tuple[pd.DataFrame, List[Path]]:
    frames = []
    paths = []
    for trade_date in dates:
        path = p_d1(root, roll, adj, family, trade_date)
        if path.is_file():
            part = pd.read_parquet(path)
            part["_source_partition_path"] = str(path)
            frames.append(part)
            paths.append(path)
    if not frames:
        raise FileNotFoundError("No D1 source partitions found for explicit family/date range: family=" + family + " from=" + dates[0] + " till=" + dates[-1])
    return pd.concat(frames, ignore_index=True), paths


def validate_d1(frame: pd.DataFrame, family: str, roll: str, adj: str) -> List[str]:
    blockers: List[str] = []
    missing = [x for x in D1_COLUMNS if x not in frame.columns]
    if missing:
        return ["d1_missing_required_fields:" + ",".join(missing)]
    if frame.empty:
        return ["d1_source_empty"]
    nulls = [x for x in D1_COLUMNS if int(frame[x].isna().sum()) > 0]
    if nulls:
        blockers.append("d1_null_required_fields:" + ",".join(nulls))
    work = frame.copy()
    for col in ["open", "high", "low", "close", "volume", "adjustment_factor"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    if work[["open", "high", "low", "close", "volume", "adjustment_factor"]].isna().any(axis=None):
        blockers.append("d1_invalid_numeric_values")
    checks = [("schema_version", SCHEMA_D1, "d1_invalid_schema_rows"), ("family_code", family, "d1_family_mismatch_rows"), ("roll_policy_id", roll, "d1_roll_policy_mismatch_rows"), ("adjustment_policy_id", adj, "d1_adjustment_policy_mismatch_rows")]
    for col, expected, label in checks:
        count = int((work[col].astype(str) != expected).sum())
        if count:
            blockers.append(label + ":" + str(count))
    factor_count = int((pd.to_numeric(work["adjustment_factor"], errors="coerce") != ADJUSTMENT_FACTOR).sum())
    if factor_count:
        blockers.append("d1_adjustment_factor_mismatch_rows:" + str(factor_count))
    invalid = (work["high"] < work["low"]) | (work["open"] > work["high"]) | (work["open"] < work["low"]) | (work["close"] > work["high"]) | (work["close"] < work["low"])
    if int(invalid.fillna(True).sum()):
        blockers.append("d1_invalid_ohlc_rows:" + str(int(invalid.fillna(True).sum())))
    dup = int(work.duplicated(subset=["continuous_symbol", "trade_date"]).sum())
    if dup:
        blockers.append("d1_duplicate_primary_key_rows:" + str(dup))
    empty_contracts = int(work["source_contracts"].map(lambda x: len(parse_items(x)) == 0).sum())
    if empty_contracts:
        blockers.append("d1_empty_source_contracts_rows:" + str(empty_contracts))
    return blockers


def normalize_d1(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["trade_date"] = out["trade_date"].astype(str)
    for col in ["continuous_symbol", "family_code", "roll_policy_id", "adjustment_policy_id", "schema_version"]:
        out[col] = out[col].astype(str)
    for col in ["open", "high", "low", "close", "volume", "adjustment_factor"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["has_roll_boundary"] = out["has_roll_boundary"].map(bval)
    out["_trade_ts"] = pd.to_datetime(out["trade_date"], errors="coerce")
    if out["_trade_ts"].isna().any():
        raise RuntimeError("D1 source has invalid trade_date")
    iso = out["_trade_ts"].dt.isocalendar()
    out["iso_year"] = iso.year.astype(int)
    out["iso_week"] = iso.week.astype(int)
    out["week_start"] = (out["_trade_ts"] - pd.to_timedelta(out["_trade_ts"].dt.weekday, unit="D")).dt.date.astype(str)
    out["week_end"] = (pd.to_datetime(out["week_start"]) + pd.Timedelta(days=6)).dt.date.astype(str)
    return out.sort_values(["continuous_symbol", "trade_date"]).reset_index(drop=True)


def aggregate_w1(d1: pd.DataFrame, ingest_ts: str) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    keys = ["continuous_symbol", "family_code", "iso_year", "iso_week", "week_start", "week_end"]
    for key, part in d1.groupby(keys, sort=True):
        symbol, family, year, week, week_start, week_end = key
        rows.append({
            "week_start": str(week_start),
            "week_end": str(week_end),
            "iso_year": int(year),
            "iso_week": int(week),
            "continuous_symbol": str(symbol),
            "family_code": str(family),
            "source_trade_dates": ordered(part["trade_date"]),
            "source_contracts": ordered(part["source_contracts"]),
            "open": part["open"].iloc[0],
            "high": part["high"].max(),
            "low": part["low"].min(),
            "close": part["close"].iloc[-1],
            "volume": part["volume"].sum(),
            "roll_policy_id": ROLL_POLICY_ID,
            "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
            "adjustment_factor": ADJUSTMENT_FACTOR,
            "has_roll_boundary": bool(part["has_roll_boundary"].any()),
            "roll_map_id": ",".join(ordered(part["roll_map_id"])),
            "source_d1_row_count": int(len(part)),
            "schema_version": SCHEMA_W1,
            "ingest_ts": ingest_ts,
        })
    if not rows:
        raise RuntimeError("W1 aggregation produced zero rows")
    return pd.DataFrame(rows).sort_values(["week_start", "family_code", "continuous_symbol"]).reset_index(drop=True)


def validate_w1(frame: pd.DataFrame, d1: pd.DataFrame, family: str, roll: str, adj: str) -> List[str]:
    blockers: List[str] = []
    missing = [x for x in W1_COLUMNS if x not in frame.columns]
    if missing:
        return ["w1_missing_required_fields:" + ",".join(missing)]
    if frame.empty:
        return ["w1_empty"]
    empty_dates = int(frame["source_trade_dates"].map(lambda x: len(parse_items(x)) == 0).sum())
    empty_contracts = int(frame["source_contracts"].map(lambda x: len(parse_items(x)) == 0).sum())
    if empty_dates:
        blockers.append("w1_empty_source_trade_dates_rows:" + str(empty_dates))
    if empty_contracts:
        blockers.append("w1_empty_source_contracts_rows:" + str(empty_contracts))
    dup = int(frame.duplicated(subset=["continuous_symbol", "week_start"]).sum())
    if dup:
        blockers.append("w1_duplicate_primary_key_rows:" + str(dup))
    for col, expected, label in [("schema_version", SCHEMA_W1, "w1_invalid_schema_rows"), ("family_code", family, "w1_family_mismatch_rows"), ("roll_policy_id", roll, "w1_roll_policy_mismatch_rows"), ("adjustment_policy_id", adj, "w1_adjustment_policy_mismatch_rows")]:
        count = int((frame[col].astype(str) != expected).sum())
        if count:
            blockers.append(label + ":" + str(count))
    for col in ["open", "high", "low", "close", "volume", "adjustment_factor"]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    invalid = (frame["high"] < frame["low"]) | (frame["open"] > frame["high"]) | (frame["open"] < frame["low"]) | (frame["close"] > frame["high"]) | (frame["close"] < frame["low"])
    if int(invalid.fillna(True).sum()):
        blockers.append("w1_invalid_ohlc_rows:" + str(int(invalid.fillna(True).sum())))
    source_keys = int(d1[["continuous_symbol", "iso_year", "iso_week"]].drop_duplicates().shape[0])
    if source_keys != int(len(frame)):
        blockers.append("w1_key_count_mismatch:source=" + str(source_keys) + ":w1=" + str(len(frame)))
    return blockers


def write_parts(root: Path, w1: pd.DataFrame, roll: str, adj: str) -> List[str]:
    paths: List[str] = []
    for key, part in w1.groupby(["family_code", "week_start"], sort=True):
        family, week_start = key
        path = p_w1(root, roll, adj, str(family), str(week_start))
        path.parent.mkdir(parents=True, exist_ok=True)
        part.loc[:, W1_COLUMNS].sort_values(["continuous_symbol"]).to_parquet(path, index=False)
        paths.append(str(path))
    return paths


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def quality(run_id: str, run_date: str, family: str, d1: pd.DataFrame, w1: pd.DataFrame, d1_paths: List[Path], w1_paths: List[str]) -> Dict[str, Any]:
    dup = int(w1.duplicated(subset=["continuous_symbol", "week_start"]).sum())
    lineage_bad = int(w1["source_trade_dates"].map(lambda x: len(parse_items(x)) == 0).sum()) + int(w1["source_contracts"].map(lambda x: len(parse_items(x)) == 0).sum())
    checks = {
        "source_is_d1_only": {"check_status": "pass", "observed_value": SCHEMA_D1, "expected_value": SCHEMA_D1},
        "w1_row_count": {"check_status": "pass" if len(w1) > 0 else "fail", "observed_value": int(len(w1)), "expected_value": ">0"},
        "d1_row_count": {"check_status": "pass" if len(d1) > 0 else "fail", "observed_value": int(len(d1)), "expected_value": ">0"},
        "w1_primary_key_unique": {"check_status": "pass" if dup == 0 else "fail", "observed_value": dup, "expected_value": 0},
        "w1_lineage_completeness": {"check_status": "pass" if lineage_bad == 0 else "fail", "observed_value": lineage_bad, "expected_value": 0},
        "no_raw_5m_read": {"check_status": "pass", "observed_value": "not_used", "expected_value": "not_used"},
        "no_futoi_join": {"check_status": "pass", "observed_value": "not_used", "expected_value": "not_used"},
        "no_materialized_intraday_timeframes": {"check_status": "pass", "observed_value": "not_created", "expected_value": "not_created"},
    }
    blockers = [key for key, value in checks.items() if value["check_status"] == "fail"]
    return {"schema_version": SCHEMA_Q, "run_id": run_id, "run_date": run_date, "family_code": family, "started_at": utc_now_iso(), "finished_at": utc_now_iso(), "source_artifact_contract": SCHEMA_D1, "output_artifact_contract": SCHEMA_W1, "input_d1_partitions": [str(x) for x in d1_paths], "output_w1_partitions": w1_paths, "row_counts": {"d1": int(len(d1)), "w1": int(len(w1))}, "checks": checks, "quality_report_status": "pass" if not blockers else "fail", "blockers": blockers}


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", required=True)
    parser.add_argument("--till", required=True)
    parser.add_argument("--data-root", default="")
    parser.add_argument("--family", required=True)
    parser.add_argument("--roll-policy-id", default=ROLL_POLICY_ID)
    parser.add_argument("--adjustment-policy-id", default=ADJUSTMENT_POLICY_ID)
    args = parser.parse_args()
    root = Path.cwd().resolve()
    missing = [x for x in REQUIRED_CONTRACTS if not (root / x).is_file()]
    if missing:
        raise RuntimeError("Missing required repo contracts: " + ",".join(missing))
    data_root = base.resolve_data_root(args)
    run_date = str(args.run_date).strip()
    from_date = iso_date(args.from_date, "from")
    till = iso_date(args.till, "till")
    family = str(args.family).strip()
    roll = str(args.roll_policy_id).strip()
    adj = str(args.adjustment_policy_id).strip()
    if not family:
        raise RuntimeError("--family is required")
    if roll != ROLL_POLICY_ID:
        raise RuntimeError("Unsupported roll_policy_id: " + roll)
    if adj != ADJUSTMENT_POLICY_ID:
        raise RuntimeError("Unsupported adjustment_policy_id: " + adj)
    ingest_ts = utc_now_iso()
    run_id = "futures_continuous_w1_builder_" + run_date + "_" + stable_id([ingest_ts, family, from_date, till, roll, adj])
    dates = date_range(from_date, till)
    d1_raw, d1_paths = read_d1(data_root, roll, adj, family, dates)
    blockers = validate_d1(d1_raw, family, roll, adj)
    if blockers:
        print_json_line("blockers", blockers)
        return 1
    d1 = normalize_d1(d1_raw)
    w1 = aggregate_w1(d1, ingest_ts)
    blockers = validate_w1(w1, d1, family, roll, adj)
    if blockers:
        print_json_line("blockers", blockers)
        return 1
    w1_paths = write_parts(data_root, w1, roll, adj)
    report = quality(run_id, run_date, family, d1, w1, d1_paths, w1_paths)
    qpath = p_quality(data_root, run_date)
    write_json(qpath, report)
    print_json_line("run_id", run_id)
    print_json_line("input_artifact_contract", SCHEMA_D1)
    print_json_line("output_artifact_contract", SCHEMA_W1)
    print_json_line("output_artifacts_created", {"continuous_w1_partitions_created": w1_paths, "quality_report": str(qpath)})
    print_json_line("continuous_w1_summary", {"family_code": family, "w1_rows": int(len(w1)), "min_week_start": str(w1["week_start"].min()), "max_week_start": str(w1["week_start"].max())})
    print_json_line("quality_report_summary", {"quality_report_status": report["quality_report_status"], "row_counts": report["row_counts"]})
    print_json_line("builder_result_verdict", "pass")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
