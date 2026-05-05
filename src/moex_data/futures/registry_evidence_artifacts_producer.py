#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

sys.path.insert(0, str(Path.cwd() / "src"))

import pandas as pd

from moex_data.futures import algopack_availability_probe as availability

SCHEMA_FAMILY_MAPPING = "futures_family_mapping.v1"
FAMILY_MAPPING_CONTRACT = "contracts/datasets/futures_family_mapping_contract.md"
REQUIRED_CONTRACTS = list(availability.REQUIRED_CONTRACTS) + [FAMILY_MAPPING_CONTRACT]
REQUIRED_CONFIGS = list(availability.REQUIRED_CONFIGS)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def extract_contract_value(text: str, key: str) -> str:
    prefix = key + ":"
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return ""


def contract_path(root: Path, data_root: Path, contract_rel: str, snapshot_date: str) -> Path:
    text = read_text(root / contract_rel)
    pattern = extract_contract_value(text, "path_pattern")
    prefix = "${MOEX_DATA_ROOT}"
    if not pattern.startswith(prefix):
        raise RuntimeError("Unsupported contract path_pattern: " + contract_rel)
    rel = pattern[len(prefix):].lstrip("/")
    rel = rel.replace("{snapshot_date}", snapshot_date).replace("YYYY-MM-DD", snapshot_date)
    return data_root / rel


def is_rfud(row: pd.Series) -> bool:
    board = str(row.get("board", "") or "").lower()
    engine = str(row.get("engine", "futures") or "futures").lower()
    market = str(row.get("market", "forts") or "forts").lower()
    secid = str(row.get("secid", "") or "").strip()
    return bool(secid) and board == "rfud" and engine == "futures" and market == "forts"


def select_all_rfud_instruments(normalized: pd.DataFrame) -> pd.DataFrame:
    if normalized.empty:
        raise RuntimeError("Normalized registry is empty")
    required = ["secid", "family_code", "board"]
    missing = [x for x in required if x not in normalized.columns]
    if missing:
        raise RuntimeError("Normalized registry missing fields: " + ", ".join(missing))
    selected = normalized.loc[normalized.apply(is_rfud, axis=1)].copy()
    if selected.empty:
        raise RuntimeError("No RFUD futures instruments found in normalized registry")
    selected["selection_status"] = "selected_from_all_rfud_registry"
    return selected.sort_values(["family_code", "secid"]).reset_index(drop=True)


def family_mapping_status(family_code: str) -> str:
    family = str(family_code or "").strip()
    if family and family.upper() != "UNKNOWN":
        return "pass"
    return "unresolved"


def build_family_mapping(normalized: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    instruments = select_all_rfud_instruments(normalized)
    rows: List[Dict[str, Any]] = []
    for _, row in instruments.iterrows():
        snapshot_id = str(row.get("snapshot_id", "") or "futures_registry_snapshot_" + snapshot_date)
        board = str(row.get("board", "rfud") or "rfud")
        secid = str(row.get("secid", "") or "").strip()
        family = str(row.get("family_code", "") or "").strip()
        status = family_mapping_status(family)
        rows.append({
            "mapping_id": availability.stable_id(["family_mapping", snapshot_id, board, secid]),
            "snapshot_id": snapshot_id,
            "snapshot_date": snapshot_date,
            "board": board,
            "secid": secid,
            "family_code": family if status == "pass" else None,
            "mapping_source": "derived_rule" if status == "pass" else "unresolved",
            "schema_version": SCHEMA_FAMILY_MAPPING,
            "contract_code": row.get("contract_code", None),
            "underlying": row.get("underlying", None),
            "override_reason": None,
            "review_notes": None if status == "pass" else "family_code unresolved by normalized registry rule",
            "mapping_status": status,
            "override_status": "not_applicable",
            "validation_status": "pass" if status == "pass" else "failed",
        })
    frame = pd.DataFrame(rows)
    duplicates = int(frame.duplicated(subset=["mapping_id", "snapshot_id", "board", "secid"]).sum())
    if duplicates > 0:
        raise RuntimeError("family_mapping duplicate primary key rows: " + str(duplicates))
    return frame


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
    except Exception as exc:
        raise RuntimeError("Cannot write parquet " + str(path) + ": " + exc.__class__.__name__ + ": " + str(exc)) from exc


def status_summary(frame: pd.DataFrame, status_col: str) -> Dict[str, Any]:
    if status_col not in frame.columns:
        return {"rows": int(len(frame)), "status_counts": {}}
    counts = frame[status_col].astype(str).value_counts(dropna=False).to_dict()
    by_family: Dict[str, Dict[str, int]] = {}
    if "family_code" in frame.columns:
        for family, sub in frame.groupby("family_code", dropna=False):
            by_family[str(family)] = {str(k): int(v) for k, v in sub[status_col].astype(str).value_counts(dropna=False).to_dict().items()}
    return {"rows": int(len(frame)), "status_counts": {str(k): int(v) for k, v in counts.items()}, "by_family": by_family}


def fetch_or_build_registry(args: argparse.Namespace, root: Path, snapshot_date: str, data_root: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    registry_raw = availability.fetch_paged_frame(
        args.iss_base_url,
        "/iss/engines/futures/markets/forts/boards/rfud/securities.json",
        {},
        "securities",
        args.timeout,
        False,
    )
    if registry_raw.empty:
        raise RuntimeError("MOEX futures registry returned zero rows")
    registry = availability.build_registry_snapshot(registry_raw, snapshot_date)
    if registry.empty:
        raise RuntimeError("Registry snapshot normalization produced zero rows")
    registry["raw_payload_json"] = registry["raw_payload_json"].astype(str)
    normalized = availability.build_normalized_registry(registry)
    registry_path = availability.contract_output_path(data_root, "contracts/datasets/futures_registry_snapshot_contract.md", snapshot_date)
    normalized_path = availability.contract_output_path(data_root, "contracts/datasets/futures_normalized_instrument_registry_contract.md", snapshot_date)
    write_parquet(registry, registry_path)
    write_parquet(normalized, normalized_path)
    return registry, normalized


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=availability.today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--data-root", default="")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", availability.DEFAULT_ISS_BASE_URL))
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", availability.DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=35.0)
    args = parser.parse_args()

    root = Path.cwd().resolve()
    snapshot_date = str(args.snapshot_date).strip()
    data_root = availability.resolve_data_root(args)
    probe_from, probe_till = availability.date_range_defaults(snapshot_date, args)

    availability.assert_files_exist(root, REQUIRED_CONTRACTS + REQUIRED_CONFIGS)
    sources_config = availability.read_json(root / "configs/datasets/futures_algopack_availability_sources_config.json")

    registry, normalized = fetch_or_build_registry(args, root, snapshot_date, data_root)
    instruments = select_all_rfud_instruments(normalized)

    family_mapping = build_family_mapping(normalized, snapshot_date)
    family_mapping_path = contract_path(root, data_root, FAMILY_MAPPING_CONTRACT, snapshot_date)
    write_parquet(family_mapping, family_mapping_path)

    source_items = sources_config.get("sources") or []
    if not isinstance(source_items, list):
        raise RuntimeError("sources config is not a list")

    report_paths: Dict[str, str] = {}
    report_summaries: Dict[str, Any] = {}
    for source in source_items:
        if not isinstance(source, dict):
            continue
        endpoint_id = str(source.get("endpoint_id", "")).strip()
        if endpoint_id not in availability.REPORT_SCHEMA_BY_ENDPOINT:
            continue
        contract_rel = str(source.get("dataset_contract") or availability.CONTRACT_BY_ENDPOINT.get(endpoint_id) or "").strip()
        endpoint_path = str(source.get("endpoint_path", "")).strip()
        if not contract_rel or not endpoint_path:
            raise RuntimeError("Unresolved endpoint contract/path for " + endpoint_id)
        report = availability.build_availability_report(
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
        out_path = availability.contract_output_path(data_root, contract_rel, snapshot_date)
        write_parquet(report, out_path)
        report_paths[endpoint_id] = str(out_path)
        report_summaries[endpoint_id] = status_summary(report, "availability_status")

    output_paths = {
        "registry_snapshot": str(availability.contract_output_path(data_root, "contracts/datasets/futures_registry_snapshot_contract.md", snapshot_date)),
        "normalized_registry": str(availability.contract_output_path(data_root, "contracts/datasets/futures_normalized_instrument_registry_contract.md", snapshot_date)),
        "family_mapping": str(family_mapping_path),
    }
    output_paths.update(report_paths)

    availability.print_json_line("output_artifacts_created", output_paths)
    availability.print_json_line("registry_snapshot_summary", {"rows": int(len(registry)), "unique_secid": int(registry["secid"].nunique()), "snapshot_date": snapshot_date})
    availability.print_json_line("normalized_registry_summary", {"rows": int(len(normalized)), "unique_family_code": int(normalized["family_code"].nunique()), "selected_rfud_instruments": int(len(instruments))})
    availability.print_json_line("family_mapping_summary", status_summary(family_mapping, "mapping_status"))
    for endpoint_id in ["algopack_fo_tradestats", "moex_futoi", "algopack_fo_obstats", "algopack_fo_hi2"]:
        availability.print_json_line(endpoint_id + "_availability_summary", report_summaries.get(endpoint_id, {"rows": 0, "status_counts": {}}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
