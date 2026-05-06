#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from dotenv import load_dotenv
import pandas as pd

from moex_data.futures.slice1_common import print_json_line, stable_id, today_msk

SCHEMA_VERSION = "futures_family_mapping.v1"
NORMALIZED_CONTRACT_REL = "contracts/datasets/futures_normalized_instrument_registry_contract.md"
FAMILY_MAPPING_CONTRACT_REL = "contracts/datasets/futures_family_mapping_contract.md"
DEFAULT_CONFIG_REL = "configs/datasets/futures_family_mapping_overrides_config.json"
PRIMARY_KEY = ["mapping_id", "snapshot_id", "board", "secid"]
REQUIRED_FIELDS = [
    "mapping_id",
    "snapshot_id",
    "snapshot_date",
    "board",
    "secid",
    "family_code",
    "mapping_source",
    "schema_version",
]
STATUS_FIELDS = ["mapping_status", "override_status", "validation_status"]
MONTH_CODES = "FGHJKMNQUVXZ"
DERIVED_EXPIRING_RE = re.compile(r"^([A-Za-z]+)[" + MONTH_CODES + r"]\d{1,2}$")


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise RuntimeError("JSON root is not object: " + str(path))
    return data


def extract_contract_value(text: str, key: str) -> str:
    prefix = key + ":"
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return ""


def contract_path(root: Path, data_root: Path, contract_rel: str, snapshot_date: str) -> Path:
    text = (root / contract_rel).read_text(encoding="utf-8")
    pattern = extract_contract_value(text, "path_pattern")
    prefix = "${MOEX_DATA_ROOT}"
    if not pattern.startswith(prefix):
        raise RuntimeError("Unsupported contract path_pattern: " + contract_rel)
    tail = pattern[len(prefix):].lstrip("/")
    tail = tail.replace("{snapshot_date}", snapshot_date).replace("YYYY-MM-DD", snapshot_date)
    if not tail:
        raise RuntimeError("Empty contract path tail: " + contract_rel)
    return data_root / tail


def resolve_data_root(raw_value: str) -> Path:
    raw = str(raw_value or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        raise RuntimeError("MOEX_DATA_ROOT is required for futures family mapping generation")
    path = Path(raw).expanduser().resolve()
    if not path.is_absolute():
        raise RuntimeError("MOEX_DATA_ROOT must resolve to an absolute path")
    return path


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def optional_text(value: Any) -> Optional[str]:
    text = clean_text(value)
    return text if text else None


def normalize_board(value: Any) -> str:
    return clean_text(value).lower()


def validate_normalized_registry(frame: pd.DataFrame) -> None:
    required = [
        "snapshot_id",
        "snapshot_date",
        "secid",
        "board",
        "family_code",
        "contract_code",
        "instrument_kind",
        "source_snapshot_id",
        "schema_version",
    ]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        raise RuntimeError("normalized registry missing required fields: " + ", ".join(missing))
    null_required = [x for x in ["snapshot_id", "snapshot_date", "secid", "board", "source_snapshot_id"] if frame[x].isna().any()]
    if null_required:
        raise RuntimeError("normalized registry has null required fields: " + ", ".join(null_required))
    if frame.duplicated(subset=["snapshot_id", "board", "secid"]).any():
        raise RuntimeError("normalized registry primary key is not unique")


def override_matches(match: Dict[str, Any], secid: str) -> bool:
    if not isinstance(match, dict):
        return False
    exact = clean_text(match.get("secid"))
    if exact and secid.upper() == exact.upper():
        return True
    prefix = clean_text(match.get("secid_prefix"))
    if prefix and secid.upper().startswith(prefix.upper()):
        return True
    return False


def validate_overrides(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    overrides = config.get("overrides", [])
    if not isinstance(overrides, list):
        raise RuntimeError("overrides config value must be a list")
    out: List[Dict[str, Any]] = []
    for item in overrides:
        if not isinstance(item, dict):
            raise RuntimeError("override item must be object")
        family_code = clean_text(item.get("family_code"))
        mapping_source = clean_text(item.get("mapping_source"))
        override_reason = clean_text(item.get("override_reason"))
        match = item.get("match")
        if not family_code:
            raise RuntimeError("manual override missing family_code")
        if mapping_source != "manual_override":
            raise RuntimeError("manual override mapping_source must equal manual_override")
        if not override_reason:
            raise RuntimeError("manual override missing override_reason")
        if not isinstance(match, dict) or not match:
            raise RuntimeError("manual override missing match object")
        out.append(item)
    return out


def find_override(overrides: Iterable[Dict[str, Any]], secid: str) -> Optional[Dict[str, Any]]:
    for item in overrides:
        if override_matches(item.get("match", {}), secid):
            return item
    return None


def derive_family_from_code(secid: str) -> str:
    text = clean_text(secid)
    if not text:
        return ""
    match = DERIVED_EXPIRING_RE.match(text)
    if match:
        return match.group(1)
    return ""


def build_family_mapping(normalized: pd.DataFrame, config: Dict[str, Any], snapshot_date: str) -> pd.DataFrame:
    validate_normalized_registry(normalized)
    overrides = validate_overrides(config)
    rows: List[Dict[str, Any]] = []
    for _, item in normalized.sort_values(["snapshot_id", "board", "secid"]).iterrows():
        snapshot_id = clean_text(item.get("snapshot_id"))
        board = normalize_board(item.get("board"))
        secid = clean_text(item.get("secid"))
        contract_code = optional_text(item.get("contract_code"))
        source_family = clean_text(item.get("family_code"))
        override = find_override(overrides, secid)
        override_reason = None
        review_notes = None
        if override is not None:
            family_code = clean_text(override.get("family_code"))
            mapping_source = "manual_override"
            override_status = "applied"
            override_reason = clean_text(override.get("override_reason"))
            review_notes = optional_text(override.get("review_notes"))
        else:
            family_code = source_family or derive_family_from_code(contract_code or secid)
            mapping_source = "derived_rule" if family_code else "unresolved"
            override_status = "not_applicable"
        mapping_status = "pass" if family_code else "unresolved"
        validation_status = "pass" if family_code and secid and board and snapshot_id else "fail"
        rows.append({
            "mapping_id": stable_id(["futures_family_mapping", snapshot_id, board, secid]),
            "snapshot_id": snapshot_id,
            "snapshot_date": clean_text(item.get("snapshot_date")) or snapshot_date,
            "board": board,
            "secid": secid,
            "family_code": family_code,
            "mapping_source": mapping_source,
            "schema_version": SCHEMA_VERSION,
            "contract_code": contract_code,
            "underlying": optional_text(item.get("underlying")) or optional_text(item.get("asset_code")),
            "override_reason": override_reason,
            "review_notes": review_notes,
            "mapping_status": mapping_status,
            "override_status": override_status,
            "validation_status": validation_status,
            "engine": optional_text(item.get("engine")),
            "market": optional_text(item.get("market")),
            "shortname": optional_text(item.get("shortname")),
            "secname": optional_text(item.get("secname")),
            "instrument_kind": optional_text(item.get("instrument_kind")),
            "is_perpetual_candidate": item.get("is_perpetual_candidate") if "is_perpetual_candidate" in item.index else None,
            "source_snapshot_id": optional_text(item.get("source_snapshot_id")),
            "normalized_schema_version": optional_text(item.get("schema_version")),
        })
    out = pd.DataFrame(rows)
    validate_family_mapping(out)
    return out


def validate_family_mapping(frame: pd.DataFrame) -> None:
    missing = [x for x in REQUIRED_FIELDS + STATUS_FIELDS if x not in frame.columns]
    if missing:
        raise RuntimeError("family mapping missing required fields: " + ", ".join(missing))
    null_required = [x for x in REQUIRED_FIELDS if frame[x].isna().any() or (frame[x].astype(str).str.strip() == "").any()]
    if null_required:
        raise RuntimeError("family mapping has empty required fields: " + ", ".join(null_required))
    allowed_sources = {"derived_rule", "manual_override", "unresolved"}
    bad_sources = sorted(set(frame["mapping_source"].astype(str)) - allowed_sources)
    if bad_sources:
        raise RuntimeError("family mapping has invalid mapping_source values: " + ", ".join(bad_sources))
    manual = frame.loc[frame["mapping_source"].astype(str) == "manual_override"]
    if not manual.empty:
        missing_reason = manual["override_reason"].isna() | (manual["override_reason"].astype(str).str.strip() == "")
        if missing_reason.any():
            raise RuntimeError("manual overrides must include override_reason")
    if frame.duplicated(subset=PRIMARY_KEY).any():
        raise RuntimeError("family mapping primary key is not unique")
    active = frame.loc[frame.get("instrument_kind", pd.Series(dtype=str)).astype(str).str.lower() != "technical"]
    unresolved = active.loc[active["family_code"].astype(str).str.strip() == ""]
    if not unresolved.empty:
        sample = ", ".join(unresolved["secid"].astype(str).head(10).tolist())
        raise RuntimeError("active non-technical instruments have unresolved family_code: " + sample)


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    parser.add_argument("--config", default=DEFAULT_CONFIG_REL)
    parser.add_argument("--input", default="")
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    root = Path.cwd().resolve()
    snapshot_date = str(args.snapshot_date)
    data_root = resolve_data_root(str(args.data_root))
    config = read_json(root / str(args.config))
    input_path = Path(args.input).expanduser().resolve() if args.input else contract_path(root, data_root, NORMALIZED_CONTRACT_REL, snapshot_date)
    output_path = Path(args.output).expanduser().resolve() if args.output else contract_path(root, data_root, FAMILY_MAPPING_CONTRACT_REL, snapshot_date)
    if not input_path.exists():
        raise FileNotFoundError("Missing normalized registry artifact: " + str(input_path))
    normalized = pd.read_parquet(input_path)
    family_mapping = build_family_mapping(normalized, config, snapshot_date)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    family_mapping.to_parquet(output_path, index=False)
    check = pd.read_parquet(output_path)
    validate_family_mapping(check)
    print_json_line("family_mapping_input", str(input_path))
    print_json_line("family_mapping_output", str(output_path))
    print_json_line("family_mapping_rows", int(len(check)))
    print_json_line("family_mapping_schema_validation", "pass")
    print_json_line("family_mapping_primary_key_validation", "pass")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
