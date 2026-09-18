#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import stat
import subprocess
import tempfile
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures.slice1_common import DEFAULT_EXCLUDED
from moex_data.futures.slice1_common import DEFAULT_WHITELIST
from moex_data.futures.slice1_common import SHORT_HISTORY_ALLOWED
from moex_data.futures.slice1_common import parse_list
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

SCHEMA_MANIFEST = "futures_registry_refresh_manifest.v1"
REQUIRED_CONTRACTS = [
    "contracts/datasets/futures_registry_snapshot_contract.md",
    "contracts/datasets/futures_normalized_instrument_registry_contract.md",
    "contracts/datasets/futures_family_mapping_contract.md",
    "contracts/datasets/futures_algopack_tradestats_availability_report_contract.md",
    "contracts/datasets/futures_futoi_availability_report_contract.md",
    "contracts/datasets/futures_obstats_availability_report_contract.md",
    "contracts/datasets/futures_hi2_availability_report_contract.md",
    "contracts/datasets/futures_liquidity_screen_contract.md",
    "contracts/datasets/futures_history_depth_screen_contract.md",
    "contracts/datasets/futures_registry_refresh_manifest_contract.md",
]
REQUIRED_CONFIGS = [
    "configs/datasets/futures_algopack_availability_sources_config.json",
    "configs/datasets/futures_slice1_universe_config.json",
    "configs/datasets/futures_liquidity_screen_thresholds_config.json",
    "configs/datasets/futures_history_depth_thresholds_config.json",
]
CONTRACTS = {
    "registry_snapshot": "contracts/datasets/futures_registry_snapshot_contract.md",
    "normalized_registry": "contracts/datasets/futures_normalized_instrument_registry_contract.md",
    "family_mapping": "contracts/datasets/futures_family_mapping_contract.md",
    "algopack_fo_tradestats": "contracts/datasets/futures_algopack_tradestats_availability_report_contract.md",
    "moex_futoi": "contracts/datasets/futures_futoi_availability_report_contract.md",
    "algopack_fo_obstats": "contracts/datasets/futures_obstats_availability_report_contract.md",
    "algopack_fo_hi2": "contracts/datasets/futures_hi2_availability_report_contract.md",
    "liquidity_screen": "contracts/datasets/futures_liquidity_screen_contract.md",
    "history_depth_screen": "contracts/datasets/futures_history_depth_screen_contract.md",
}


def contract_value(root, rel, key):
    prefix = key + ":"
    for raw in (root / rel).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return ""


def contract_path(root, data_root, rel, snapshot_date):
    pattern = contract_value(root, rel, "path_pattern")
    if not pattern.startswith("${MOEX_DATA_ROOT}"):
        raise RuntimeError("Unsupported contract path_pattern: " + rel)
    tail = pattern[len("${MOEX_DATA_ROOT}"):].lstrip("/")
    tail = tail.replace("{snapshot_date}", snapshot_date).replace("YYYY-MM-DD", snapshot_date)
    return data_root / tail


def output_paths(root, data_root, snapshot_date, run_date):
    out = {key: str(contract_path(root, data_root, rel, snapshot_date)) for key, rel in CONTRACTS.items()}
    out["manifest"] = str(data_root / "futures" / "runs" / "registry_refresh" / ("run_date=" + run_date) / "manifest.json")
    return out



def _read_manifest_bytes(path):
    """Read original bytes without following a file symlink or opening a FIFO."""
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise RuntimeError("manifest_not_regular_file: " + str(path))
        return source.read()


def _sync_manifest_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _manifest_temp(directory, payload, mode=None):
    """A partial write is never published as an archive or daily manifest."""
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", dir=directory, prefix=".manifest.", delete=False) as output:
            temporary = Path(output.name)
            output.write(payload)
            output.flush()
            if mode is not None:
                os.fchmod(output.fileno(), mode)
            os.fsync(output.fileno())
        return temporary
    except BaseException:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise


def _archive_manifest_bytes(directory, payload):
    """Publish a byte-addressed archive with no overwrite, including on retry."""
    archive = directory / (hashlib.sha256(payload).hexdigest() + ".json")
    try:
        existing = _read_manifest_bytes(archive)
    except FileNotFoundError:
        temporary = _manifest_temp(directory, payload)
        try:
            try:
                os.link(temporary, archive)
            except FileExistsError:
                pass
            existing = _read_manifest_bytes(archive)
        finally:
            temporary.unlink(missing_ok=True)
    if existing != payload:
        raise RuntimeError("manifest_archive_conflict: " + str(archive))
    _sync_manifest_directory(directory)


def write_manifest(path, manifest):
    """Retain every distinct manifest before atomic daily publication (POSIX)."""
    import fcntl

    payload = (json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    # Keep the lock inode: unlinking it would let later writers bypass a holder.
    fd = os.open(path.parent / ".manifest.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(fd, "rb") as lock:
        if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
            raise RuntimeError("manifest_lock_not_regular_file")
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            previous = _read_manifest_bytes(path)
        except FileNotFoundError:
            previous = None
        mode = stat.S_IMODE(path.stat(follow_symlinks=False).st_mode) if previous is not None else None
        history = path.parent / "manifest_history"
        history.mkdir(exist_ok=True)
        if history.is_symlink() or not history.is_dir():
            raise RuntimeError("manifest_history_not_directory: " + str(history))
        if previous is not None:
            _archive_manifest_bytes(history, previous)
        _archive_manifest_bytes(history, payload)
        # Persist the history directory's entry before replacing the daily file.
        _sync_manifest_directory(path.parent)
        temporary = _manifest_temp(path.parent, payload, mode)
        try:
            os.replace(temporary, path)
            _sync_manifest_directory(path.parent)
        finally:
            temporary.unlink(missing_ok=True)


def run_child(root, component_id, command, expected):
    started_at = time.time()
    # A child interpreter does not inherit this process's sys.path.
    child_env = os.environ.copy()
    source_root = str((root / "src").resolve())
    inherited_pythonpath = child_env.get("PYTHONPATH", "")
    child_env["PYTHONPATH"] = source_root + (os.pathsep + inherited_pythonpath if inherited_pythonpath else "")
    proc = subprocess.run(command, cwd=str(root), text=True, capture_output=True, env=child_env)
    completed_at = time.time()
    item = {"component_id": component_id, "command": command, "returncode": int(proc.returncode), "stdout_tail": proc.stdout[-4000:], "stderr_tail": proc.stderr[-4000:], "duration_sec": round(completed_at - started_at, 3), "json_line_outputs": parse_json_line_output(proc.stdout), "status": "fail", "validation_status": "not_validated"}
    if proc.returncode != 0:
        item["failure_reason"] = "component_returncode_nonzero"
        return item
    missing = []
    stale = []
    for key, raw_path in expected.items():
        path = Path(str(raw_path))
        if not path.exists():
            missing.append(key + "=" + str(path))
        elif path.stat().st_mtime < started_at - 1.0:
            stale.append(key + "=" + str(path))
    if missing:
        item["failure_reason"] = "expected_output_missing: " + "; ".join(missing)
        item["validation_status"] = "fail"
        return item
    if stale:
        item["failure_reason"] = "expected_output_stale: " + "; ".join(stale)
        item["validation_status"] = "fail"
        return item
    item["status"] = "pass"
    item["validation_status"] = "pass"
    item["expected_outputs"] = expected
    return item


def parse_json_line_output(text):
    parsed = {}
    for raw in str(text or "").splitlines():
        if ":" not in raw:
            continue
        key, value = raw.split(":", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            continue
        try:
            parsed[key] = json.loads(value)
        except Exception:
            continue
    return parsed


def basic_rows_summary(path):
    frame = pd.read_parquet(path)
    return {"rows": int(len(frame)), "validation_status": "pass" if int(len(frame)) > 0 else "fail"}


def family_mapping_summary(path):
    frame = pd.read_parquet(path)
    if "mapping_status" not in frame.columns:
        return {"rows": int(len(frame)), "validation_status": "fail", "failure_reason": "missing mapping_status"}
    counts = {str(k): int(v) for k, v in frame["mapping_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    return {"rows": int(len(frame)), "status_counts": counts, "validation_status": "pass" if int(len(frame)) > 0 else "fail"}


def availability_summary(path, whitelist):
    frame = pd.read_parquet(path)
    required = ["secid", "availability_status"]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        return {"rows": int(len(frame)), "validation_status": "fail", "failure_reason": "missing " + ",".join(missing)}
    counts = {str(k): int(v) for k, v in frame["availability_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    by_secid = {}
    status = "pass" if int(len(frame)) > 0 else "fail"
    for secid in whitelist:
        row = frame.loc[frame["secid"].astype(str).str.upper() == secid.upper()].tail(1)
        if row.empty:
            by_secid[secid] = "missing"
            status = "fail"
            continue
        value = str(row.iloc[0].get("availability_status", ""))
        by_secid[secid] = value
        if value != "available":
            status = "fail"
    return {"rows": int(len(frame)), "status_counts": counts, "whitelist_status": by_secid, "validation_status": status}


def history_depth_row_status(row, secid):
    value = str(row.get("history_depth_status", ""))
    validation_status = str(row.get("validation_status", ""))
    review_status = str(row.get("review_status", ""))
    if value == "pass":
        return "pass", "history_depth_pass"
    if value == "review_required" and validation_status == "metrics_computed" and review_status == "ready_for_pm_review":
        return "pass", "history_depth_review_ready"
    if secid in SHORT_HISTORY_ALLOWED and value == "review_required" and validation_status == "metrics_computed" and review_status == "ready_for_pm_review":
        return "pass", "short_history_review_ready"
    if value in ["fail", "not_checked", "blocked", "missing", ""]:
        return "fail", "history_depth_blocked:" + value
    return "fail", "history_depth_malformed:" + value


def screen_summary(path, field, whitelist):
    frame = pd.read_parquet(path)
    if "secid" not in frame.columns or field not in frame.columns:
        return {"rows": int(len(frame)), "validation_status": "fail", "failure_reason": "missing required screen fields"}
    if field == "history_depth_status":
        missing = [x for x in ["validation_status", "review_status"] if x not in frame.columns]
        if missing:
            return {"rows": int(len(frame)), "validation_status": "fail", "failure_reason": "missing " + ",".join(missing)}
    by_secid = {}
    review_gate = {}
    status = "pass" if int(len(frame)) > 0 else "fail"
    for secid in whitelist:
        row = frame.loc[frame["secid"].astype(str).str.upper() == secid.upper()].tail(1)
        if row.empty:
            by_secid[secid] = "missing"
            status = "fail"
            continue
        value = str(row.iloc[0].get(field, ""))
        by_secid[secid] = value
        if field == "history_depth_status":
            row_status, reason = history_depth_row_status(row.iloc[0], secid)
            review_gate[secid] = reason
            if row_status != "pass":
                status = "fail"
        elif value != "pass":
            status = "fail"
    counts = {str(k): int(v) for k, v in frame[field].astype(str).value_counts(dropna=False).to_dict().items()}
    result = {"rows": int(len(frame)), "status_counts": counts, "whitelist_status": by_secid, "validation_status": status}
    if field == "history_depth_status":
        result["review_gate_status"] = review_gate
    return result


def validate_outputs(outputs, whitelist):
    summaries = {
        "registry_snapshot": basic_rows_summary(outputs["registry_snapshot"]),
        "normalized_registry": basic_rows_summary(outputs["normalized_registry"]),
        "family_mapping": family_mapping_summary(outputs["family_mapping"]),
        "algopack_fo_tradestats": availability_summary(outputs["algopack_fo_tradestats"], whitelist),
        "moex_futoi": availability_summary(outputs["moex_futoi"], whitelist),
        "algopack_fo_obstats": availability_summary(outputs["algopack_fo_obstats"], whitelist),
        "algopack_fo_hi2": availability_summary(outputs["algopack_fo_hi2"], whitelist),
        "liquidity_screen": screen_summary(outputs["liquidity_screen"], "liquidity_status", whitelist),
        "history_depth_screen": screen_summary(outputs["history_depth_screen"], "history_depth_status", whitelist),
    }
    blockers = [key + "_validation_failed" for key, value in summaries.items() if value.get("validation_status") != "pass"]
    return summaries, blockers


# The current mode validates candidate evidence, never admission to a dataset.
SLICE1_COMPAT = "slice1_compat"
CURRENT_REGISTRY = "current_registry"
AVAILABILITY_ENDPOINTS = (
    "algopack_fo_tradestats", "moex_futoi", "algopack_fo_obstats", "algopack_fo_hi2",
)


def _require_current(condition, message):
    if not condition:
        raise ValueError(message)


def _current_text(frame, columns):
    missing = [name for name in columns if name not in frame.columns]
    _require_current(not missing, "missing_fields:" + ",".join(missing))
    for name in columns:
        valid = frame[name].map(lambda value: isinstance(value, str) and bool(value.strip()) and value == value.strip())
        _require_current(bool(valid.all()), "invalid_text_field:" + name)


def _current_keys(frame):
    _current_text(frame, ("board", "secid"))
    keys = list(zip(frame["board"].str.upper(), frame["secid"].str.upper()))
    _require_current(len(set(keys)) == len(keys), "duplicate_instrument_identity")
    return set(keys)


def _current_frame(path, snapshot_date, required, schema=None):
    frame = pd.read_parquet(path)
    _require_current(frame.columns.is_unique, "duplicate_columns")
    _current_text(frame, ("snapshot_date", "board", "secid", *required))
    _require_current(bool(frame["snapshot_date"].eq(snapshot_date).all()), "snapshot_date_mismatch")
    _current_keys(frame)
    if schema is not None:
        _current_text(frame, ("schema_version",))
        _require_current(bool(frame["schema_version"].eq(schema).all()), "schema_version_mismatch")
    return frame


def _current_coverage(frame, expected):
    actual, wanted = _current_keys(frame), _current_keys(expected)
    missing, extra = sorted(wanted - actual), sorted(actual - wanted)
    _require_current(not missing and not extra, "coverage_mismatch:missing=" + str(missing[:10]) + ";extra=" + str(extra[:10]))


def _current_agreement(frame, expected, field, expected_field=None):
    _current_text(frame, (field,))
    expected_field = expected_field or field
    _current_text(expected, (expected_field,))
    wanted = {(row.board.upper(), row.secid.upper()): getattr(row, expected_field)
              for row in expected.itertuples(index=False)}
    for row in frame.itertuples(index=False):
        _require_current(getattr(row, field) == wanted.get((row.board.upper(), row.secid.upper())),
                         "inconsistent_registry_field:" + field)


def _current_window(frame, prefix, bounds):
    first, last = prefix + "_from", prefix + "_till"
    _current_text(frame, (first, last))
    _require_current(bool(frame[first].eq(bounds[0]).all()) and bool(frame[last].eq(bounds[1]).all()),
                     "request_window_mismatch:" + prefix)


def validate_current_outputs(outputs, snapshot_date, *, from_date="", till="", evidence_only=False):
    """Validate exact producer scopes; retain negative outcomes for eligibility."""
    from moex_data.futures import registry_evidence_artifacts_producer as evidence

    summaries = {}
    key = "registry_snapshot"
    try:
        source = evidence.availability
        probe_bounds = source.date_range_defaults(
            snapshot_date, argparse.Namespace(from_date=from_date, till=till, lookback_days=14))
        screen_bounds = base.date_range_defaults(
            snapshot_date, argparse.Namespace(from_date=from_date, till=till, history_lookback_days=365))

        def record(name, frame, field=None):
            item = {"rows": int(len(frame)), "validation_status": "pass",
                    "validation_scope": "current_registry_evidence_only"}
            if field is not None:
                item["status_counts"] = {str(k): int(v) for k, v in frame[field].value_counts().items()}
            summaries[name] = item

        registry = _current_frame(outputs[key], snapshot_date, ("snapshot_id", "engine", "market"))
        _require_current(not registry.empty, "empty_registry")
        _require_current(registry["snapshot_id"].nunique() == 1, "ambiguous_snapshot_id")
        record(key, registry)

        key = "normalized_registry"
        normalized = _current_frame(outputs[key], snapshot_date,
                                    ("snapshot_id", "source_snapshot_id", "engine", "market", "family_code"),
                                    source.SCHEMA_NORMALIZED_REGISTRY)
        _current_coverage(normalized, registry)
        for field in ("snapshot_id", "engine", "market"):
            _current_agreement(normalized, registry, field)
        _current_agreement(normalized, registry, "source_snapshot_id", "snapshot_id")
        # Reuse the acquisition producer's RFUD candidate scope, not a new whitelist.
        candidates = evidence.select_all_rfud_instruments(normalized)
        record(key, normalized)
        summaries[key]["candidate_count"] = int(len(candidates))

        key = "family_mapping"
        mapping = _current_frame(outputs[key], snapshot_date,
                                 ("mapping_id", "snapshot_id", "mapping_status", "mapping_source", "validation_status"),
                                 evidence.SCHEMA_FAMILY_MAPPING)
        _require_current("family_code" in mapping.columns, "missing_fields:family_code")
        _current_coverage(mapping, candidates)
        _current_agreement(mapping, candidates, "snapshot_id")
        _require_current(bool(mapping["mapping_status"].isin(("pass", "unresolved")).all()), "invalid_mapping_status")
        mapped = mapping["mapping_status"].eq("pass")
        _current_agreement(mapping.loc[mapped], candidates, "family_code")
        _require_current(bool(mapping.loc[mapped, "mapping_source"].eq("derived_rule").all())
                         and bool(mapping.loc[mapped, "validation_status"].eq("pass").all()),
                         "incoherent_mapping_status")
        _require_current(bool(mapping.loc[~mapped, "mapping_source"].eq("unresolved").all())
                         and bool(mapping.loc[~mapped, "validation_status"].eq("failed").all()),
                         "incoherent_unresolved_mapping")
        record(key, mapping, "mapping_status")

        reports = {}
        for key in AVAILABILITY_ENDPOINTS:
            frame = _current_frame(outputs[key], snapshot_date,
                                   ("availability_report_id", "family_code", "endpoint_id", "source_endpoint_url",
                                    "availability_status", "probe_status"),
                                   source.REPORT_SCHEMA_BY_ENDPOINT[key])
            _current_coverage(frame, candidates)
            _current_agreement(frame, candidates, "family_code")
            _require_current(bool(frame["endpoint_id"].eq(key).all()), "endpoint_id_mismatch")
            _current_window(frame, "probe", probe_bounds)
            _require_current(bool(frame["probe_status"].eq("completed").all()), "probe_not_completed")
            _require_current(bool(frame["availability_status"].isin(
                ("available", "unavailable", "partial", "error", "not_checked")).all()), "invalid_availability_status")
            record(key, frame, "availability_status")
            reports[key] = frame

        # Use the existing metrics producer's selection; never require all candidates available.
        key = "liquidity_screen"
        selected = base.selected_instruments_from_artifacts(normalized, reports["algopack_fo_tradestats"])
        if evidence_only:
            return summaries, []
        for key, field, schema in (
            ("liquidity_screen", "liquidity_status", base.SCHEMA_LIQUIDITY_SCREEN),
            ("history_depth_screen", "history_depth_status", base.SCHEMA_HISTORY_DEPTH_SCREEN),
        ):
            frame = _current_frame(outputs[key], snapshot_date,
                                   ("family_code", key + "_id", field, "validation_status", "review_status", "fetch_status", "review_notes"),
                                   schema)
            _current_coverage(frame, selected)
            _current_agreement(frame, selected, "family_code")
            _current_window(frame, "screen", screen_bounds)
            _require_current(bool(frame[field].isin(("pass", "fail", "review_required")).all()), "invalid_screen_status")
            _require_current(bool(frame["fetch_status"].isin(("completed", "failed")).all()), "invalid_fetch_status")
            computed = frame[field].ne("fail")
            _require_current(bool(frame.loc[computed, "validation_status"].eq("metrics_computed").all())
                             and bool(frame.loc[computed, "review_status"].eq("ready_for_pm_review").all())
                             and bool(frame.loc[computed, "fetch_status"].eq("completed").all()),
                             "incoherent_computed_screen")
            _require_current(bool(frame.loc[~computed, "validation_status"].eq("failed").all())
                             and bool(frame.loc[~computed, "review_status"].eq("blocked").all()),
                             "incoherent_failed_screen")
            record(key, frame, field)
            summaries[key]["expected_instrument_count"] = int(len(selected))
        return summaries, []
    except Exception as exc:
        summaries[key] = {"validation_status": "fail", "validation_scope": "current_registry_evidence_only",
                          "failure_reason": type(exc).__name__ + ":" + str(exc)[:600]}
        return summaries, [key + "_validation_failed"]


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", base.DEFAULT_ISS_BASE_URL))
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", base.DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--availability-max-workers", type=int, default=int(os.getenv("MOEX_AVAILABILITY_MAX_WORKERS", "4")))
    parser.add_argument("--validation-mode", choices=[SLICE1_COMPAT, CURRENT_REGISTRY], default=SLICE1_COMPAT)
    parser.add_argument("--whitelist", default=None)
    parser.add_argument("--excluded", default=None)
    args = parser.parse_args()
    current_mode = args.validation_mode == CURRENT_REGISTRY
    if current_mode and (args.whitelist is not None or args.excluded is not None):
        parser.error("current_registry forbids --whitelist and --excluded; use eligibility downstream")
    root = Path.cwd().resolve()
    data_root = base.resolve_data_root(args)
    whitelist = [] if current_mode else parse_list(args.whitelist, DEFAULT_WHITELIST)
    excluded = [] if current_mode else parse_list(args.excluded, DEFAULT_EXCLUDED)
    base.assert_files_exist(root, REQUIRED_CONTRACTS + REQUIRED_CONFIGS)
    outputs = output_paths(root, data_root, args.snapshot_date, args.run_date)
    started_ts = utc_now_iso()
    identity = [args.snapshot_date, started_ts, ",".join(whitelist)]
    if current_mode:
        identity.append(CURRENT_REGISTRY)
    run_id = "futures_registry_refresh_" + args.run_date + "_" + stable_id(identity)
    common = ["--snapshot-date", args.snapshot_date, "--data-root", str(data_root), "--timeout", str(args.timeout), "--iss-base-url", args.iss_base_url, "--apim-base-url", args.apim_base_url]
    if args.from_date:
        common += ["--from", args.from_date]
    if args.till:
        common += ["--till", args.till]
    run_started_epoch = time.time()
    child_items = []
    evidence_cmd = [sys.executable, str(root / "src/moex_data/futures/registry_evidence_artifacts_producer.py")] + common + ["--availability-max-workers", str(args.availability_max_workers)]
    child_items.append(run_child(root, "registry_evidence_artifacts_producer", evidence_cmd, {k: outputs[k] for k in ["registry_snapshot", "normalized_registry", "family_mapping", "algopack_fo_tradestats", "moex_futoi", "algopack_fo_obstats", "algopack_fo_hi2"]}))
    output_summaries, validation_blockers = {}, []
    if current_mode and child_items[-1].get("status") == "pass":
        output_summaries, validation_blockers = validate_current_outputs(
            outputs, args.snapshot_date, from_date=args.from_date, till=args.till, evidence_only=True)
    if child_items[-1].get("status") == "pass" and not validation_blockers:
        screen_cmd = [sys.executable, "-m", "moex_data.futures.liquidity_history_metrics_probe"] + common + ["--full-history-proven"]
        child_items.append(run_child(root, "liquidity_history_metrics_probe", screen_cmd, {"liquidity_screen": outputs["liquidity_screen"], "history_depth_screen": outputs["history_depth_screen"]}))
    final_status = "pass" if len(child_items) == 2 and all(x.get("status") == "pass" for x in child_items) else "fail"
    blockers = [str(x.get("component_id")) + ":" + str(x.get("failure_reason")) for x in child_items if x.get("status") != "pass"] + validation_blockers
    child_duration_summary = {str(x.get("component_id")): x.get("duration_sec") for x in child_items if x.get("component_id")}
    availability_probe_timing_summary = {}
    if child_items:
        parsed_child_stdout = child_items[0].get("json_line_outputs") or parse_json_line_output(child_items[0].get("stdout_tail", ""))
        availability_probe_timing_summary = parsed_child_stdout.get("availability_probe_timing_summary") or {}
    if final_status == "pass":
        if current_mode:
            output_summaries, validation_blockers = validate_current_outputs(
                outputs, args.snapshot_date, from_date=args.from_date, till=args.till)
        else:
            output_summaries, validation_blockers = validate_outputs(outputs, whitelist)
        blockers += validation_blockers
        if validation_blockers:
            final_status = "fail"
    manifest = {"schema_version": SCHEMA_MANIFEST, "run_id": run_id, "run_date": args.run_date, "snapshot_date": args.snapshot_date, "refresh_from": args.from_date or None, "refresh_till": args.till or None, "started_ts": started_ts, "completed_ts": utc_now_iso(), "total_duration_sec": round(time.time() - run_started_epoch, 3), "runner_whitelist_applied": whitelist, "excluded_instruments_confirmed": excluded, "availability_max_workers": int(args.availability_max_workers), "component_execution_order": ["registry_evidence_artifacts_producer", "liquidity_history_metrics_probe"], "child_component_status": child_items, "child_duration_summary": child_duration_summary, "availability_probe_timing_summary": availability_probe_timing_summary, "child_output_references": {x["component_id"]: {"status": x.get("status"), "validation_status": x.get("validation_status"), "expected_outputs": x.get("expected_outputs")} for x in child_items}, "output_artifacts": outputs, "output_summaries": output_summaries, "artifact_validation_status": "pass" if final_status == "pass" else "fail", "registry_refresh_result_verdict": final_status, "blockers": blockers}
    manifest["validation_mode"] = args.validation_mode
    path = Path(outputs["manifest"])
    write_manifest(path, manifest)
    print_json_line("registry_refresh_manifest_path", str(path))
    print_json_line("child_component_status", manifest["child_output_references"])
    print_json_line("child_duration_summary", child_duration_summary)
    print_json_line("availability_probe_timing_summary", availability_probe_timing_summary)
    print_json_line("artifact_validation_status", manifest["artifact_validation_status"])
    print_json_line("registry_refresh_result_verdict", final_status)
    if blockers:
        print_json_line("blockers", blockers)
    return 0 if final_status == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
