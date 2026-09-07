"""Content-addressed, pair-scoped acceptance of canonical FUTOI publications."""
from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from collections.abc import Mapping

import pandas as pd

from . import futoi_live_factual_refresh_source_native as source

SCHEMA = "futoi_publication_audit.v1"
POLICY = "latest_source_timestamp_no_fallback_exact_balance_v1"


def _freeze_json(root: Path, value: dict) -> dict:
    content = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    with tempfile.NamedTemporaryFile(dir=root, suffix=".json", delete=False) as handle:
        path = Path(handle.name)
        handle.write(content)
    try:
        digest = hashlib.sha256(content).hexdigest()
        frozen = source._freeze_artifact(root, path, digest)
        return {"ref": source._rooted_ref(root, frozen), "sha256": digest}
    finally:
        path.unlink(missing_ok=True)


def audited_latest(root: Path, frame: pd.DataFrame, provenance: dict, **identity) -> dict:
    """Archive all per-timestamp outcomes before returning or rejecting the frontier."""
    # The bytes used for replay must be the canonical, already frozen partition.
    path = _verified_path(root, provenance["raw_partition_ref"], provenance["raw_partition_sha256"])
    frozen_frame = pd.read_parquet(path)
    pd.testing.assert_frame_equal(frame, frozen_frame)
    publications = []
    parsed = pd.to_datetime(frame["ts"], errors="raise")
    if parsed.isna().any():
        raise source.FutoiSourceNativeRefreshError("invalid source timestamp in publication audit")
    for ts, rows in frame.groupby(parsed, sort=True):
        item = {"source_ts": str(ts), "row_count": len(rows)}
        # Versions and raw values are retained in the hash-bound partition.
        try:
            fact = source.latest_aligned_factual(rows, **identity)
            item.update(status="PASS", factual=fact)
        except source.FutoiSourceNativeRefreshError as exc:
            item.update(status="REJECTED", reason=str(exc))
        publications.append(item)
    failure = None
    try:
        factual = source.latest_aligned_factual(frame, **identity)
    except source.FutoiSourceNativeRefreshError as exc:
        failure = exc
        factual = None
    report = {"schema_version": SCHEMA, "policy": POLICY,
              "instrument_id": identity["expected_instrument_id"],
              "trade_date": identity["expected_trade_date"], "provenance": provenance,
              "publications": publications, "publication_count": len(publications),
              "rejected_count": sum(p["status"] == "REJECTED" for p in publications),
              "latest_status": "REJECTED" if failure else "PASS", "latest_factual": factual,
              "latest_error": str(failure) if failure else None,
              "historical_authority": False, "provider_root_cause_established": False}
    receipt = _freeze_json(root, report)
    provenance["publication_audit"] = receipt
    if failure:
        failure.publication_audit = receipt
        failure.attempt_provenance = dict(provenance)
        raise failure
    return factual


def _verified_path(root: Path, ref: str, digest: str) -> Path:
    if not isinstance(ref, str) or not ref.startswith(source.ROOT_REF_PREFIX):
        raise ValueError("invalid audit evidence reference")
    path = root / ref.removeprefix(source.ROOT_REF_PREFIX)
    source._rooted_ref(root, path)
    if not isinstance(digest, str) or len(digest) != 64 or source._sha256_file(path) != digest:
        raise ValueError("audit evidence SHA mismatch")
    return path


def verify_current(root: Path, record: Mapping) -> dict:
    """Bind consumer admission to archived audit, raw bytes and exact current fact."""
    if record.get("status") != "FRESH":
        raise ValueError("latest attempt is not fresh")
    provenance = record.get("provenance")
    if not isinstance(provenance, Mapping):
        raise ValueError("audit provenance missing")
    receipt = provenance.get("publication_audit")
    if not isinstance(receipt, Mapping):
        raise ValueError("publication audit missing")
    path = _verified_path(root, receipt.get("ref"), receipt.get("sha256"))
    report = json.loads(path.read_text())
    if not isinstance(report, Mapping) or not isinstance(report.get("provenance"), Mapping):
        raise ValueError("invalid publication audit structure")
    if (report.get("schema_version") != SCHEMA or report.get("policy") != POLICY
            or report.get("instrument_id") != source.CR_INSTRUMENT_ID
            or report.get("latest_status") != "PASS"
            or report.get("latest_factual") != record.get("factual")):
        raise ValueError("publication audit/current fact mismatch")
    for key in ("raw_partition", "raw_quality_report", "raw_refresh_manifest"):
        ref, digest = provenance.get(key + "_ref"), provenance.get(key + "_sha256")
        if report["provenance"].get(key + "_ref") != ref or report["provenance"].get(key + "_sha256") != digest:
            raise ValueError("publication audit provenance mismatch")
        _verified_path(root, ref, digest)
    return {"policy": POLICY, "audit_sha256": receipt["sha256"],
            "publication_count": report["publication_count"], "rejected_count": report["rejected_count"],
            "scope": "current_intraday_latest_pair_only"}
