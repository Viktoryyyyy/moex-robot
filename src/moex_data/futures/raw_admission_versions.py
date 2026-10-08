"""Immutable byte versions for the existing raw producers and admission readers.

These receipts attest publication now. They do not reconstruct lost source
versions or turn a current acquisition into historical point-in-time evidence.
"""
import hashlib
import io
import json
import os
import re
import tempfile
from pathlib import Path

import pandas as pd

SCHEMA = "futures_raw_5m_admission_versions.v1"
STORE = Path("futures/raw_5m_admission")


def encoded(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, default=str) + "\n").encode("utf-8")


def sha256(payload):
    return hashlib.sha256(payload).hexdigest()


def checked_relative(root, path):
    root, path = Path(root).resolve(), Path(path)
    # Reject aliases in every component, including the object store itself.
    lexical = Path(os.path.abspath(path))
    relative = lexical.relative_to(root)
    candidate = root
    for component in relative.parts:
        candidate = candidate / component
        if candidate.is_symlink():
            raise RuntimeError("raw admission refuses a symlink")
    relative = path.resolve().relative_to(root)
    if relative.parts[:1] != ("futures",):
        raise RuntimeError("raw admission path outside futures")
    return relative


def immutable(path, payload):
    """Create once; retries compare bytes, never overwrite an existing version."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.read_bytes() != payload:
            raise RuntimeError("immutable raw admission version conflict")
        return
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.is_symlink() or path.read_bytes() != payload:
                raise RuntimeError("immutable raw admission version conflict")
    finally:
        temporary.unlink(missing_ok=True)


def object_path(root, digest):
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise RuntimeError("invalid raw admission object digest")
    path = Path(root) / STORE / "objects" / digest
    checked_relative(root, path)
    return path


def read_object(root, digest):
    path = object_path(root, digest)
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("raw admission object missing or not regular")
    payload = path.read_bytes()
    if sha256(payload) != digest:
        raise RuntimeError("raw admission object digest mismatch")
    return payload


def read_frame(root, digest):
    return pd.read_parquet(io.BytesIO(read_object(root, digest)))


def preserve(root, path, payload):
    relative = checked_relative(root, path)
    digest = sha256(payload)
    immutable(object_path(root, digest), payload)
    version_path = Path(root) / STORE / "versions" / relative / (digest + ".json")
    checked_relative(root, version_path)
    immutable(version_path,
              encoded({"path": relative.as_posix(), "sha256": digest}))
    return digest


def publish_bytes(root, path, payload):
    """Archive previous and next exact bytes before replacing a current view."""
    path = Path(path)
    checked_relative(root, path)
    if path.exists():
        previous = path.read_bytes()
        preserve(root, path, previous)
        if previous == payload:
            return sha256(payload)
    digest = preserve(root, path, payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return digest


def parquet_bytes(frame):
    stream = io.BytesIO()
    frame.to_parquet(stream, index=False)
    return stream.getvalue()


def publish_partition(root, path, frame):
    return publish_bytes(root, path, parquet_bytes(frame))


def publish_admission(root, quality_path, quality, manifest_path, manifest):
    """Freeze a complete producer cohort, including all versions it wrote."""
    root = Path(root)
    members = manifest.get("output_partitions", manifest.get("partition_paths_created", []))
    versions = {}
    for name in members:
        path = Path(name)
        relative = checked_relative(root, path).as_posix()
        if not relative.startswith("futures/raw_5m/"):
            raise RuntimeError("admission member is not a raw 5m partition")
        versions[relative] = preserve(root, path, path.read_bytes())
    versioned = dict(manifest, raw_partition_versions=versions,
                     admission_version_scope="publication_bytes_not_historical_PIT")
    qbytes, mbytes = parquet_bytes(quality), encoded(versioned)
    qhash = preserve(root, quality_path, qbytes)
    mhash = preserve(root, manifest_path, mbytes)
    receipt = {"schema_version": SCHEMA, "quality_sha256": qhash,
               "quality_path": checked_relative(root, quality_path).as_posix(),
               "manifest_sha256": mhash,
               "manifest_path": checked_relative(root, manifest_path).as_posix()}
    rbytes = encoded(receipt)
    # The immutable evidence is complete before current quality/manifest views
    # are published. A failed view write cannot destroy the prior versions.
    receipt_path = root / STORE / "receipts" / (sha256(rbytes) + ".json")
    checked_relative(root, receipt_path)
    immutable(receipt_path, rbytes)
    publish_bytes(root, quality_path, qbytes)
    publish_bytes(root, manifest_path, mbytes)
    return versioned


def admission_reports(root):
    """Return verified immutable quality/manifest pairs, never current aliases."""
    reports = []
    checked_relative(root, Path(root) / STORE / "receipts")
    for path in sorted((Path(root) / STORE / "receipts").glob("*.json")):
        if path.is_symlink():
            raise RuntimeError("raw admission receipt is a symlink")
        payload = path.read_bytes()
        if path.stem != sha256(payload):
            raise RuntimeError("raw admission receipt digest mismatch")
        receipt = json.loads(payload)
        if receipt.get("schema_version") != SCHEMA:
            raise RuntimeError("unsupported raw admission receipt schema")
        qbytes = read_object(root, receipt.get("quality_sha256"))
        read_object(root, receipt.get("manifest_sha256"))
        frame = pd.read_parquet(io.BytesIO(qbytes))
        qp = object_path(root, receipt["quality_sha256"])
        mp = object_path(root, receipt["manifest_sha256"])
        frame["_quality_report_path"] = str(qp)
        frame["_quality_report_sha256"] = receipt["quality_sha256"]
        frame["_admission_manifest_path"] = str(mp)
        reports.append((frame, str(qp), mp))
    return reports
