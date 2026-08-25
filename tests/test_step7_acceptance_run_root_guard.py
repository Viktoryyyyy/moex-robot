import json
from pathlib import Path

import pytest

from moex_data import step7_rub_native_d1_w1_acceptance as acceptance

ROOT_PREFIX = "${MOEX_DATA_ROOT}/"


def _rooted(root: Path, path: Path) -> str:
    return ROOT_PREFIX + path.relative_to(root).as_posix()


def _manifest(root: Path, frozen_ref: str) -> Path:
    run_root = root / "runs" / "step7_rub_native_d1_w1" / "run_id=guard"
    manifest = run_root / "state" / "frozen_inputs" / "instrument_id=usdrubf_futures_family" / "frozen_raw_manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps({"partitions": [{"frozen_ref": frozen_ref}]}), encoding="utf-8")
    return manifest


def test_stage7_guard_accepts_frozen_partition_inside_declared_run_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    frozen = tmp_path / "runs" / "step7_rub_native_d1_w1" / "run_id=guard" / "inputs" / "dataset_id=futures_raw_5m" / "instrument_id=usdrubf_futures_family" / "trade_date=2026-08-17" / "part.parquet"
    frozen.parent.mkdir(parents=True, exist_ok=True)
    frozen.write_bytes(b"fixture")
    manifest = _manifest(tmp_path, _rooted(tmp_path, frozen))
    assert acceptance._guard_frozen_refs_inside_run_root(manifest).name == "run_id=guard"


def test_stage7_guard_rejects_frozen_partition_outside_declared_run_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    escaped = tmp_path / "market" / "raw" / "part.parquet"
    escaped.parent.mkdir(parents=True, exist_ok=True)
    escaped.write_bytes(b"fixture")
    manifest = _manifest(tmp_path, _rooted(tmp_path, escaped))
    with pytest.raises(ValueError, match="escaped approved root|escaped immutable Stage 7 input root"):
        acceptance._guard_frozen_refs_inside_run_root(manifest)
