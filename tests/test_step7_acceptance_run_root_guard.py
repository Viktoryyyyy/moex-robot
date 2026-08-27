import hashlib
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from moex_data import step7_rub_native_d1_w1_acceptance as acceptance
from moex_data import step7_rub_native_d1_w1_acceptance_base as acceptance_base

ROOT_PREFIX = "${MOEX_DATA_ROOT}/"


def _rooted(root: Path, path: Path) -> str:
    return ROOT_PREFIX + path.relative_to(root).as_posix()


def _manifest(root: Path, frozen_ref: str, **extra) -> Path:
    run_root = root / "runs" / "step7_rub_native_d1_w1" / "run_id=guard"
    manifest = run_root / "state" / "frozen_inputs" / "instrument_id=usdrubf_futures_family" / "frozen_raw_manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    values = {"partitions": [{"frozen_ref": frozen_ref}]}
    values.update(extra)
    manifest.write_text(json.dumps(values), encoding="utf-8")
    return manifest


def test_stage7_wrapper_and_base_share_hardened_surfaces() -> None:
    assert acceptance.promote is acceptance_base.promote
    assert acceptance.validate_pilot is acceptance_base.validate_pilot
    assert acceptance._revalidate_frozen is acceptance_base._revalidate_frozen
    assert acceptance._oracle_d1 is acceptance_base._oracle_d1
    assert acceptance._oracle_technical is acceptance_base._oracle_technical


def test_stage7_impl_include_has_no_direct_cli_execution() -> None:
    impl = Path(acceptance_base.__file__).with_name("step7_rub_native_d1_w1_acceptance_impl.inc")
    result = subprocess.run(
        [sys.executable, impl.as_posix(), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""


def test_stage7_guard_accepts_frozen_partition_inside_declared_run_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    frozen = tmp_path / "runs" / "step7_rub_native_d1_w1" / "run_id=guard" / "inputs" / "dataset_id=futures_raw_5m" / "instrument_id=usdrubf_futures_family" / "trade_date=2026-08-17" / "part.parquet"
    frozen.parent.mkdir(parents=True, exist_ok=True)
    frozen.write_bytes(b"fixture")
    manifest = _manifest(tmp_path, _rooted(tmp_path, frozen))
    assert acceptance_base._guard_frozen_refs_inside_run_root(manifest).name == "run_id=guard"


def test_stage7_guard_rejects_frozen_partition_outside_declared_run_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    escaped = tmp_path / "market" / "raw" / "part.parquet"
    escaped.parent.mkdir(parents=True, exist_ok=True)
    escaped.write_bytes(b"fixture")
    manifest = _manifest(tmp_path, _rooted(tmp_path, escaped))
    with pytest.raises(ValueError, match="escaped approved root|escaped immutable Stage 7 input root"):
        acceptance_base._guard_frozen_refs_inside_run_root(manifest)


def _current_scope() -> SimpleNamespace:
    return SimpleNamespace(
        acceptance_run_id="generation_1",
        pointer_ref=ROOT_PREFIX + "state/accepted_manifests/raw_history_content_attestation/current_batch.json",
        marker_sha256="a" * 64,
        manifest_ref=ROOT_PREFIX + "state/accepted_manifests/raw_history_content_attestation/generation_id=generation_1/manifests/dataset_id=futures_raw_5m/instrument_id=usdrubf_futures_family/accepted_manifest.json",
        manifest_sha256="b" * 64,
        partition_content_set_sha256="c" * 64,
        partition_dates_sha256="d" * 64,
        accepted_dates=("2026-08-17",),
        row_count=10,
    )


def _content_manifest(root: Path, *, marker_sha: str = "a" * 64) -> Path:
    frozen = root / "runs" / "step7_rub_native_d1_w1" / "run_id=guard" / "inputs" / "dataset_id=futures_raw_5m" / "instrument_id=usdrubf_futures_family" / "trade_date=2026-08-17" / "part.parquet"
    frozen.parent.mkdir(parents=True, exist_ok=True)
    frozen.write_bytes(b"fixture")
    scope = _current_scope()
    return _manifest(
        root,
        _rooted(root, frozen),
        source_mode="stage2_content_attested_generation_snapshots_only",
        legacy_pointer_consumption_used=False,
        network_calls_used=False,
        latest_autodetect_used=False,
        content_attestation_generation_id=scope.acceptance_run_id,
        content_attestation_marker_ref=scope.pointer_ref,
        content_attestation_marker_sha256=marker_sha,
        content_attested_manifest_ref=scope.manifest_ref,
        content_attested_manifest_sha256=scope.manifest_sha256,
        content_attested_partition_content_set_sha256=scope.partition_content_set_sha256,
        frozen_content_sha256=scope.partition_content_set_sha256,
        accepted_partition_dates_sha256=scope.partition_dates_sha256,
        partition_count=1,
        row_count=10,
    )


def test_stage7_acceptance_requires_exact_current_content_attestation(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    manifest = _content_manifest(tmp_path)
    monkeypatch.setattr(acceptance_base, "_content_attested_history", lambda *args, **kwargs: _current_scope())
    acceptance_base._guard_current_content_attestation(
        repo_root=tmp_path,
        data_root=tmp_path,
        manifest_path=manifest,
        instrument_id="usdrubf_futures_family",
        start="2026-08-17",
        end="2026-08-17",
    )


def test_stage7_acceptance_rejects_stale_content_attestation_marker(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    manifest = _content_manifest(tmp_path, marker_sha="f" * 64)
    monkeypatch.setattr(acceptance_base, "_content_attested_history", lambda *args, **kwargs: _current_scope())
    with pytest.raises(ValueError, match="content_attestation_marker_sha256"):
        acceptance_base._guard_current_content_attestation(
            repo_root=tmp_path,
            data_root=tmp_path,
            manifest_path=manifest,
            instrument_id="usdrubf_futures_family",
            start="2026-08-17",
            end="2026-08-17",
        )


def test_stage7_current_attestation_uses_explicit_repo_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    explicit_repo = tmp_path / "explicit_repo"
    explicit_repo.mkdir()
    manifest = _content_manifest(tmp_path)
    seen: dict[str, Path] = {}

    def fake_current(root, instrument_id, start, end, *, repo_root="."):
        seen["repo_root"] = Path(repo_root).resolve()
        return _current_scope()

    monkeypatch.setattr(acceptance_base, "_content_attested_history", fake_current)
    acceptance_base._guard_current_content_attestation(
        repo_root=explicit_repo,
        data_root=tmp_path,
        manifest_path=manifest,
        instrument_id="usdrubf_futures_family",
        start="2026-08-17",
        end="2026-08-17",
    )
    assert seen["repo_root"] == explicit_repo.resolve()


def test_stage7_oracle_d1_requires_captured_validated_frame() -> None:
    with pytest.raises(ValueError, match="requires captured validated frame"):
        acceptance_base._oracle_d1(
            [{"trade_date": "2026-08-17", "sha256": "a" * 64}],
            "usdrubf_futures_family",
        )


def test_stage7_oracle_technical_rejects_zero_previous_close() -> None:
    rows = []
    for n, close in enumerate((100.0, 0.0, 102.0)):
        trade_date = f"2026-08-{17 + n:02d}"
        rows.append({
            "instrument_id": "usdrubf_futures_family",
            "secid": "USDRUBF",
            "timeframe": "1D",
            "period_start_date": trade_date,
            "period_end_date": trade_date,
            "trade_date": trade_date,
            "availability_ts_utc": "2026-08-20T03:00:00+00:00",
            "open": close,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
        })
    with pytest.raises(ValueError, match="previous close denominator is zero"):
        acceptance_base._oracle_technical(pd.DataFrame(rows), "fixture")


def test_stage7_output_parquet_hash_and_parse_use_same_captured_bytes(tmp_path: Path) -> None:
    partition = tmp_path / "part.parquet"
    pd.DataFrame({"value": [1, 2, 3]}).to_parquet(partition, index=False)
    raw = partition.read_bytes()
    frame, digest, identity = acceptance_base._capture_output_parquet(partition)
    assert frame["value"].tolist() == [1, 2, 3]
    assert digest == hashlib.sha256(raw).hexdigest()
    assert identity[2] == len(raw)


def test_stage7_output_path_rejects_symlink(tmp_path: Path) -> None:
    run_root = tmp_path / "run_id=guard"
    run_root.mkdir()
    target = run_root / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = run_root / "link.json"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="regular non-symlink"):
        acceptance_base._inside_run(link.as_posix(), run_root, "output")


def test_stage7_output_recheck_rejects_replaced_validated_bytes(tmp_path: Path) -> None:
    paths = {
        "partition": tmp_path / "part.parquet",
        "manifest_path": tmp_path / "manifest.json",
        "quality_path": tmp_path / "quality.json",
    }
    paths["partition"].write_bytes(b"partition-v1")
    paths["manifest_path"].write_bytes(b"manifest-v1")
    paths["quality_path"].write_bytes(b"quality-v1")
    item: dict[str, object] = dict(paths)
    for path_field, sha_field, identity_field, label in (
        ("partition", "partition_sha256", "partition_identity", "partition"),
        ("manifest_path", "manifest_sha256", "manifest_identity", "manifest"),
        ("quality_path", "quality_report_sha256", "quality_report_identity", "quality"),
    ):
        _, digest, identity = acceptance_base._capture_regular_bytes(Path(item[path_field]), label)
        item[sha_field] = digest
        item[identity_field] = identity
    acceptance_base._recheck_validated_output(item)
    paths["partition"].write_bytes(b"partition-v2")
    with pytest.raises(ValueError, match="changed after validation"):
        acceptance_base._recheck_validated_output(item)
