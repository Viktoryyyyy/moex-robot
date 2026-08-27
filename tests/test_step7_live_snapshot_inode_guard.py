from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from moex_data import step7_rub_native_d1_w1_acceptance_base as acceptance_base


ROOT_PREFIX = "${MOEX_DATA_ROOT}/"


def _rooted(root: Path, path: Path) -> str:
    return ROOT_PREFIX + path.relative_to(root).as_posix()


def test_stage7_rejects_frozen_hardlink_to_live_snapshot_even_with_forged_manifest_identity(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    trade_date = "2026-08-17"
    payload = b"attested-snapshot-bytes"
    digest = hashlib.sha256(payload).hexdigest()

    live = tmp_path / "state" / "accepted_manifests" / "raw_history_content_attestation" / "generation_id=generation_1" / "snapshots" / "part.parquet"
    live.parent.mkdir(parents=True, exist_ok=True)
    live.write_bytes(payload)

    run_root = tmp_path / "runs" / "step7_rub_native_d1_w1" / "run_id=guard"
    frozen = run_root / "inputs" / "dataset_id=futures_raw_5m" / "instrument_id=usdrubf_futures_family" / f"trade_date={trade_date}" / "part.parquet"
    frozen.parent.mkdir(parents=True, exist_ok=True)
    os.link(live, frozen)

    manifest = run_root / "state" / "frozen_inputs" / "instrument_id=usdrubf_futures_family" / "frozen_raw_manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "step7_frozen_raw_5m_manifest.v1",
                "dataset_id": "futures_raw_5m",
                "instrument_id": "usdrubf_futures_family",
                "source_id": "moex_algopack_fo_tradestats_5m",
                "freeze_method": "validated_descriptor_create_only_independent_inode_exact_byte_copy",
                "mutable_canonical_raw_read_after_freeze_allowed": False,
                "partition_count": 1,
                "row_count": 1,
                "frozen_content_sha256": "0" * 64,
                "partitions": [
                    {
                        "trade_date": trade_date,
                        "frozen_ref": _rooted(tmp_path, frozen),
                        "sha256": digest,
                        "row_count": 1,
                        "secids": ["USDRUBF"],
                        "independent_inode_exact_byte_copy": True,
                        "validated_source_identity": {"st_dev": -999, "st_ino": -999},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    current = SimpleNamespace(
        accepted_dates=(trade_date,),
        row_count=1,
        partition_content_set_sha256="0" * 64,
        records=(
            {
                "trade_date": trade_date,
                "snapshot_path": live.as_posix(),
                "sha256": digest,
                "row_count": 1,
            },
        ),
    )
    monkeypatch.setattr(acceptance_base, "_guard_current_content_attestation", lambda **kwargs: current)
    monkeypatch.setattr(acceptance_base, "_quote_validation_expectation", lambda *args, **kwargs: object())
    monkeypatch.setattr(
        acceptance_base,
        "_capture_frozen_frame",
        lambda path, expected_sha: (pd.DataFrame({"fixture": [1]}), os.stat(path, follow_symlinks=False)),
    )

    with pytest.raises(ValueError, match="shares live content-attested snapshot inode"):
        acceptance_base._revalidate_frozen(
            repo_root=tmp_path,
            data_root=tmp_path,
            manifest_path=manifest,
            instrument_id="usdrubf_futures_family",
            start=trade_date,
            end=trade_date,
            validation_run_id="guard",
        )


def test_stage7_contract_requires_live_snapshot_independence_oracle() -> None:
    contract = (Path(__file__).resolve().parents[1] / "contracts" / "datasets" / "step7_rub_native_d1_w1_technical_acceptance.v1.yaml").read_text(encoding="utf-8")
    for token in (
        "current_content_attested_snapshot_record_map_required: true",
        "current_content_attested_snapshot_same_byte_sha256_identity_revalidation_required: true",
        "frozen_raw_inode_must_differ_from_live_content_attested_snapshot: true",
        "run_manifest_validated_source_identity_is_not_independence_oracle: true",
    ):
        assert token in contract
