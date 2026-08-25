from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

from moex_data.futures import freeze_step7_accepted_raw_5m as freeze


def test_freeze_hardlink_remains_bound_to_validated_bytes_after_canonical_replace(monkeypatch, tmp_path: Path) -> None:
    instrument = "usdrubf_futures_family"
    trade_date = "2026-08-17"
    source = freeze.canonical_partition_path(tmp_path, instrument, trade_date)
    source.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"marker": [1]}).to_parquet(source, index=False)

    def stale_pre_promotion_gate(*args, **kwargs):
        raise AssertionError("legacy stage2 _expectation must not be called by Stage 7 runtime")

    monkeypatch.setattr(freeze.stage2, "_expectation", stale_pre_promotion_gate)
    monkeypatch.setattr(freeze.stage2, "_validate_quote_partition", lambda *args, **kwargs: (1, ("USDRUBF",)))

    run_root = tmp_path / "runs" / "step7_fixture"
    record = freeze._freeze_one(
        repo_root=tmp_path,
        data_root=tmp_path,
        instrument_id=instrument,
        trade_date=trade_date,
        freeze_root=run_root / "inputs" / "dataset_id=futures_raw_5m",
        validation_run_id="freeze_fixture",
    )
    frozen = tmp_path / str(record["frozen_ref"])[len(freeze.ROOT_PREFIX):]
    old_sha = hashlib.sha256(frozen.read_bytes()).hexdigest()
    assert old_sha == record["sha256"]

    replacement = source.with_name("replacement.parquet")
    pd.DataFrame({"marker": [2]}).to_parquet(replacement, index=False)
    replacement.replace(source)

    assert hashlib.sha256(source.read_bytes()).hexdigest() != old_sha
    assert hashlib.sha256(frozen.read_bytes()).hexdigest() == old_sha
    assert pd.read_parquet(frozen)["marker"].tolist() == [1]
