from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import stage2_raw_history_content_reattestation as content
from moex_data.futures import stage2_raw_history_acceptance as stage2


def test_content_set_sha_is_ordered_exact_serialization() -> None:
    records = [
        {"trade_date": "2026-08-14", "sha256": "a" * 64},
        {"trade_date": "2026-08-15", "sha256": "b" * 64},
    ]
    payload = (
        "2026-08-14\t" + "a" * 64 + "\n"
        + "2026-08-15\t" + "b" * 64 + "\n"
    ).encode("utf-8")
    assert content._content_set_sha(records) == hashlib.sha256(payload).hexdigest()


def test_validate_and_hash_partition_hashes_same_parquet_bytes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dataset = stage2.QUOTE_DATASET_ID
    instrument = "usdrubf_futures_family"
    trade_date = "2026-08-17"
    partition = tmp_path / "part.parquet"
    pd.DataFrame({"marker": [1, 2, 3]}).to_parquet(partition, index=False)
    expected_sha = hashlib.sha256(partition.read_bytes()).hexdigest()
    expectation = stage2.HistoryExpectation(
        target_dataset_id=dataset,
        instrument_id=instrument,
        source_id=stage2.QUOTE_SOURCE_ID,
        date_start=trade_date,
        date_end=trade_date,
        expected_partitions=1,
        expected_rows=3,
        expected_secid="USDRUBF",
    )
    pointer = content.PriorPointerSnapshot(dataset, instrument, tmp_path / "pointer.json", {}, b"{}", "0" * 64)
    state = content.PriorAcceptedState(
        pointer=pointer,
        manifest_path=tmp_path / "manifest.json",
        manifest_values={},
        manifest_sha256="1" * 64,
        expectation=expectation,
        accepted_dates=(trade_date,),
        missing_dates=(),
        target_contract_ref=stage2.QUOTE_CONTRACT_PATH,
    )
    monkeypatch.setattr(content.stage2, "_contract_path", lambda *args, **kwargs: "unused")
    monkeypatch.setattr(content.stage2, "_partition_path", lambda **kwargs: partition)
    monkeypatch.setattr(content.stage2, "_validate_quote_partition", lambda *args, **kwargs: (3, ("USDRUBF",)))
    monkeypatch.setattr(content, "_rooted_ref", lambda path: "${MOEX_DATA_ROOT}/fixture/part.parquet")

    record = content._validate_and_hash_partition(tmp_path, state, trade_date, "validation_run")
    assert record["sha256"] == expected_sha
    assert record["row_count"] == 3
    assert record["secid_scope"] == ["USDRUBF"]
    assert record["validated_inode"]["st_size"] == partition.stat().st_size


def test_reattest_rejects_stale_explicit_prior_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dataset = stage2.QUOTE_DATASET_ID
    instrument = "usdrubf_futures_family"
    pointer_path = tmp_path / "pointer.json"
    pointer_path.write_text("{}", encoding="utf-8")
    pointer = content.PriorPointerSnapshot(
        dataset,
        instrument,
        pointer_path,
        {"run_id": "old_run"},
        b"{}",
        hashlib.sha256(b"{}").hexdigest(),
    )
    expectation = stage2.HistoryExpectation(
        target_dataset_id=dataset,
        instrument_id=instrument,
        source_id=stage2.QUOTE_SOURCE_ID,
        date_start="2026-08-17",
        date_end="2026-08-17",
        expected_partitions=1,
        expected_rows=1,
        expected_secid="USDRUBF",
    )
    state = content.PriorAcceptedState(
        pointer=pointer,
        manifest_path=tmp_path / "manifest.json",
        manifest_values={},
        manifest_sha256="1" * 64,
        expectation=expectation,
        accepted_dates=("2026-08-17",),
        missing_dates=(),
        target_contract_ref=stage2.QUOTE_CONTRACT_PATH,
    )
    monkeypatch.setattr(content, "SCOPES", ((dataset, instrument),))
    monkeypatch.setattr(content, "_prior_state", lambda *args, **kwargs: state)

    with pytest.raises(content.RawHistoryContentReattestationError, match="explicit expected prior four-pointer state"):
        content.reattest(run_id="reattest_fixture", expected_prior_state_sha256="f" * 64, repo_root=tmp_path)


def test_transactional_replace_rolls_back_all_applied_targets(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text('{"old":1}\n', encoding="utf-8")
    second.write_text('{"old":2}\n', encoding="utf-8")
    old_first = first.read_bytes()
    old_second = second.read_bytes()
    original_replace = Path.replace
    calls = {"stage": 0}

    def flaky_replace(self: Path, target: Path):
        if self.suffix == ".stage":
            calls["stage"] += 1
            if calls["stage"] == 2:
                raise OSError("injected second pointer failure")
        return original_replace(self, target)

    monkeypatch.setattr(Path, "replace", flaky_replace)
    with pytest.raises(content.RawHistoryContentReattestationError, match="pointer transaction failed"):
        content._transactional_replace(
            [
                (first, {"new": 1}),
                (second, {"new": 2}),
            ]
        )
    assert first.read_bytes() == old_first
    assert second.read_bytes() == old_second


def test_scope_is_exact_four_canonical_stage2_pointers() -> None:
    assert content.SCOPES == (
        (stage2.QUOTE_DATASET_ID, "usdrubf_futures_family"),
        (stage2.QUOTE_DATASET_ID, "cnyrubf_futures_family"),
        (stage2.FUTOI_DATASET_ID, "si_futures_family"),
        (stage2.FUTOI_DATASET_ID, "cr_futures_family"),
    )
