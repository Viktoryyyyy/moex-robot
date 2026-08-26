from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from moex_data import step5_futoi_positioning_acceptance_base as acceptance_base


def test_stage5_transactional_replace_serializes_concurrent_publishers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    monkeypatch.setattr(
        acceptance_base,
        "_final_content_attestation_write_gate",
        lambda records: ("generation_current", "${MOEX_DATA_ROOT}/state/current_batch.json", "a" * 64),
    )

    state_lock = threading.Lock()
    active = 0
    max_active = 0

    def fake_transaction(records) -> None:
        nonlocal active, max_active
        with state_lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.05)
        with state_lock:
            active -= 1

    monkeypatch.setattr(acceptance_base, "_BASE_TRANSACTIONAL_REPLACE", fake_transaction)
    records = [(tmp_path / "current_accepted_manifest.json", {"dataset_id": "fixture"})]
    barrier = threading.Barrier(3)
    failures: list[BaseException] = []

    def worker() -> None:
        try:
            barrier.wait()
            acceptance_base._transactional_replace(records)
        except BaseException as exc:
            failures.append(exc)

    threads = [threading.Thread(target=worker), threading.Thread(target=worker)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=2.0)

    assert not failures
    assert all(not thread.is_alive() for thread in threads)
    assert max_active == 1
