from __future__ import annotations

import threading
import time

import pytest

from moex_data import step5_futoi_positioning_acceptance as acceptance
from moex_data import step5_futoi_positioning_acceptance_base as base


def test_stage5_base_cli_is_fail_closed() -> None:
    with pytest.raises(ValueError, match="direct Stage 5 acceptance base CLI is forbidden"):
        base._reject_direct_base_cli()


def test_stage5_canonical_validator_swap_is_serialized_across_threads() -> None:
    original = base._validate_output_record
    state_lock = threading.Lock()
    barrier = threading.Barrier(3)
    active = 0
    max_active = 0
    failures: list[BaseException] = []

    def guarded_callable() -> None:
        nonlocal active, max_active
        try:
            assert base._validate_output_record is acceptance._validate_output_record
            with state_lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.05)
            assert base._validate_output_record is acceptance._validate_output_record
        finally:
            with state_lock:
                active -= 1

    def worker() -> None:
        try:
            barrier.wait()
            acceptance._with_wrapped_output_validator(guarded_callable)
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
    assert base._validate_output_record is original
