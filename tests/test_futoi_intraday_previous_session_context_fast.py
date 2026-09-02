from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from threading import Barrier

import pandas as pd

from moex_data.futures import futoi_intraday_previous_session_context_fast as fast
from moex_data.futures import futoi_live_factual_refresh_source_native as source
from moex_data.futures import materialize_futoi_instrument as materializer


NOW = datetime(2026, 9, 2, 19, 0, 0, tzinfo=timezone.utc)


def test_materialize_record_uses_single_materializer_fetch(monkeypatch) -> None:
    calls = {"materialize": 0}

    monkeypatch.setattr(
        source,
        "source_identity",
        lambda instrument_id: {
            "instrument_id": instrument_id,
            "source_id": source.SOURCE_ID,
            "source_ticker": "Si",
            "secid": "SiU6",
        },
    )

    def materialize_target(*args, **kwargs):
        calls["materialize"] += 1
        return Path("/tmp/futoi.parquet"), {"source": "test"}

    monkeypatch.setattr(source, "_materialize_target", materialize_target)
    monkeypatch.setattr(fast.pd, "read_parquet", lambda path: pd.DataFrame({"x": [1]}))
    monkeypatch.setattr(
        source,
        "latest_aligned_factual",
        lambda frame, **kwargs: {
            "trade_date": "2026-09-02",
            "snapshot_ts": "2026-09-02T18:59:00+00:00",
        },
    )
    monkeypatch.setattr(
        source,
        "_probe_exact_date",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("probe must not run")),
    )

    result = fast._materialize_record(
        root=Path("/tmp"),
        instrument_id=source.SI_INSTRUMENT_ID,
        trade_date="2026-09-02",
        role=fast.CURRENT_ROLE,
        run_id="test_run",
        timeout=60.0,
        attempted_at="2026-09-02T19:00:00+00:00",
        now_fn=lambda: NOW,
    )

    assert calls["materialize"] == 1
    assert result["status"] == "FRESH"
    assert result["trade_date"] == "2026-09-02"


def test_empty_materializer_source_preserves_pending_semantics(monkeypatch) -> None:
    monkeypatch.setattr(
        source,
        "source_identity",
        lambda instrument_id: {
            "instrument_id": instrument_id,
            "source_id": source.SOURCE_ID,
            "source_ticker": "Si",
            "secid": "SiU6",
        },
    )

    def materialize_target(*args, **kwargs):
        raise materializer.FutoiMaterializationError(source.EXPLICIT_EMPTY_ERROR)

    monkeypatch.setattr(source, "_materialize_target", materialize_target)

    result = fast._record_with_failure_semantics(
        root=Path("/tmp"),
        instrument_id=source.SI_INSTRUMENT_ID,
        trade_date="2026-09-02",
        role=fast.CURRENT_ROLE,
        run_id="test_run",
        timeout=60.0,
        attempted_at="2026-09-02T19:00:00+00:00",
        prior=None,
        now_fn=lambda: NOW,
    )

    assert result["status"] == "PENDING"
    assert "pending on authoritative observed TradeStats date" in result["refresh_error"]


def test_run_refresh_all_executes_si_and_cr_in_parallel(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    barrier = Barrier(len(source.LIVE_INSTRUMENT_IDS))

    def fake_run_refresh(*, through_date, instrument_id, run_id, timeout, now_fn):
        barrier.wait(timeout=2.0)
        return {
            "status": "PASS",
            "instrument_id": instrument_id,
            "through_date": through_date,
            "run_id": run_id,
        }

    monkeypatch.setattr(fast, "run_refresh", fake_run_refresh)

    result = fast.run_refresh_all(
        through_date="2026-09-02",
        run_id="parallel_test",
        now_fn=lambda: NOW,
    )

    assert result["status"] == "PASS"
    assert result["failed_instrument_ids"] == []
    assert list(result["instrument_results"]) == list(source.LIVE_INSTRUMENT_IDS)
