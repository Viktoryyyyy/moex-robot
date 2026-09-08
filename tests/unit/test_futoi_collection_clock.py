from contextlib import nullcontext
from datetime import datetime, timedelta, timezone

import pytest
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current_context as context
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live


@pytest.mark.parametrize('runner', [context, live])
def test_futoi_collection_receipt_uses_advancing_clock(monkeypatch, tmp_path, runner):
    start = datetime(2026, 9, 7, 19, 0, tzinfo=timezone.utc)
    times = iter([start, start + timedelta(seconds=10), start + timedelta(seconds=20)])
    base = runner.base
    monkeypatch.setattr(base, 'load_dotenv', lambda *args, **kwargs: None)
    monkeypatch.setattr(base, 'install_timestamp_policy', lambda: None)
    monkeypatch.setattr(base, '_data_root', lambda: tmp_path)
    monkeypatch.setattr(base, 'snapshot_state_dir', lambda root: tmp_path)
    monkeypatch.setattr(base, '_single_refresh_lock', lambda state: nullcontext())
    monkeypatch.setattr(base, '_load_previous', lambda path: None)
    # This fixture stops inside FUTOI collection, before unrelated producers.
    monkeypatch.setattr(context.current, 'current_producers', lambda: {})
    observed = []

    class StopAfterCollection(Exception):
        pass

    def collector(**kwargs):
        observed.extend([kwargs['now_fn'](), kwargs['now_fn']()])
        raise StopAfterCollection

    monkeypatch.setattr(context.context, 'run_refresh_all', collector)
    with pytest.raises(StopAfterCollection):
        runner.refresh_snapshot(now_fn=lambda: next(times))
    assert observed == [start + timedelta(seconds=10), start + timedelta(seconds=20)]
