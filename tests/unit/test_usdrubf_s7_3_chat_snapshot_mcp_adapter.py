from pathlib import Path

from src.moex_research.runners import usdrubf_s7_3_chat_snapshot_mcp_adapter as adapter


def test_mcp_adapter_returns_reader_enriched_snapshot_without_refresh(monkeypatch):
    expected = {
        "schema_version": "rub_chat_analysis_snapshot.v1",
        "identity": {"project": "MOEX_Bot", "generated_at_utc": "2026-08-29T15:37:34+00:00"},
        "read_freshness": {"status": "FRESH", "snapshot_age_seconds": 5},
    }
    calls = {"read": 0}

    def fake_read_current_snapshot():
        calls["read"] += 1
        return expected, Path("/ignored/current.json")

    monkeypatch.setattr(adapter.snapshot_runner, "read_current_snapshot", fake_read_current_snapshot)

    result = adapter.read_rub_analysis_snapshot_for_mcp()

    assert calls == {"read": 1}
    assert result == expected
    assert result is not expected


def test_mcp_adapter_propagates_reader_failure(monkeypatch):
    def fail_read_current_snapshot():
        raise RuntimeError("snapshot unavailable")

    monkeypatch.setattr(adapter.snapshot_runner, "read_current_snapshot", fail_read_current_snapshot)

    try:
        adapter.read_rub_analysis_snapshot_for_mcp()
    except RuntimeError as exc:
        assert str(exc) == "snapshot unavailable"
    else:
        raise AssertionError("reader failure must propagate")
