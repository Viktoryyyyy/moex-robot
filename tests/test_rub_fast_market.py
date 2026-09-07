from copy import deepcopy
from datetime import datetime, timedelta
import json
from unittest.mock import patch

import pytest

from moex_data import rub_fast_market as fast
from moex_data.rub_snapshot_read_freshness import apply_read_freshness, LIVE, BASIS
from moex_data.synchronized_live_market_oi_context_partial import _reclassify
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
from test_synchronized_live_market_oi_context import _build, _payloads

NOW = datetime.fromisoformat("2026-09-02T10:00:23+00:00")


def market():
    return _reclassify(_build(*_payloads()))


def snapshot():
    return {
        "schema_version": base.SCHEMA_VERSION,
        "identity": {"project": base.PROJECT, "generated_at_utc": (NOW - timedelta(minutes=5)).isoformat()},
        "components": {"futoi_cr": {"status": "GOVERNED_BLOCKED", "data": {"factual_authority": False}},
                       "cbr_macro": {"status": "READY", "data": {"unchanged": True}}},
        "authority": {"broker_execution": False, "futoi_cr_factual_authority": False},
        "analysis_views": {}, "analysis_workflow": {},
    }


def publish(root, data=None):
    value = fast.refresh(root, loader=lambda: market() if data is None else data, clock=lambda: NOW)
    (fast.state_path(root) / "enabled").write_text(fast.SCHEMA)
    return value


def read(root, now=NOW):
    return apply_read_freshness(fast.apply(snapshot(), root=root, now=now), now=now)


def test_opt_out_does_not_change_snapshot(tmp_path):
    original = snapshot()
    assert fast.apply(original, root=tmp_path, now=NOW) is original
    assert list(tmp_path.iterdir()) == []


def test_fresh_overlay_preserves_slow_data_identity_and_no_network_on_read(tmp_path):
    publish(tmp_path)
    original = snapshot()
    with patch("moex_data.synchronized_live_market_oi_context_partial.fetch_live_snapshot", side_effect=AssertionError("network")):
        result = fast.apply(original, root=tmp_path, now=NOW)
    assert original == snapshot()
    assert result["identity"] == original["identity"]
    for key in original["components"]:
        assert result["components"][key] == original["components"][key]
    assert result["authority"]["live_market_oi_factual_authority"] is True
    assert result["authority"]["broker_execution"] is False
    assert result["authority"]["futoi_cr_factual_authority"] is False
    assert result["fast_market_read"]["network_fetch_performed"] is False


@pytest.mark.parametrize("corruption", ["missing", "json", "list", "schema", "hash", "scope", "identity", "future", "receipt", "marker"])
def test_corrupt_generation_removes_live_and_basis_authority(tmp_path, corruption):
    value = publish(tmp_path)
    path = fast.state_path(tmp_path) / "current.json"
    if corruption == "missing":
        path.unlink()
    elif corruption == "json":
        path.write_text("{")
    elif corruption == "list":
        path.write_text("[]")
    elif corruption == "marker":
        (path.parent / "enabled").write_text("wrong")
    else:
        if corruption == "schema":
            value["schema_version"] = "wrong"
        elif corruption == "hash":
            value["market"]["status"] = "changed"
        elif corruption == "scope":
            value["market"]["instruments"].pop("si_front")
        elif corruption == "identity":
            value["market"]["instruments"]["si_front"]["secid"] = "WRONG"
        elif corruption == "future":
            value["completed_at"] = (NOW + timedelta(seconds=1)).isoformat()
        elif corruption == "receipt":
            value["market"]["instruments"]["si_front"]["received_at_utc"] = (NOW + timedelta(seconds=1)).isoformat()
        if corruption != "hash":
            value["market_sha256"] = fast._digest(value["market"])
        path.write_text(json.dumps(value))
    result = read(tmp_path)
    assert result["fast_market_read"]["error"]
    assert result["authority"]["live_market_oi_factual_authority"] is False
    assert result["authority"]["live_basis_carry_factual_authority"] is False


def test_generation_ttl_and_source_ttl_are_separate(tmp_path):
    publish(tmp_path)
    result = fast.apply(snapshot(), root=tmp_path, now=NOW + timedelta(seconds=60))
    assert result["fast_market_read"]["error"] is None
    assert read(tmp_path, NOW + timedelta(seconds=60))["authority"]["live_market_oi_factual_authority"] is False
    assert read(tmp_path, NOW + timedelta(seconds=60, microseconds=1))["fast_market_read"]["error"]


def test_failed_attempt_replaces_success_without_fallback(tmp_path):
    publish(tmp_path)
    def fail():
        raise TimeoutError("private details must not be persisted")
    failed = fast.refresh(tmp_path, loader=fail, clock=lambda: NOW)
    assert failed["status"] == "FAILED"
    assert "private details" not in json.dumps(failed)
    assert read(tmp_path)["authority"]["live_market_oi_factual_authority"] is False


def test_clock_regression_publishes_failure(tmp_path):
    publish(tmp_path)
    times = iter([NOW, NOW - timedelta(seconds=1)])
    assert fast.refresh(tmp_path, loader=market, clock=lambda: next(times))["status"] == "FAILED"
    assert read(tmp_path)["authority"]["live_market_oi_factual_authority"] is False


def test_fresh_generation_cannot_renew_old_source_rows(tmp_path):
    data = market()
    for item in data["instruments"].values():
        item["timestamp"] = (NOW - timedelta(minutes=3)).isoformat()
    publish(tmp_path, data)
    result = read(tmp_path)
    assert not result["authority"]["live_market_oi_factual_authority"]
    assert not result["authority"]["live_basis_carry_factual_authority"]


def test_stale_spot_keeps_only_fresh_futures(tmp_path):
    data = market()
    data["instruments"]["cnyrub_tom"]["timestamp"] = (NOW - timedelta(hours=2)).isoformat()
    publish(tmp_path, data)
    result = read(tmp_path)
    assert result["components"][LIVE]["status"] == "PARTIAL"
    assert result["components"][LIVE]["data"]["quality"]["spot_price_usable"] is False
    assert result["components"][LIVE]["data"]["synchronization"]["futures_synchronized"] is True


def test_current_reader_preserves_heavy_snapshot_age(tmp_path, monkeypatch):
    publish(tmp_path)
    base._atomic_write(base.current_snapshot_path(tmp_path), snapshot())
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    result, _ = base.read_current_snapshot(now_fn=lambda: NOW)
    assert result["read_freshness"]["snapshot_age_seconds"] == 300
    assert result["fast_market_read"]["error"] is None
    assert result["authority"]["live_market_oi_factual_authority"]


def test_atomic_publish_failure_keeps_complete_prior_file(tmp_path):
    publish(tmp_path)
    path = fast.state_path(tmp_path) / "current.json"
    before = path.read_bytes()
    with patch.object(base.os, "replace", side_effect=OSError("disk")):
        with pytest.raises(OSError):
            fast.refresh(tmp_path, loader=market, clock=lambda: NOW)
    assert path.read_bytes() == before
    assert not list(path.parent.glob("*.tmp"))


def test_symlink_cache_is_rejected(tmp_path):
    publish(tmp_path)
    path = fast.state_path(tmp_path) / "current.json"
    other = tmp_path / "other.json"
    path.rename(other)
    path.symlink_to(other)
    assert read(tmp_path)["authority"]["live_market_oi_factual_authority"] is False
