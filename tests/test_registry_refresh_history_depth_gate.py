from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

from moex_data.futures import registry_refresh_runner as runner


def write_history_depth(tmp_path, rows):
    path = tmp_path / "history_depth.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def row(secid, status="review_required", validation_status="metrics_computed", review_status="ready_for_pm_review"):
    return {
        "history_depth_screen_id": "screen_" + secid,
        "snapshot_date": "2026-05-20",
        "board": "RFUD",
        "secid": secid,
        "family_code": "USDRUBF" if secid == "USDRUBF" else "Si",
        "screen_from": "2026-05-01",
        "screen_till": "2026-05-20",
        "history_depth_status": status,
        "validation_status": validation_status,
        "review_status": review_status,
        "schema_version": "futures_history_depth_screen.v1",
    }


def test_history_depth_review_required_ready_for_pm_review_is_allowed(tmp_path):
    whitelist = ["SiM6", "SiU6", "SiU7", "SiZ6", "USDRUBF"]
    path = write_history_depth(tmp_path, [row(secid) for secid in whitelist])

    summary = runner.screen_summary(path, "history_depth_status", whitelist)

    assert summary["validation_status"] == "pass"
    assert summary["whitelist_status"] == {secid: "review_required" for secid in whitelist}
    assert set(summary["review_gate_status"].values()) == {"history_depth_review_ready"}


def test_history_depth_review_required_without_ready_review_fails_closed(tmp_path):
    whitelist = ["SiM6"]
    path = write_history_depth(tmp_path, [row("SiM6", review_status="pending")])

    summary = runner.screen_summary(path, "history_depth_status", whitelist)

    assert summary["validation_status"] == "fail"
    assert summary["review_gate_status"]["SiM6"] == "history_depth_malformed:review_required"


def test_history_depth_fail_not_checked_and_blocked_fail_closed(tmp_path):
    whitelist = ["SiM6", "SiU6", "SiZ6"]
    path = write_history_depth(tmp_path, [
        row("SiM6", status="fail"),
        row("SiU6", status="not_checked"),
        row("SiZ6", status="blocked"),
    ])

    summary = runner.screen_summary(path, "history_depth_status", whitelist)

    assert summary["validation_status"] == "fail"
    assert summary["review_gate_status"]["SiM6"] == "history_depth_blocked:fail"
    assert summary["review_gate_status"]["SiU6"] == "history_depth_blocked:not_checked"
    assert summary["review_gate_status"]["SiZ6"] == "history_depth_blocked:blocked"


def test_history_depth_missing_review_columns_fails_closed(tmp_path):
    path = tmp_path / "history_depth.parquet"
    pd.DataFrame([{"secid": "SiM6", "history_depth_status": "review_required"}]).to_parquet(path, index=False)

    summary = runner.screen_summary(path, "history_depth_status", ["SiM6"])

    assert summary["validation_status"] == "fail"
    assert summary["failure_reason"] == "missing validation_status,review_status"
