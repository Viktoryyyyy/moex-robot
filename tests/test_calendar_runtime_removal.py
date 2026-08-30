from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from moex_data import step10_rub_refresh_scheduler as step10
from moex_data.futures import refresh_forts_raw_5m_incremental as refresh


class FakeResponse:
    def __init__(self, payload: dict[str, object], url: str) -> None:
        self._payload = payload
        self.url = url

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def _tradestats_payload(rows: list[list[object]]) -> dict[str, object]:
    return {
        "tradestats": {
            "columns": ["SECID", "TRADEDATE"],
            "data": rows,
        }
    }


def test_stage10_date_source_requests_only_algopack_tradestats(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_API_KEY", "test-key")
    calls: list[str] = []

    def fake_get(url, *, params, headers, timeout):
        calls.append(str(url))
        if int(params["start"]) == 0:
            return FakeResponse(
                _tradestats_payload(
                    [
                        ["USDRUBF", "2026-06-12"],
                        ["USDRUBF", "2026-06-15"],
                    ]
                ),
                str(url),
            )
        return FakeResponse(_tradestats_payload([]), str(url))

    monkeypatch.setattr(refresh.requests, "get", fake_get)
    monkeypatch.setenv("MOEX_API_URL", "https://apim.test")

    dates = step10._calendar_dates(start_date="2026-06-12", end_date="2026-06-15", timeout=1.0)

    forbidden = "/iss/" + "calendars.json"
    assert dates == ["2026-06-12", "2026-06-15"]
    assert calls
    assert all(url.endswith(refresh.OBSERVED_DATE_SOURCE_ENDPOINT) for url in calls)
    assert all(forbidden not in url for url in calls)


def test_incremental_refresh_source_loader_never_requests_calendar_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_API_KEY", "test-key")
    monkeypatch.setenv("MOEX_API_URL", "https://apim.test")
    calls: list[str] = []

    def fake_get(url, *, params, headers, timeout):
        calls.append(str(url))
        if int(params["start"]) == 0:
            return FakeResponse(
                _tradestats_payload(
                    [
                        ["USDRUBF", "2026-06-12"],
                        ["USDRUBF", "2026-06-15"],
                        ["USDRUBF", "2026-06-17"],
                    ]
                ),
                str(url),
            )
        return FakeResponse(_tradestats_payload([]), str(url))

    monkeypatch.setattr(refresh.requests, "get", fake_get)

    dates = refresh.fetch_observed_tradestats_dates(
        "2026-06-12",
        "2026-06-17",
        secid="USDRUBF",
        timeout=1.0,
    )

    forbidden = "/iss/" + "calendars.json"
    assert dates == ["2026-06-12", "2026-06-15", "2026-06-17"]
    assert calls
    assert all(url.endswith(refresh.OBSERVED_DATE_SOURCE_ENDPOINT) for url in calls)
    assert all(forbidden not in url for url in calls)


def test_observed_source_absence_is_contextual_and_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_API_KEY", "test-key")
    monkeypatch.setenv("MOEX_API_URL", "https://apim.test")
    monkeypatch.setattr(
        refresh.requests,
        "get",
        lambda url, *, params, headers, timeout: FakeResponse(_tradestats_payload([]), str(url)),
    )

    with pytest.raises(ValueError) as error:
        refresh.fetch_observed_tradestats_dates(
            "2026-06-13",
            "2026-06-14",
            secid="USDRUBF",
            timeout=1.0,
        )

    message = str(error.value)
    assert "fetch_observed_tradestats_dates" in message
    assert refresh.SOURCE_ARTIFACT_ID in message
    assert refresh.OBSERVED_DATE_SOURCE_ENDPOINT in message
    assert "secid=USDRUBF" in message
    assert "authoritative AlgoPack TradeStats source returned no observed trade dates" in message


def test_weekends_and_gaps_are_not_fabricated_by_stage10_date_source(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = ["2026-06-12", "2026-06-15", "2026-06-17"]

    def source_loader(date_start, date_end, *, secid, timeout, apim_base_url=None):
        assert date_start == "2026-06-12"
        assert date_end == "2026-06-17"
        assert secid == "USDRUBF"
        return observed

    monkeypatch.setattr(step10.forts_incremental, "fetch_observed_tradestats_dates", source_loader)

    result = step10._calendar_dates(start_date="2026-06-12", end_date="2026-06-17", timeout=1.0)

    assert result == observed
    assert "2026-06-13" not in result
    assert "2026-06-14" not in result
    assert "2026-06-16" not in result


def test_stage5_futoi_factual_materializer_path_is_unchanged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    repo = tmp_path / "repo"
    repo.mkdir()
    run_root = root / "runs" / "step10_rub_daily_refresh" / "run_id=test"
    base_frames = {
        instrument: pd.DataFrame([{"instrument_id": instrument, "trade_date": "2026-06-12"}])
        for instrument in step10.STAGE5_INSTRUMENTS
    }
    factual_calls: list[tuple[str, str]] = []

    def fake_futoi_materialize(**kwargs):
        factual_calls.append((kwargs["instrument_id"], kwargs["trade_date"]))
        return {
            "quality_status": "pass",
            "row_count": 1,
            "storage_partition_path": "/unused/futoi.parquet",
        }

    monkeypatch.setattr(step10.futoi_raw, "materialize_futoi_partition", fake_futoi_materialize)
    monkeypatch.setattr(
        step10,
        "_freeze_file",
        lambda *_args, **_kwargs: {
            "frozen_ref": "${MOEX_DATA_ROOT}/frozen.parquet",
            "canonical_ref": "${MOEX_DATA_ROOT}/canonical.parquet",
            "sha256": "a" * 64,
        },
    )
    monkeypatch.setattr(step10.pd, "read_parquet", lambda _path: pd.DataFrame([{"dummy": 1}]))
    monkeypatch.setattr(
        step10.futoi_eod,
        "_single_eod_row",
        lambda _frame, *, instrument_id, trade_date, **_kwargs: {
            "instrument_id": instrument_id,
            "trade_date": trade_date,
        },
    )
    monkeypatch.setattr(step10.futoi_features, "build_features", lambda frame, *, instrument_id: frame.copy())
    monkeypatch.setattr(step10, "_rooted_ref", lambda *_args: "${MOEX_DATA_ROOT}/prepared.parquet")

    def fake_write(**kwargs):
        return {
            "dataset_id": kwargs["dataset_id"],
            "instrument_id": kwargs["instrument_id"],
            "timeframe": None,
            "run_id": kwargs["producer_run_id"],
            "partition_path": Path("/tmp/prepared.parquet"),
            "manifest_path": Path("/tmp/manifest.json"),
            "quality_path": Path("/tmp/quality.json"),
            "row_count": len(kwargs["frame"].index),
        }

    monkeypatch.setattr(step10, "_write_stage5_output", fake_write)

    outputs = step10._stage5_refresh(
        root=root,
        repo=repo,
        run_root=run_root,
        run_id="test",
        base_frames=base_frames,
        trading_dates=["2026-06-15"],
        timeout=1.0,
    )

    assert factual_calls == [
        ("si_futures_family", "2026-06-15"),
        ("cr_futures_family", "2026-06-15"),
    ]
    assert len(outputs) == 4
