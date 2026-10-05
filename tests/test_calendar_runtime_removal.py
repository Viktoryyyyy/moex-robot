from __future__ import annotations

import ast
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


def _without_retained_source_identifier(text: str) -> str:
    """Allow one historical metadata value, never a calendar client dependency."""
    approved = "MOEX_APIM_XML:/iss/calendars"
    tree = ast.parse(text)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    mappings = [node.value for node in tree.body if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == "ROLL_SOURCES" for target in node.targets)]
    assert len(mappings) == 1 and isinstance(mappings[0], ast.Dict)
    legacy = [value for key, value in zip(mappings[0].keys, mappings[0].values)
              if isinstance(key, ast.Name) and key.id == "XML"]
    assert len(legacy) == 1 and isinstance(legacy[0], ast.Constant) and legacy[0].value == approved
    assert text.count(approved) == 1
    allowed_imports = {"hashlib", "json", "os", "re", "tempfile", "pathlib", "urllib.parse",
                       "numpy", "pandas", "moex_data.futures", "moex_data.futures.slice1_common"}
    allowed_observed = {"OBSERVED_DATE_SOURCE_ID", "OBSERVED_DATE_SOURCE_ENDPOINT",
                        "observed_date_source_endpoint"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert all(alias.name in allowed_imports for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.module in allowed_imports
            if node.module == "moex_data.futures":
                assert [(alias.name, alias.asname) for alias in node.names] == [
                    ("refresh_forts_raw_5m_incremental", "observed")]
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "observed":
            assert node.attr in allowed_observed
            if node.attr == "observed_date_source_endpoint":
                parent = parents.get(node)
                assert isinstance(parent, ast.Call) and parent.func is node
        elif isinstance(node, ast.Name) and node.id == "observed":
            parent = parents.get(node)
            assert isinstance(node.ctx, ast.Load)
            assert isinstance(parent, ast.Attribute) and parent.value is node and parent.attr in allowed_observed
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id not in {"__import__", "eval", "exec", "getattr"}
    return text.replace(approved, "")


@pytest.mark.parametrize("network_dependency", [
    "import requests\n",
    "from urllib.request import urlopen\n",
    "observed.requests.get('https://example.invalid')\n",
    "observed.fetch_observed_tradestats_dates('2026-10-01', '2026-10-02')\n",
    "client = observed\nclient.fetch_observed_tradestats_dates('2026-10-01', '2026-10-02')\n",
    "observed = object()\n",
    "observed.observed_date_source_endpoint.__globals__['fetch_observed_tradestats_dates']('2026-10-01', '2026-10-02')\n",
    "formatter = observed.observed_date_source_endpoint\n",
])
def test_retained_source_identifier_does_not_allow_network_dependency(network_dependency: str) -> None:
    text = Path("src/moex_data/futures/date_source_provenance.py").read_text(encoding="utf-8")
    with pytest.raises(AssertionError):
        _without_retained_source_identifier(text + "\n" + network_dependency)


def test_active_src_has_no_legacy_calendar_runtime_dependency() -> None:
    forbidden = (
        "/iss/" + "calendars",
        "moex_iss_futures_" + "calendar",
        "fetch_futures_" + "calendar_rows",
        "select_completed_trading_" + "dates",
        "_calendar_" + "map",
        "calendar_" + "base_url",
        "MOEX_" + "CALENDAR_BASE_URL",
        "CALENDAR_" + "MODE",
    )
    violations: list[str] = []
    for path in sorted(Path("src").rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        if path.as_posix() == 'src/moex_data/rub_futures_calendar.py':
            # A separately verified authenticated published plan is permitted.
            # Every other legacy token remains banned, including in this module;
            # Stage10/materializer runtime date selection remains independent.
            from moex_data import rub_futures_calendar as published
            approved = 'https://apim.moex.com/iss/calendars/futures.json'
            assert published.BASE_URL == approved
            assert published.SCOPE == 'PUBLISHED_CALENDAR_ONLY'
            assert {'session_completion_proven', 'forecast_trading_targets_accepted',
                    'historical_pit_acceptance', 'action_authority'} <= set(published.DENIED)
            assert text.count(approved) == 1
            text = text.replace(approved, '')
        if path.as_posix() == 'src/moex_data/futures/date_source_provenance.py':
            # Retained roll metadata is compared locally; it cannot supply dates.
            # Every other forbidden token in this module remains checked below.
            text = _without_retained_source_identifier(text)
        for token in forbidden:
            if token in text:
                violations.append(path.as_posix() + ":" + token)
    assert violations == []


def test_stage10_date_source_requests_only_algopack_tradestats(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_API_KEY", "test-key")
    calls: list[tuple[str, dict[str, object]]] = []
    data_dates = {"2026-06-12", "2026-06-15"}

    def fake_get(url, *, params, headers, timeout):
        calls.append((str(url), dict(params)))
        request_date = str(params["date"])
        if int(params["start"]) > 0:
            return FakeResponse(_tradestats_payload([]), str(url))
        if request_date in data_dates:
            return FakeResponse(_tradestats_payload([["USDRUBF", request_date]]), str(url))
        return FakeResponse(_tradestats_payload([]), str(url))

    monkeypatch.setattr(refresh.requests, "get", fake_get)
    monkeypatch.setenv("MOEX_API_URL", "https://apim.moex.com")

    dates = step10._calendar_dates(start_date="2026-06-12", end_date="2026-06-15", timeout=1.0)

    forbidden = "/iss/" + "calendars"
    assert dates == ["2026-06-12", "2026-06-15"]
    assert calls
    assert all(url.endswith(refresh.OBSERVED_DATE_SOURCE_ENDPOINT) for url, _params in calls)
    assert all(forbidden not in url for url, _params in calls)
    first_requests = [params for _url, params in calls if int(params["start"]) == 0]
    assert [str(params["date"]) for params in first_requests] == [
        "2026-06-12",
        "2026-06-13",
        "2026-06-14",
        "2026-06-15",
    ]
    assert all(params["date"] == params["from"] == params["till"] for params in first_requests)
    assert all(params["secid"] == "USDRUBF" for params in first_requests)


def test_incremental_refresh_source_loader_never_requests_calendar_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_API_KEY", "test-key")
    monkeypatch.setenv("MOEX_API_URL", "https://apim.moex.com")
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

    forbidden = "/iss/" + "calendars"
    assert dates == ["2026-06-12", "2026-06-15", "2026-06-17"]
    assert calls
    # The historical loader now resolves an instrument path; exact-date Stage10 stays separate.
    assert all(url.endswith(refresh.observed_date_source_endpoint("USDRUBF")) for url in calls)
    assert all(forbidden not in url for url in calls)


def test_observed_source_absence_is_contextual_and_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_API_KEY", "test-key")
    monkeypatch.setenv("MOEX_API_URL", "https://apim.moex.com")
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
    assert refresh.observed_date_source_endpoint("USDRUBF") in message
    assert "secid=USDRUBF" in message
    assert "authoritative AlgoPack TradeStats source returned no observed trade dates" in message


def test_weekends_and_gaps_are_not_fabricated_by_stage10_date_source(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = ["2026-06-12", "2026-06-15", "2026-06-17"]

    def source_loader(date_start, date_end, *, instrument_id, registry_path=None, timeout, apim_base_url=None):
        assert date_start == "2026-06-12"
        assert date_end == "2026-06-17"
        assert instrument_id == "usdrubf_futures_family"
        assert registry_path is None
        assert timeout == 1.0
        assert apim_base_url is None
        return observed

    monkeypatch.setattr(step10.observed_dates, "observed_dates", source_loader)

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
