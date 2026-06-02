from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from moex_data.futures import source_snapshot


def _row(
    tradetime: str = "10:00:00",
    secid: str = "SiM6",
    asset_code: str = "Si",
    tradedate: str = "2026-06-02",
    pr_open: object = 100,
    pr_high: object = 110,
    pr_low: object = 90,
    pr_close: object = 105,
    vol: object = 1,
    val: object = 1000,
    trades: object = 2,
) -> list[object]:
    values = {
        "tradedate": tradedate,
        "tradetime": tradetime,
        "secid": secid,
        "asset_code": asset_code,
        "pr_open": pr_open,
        "pr_high": pr_high,
        "pr_low": pr_low,
        "pr_close": pr_close,
        "vol": vol,
        "val": val,
        "trades": trades,
        "SYSTIME": "2026-06-02 10:00:01",
    }
    return [values[column] for column in source_snapshot.REQUIRED_SOURCE_COLUMNS]


def _payload(rows: list[list[object]] | None = None, columns: tuple[str, ...] | None = None) -> dict[str, object]:
    return {
        "data": {
            "columns": list(columns or source_snapshot.REQUIRED_SOURCE_COLUMNS),
            "data": rows if rows is not None else [_row(), _row("10:05:00")],
        },
        "data.cursor": {
            "columns": ["INDEX", "TOTAL", "PAGESIZE"],
            "data": [[0, 2, 100]],
        },
    }


def _patch_fetch(monkeypatch: pytest.MonkeyPatch, payload: dict[str, object] | None = None) -> None:
    body = json.dumps(payload or _payload())

    def fake_fetch(url: str) -> tuple[dict[str, object], str]:
        return json.loads(body), body

    monkeypatch.setattr(source_snapshot, "fetch_iss_json", fake_fetch)


def test_contract_file_loads_and_exact_source_contract_id_is_enforced() -> None:
    contract = source_snapshot.load_source_contract()
    assert contract["source_contract_id"] == source_snapshot.EXPECTED_SOURCE_CONTRACT_ID

    bad_contract = dict(contract)
    bad_contract["source_contract_id"] = "wrong"
    with pytest.raises(source_snapshot.SourceSnapshotError):
        source_snapshot.validate_source_contract_values(bad_contract)


def test_only_accepted_endpoint_and_query_are_allowed() -> None:
    contract = source_snapshot.load_source_contract()
    endpoint, query, url = source_snapshot.endpoint_and_query(contract)

    assert endpoint == "https://apim.moex.com/iss/datashop/algopack/fo/tradestats/SiM6.json"
    assert query == {"secid": "SiM6", "from": "2026-06-02", "till": "2026-06-02", "latest": 0}
    assert "latest=0" in url
    assert "from=2026-06-02" in url
    assert "till=2026-06-02" in url
    assert "secid=SiM6" in url

    bad_contract = dict(contract)
    bad_contract["endpoint"] = "https://iss.moex.com/iss/engines/futures/markets/forts/boards/rfud/securities/SiM6.json"
    with pytest.raises(source_snapshot.SourceSnapshotError):
        source_snapshot.validate_source_contract_values(bad_contract)

    bad_contract = dict(contract)
    bad_contract["query_parameters"] = {"secid": "SiM6", "from": "2026-06-02", "till": "2026-06-03", "latest": 0}
    with pytest.raises(source_snapshot.SourceSnapshotError):
        source_snapshot.validate_source_contract_values(bad_contract)


def test_missing_output_path_fails_closed() -> None:
    with pytest.raises(SystemExit):
        source_snapshot.build_arg_parser().parse_args(["--source-contract-id", source_snapshot.EXPECTED_SOURCE_CONTRACT_ID])


@pytest.mark.parametrize("marker", ["latest", "current", "autodetect"])
def test_dynamic_output_path_markers_fail_closed(tmp_path: Path, marker: str, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_fetch(monkeypatch)
    output_path = tmp_path / marker / "snapshot.csv"
    result = source_snapshot.main(
        [
            "--source-contract-id",
            source_snapshot.EXPECTED_SOURCE_CONTRACT_ID,
            "--output-path",
            str(output_path),
        ]
    )
    assert result == 1
    assert not output_path.exists()


@pytest.mark.parametrize(
    ("row_kwargs", "expected_fragment"),
    [
        ({"secid": "BRM6"}, "secid"),
        ({"asset_code": "BR"}, "asset_code"),
        ({"tradedate": "2026-06-03"}, "tradedate"),
    ],
)
def test_wrong_secid_date_family_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    row_kwargs: dict[str, object],
    expected_fragment: str,
) -> None:
    payload = _payload([_row(**row_kwargs)])
    _patch_fetch(monkeypatch, payload)
    output_path = tmp_path / "snapshot.csv"

    with pytest.raises(source_snapshot.SourceSnapshotError, match=expected_fragment):
        source_snapshot.create_source_snapshot(output_path)

    assert not output_path.exists()


def test_non_json_response_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeHeaders:
        def get(self, name: str, default: str = "") -> str:
            return "text/html"

    class FakeResponse:
        status = 200
        headers = FakeHeaders()

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

        def getcode(self) -> int:
            return 200

        def read(self) -> bytes:
            return b"<html></html>"

    monkeypatch.setattr(source_snapshot, "urlopen", lambda request, timeout: FakeResponse())

    with pytest.raises(source_snapshot.SourceSnapshotError, match="JSON"):
        source_snapshot.fetch_iss_json("https://apim.moex.com/iss/datashop/algopack/fo/tradestats/SiM6.json")


def test_http_non_200_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeHeaders:
        def get(self, name: str, default: str = "") -> str:
            return "application/json"

    class FakeResponse:
        status = 500
        headers = FakeHeaders()

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

        def getcode(self) -> int:
            return 500

        def read(self) -> bytes:
            return b"{}"

    monkeypatch.setattr(source_snapshot, "urlopen", lambda request, timeout: FakeResponse())

    with pytest.raises(source_snapshot.SourceSnapshotError, match="HTTP status"):
        source_snapshot.fetch_iss_json("https://apim.moex.com/iss/datashop/algopack/fo/tradestats/SiM6.json")


def test_missing_required_columns_fail_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    columns = tuple(column for column in source_snapshot.REQUIRED_SOURCE_COLUMNS if column != "SYSTIME")
    payload = _payload(rows=[_row()[:-1]], columns=columns)
    _patch_fetch(monkeypatch, payload)
    output_path = tmp_path / "snapshot.csv"

    with pytest.raises(source_snapshot.SourceSnapshotError, match="missing required"):
        source_snapshot.create_source_snapshot(output_path)

    assert not output_path.exists()


def test_zero_rows_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_fetch(monkeypatch, _payload(rows=[]))
    output_path = tmp_path / "snapshot.csv"

    with pytest.raises(source_snapshot.SourceSnapshotError, match="zero"):
        source_snapshot.create_source_snapshot(output_path)

    assert not output_path.exists()


def test_duplicate_tradedate_tradetime_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_fetch(monkeypatch, _payload(rows=[_row("10:00:00"), _row("10:00:00")]))
    output_path = tmp_path / "snapshot.csv"

    with pytest.raises(source_snapshot.SourceSnapshotError, match="duplicate"):
        source_snapshot.create_source_snapshot(output_path)

    assert not output_path.exists()


def test_non_monotonic_timestamps_fail_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_fetch(monkeypatch, _payload(rows=[_row("10:05:00"), _row("10:00:00")]))
    output_path = tmp_path / "snapshot.csv"

    with pytest.raises(source_snapshot.SourceSnapshotError, match="non-monotonic"):
        source_snapshot.create_source_snapshot(output_path)

    assert not output_path.exists()


@pytest.mark.parametrize(
    "row_kwargs",
    [
        {"pr_open": None},
        {"pr_high": 80},
        {"pr_low": 120},
        {"pr_close": 120},
        {"pr_open": -1},
    ],
)
def test_null_or_invalid_ohlc_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    row_kwargs: dict[str, object],
) -> None:
    _patch_fetch(monkeypatch, _payload(rows=[_row(**row_kwargs)]))
    output_path = tmp_path / "snapshot.csv"

    with pytest.raises(source_snapshot.SourceSnapshotError):
        source_snapshot.create_source_snapshot(output_path)

    assert not output_path.exists()


@pytest.mark.parametrize("row_kwargs", [{"vol": -1}, {"val": -1}, {"trades": -1}])
def test_negative_vol_val_trades_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    row_kwargs: dict[str, object],
) -> None:
    _patch_fetch(monkeypatch, _payload(rows=[_row(**row_kwargs)]))
    output_path = tmp_path / "snapshot.csv"

    with pytest.raises(source_snapshot.SourceSnapshotError, match="negative"):
        source_snapshot.create_source_snapshot(output_path)

    assert not output_path.exists()


def test_incomplete_cursor_pagination_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _payload()
    payload["data.cursor"] = {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[0, 200, 100]]}
    _patch_fetch(monkeypatch, payload)
    output_path = tmp_path / "snapshot.csv"

    with pytest.raises(source_snapshot.SourceSnapshotError, match="incomplete pagination"):
        source_snapshot.create_source_snapshot(output_path)

    assert not output_path.exists()


def test_valid_fake_response_writes_normalized_csv_to_explicit_output_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_fetch(monkeypatch)
    output_path = tmp_path / "explicit_snapshot.csv"

    result = source_snapshot.create_source_snapshot(output_path)

    assert result.source_artifact_path == str(output_path)
    assert result.row_count == 2
    assert result.endpoint == "https://apim.moex.com/iss/datashop/algopack/fo/tradestats/SiM6.json"
    assert result.query_parameters == {"secid": "SiM6", "from": "2026-06-02", "till": "2026-06-02", "latest": 0}
    assert result.source_sha256
    assert output_path.exists()

    with output_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert tuple(rows[0].keys()) == source_snapshot.RAW_5M_OUTPUT_COLUMNS
    assert rows[0]["trade_date"] == "2026-06-02"
    assert rows[0]["session_date"] == "2026-06-02"
    assert rows[0]["secid"] == "SiM6"
    assert rows[0]["family"] == "Si"
    assert rows[0]["board"] == "RFUD"
    assert rows[0]["source"] == "algopack.fo.tradestats.v1"


def test_output_file_schema_matches_materializer_required_columns() -> None:
    assert source_snapshot.RAW_5M_OUTPUT_COLUMNS == (
        "trade_date",
        "ts",
        "session_date",
        "secid",
        "family",
        "board",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "num_trades",
        "source",
        "ingest_ts",
    )


def test_no_data_lake_partition_path_is_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_fetch(monkeypatch)
    output_path = tmp_path / "source_snapshots" / "snapshot.csv"

    source_snapshot.create_source_snapshot(output_path)

    forbidden_partition = tmp_path / "futures" / "raw_5m" / "trade_date=2026-06-02" / "family=Si" / "secid=SiM6" / "part.parquet"
    assert output_path.exists()
    assert not forbidden_partition.exists()

    data_lake_output = tmp_path / "futures" / "raw_5m" / "trade_date=2026-06-02" / "family=Si" / "secid=SiM6" / "part.csv"
    with pytest.raises(source_snapshot.SourceSnapshotError, match="target partition"):
        source_snapshot.create_source_snapshot(data_lake_output)


def test_no_materialization_function_is_called_or_imported() -> None:
    source_text = Path(source_snapshot.__file__).read_text(encoding="utf-8")

    assert "materialize_single_raw_5m_partition" not in source_text
    assert "materialize_raw_5m" not in source_text
    assert "Raw5mMaterialization" not in source_text


def test_no_strategy_research_backtest_runtime_live_imports() -> None:
    source_text = Path(source_snapshot.__file__).read_text(encoding="utf-8")

    forbidden_terms = (
        "moex_research",
        "moex_backtest",
        "moex_runtime",
        "strategies.",
        "live_adapter",
        "backtest_adapter",
    )
    for term in forbidden_terms:
        assert term not in source_text


def test_no_fallback_endpoint_logic_exists() -> None:
    source_text = Path(source_snapshot.__file__).read_text(encoding="utf-8")

    forbidden_terms = (
        "candles",
        "marketdata",
        "orderbook",
        "trades.json",
        "/iss/engines/futures",
        "fallback",
    )
    for term in forbidden_terms:
        assert term not in source_text
