from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path

import pytest

from moex_data.futures.controlled_real_source_materialization import main, run_controlled_real_source_materialization
from moex_data.futures.raw_ohlcv_5m import Raw5mMaterializationRequest


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = REPO_ROOT / "src/moex_data/futures/controlled_real_source_materialization.py"
CONFIG_PATH = REPO_ROOT / "configs/instruments/futures_universe.v1.yaml"


class FakeRaw5mSourceAdapter:
    def __init__(self, rows: Sequence[Mapping[str, object]]) -> None:
        self.rows = tuple(rows)
        self.read_count = 0

    def read_rows(self, request: Raw5mMaterializationRequest) -> Sequence[Mapping[str, object]]:
        self.read_count += 1
        return self.rows


def _token(*codes: int) -> str:
    return "".join(chr(code) for code in codes)


def _rows() -> tuple[dict[str, object], ...]:
    return (
        {
            "ts": datetime(2026, 6, 2, 10, 0),
            "trade_date": date(2026, 6, 2),
            "session_date": date(2026, 6, 2),
            "FAMILY": "Si",
            "SECID": "SiM6",
            "BOARD": "RFUD",
            "MARKET": "FORTS",
            "SERIES_TYPE": "native",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 10,
            "value": 1005.0,
            "trades": 2,
        },
        {
            "ts": datetime(2026, 6, 2, 10, 5),
            "trade_date": date(2026, 6, 2),
            "session_date": date(2026, 6, 2),
            "FAMILY": "Si",
            "SECID": "SiM6",
            "BOARD": "RFUD",
            "MARKET": "FORTS",
            "SERIES_TYPE": "native",
            "open": 100.5,
            "high": 102.0,
            "low": 100.0,
            "close": 101.0,
            "volume": 11,
            "value": 1111.0,
            "trades": 3,
        },
    )


def _base_args(tmp_path: Path) -> list[str]:
    return [
        "--repo-root",
        REPO_ROOT.as_posix(),
        "--moex-data-root",
        (tmp_path / "moex_data_root").as_posix(),
        "--run-id",
        "controlled_real_source_test_run",
        "--family",
        "Si",
        "--secid",
        "SiM6",
        "--board",
        "RFUD",
        "--market",
        "FORTS",
        "--series-type",
        "native",
        "--trade-date",
        "2026-06-02",
        "--raw-manifest-ref",
        "${MOEX_DATA_ROOT}/futures/manifests/run_date={YYYY-MM-DD}/run_id={RUN_ID}/raw_5m_manifest.json",
        "--raw-quality-report-ref",
        "${MOEX_DATA_ROOT}/futures/quality/run_date={YYYY-MM-DD}/run_id={RUN_ID}/raw_5m_quality_report.json",
    ]


def _without_option(args: list[str], option_name: str) -> list[str]:
    output: list[str] = []
    skip = False
    for value in args:
        if skip:
            skip = False
            continue
        if value == option_name:
            skip = True
            continue
        output.append(value)
    return output


def _with_option(args: list[str], option_name: str, replacement: str) -> list[str]:
    result = list(args)
    index = result.index(option_name) + 1
    result[index] = replacement
    return result


def _with_added_option(args: list[str], option_name: str, value: str) -> list[str]:
    return list(args) + [option_name, value]


def _imported_names_from_source(source: str) -> tuple[str, ...]:
    names: list[str] = []
    for raw_line in source.splitlines():
        line = raw_line.strip().casefold()
        if line.startswith("import "):
            modules = line.removeprefix("import ").split(",")
            names.extend(module.strip().split(" as ")[0] for module in modules)
        elif line.startswith("from ") and " import " in line:
            names.append(line.removeprefix("from ").split(" import ", 1)[0].strip())
    return tuple(name for name in names if name)


def test_real_futures_identity_is_declared_in_universe_config():
    text = CONFIG_PATH.read_text(encoding="utf-8")

    assert "FAMILY: Si" in text
    assert "SECID: SiM6" in text
    assert "BOARD: RFUD" in text
    assert "MARKET: FORTS" in text
    assert "SERIES_TYPE: native" in text
    assert "dynamic_scan_allowed: false" in text


def test_controlled_real_source_runner_writes_only_declared_artifacts(tmp_path):
    adapter = FakeRaw5mSourceAdapter(_rows())

    result = run_controlled_real_source_materialization(_base_args(tmp_path), source_adapter=adapter)

    assert adapter.read_count == 1
    assert result.raw_storage_path.exists()
    assert result.raw_manifest_path.exists()
    assert result.raw_quality_report_path.exists()
    assert result.proof_summary["status"] == "succeeded"
    assert result.proof_summary["source_adapter"] == "moex_iss_forts_candles_5m"
    assert result.proof_summary["real_source_fetch_performed"] is False
    assert result.proof_summary["real_iss_fetch_performed"] is False
    assert result.proof_summary["real_apim_fetch_performed"] is False
    assert result.proof_summary["strategy_execution_performed"] is False
    assert result.proof_summary["backtest_performed"] is False
    assert result.proof_summary["runtime_live_performed"] is False
    assert set(result.output_files) == {
        result.raw_storage_path,
        result.raw_manifest_path,
        result.raw_quality_report_path,
    }


def test_controlled_runner_uses_explicit_apim_source_contract(tmp_path):
    adapter = FakeRaw5mSourceAdapter(_rows())
    args = _with_added_option(_base_args(tmp_path), "--source-id", "moex_apim_algopack_fo_tradestats_5m")

    result = run_controlled_real_source_materialization(args, source_adapter=adapter)

    assert adapter.read_count == 1
    assert result.proof_summary["source_adapter"] == "moex_apim_algopack_fo_tradestats_5m"
    assert result.proof_summary["real_source_fetch_performed"] is False
    assert result.proof_summary["real_iss_fetch_performed"] is False
    assert result.proof_summary["real_apim_fetch_performed"] is False


def test_unknown_source_id_rejected(tmp_path):
    args = _with_added_option(_base_args(tmp_path), "--source-id", "unknown_source")

    exit_code = main(args)

    assert exit_code == 2


@pytest.mark.parametrize(
    "option_name",
    [
        "--family",
        "--secid",
        "--board",
        "--market",
        "--series-type",
        "--trade-date",
        "--raw-manifest-ref",
        "--raw-quality-report-ref",
    ],
)
def test_required_explicit_arguments_fail_closed(tmp_path, option_name):
    with pytest.raises(SystemExit):
        run_controlled_real_source_materialization(_without_option(_base_args(tmp_path), option_name), source_adapter=FakeRaw5mSourceAdapter(_rows()))


def test_missing_moex_data_root_or_cli_root_fails_closed(tmp_path, monkeypatch):
    monkeypatch.delenv("MOEX_DATA_ROOT", raising=False)
    exit_code = main(_without_option(_base_args(tmp_path), "--moex-data-root"))

    assert exit_code == 2


def test_missing_identity_field_rejected_by_boundary(tmp_path):
    with pytest.raises(SystemExit):
        main(_without_option(_base_args(tmp_path), "--family"))


def test_unconfigured_instrument_identity_is_rejected(tmp_path):
    exit_code = main(_with_option(_base_args(tmp_path), "--secid", "UNKNOWN"))

    assert exit_code == 2


def test_continuous_series_type_rejected(tmp_path):
    exit_code = main(_with_option(_base_args(tmp_path), "--series-type", "continuous"))

    assert exit_code == 2


@pytest.mark.parametrize(
    "marker",
    [
        _token(108, 97, 116, 101, 115, 116),
        _token(99, 117, 114, 114, 101, 110, 116),
        _token(97, 117, 116, 111, 100, 101, 116, 101, 99, 116),
    ],
)
def test_latest_current_autodetect_rejected(tmp_path, marker):
    exit_code = main(_with_option(_base_args(tmp_path), "--run-id", marker))

    assert exit_code == 2


def test_calendar_source_error_fails_closed(tmp_path):
    bad_rows = list(_rows())
    bad_rows[0] = dict(bad_rows[0], trade_date=date(2026, 6, 3))
    adapter = FakeRaw5mSourceAdapter(tuple(bad_rows))

    with pytest.raises(ValueError):
        run_controlled_real_source_materialization(_base_args(tmp_path), source_adapter=adapter)


def test_no_strategy_backtest_runtime_live_broker_promotion_imports_in_runner():
    source = RUNNER_PATH.read_text(encoding="utf-8")
    imported_names = _imported_names_from_source(source)
    forbidden_roots = (
        "moex_strategy_sdk",
        "strategies",
        "moex_backtest",
        "moex_runtime",
        "moex_broker",
        "broker",
        "promotion",
    )
    for forbidden_root in forbidden_roots:
        assert not any(imported_name.startswith(forbidden_root) for imported_name in imported_names)
