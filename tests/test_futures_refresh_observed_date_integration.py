"""Real refresh entrypoints and Parquet handoff; no live source requests."""

from argparse import Namespace
from datetime import date, timedelta
import json
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from moex_data.futures import algopack_availability_probe as source
from moex_data.futures import all_universe_eligibility_snapshot_runner as eligibility
from moex_data.futures import all_universe_futoi_raw_backfill_slice as futoi
from moex_data.futures import all_universe_raw_5m_backfill_slice as raw
from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures import universal_daily_refresh_runner as universal

DAY = "2026-09-18"
# Includes a Sunday and gaps: use supplied observations, not weekday inference.
DATES = ("2026-09-13", "2026-09-16", "2026-09-17")
APIM = "https://configured-apim.invalid"
ISS = "https://configured-iss.invalid"


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("live network forbidden")
    monkeypatch.setattr(requests.sessions.Session, "request", denied)
    for module in (eligibility, futoi, universal):
        monkeypatch.setattr(module, "load_dotenv", None)
    monkeypatch.setenv("MOEX_API_KEY", "synthetic-test-token")
    monkeypatch.setenv("MOEX_API_URL", "https://environment-apim.invalid")
    monkeypatch.setenv("MOEX_ISS_BASE_URL", "https://environment-iss.invalid")


def arguments(root, **overrides):
    values = dict(
        snapshot_date=DAY, run_date=DAY, from_date="", till="",
        data_root_resolved=root, apim_base_url=APIM, iss_base_url=ISS,
        timeout=2.5, availability_max_workers=1, family="", secid="",
    )
    values.update(overrides)
    return Namespace(**values)


def seed_inputs(root, secids):
    payload = pd.DataFrame({
        "SECID": list(secids), "BOARDID": ["RFUD"] * len(secids),
        "LASTTRADEDATE": ["" if value == "USDRUBF" else "2026-12-17" for value in secids],
    })
    snapshot = source.build_registry_snapshot(payload, DAY)
    normalized = source.build_normalized_registry(snapshot)
    contracts = base.load_contract_values(ROOT)
    normalized_path = base.resolve_contract_path(
        root, contracts, base.CONTRACT_BY_ID["normalized_registry"], DAY)
    normalized_path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_parquet(normalized_path, index=False)
    # Existing upstream availability evidence, not a mocked loader or selector.
    availability_path = futoi.resolve_availability_path(ROOT, root, DAY)
    availability_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        dict(snapshot_date=DAY, board=row.board, secid=row.secid,
             family_code=row.family_code, availability_status="available",
             probe_status="completed", observed_rows=1,
             first_available_date=DATES[0], last_available_date=DATES[-1],
             source_endpoint_url=APIM + "/iss/analyticalproducts/futoi/securities/"
             + futoi.futoi.ticker_for_instrument(row.secid, row.family_code) + ".json")
        for row in normalized.itertuples(index=False)
    ]).to_parquet(availability_path, index=False)
    return normalized_path, availability_path


def invoke(monkeypatch, module, stage, args):
    command = universal.command_for_stage(ROOT, stage, args)
    with monkeypatch.context() as patch:
        patch.chdir(ROOT)
        patch.setattr(sys, "argv", command[1:])
        return module.main()


def external_sources(monkeypatch):
    date_calls, futoi_calls = [], []

    def observed(first, last, *, secid, timeout, apim_base_url):
        date_calls.append((first, last, secid, timeout, apim_base_url))
        return {value for value in DATES if first <= value <= last}

    def request(origin, path, params, timeout, use_apim):
        assert origin == APIM and timeout == 2.5 and use_apim is True
        assert path.startswith("/iss/analyticalproducts/futoi/securities/")
        assert params["latest"] == 1 and params["start"] == 0
        futoi_calls.append((origin, path, dict(params)))
        ticker = path.rsplit("/", 1)[1].removesuffix(".json").upper()
        rows = []
        for value in DATES:
            if params["from"] <= value <= params["till"]:
                for group, long, short in (("FIZ", 10, -6), ("YUR", 6, -10)):
                    rows.append([value, "10:00:00", ticker, group, long + short,
                                 long, short, 2, 3, 7, 10, value + " 10:00:09"])
        columns = ["tradedate", "tradetime", "ticker", "clgroup", "pos",
                   "pos_long", "pos_short", "pos_long_num", "pos_short_num",
                   "sess_id", "seqnum", "systime"]
        return {"futoi": {"columns": columns, "data": rows}}

    # These are external read seams. Reference/date wrappers, CLI, builders,
    # selection, FUTOI pagination, normalization, quality and writers stay real.
    monkeypatch.setattr(base.observed_date_source, "fetch_observed_tradestats_dates", observed)
    monkeypatch.setattr(base, "request_json", request)
    return date_calls, futoi_calls


def connected_run(monkeypatch, tmp_path, capsys, secids=("SiZ6", "USDRUBF"), bounds=("", "")):
    normalized_path, availability_path = seed_inputs(tmp_path, secids)
    date_calls, futoi_calls = external_sources(monkeypatch)
    args = arguments(tmp_path, from_date=bounds[0], till=bounds[1])
    assert invoke(monkeypatch, eligibility, "all_universe_eligibility_snapshot", args) == 0
    eligibility_summary = json.loads(capsys.readouterr().out)
    input_paths = [normalized_path, availability_path]
    input_paths.extend(Path(value) for value in eligibility_summary["outputs"].values())
    before = {path: path.read_bytes() for path in input_paths}
    assert invoke(monkeypatch, futoi, "futoi_raw_refresh", args) == 0
    summary = json.loads(capsys.readouterr().out)
    manifest = json.loads(Path(summary["outputs"]["chunk_manifest"]).read_text())
    assert all(path.read_bytes() == content for path, content in before.items())
    return args, eligibility_summary, summary, manifest, date_calls, futoi_calls, before


@pytest.mark.parametrize("stage", [
    "registry_refresh", "all_universe_eligibility_snapshot", "raw_5m_refresh", "futoi_raw_refresh",
])
def test_universal_preserves_explicit_source_origin_at_every_acquisition_boundary(tmp_path, stage):
    args = arguments(tmp_path)
    command = universal.command_for_stage(ROOT, stage, args)
    assert command.count("--apim-base-url") == 1
    assert command[command.index("--apim-base-url") + 1] == APIM
    assert command[command.index("--timeout") + 1] == "2.5"
    if stage == "futoi_raw_refresh":
        assert "--iss-base-url" not in command
    else:
        assert command[command.index("--iss-base-url") + 1] == ISS


@pytest.mark.parametrize("secids", [("SiZ6", "USDRUBF"), ("SiZ6",)])
@pytest.mark.parametrize("bounds", [("", ""), ("2026-09-16", "2026-09-17")])
def test_generated_eligibility_to_futoi_chain_uses_actual_parquet_and_producers(
    monkeypatch, tmp_path, capsys, secids, bounds,
):
    args, esummary, summary, manifest, dates, requests_seen, _ = connected_run(
        monkeypatch, tmp_path, capsys, secids, bounds)
    first, last = bounds if bounds[0] else (DATES[0], DATES[-1])
    expected_dates = {value for value in DATES if first <= value <= last}
    assert dates == [
        ((date.fromisoformat(DAY) - timedelta(days=30)).isoformat(), DAY, "SiZ6", 2.5, APIM),
        (first, last, "USDRUBF" if "USDRUBF" in secids else "SiZ6", 2.5, APIM),
    ]
    assert esummary["selected_universe"]["trading_dates"] == list(DATES)
    assert set(summary["selected_universe"]["secids"]) == set(secids)
    assert manifest["status"] == "succeeded"
    assert manifest["calendar_validation_summary"]["calendar_denominator_status"] == base.OBSERVED_DATE_STATUS
    assert manifest["calendar_validation_summary"]["expected_trading_days"] == len(expected_dates)
    assert len(requests_seen) == len(secids)
    assert {(call[2]["from"], call[2]["till"]) for call in requests_seen} == {(first, last)}
    quality = pd.read_parquet(summary["outputs"]["quality_report"])
    assert quality["quality_status"].tolist() == ["pass"] * len(secids)
    assert quality["rows_written"].tolist() == [len(expected_dates) * 2] * len(secids)
    assert set(quality["calendar_status"]) == {base.OBSERVED_DATE_STATUS}
    assert len(manifest["output_partitions"]) == len(expected_dates) * len(secids)
    raw_frames = [pd.read_parquet(path) for path in manifest["output_partitions"]]
    data = pd.concat(raw_frames, ignore_index=True)
    assert set(data.trade_date) == expected_dates
    assert set(data.secid) == set(secids)
    assert not data.duplicated(["trade_date", "ts", "secid", "clgroup"]).any()
    original = pd.read_parquet(esummary["outputs"]["eligibility_snapshot"])
    derived = pd.read_parquet(summary["outputs"]["futoi_eligibility_snapshot"])
    assert not original.futoi_eligible.any()
    assert derived.futoi_eligible.all()
    assert derived.registry_snapshot_id.tolist() == original.registry_snapshot_id.tolist()
    assert not derived.continuous_v1_eligible.any()
    # Same scope can be run again without multiplying raw primary keys.
    assert invoke(monkeypatch, futoi, "futoi_raw_refresh", args) == 0
    again = json.loads(capsys.readouterr().out)
    again_manifest = json.loads(Path(again["outputs"]["chunk_manifest"]).read_text())
    after = pd.concat([pd.read_parquet(path) for path in again_manifest["output_partitions"]])
    assert len(after) == len(data)
    assert not after.duplicated(["trade_date", "ts", "secid", "clgroup"]).any()


@pytest.mark.parametrize("mode,secids,reference", [
    (raw.MODE_L3_2, ("SiM6", "SiU6", "USDRUBF"), "SiM6"),
    (raw.MODE_L3_3, ("SiZ6", "USDRUBF"), "SiZ6"),
])
def test_eligibility_cli_env_origin_keeps_existing_stage_reference_policy(
    monkeypatch, tmp_path, capsys, mode, secids, reference,
):
    seed_inputs(tmp_path, secids)
    date_calls, _ = external_sources(monkeypatch)
    with monkeypatch.context() as patch:
        patch.chdir(ROOT)
        patch.setattr(sys, "argv", ["eligibility", "--snapshot-date", DAY, "--run-date", DAY,
                                   "--data-root", str(tmp_path), "--selection-mode", mode,
                                   "--timeout", "2.5", "--iss-base-url", ISS])
        assert eligibility.main() == 0
    capsys.readouterr()
    assert date_calls[0][2:] == (reference, 2.5, "https://environment-apim.invalid")


@pytest.mark.parametrize("fault", ["empty", "exception", "too_few"])
def test_eligibility_source_failure_preserves_previous_handoff(monkeypatch, tmp_path, capsys, fault):
    _, _, _, _, _, _, before = connected_run(monkeypatch, tmp_path, capsys)

    def failed(*args, **kwargs):
        if fault == "exception":
            raise RuntimeError("synthetic date source failure")
        return set() if fault == "empty" else set(DATES[:2])

    monkeypatch.setattr(base.observed_date_source, "fetch_observed_tradestats_dates", failed)
    with pytest.raises(RuntimeError, match="observed TradeStats dates"):
        invoke(monkeypatch, eligibility, "all_universe_eligibility_snapshot", arguments(tmp_path))
    assert all(path.read_bytes() == content for path, content in before.items())


@pytest.mark.parametrize("fault", ["empty", "exception"])
def test_futoi_date_failure_does_not_fetch_positions_or_change_accepted_raw(
    monkeypatch, tmp_path, capsys, fault,
):
    args, _, _, manifest, _, requests_seen, before = connected_run(monkeypatch, tmp_path, capsys)
    accepted = {Path(path): Path(path).read_bytes() for path in manifest["output_partitions"]}
    requests_seen.clear()

    def failed(*args, **kwargs):
        if fault == "exception":
            raise RuntimeError("synthetic date source failure")
        return set()

    monkeypatch.setattr(base.observed_date_source, "fetch_observed_tradestats_dates", failed)
    with pytest.raises(RuntimeError, match="authoritative observed TradeStats date validation failed"):
        invoke(monkeypatch, futoi, "futoi_raw_refresh", args)
    assert requests_seen == []
    assert all(path.read_bytes() == content for path, content in accepted.items())
    assert all(path.read_bytes() == content for path, content in before.items())


@pytest.mark.parametrize("dates,status", [
    (set(DATES), "canonical_apim_futures_xml"),
    (set(DATES), "unresolved"),
    (None, base.OBSERVED_DATE_STATUS),
    (set(), base.OBSERVED_DATE_STATUS),
])
def test_futoi_does_not_relabel_an_invalid_denominator(monkeypatch, tmp_path, dates, status):
    selected = pd.DataFrame([dict(secid="SiZ6", family_code="Si",
                                 selected_trading_dates_json=json.dumps(DATES))])
    monkeypatch.setattr(base, "fetch_observed_trading_dates", lambda *args: (dates, status))

    def forbidden(*args, **kwargs):
        raise AssertionError("date failure must precede instrument execution")

    monkeypatch.setattr(futoi, "run_instrument", forbidden)
    with pytest.raises(RuntimeError, match="authoritative observed TradeStats date validation failed"):
        futoi.run_chunk(arguments(tmp_path, exact_contract_only=False), tmp_path, selected, "run", "chunk")
    assert list(tmp_path.iterdir()) == []


CLI_NETWORK_GUARD = """
import runpy, sys, requests
def denied(*args, **kwargs):
    raise AssertionError("live network forbidden")
requests.sessions.Session.request = denied
sys.argv = sys.argv[1:]
runpy.run_path(sys.argv[0], run_name="__main__")
"""


@pytest.mark.parametrize("stage,missing", [
    ("all_universe_eligibility_snapshot", "Missing normalized registry artifact"),
    ("raw_5m_refresh", "Missing normalized registry artifact"),
    ("futoi_raw_refresh", "Missing required eligibility snapshot"),
])
def test_actual_generated_cli_reaches_input_validation_not_import_or_parser_failure(tmp_path, stage, missing):
    command = universal.command_for_stage(ROOT, stage, arguments(tmp_path))
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(ROOT / "src"))
    result = subprocess.run(
        [command[0], "-B", "-c", CLI_NETWORK_GUARD, *command[1:]],
        cwd=ROOT, env=env, text=True, capture_output=True, timeout=30,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert missing in result.stderr
    assert "unrecognized arguments" not in result.stderr
    assert "ImportError" not in result.stderr
    assert "ModuleNotFoundError" not in result.stderr
    assert list(tmp_path.iterdir()) == []
