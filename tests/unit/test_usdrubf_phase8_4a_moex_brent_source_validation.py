from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import fields, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest

from moex_research.external_data import moex_brent_history as brent
from moex_research.runners import (
    usdrubf_phase8_4a_moex_brent_source_validation as runner,
)


ROOT = Path(__file__).resolve().parents[2]
RETRIEVED = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)
FULL_SHA = "a" * 40


def _dates() -> pd.DatetimeIndex:
    available = pd.bdate_range(runner.EXPECTED_FIRST_TARGET, runner.EXPECTED_LAST_TARGET)
    indices = [round(index * (len(available) - 1) / 471) for index in range(472)]
    selected = available[indices]
    assert len(selected) == len(set(selected)) == 472
    return selected


def _dataset() -> pd.DataFrame:
    dates = _dates()
    return pd.DataFrame(
        {
            "target_phase_label": [runner.CLASS_ORDER[index % 3] for index in range(472)],
            "target_is_labeled": True,
            "target_source": runner.TARGET_SOURCE,
            "target_trade_date": dates.strftime("%Y-%m-%d"),
            "target_instrument_id": runner.EXPECTED_INSTRUMENT,
            "prior_trade_date": (dates - pd.offsets.BDay(1)).strftime("%Y-%m-%d"),
        }
    )


def _phase83_gates() -> dict[str, object]:
    return {
        "G12_final_acceptance": {
            "passed": False,
            "status": runner.EXPECTED_PHASE83_STATUS,
            "failed_gates": ["G5", "G6", "G7", "G8", "G9"],
            "recommendation": runner.EXPECTED_PHASE83_RECOMMENDATION,
        }
    }


def _write_inputs(tmp_path: Path) -> tuple[runner.Phase84ARequest, dict[str, str]]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    frame = _dataset()
    paths = {
        "modeling_dataset": tmp_path / "modeling_dataset.parquet",
        "dataset_manifest": tmp_path / "manifest.json",
        "feature_schema": tmp_path / "feature_schema.json",
        "m0_validation_predictions": tmp_path / "validation_predictions.parquet",
        "phase83_aggregate_metrics": tmp_path / "aggregate_metrics_by_matrix.json",
        "phase83_gate_results": tmp_path / "gate_results.json",
        "phase81_source_contract": (
            ROOT / "contracts/experiments/usdrubf_phase8_1_external_data_acquisition_v1.json"
        ),
        "experiment_contract": (
            ROOT / "contracts/experiments/usdrubf_phase8_4a_moex_brent_source_validation_v1.json"
        ),
    }
    frame.to_parquet(paths["modeling_dataset"], index=False)
    paths["dataset_manifest"].write_text("{}\n", encoding="utf-8")
    paths["feature_schema"].write_text("{}\n", encoding="utf-8")
    frame.iloc[-320:][list(runner.IDENTITY_COLUMNS)].to_parquet(
        paths["m0_validation_predictions"], index=False
    )
    paths["phase83_aggregate_metrics"].write_text(
        json.dumps({"final_status": runner.EXPECTED_PHASE83_STATUS}) + "\n",
        encoding="utf-8",
    )
    paths["phase83_gate_results"].write_text(
        json.dumps(_phase83_gates()) + "\n", encoding="utf-8"
    )
    hashes = {
        name: hashlib.sha256(paths[name].read_bytes()).hexdigest()
        for name in runner.EXPECTED_INPUT_SHA256
    }
    request = runner.Phase84ARequest(
        modeling_dataset_path=paths["modeling_dataset"],
        dataset_manifest_path=paths["dataset_manifest"],
        feature_schema_path=paths["feature_schema"],
        m0_validation_predictions_path=paths["m0_validation_predictions"],
        phase83_aggregate_metrics_path=paths["phase83_aggregate_metrics"],
        phase83_gate_results_path=paths["phase83_gate_results"],
        phase81_source_contract_path=paths["phase81_source_contract"],
        experiment_contract_path=paths["experiment_contract"],
        output_dir=tmp_path / "output",
        run_id="synthetic_phase8_4a",
        git_commit_sha=FULL_SHA,
    )
    return request, hashes


def _description(code: str) -> bytes:
    definitions = {
        "BRU4": ("BR-9.24", "2023-07-01", "2024-09-01"),
        "BRN6": ("BR-7.26", "2024-07-01", "2026-07-01"),
    }
    short_name, first, expiration = definitions[code]
    values = {
        "SECID": code,
        "SHORTNAME": short_name,
        "FRSTTRADE": first,
        "LSTTRADE": expiration,
        "LSTDELDATE": expiration,
        "ASSETCODE": "BR",
        "GROUP": "futures_forts",
        "TYPE": "futures",
    }
    return json.dumps(
        {
            "description": {
                "columns": ["name", "title", "value"],
                "data": [[name, name, value] for name, value in values.items()],
            },
            "boards": {
                "columns": ["secid", "boardid", "history_from", "history_till"],
                "data": [[code, "RFUD", first, expiration]],
            },
        }
    ).encode()


class SyntheticMOEX:
    def __init__(self, *, empty_code: str | None = None) -> None:
        self.urls: list[str] = []
        self.empty_code = empty_code

    def __call__(self, url: str) -> bytes:
        self.urls.append(url)
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        if parsed.path.endswith("/history/engines/futures/markets/forts/boards/RFUD/securities.json"):
            trade_date = date.fromisoformat(query["date"][0])
            rows = []
            if trade_date <= date(2024, 9, 1):
                rows.append(["RFUD", "BRU4", trade_date.isoformat(), "BR-9.24", "BR"])
            rows.append(["RFUD", "BRN6", trade_date.isoformat(), "BR-7.26", "BR"])
            return json.dumps(
                {
                    "history": {
                        "columns": ["BOARDID", "SECID", "TRADEDATE", "SHORTNAME", "ASSETCODE"],
                        "data": rows,
                    },
                    "history.cursor": {
                        "columns": ["INDEX", "TOTAL", "PAGESIZE"],
                        "data": [[0, len(rows), 100]],
                    },
                }
            ).encode()
        if parsed.path.startswith("/iss/securities/"):
            return _description(Path(parsed.path).stem)
        if parsed.path.endswith("/candles.json"):
            code = parsed.path.split("/securities/", 1)[1].split("/", 1)[0]
            if code == self.empty_code:
                rows: list[list[object]] = []
            else:
                trade_date = query["from"][0]
                rows = [
                    [80.0, 81.0, 82.0, 79.0, 1000.0, 100.0, f"{trade_date} 00:00:00", f"{trade_date} 23:59:59"]
                ]
            return json.dumps(
                {
                    "candles": {
                        "columns": ["open", "close", "high", "low", "value", "volume", "begin", "end"],
                        "data": rows,
                    }
                }
            ).encode()
        raise AssertionError(f"non-MOEX or unexpected route: {url}")


def _run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[runner.Phase84ARequest, SyntheticMOEX]:
    request, hashes = _write_inputs(tmp_path)
    monkeypatch.setattr(runner, "EXPECTED_INPUT_SHA256", hashes)
    transport = SyntheticMOEX()
    result = runner.run_source_validation(
        request, transport=transport, clock=lambda: RETRIEVED
    )
    assert result.final_status == "moex_brent_source_candidate_for_phase8_5"
    return request, transport


def test_all_six_immutable_evidence_hashes_are_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, hashes = _write_inputs(tmp_path)
    monkeypatch.setattr(runner, "EXPECTED_INPUT_SHA256", hashes)
    assert runner.verify_immutable_inputs(request) == hashes
    request.phase83_gate_results_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(runner.Phase84ABrentSourceValidationError, match="hash mismatch"):
        runner.verify_immutable_inputs(request)


def test_controlled_synthetic_run_preserves_identity_PIT_roll_and_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, transport = _run(tmp_path, monkeypatch)
    assert sorted(path.name for path in request.output_dir.iterdir()) == sorted(
        runner.DECLARED_OUTPUT_ARTIFACTS
    )
    matrix = pd.read_parquet(request.output_dir / "brent_pit_acceptance_matrix.parquet")
    coverage = pd.read_csv(request.output_dir / "coverage_by_source.csv")
    rolls = pd.read_csv(request.output_dir / "contract_roll_diagnostics.csv")
    assert len(matrix) == 472
    assert coverage.loc[0, "eligible_covered_count"] == 472
    assert coverage.loc[0, "validation_covered_count"] == 320
    assert (matrix["brent_trade_date"] == matrix["prior_trade_date"]).all()
    assert matrix["brent_days_to_expiration"].ge(7).all()
    assert matrix["brent_contract_changed"].sum() == len(rolls) == 1
    assert not rolls["target_or_future_information_used"].any()
    assert not rolls["cross_contract_return_calculated"].any()
    assert not any(column.endswith("return") for column in matrix.columns)
    history_dates = {
        parse_qs(urlsplit(url).query)["date"][0]
        for url in transport.urls
        if "/iss/history/" in url
    }
    assert history_dates == set(matrix["prior_trade_date"])
    assert (
        pd.to_datetime(matrix["prior_trade_date"])
        < pd.to_datetime(matrix["target_trade_date"])
    ).all()
    gates = json.loads((request.output_dir / "gate_results.json").read_text())
    assert all(payload["passed"] for payload in gates.values())
    blockers = json.loads(
        (request.output_dir / "source_blocker_register.json").read_text()
    )
    assert blockers["status"] == "candidate_for_phase8_5"
    assert blockers["blocker_classification"] is None


def test_target_day_and_post_cutoff_candles_are_rejected() -> None:
    contract = brent.BrentContract(
        source_id=brent.SOURCE_ID,
        contract_code="BRU4",
        short_name="BR-9.24",
        asset_code="BR",
        board_id="RFUD",
        first_verified_trade_date=date(2023, 7, 1),
        expiration_date=date(2024, 9, 1),
        last_delivery_date=date(2024, 9, 1),
        metadata_route=brent.build_security_description_url("BRU4"),
        metadata_retrieved_at_utc=RETRIEVED,
        metadata_raw_payload_sha256="a" * 64,
        source_revision_status=brent.SOURCE_REVISION_STATUS,
        historical_model_use_status=brent.HISTORICAL_MODEL_USE_STATUS,
        enumerated_as_of_date=date(2024, 8, 2),
        enumeration_route=brent.build_history_universe_url(date(2024, 8, 2)),
        enumeration_retrieved_at_utc=RETRIEVED,
        enumeration_raw_payload_sha256="b" * 64,
    )
    candle = brent.BrentDailyCandle(
        source_id=brent.SOURCE_ID,
        contract_code=contract.contract_code,
        trade_date=date(2024, 8, 5),
        open=80,
        high=82,
        low=79,
        close=81,
        volume=100,
        value=1000,
        candle_begin=datetime(2024, 8, 5, tzinfo=brent.MOSCOW),
        candle_end=datetime(2024, 8, 5, 8, 0, tzinfo=brent.MOSCOW),
        expiration_date=contract.expiration_date,
        source_route=brent.build_candle_url("BRU4", date(2024, 8, 5)),
        retrieved_at_utc=RETRIEVED,
        raw_payload_sha256="c" * 64,
        source_revision_status=brent.SOURCE_REVISION_STATUS,
        historical_model_use_status=brent.HISTORICAL_MODEL_USE_STATUS,
    )
    with pytest.raises(brent.BrentHistoryError, match="prior-session"):
        brent.validate_prior_session_cutoff(
            candle,
            target_trade_date=date(2024, 8, 5),
            prior_trade_date=date(2024, 8, 2),
        )
    post_cutoff = replace(
        candle,
        trade_date=date(2024, 8, 2),
        candle_begin=datetime(2024, 8, 2, tzinfo=brent.MOSCOW),
        candle_end=datetime(2024, 8, 5, 9, 0, tzinfo=brent.MOSCOW),
    )
    with pytest.raises(brent.BrentHistoryError, match="cutoff"):
        brent.validate_prior_session_cutoff(
            post_cutoff,
            target_trade_date=date(2024, 8, 5),
            prior_trade_date=date(2024, 8, 2),
        )


def test_missing_selected_contract_candle_fails_without_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, hashes = _write_inputs(tmp_path)
    monkeypatch.setattr(runner, "EXPECTED_INPUT_SHA256", hashes)
    transport = SyntheticMOEX(empty_code="BRU4")
    with pytest.raises(
        runner.Phase84ABrentSourceValidationError, match="candle history is empty"
    ) as raised:
        runner.run_source_validation(
            request, transport=transport, clock=lambda: RETRIEVED
        )
    assert raised.value.blocker == "expired_contract_candles_not_available"
    candle_urls = [url for url in transport.urls if urlsplit(url).path.endswith("candles.json")]
    assert len(candle_urls) == 1
    assert "/BRU4/candles.json" in candle_urls[0]
    assert not any("/BRN6/candles.json" in url for url in candle_urls)


def test_preexisting_output_directory_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, hashes = _write_inputs(tmp_path)
    monkeypatch.setattr(runner, "EXPECTED_INPUT_SHA256", hashes)
    request.output_dir.mkdir()
    with pytest.raises(runner.Phase84ABrentSourceValidationError, match="pre-exist"):
        runner.run_source_validation(
            request, transport=SyntheticMOEX(), clock=lambda: RETRIEVED
        )


def test_no_write_outside_output_directory(tmp_path: Path) -> None:
    payloads = {name: {} for name in runner.DECLARED_OUTPUT_ARTIFACTS}
    escaped = ("../escape.json", *runner.DECLARED_OUTPUT_ARTIFACTS[1:])
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(runner, "DECLARED_OUTPUT_ARTIFACTS", escaped)
        escaped_payloads = {name: payloads.get(name, {}) for name in escaped}
        with pytest.raises(runner.Phase84ABrentSourceValidationError, match="outside"):
            runner._write_exact_artifacts(tmp_path / "output", escaped_payloads)
    assert not (tmp_path / "escape.json").exists()


def test_no_model_fit_serialization_subprocess_or_non_MOEX_source_access() -> None:
    source = inspect.getsource(runner).lower()
    assert ".fit(" not in source
    assert "sklearn" not in source
    assert "joblib" not in source
    assert "pickle" not in source
    assert "subprocess" not in source
    assert "requests" not in source
    assert "cmegroup" not in source
    assert "cbr.ru" not in source
    assert "ine.cn" not in source


def test_final_gate_fails_when_any_prior_gate_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, _ = _run(tmp_path, monkeypatch)
    eligible = _dataset().loc[:, [*runner.IDENTITY_COLUMNS, "prior_trade_date"]]
    validation = eligible.iloc[-320:][list(runner.IDENTITY_COLUMNS)].reset_index(drop=True)
    universe = pd.read_parquet(request.output_dir / "brent_contract_universe.parquet")
    candles = pd.read_parquet(request.output_dir / "brent_daily_candles_normalized.parquet")
    matrix = pd.read_parquet(request.output_dir / "brent_pit_acceptance_matrix.parquet")
    coverage = pd.read_csv(request.output_dir / "coverage_by_source.csv")
    rolls = pd.read_csv(request.output_dir / "contract_roll_diagnostics.csv")
    route_validation = json.loads(
        (request.output_dir / "official_route_validation.json").read_text()
    )
    gates = runner.evaluate_gates(
        immutable_inputs_verified=False,
        phase83_verified=True,
        eligible=eligible,
        validation=validation,
        universe=universe,
        candles=candles,
        matrix=matrix,
        coverage=coverage,
        rolls=rolls,
        route_validation=route_validation,
    )
    assert gates["G1_immutable_inputs"]["passed"] is False
    assert gates["G9_final_source_readiness"]["passed"] is False
    assert gates["G9_final_source_readiness"]["status"] == (
        "moex_brent_source_remains_blocked"
    )


def test_exact_runner_CLI() -> None:
    parser = runner.build_argument_parser()
    required = {
        action.option_strings[0]
        for action in parser._actions
        if action.required and action.option_strings
    }
    assert required == set(runner.REQUIRED_CLI_ARGS)
    assert "--retrieved-at-utc" not in {
        option for action in parser._actions for option in action.option_strings
    }
    arguments = [
        value
        for flag in runner.REQUIRED_CLI_ARGS
        for value in (flag, "synthetic-value")
    ]
    with pytest.raises(SystemExit):
        parser.parse_args([*arguments, "--retrieved-at-utc", RETRIEVED.isoformat()])


def test_request_and_runner_have_no_caller_supplied_or_shared_retrieval_timestamp() -> None:
    assert "retrieved_at_utc" not in {field.name for field in fields(runner.Phase84ARequest)}
    source = inspect.getsource(runner.run_source_validation)
    assert "datetime.now" not in source
    assert "retrieved_at" not in source


def test_injected_clock_is_called_once_after_every_history_metadata_and_candle_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, hashes = _write_inputs(tmp_path)
    monkeypatch.setattr(runner, "EXPECTED_INPUT_SHA256", hashes)
    transport = SyntheticMOEX()
    clock_routes: list[str] = []

    def clock() -> datetime:
        clock_routes.append(transport.urls[-1])
        return RETRIEVED + timedelta(seconds=len(clock_routes))

    runner.run_source_validation(request, transport=transport, clock=clock)
    assert clock_routes == transport.urls
    assert any("/iss/history/" in route for route in clock_routes)
    assert any("/iss/securities/" in route for route in clock_routes)
    assert any("/candles.json" in route for route in clock_routes)
    universe = pd.read_parquet(request.output_dir / "brent_contract_universe.parquet")
    candles = pd.read_parquet(request.output_dir / "brent_daily_candles_normalized.parquet")
    matrix = pd.read_parquet(request.output_dir / "brent_pit_acceptance_matrix.parquet")
    assert universe["enumeration_retrieved_at_utc"].notna().all()
    assert universe["metadata_retrieved_at_utc"].notna().all()
    assert candles["retrieved_at_utc"].nunique() == len(candles)
    assert matrix["brent_retrieved_at_utc"].nunique() == len(matrix)


@pytest.mark.parametrize(
    ("frame_name", "timestamp_column"),
    [
        ("universe", "enumeration_retrieved_at_utc"),
        ("universe", "metadata_retrieved_at_utc"),
        ("candles", "retrieved_at_utc"),
        ("matrix", "brent_retrieved_at_utc"),
    ],
)
def test_G7_fails_for_malformed_exact_payload_provenance_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    frame_name: str,
    timestamp_column: str,
) -> None:
    request, _ = _run(tmp_path, monkeypatch)
    frames = {
        "universe": pd.read_parquet(
            request.output_dir / "brent_contract_universe.parquet"
        ),
        "candles": pd.read_parquet(
            request.output_dir / "brent_daily_candles_normalized.parquet"
        ),
        "matrix": pd.read_parquet(
            request.output_dir / "brent_pit_acceptance_matrix.parquet"
        ),
    }
    frames[frame_name][timestamp_column] = frames[frame_name][
        timestamp_column
    ].astype(object)
    frames[frame_name].loc[0, timestamp_column] = "not-a-UTC-timestamp"
    eligible = _dataset().loc[:, [*runner.IDENTITY_COLUMNS, "prior_trade_date"]]
    validation = eligible.iloc[-320:][list(runner.IDENTITY_COLUMNS)].reset_index(
        drop=True
    )
    coverage = pd.read_csv(request.output_dir / "coverage_by_source.csv")
    rolls = pd.read_csv(request.output_dir / "contract_roll_diagnostics.csv")
    route_validation = json.loads(
        (request.output_dir / "official_route_validation.json").read_text()
    )
    gates = runner.evaluate_gates(
        immutable_inputs_verified=True,
        phase83_verified=True,
        eligible=eligible,
        validation=validation,
        universe=frames["universe"],
        candles=frames["candles"],
        matrix=frames["matrix"],
        coverage=coverage,
        rolls=rolls,
        route_validation=route_validation,
    )
    assert gates["G7_provenance"]["passed"] is False
    assert gates["G9_final_source_readiness"]["blocker_classification"] == (
        "provenance_not_sufficient"
    )


def test_G7_fails_when_one_metadata_payload_key_maps_to_two_valid_timestamps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, _ = _run(tmp_path, monkeypatch)
    universe = pd.read_parquet(
        request.output_dir / "brent_contract_universe.parquet"
    )
    first_code = universe.loc[0, "contract_code"]
    repeated = universe.index[universe["contract_code"].eq(first_code)]
    assert len(repeated) > 1
    universe.loc[repeated[0], "metadata_retrieved_at_utc"] = (
        pd.Timestamp(RETRIEVED) + pd.Timedelta(seconds=1)
    )
    candles = pd.read_parquet(
        request.output_dir / "brent_daily_candles_normalized.parquet"
    )
    matrix = pd.read_parquet(
        request.output_dir / "brent_pit_acceptance_matrix.parquet"
    )
    eligible = _dataset().loc[:, [*runner.IDENTITY_COLUMNS, "prior_trade_date"]]
    validation = eligible.iloc[-320:][list(runner.IDENTITY_COLUMNS)].reset_index(
        drop=True
    )
    gates = runner.evaluate_gates(
        immutable_inputs_verified=True,
        phase83_verified=True,
        eligible=eligible,
        validation=validation,
        universe=universe,
        candles=candles,
        matrix=matrix,
        coverage=pd.read_csv(request.output_dir / "coverage_by_source.csv"),
        rolls=pd.read_csv(request.output_dir / "contract_roll_diagnostics.csv"),
        route_validation=json.loads(
            (request.output_dir / "official_route_validation.json").read_text()
        ),
    )
    assert gates["G7_provenance"]["passed"] is False
    assert gates["G9_final_source_readiness"]["blocker_classification"] == (
        "provenance_not_sufficient"
    )
