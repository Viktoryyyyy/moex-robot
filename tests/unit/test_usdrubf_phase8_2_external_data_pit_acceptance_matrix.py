from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from moex_research.external_data.cbr import KEY_RATE_HEADERS, RUONIA_HEADERS
from moex_research.runners import (
    usdrubf_phase8_2_external_data_pit_acceptance_matrix as runner,
)


ROOT = Path(__file__).resolve().parents[2]
FULL_SHA = "a" * 40
RETRIEVED = datetime(2026, 7, 15, 6, 0, tzinfo=timezone.utc)


def _table(headers: tuple[str, ...], rows: list[tuple[object, ...]]) -> bytes:
    header = "".join(f"<th>{cell}</th>" for cell in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f"<html><table><tr>{header}</tr>{body}</table></html>".encode()


def _dataset() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=runner.EXPECTED_ELIGIBLE_IDENTITIES)
    return pd.DataFrame(
        {
            "target_phase_label": [
                runner.CLASS_ORDER[index % len(runner.CLASS_ORDER)]
                for index in range(len(dates))
            ],
            "target_is_labeled": True,
            "target_source": runner.TARGET_SOURCE,
            "target_trade_date": dates.strftime("%Y-%m-%d"),
            "target_instrument_id": runner.EXPECTED_INSTRUMENT,
            "unused_internal_feature": range(len(dates)),
        }
    )


def _manifest() -> dict[str, object]:
    targets = [
        "target_phase_label",
        "target_is_labeled",
        "target_source",
        "target_trade_date",
        "target_instrument_id",
    ]
    return {
        "dataset_id": runner.DATASET_ID,
        "feature_schema_id": runner.FEATURE_SCHEMA_ID,
        "target_source": runner.TARGET_SOURCE,
        "target_columns": targets,
    }


def _feature_schema() -> dict[str, object]:
    return {
        "schema_id": runner.FEATURE_SCHEMA_ID,
        "dataset_id": runner.DATASET_ID,
        "target_columns": _manifest()["target_columns"],
    }


def _write_inputs(
    tmp_path: Path,
    *,
    dataset: pd.DataFrame | None = None,
    validation_rows: int = runner.EXPECTED_VALIDATION_IDENTITIES,
) -> tuple[dict[str, Path], dict[str, str]]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    frame = dataset if dataset is not None else _dataset()
    paths = {
        "modeling_dataset": tmp_path / "modeling_dataset.parquet",
        "dataset_manifest": tmp_path / "manifest.json",
        "feature_schema": tmp_path / "feature_schema.json",
        "m0_validation_predictions": tmp_path / "validation_predictions.parquet",
        "phase81_source_contract": (
            ROOT
            / "contracts/experiments/usdrubf_phase8_1_external_data_acquisition_v1.json"
        ),
        "phase82_experiment_contract": tmp_path / "phase82_contract.json",
    }
    frame.to_parquet(paths["modeling_dataset"], index=False)
    paths["dataset_manifest"].write_text(
        json.dumps(_manifest(), sort_keys=True) + "\n", encoding="utf-8"
    )
    paths["feature_schema"].write_text(
        json.dumps(_feature_schema(), sort_keys=True) + "\n", encoding="utf-8"
    )
    frame.loc[: validation_rows - 1, [*runner.IDENTITY_COLUMNS]].assign(
        fold_id=1,
        candidate_y_pred="B",
        probability_B=1.0,
    ).to_parquet(paths["m0_validation_predictions"], index=False)

    hashes = {
        name: hashlib.sha256(paths[name].read_bytes()).hexdigest()
        for name in runner.EXPECTED_INPUT_SHA256
    }
    contract = json.loads(
        (
            ROOT
            / "contracts/experiments/usdrubf_phase8_2_external_data_pit_acceptance_matrix_v1.json"
        ).read_text(encoding="utf-8")
    )
    contract["immutable_input_sha256"] = hashes
    paths["phase82_experiment_contract"].write_text(
        json.dumps(contract, sort_keys=True) + "\n", encoding="utf-8"
    )
    return paths, hashes


def _request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    dataset: pd.DataFrame | None = None,
    validation_rows: int = runner.EXPECTED_VALIDATION_IDENTITIES,
) -> runner.Phase82AcceptanceRequest:
    paths, hashes = _write_inputs(
        tmp_path,
        dataset=dataset,
        validation_rows=validation_rows,
    )
    monkeypatch.setattr(runner, "EXPECTED_INPUT_SHA256", hashes)
    return runner.Phase82AcceptanceRequest(
        modeling_dataset_path=paths["modeling_dataset"],
        dataset_manifest_path=paths["dataset_manifest"],
        feature_schema_path=paths["feature_schema"],
        m0_validation_predictions_path=paths["m0_validation_predictions"],
        phase81_source_contract_path=paths["phase81_source_contract"],
        experiment_contract_path=paths["phase82_experiment_contract"],
        output_dir=tmp_path / "out",
        run_id="synthetic_phase82",
        git_commit_sha=FULL_SHA,
        retrieved_at_utc=RETRIEVED,
    )


def _payloads() -> tuple[bytes, bytes]:
    dates = pd.bdate_range("2024-01-01", periods=runner.EXPECTED_ELIGIBLE_IDENTITIES)
    ruonia_rows: list[tuple[object, ...]] = []
    for index, target in enumerate(dates):
        publication = target.date() - timedelta(days=1)
        observation = publication - timedelta(days=1)
        rate = 10.0 + index / 1000.0
        ruonia_rows.append(
            (
                observation.strftime("%d.%m.%Y"),
                f"{rate:.3f}",
                "500.0",
                "50",
                "20",
                f"{rate - 0.5:.3f}",
                f"{rate - 0.25:.3f}",
                f"{rate + 0.25:.3f}",
                f"{rate + 0.5:.3f}",
                "Standard",
                publication.strftime("%d.%m.%Y"),
            )
        )
    key_rate_rows = [
        (runner.KEY_RATE_HISTORY_START.strftime("%d.%m.%Y"), "5.5"),
        (dates[0].strftime("%d.%m.%Y"), "13.0"),
    ]
    return _table(RUONIA_HEADERS, ruonia_rows), _table(KEY_RATE_HEADERS, key_rate_rows)


def _run(
    request: runner.Phase82AcceptanceRequest,
) -> tuple[runner.Phase82AcceptanceResult, list[str], list[str]]:
    ruonia_payload, key_rate_payload = _payloads()
    ruonia_urls: list[str] = []
    key_rate_urls: list[str] = []

    def ruonia_transport(url: str) -> bytes:
        ruonia_urls.append(url)
        return ruonia_payload

    def key_rate_transport(url: str) -> bytes:
        key_rate_urls.append(url)
        return key_rate_payload

    result = runner.run_acceptance_matrix(
        request,
        ruonia_transport=ruonia_transport,
        key_rate_transport=key_rate_transport,
    )
    return result, ruonia_urls, key_rate_urls


def test_cli_enforces_exact_six_inputs_and_runtime_arguments(tmp_path: Path) -> None:
    parser = runner.build_argument_parser()
    required = {
        action.option_strings[0]
        for action in parser._actions
        if action.required and action.option_strings
    }
    assert required == set(runner.REQUIRED_CLI_ARGS)
    assert "--retrieved-at-utc" in {
        option for action in parser._actions for option in action.option_strings
    }
    with pytest.raises(SystemExit):
        parser.parse_args([])
    with pytest.raises(SystemExit):
        parser.parse_args([*sum(([flag, "x"] for flag in runner.REQUIRED_CLI_ARGS), []), "--unknown"])


def test_runner_writes_exact_artifacts_and_preserves_identities_and_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(tmp_path, monkeypatch)
    result, ruonia_urls, key_rate_urls = _run(request)

    assert result.final_gate_passed is True
    assert result.eligible_identity_count == 472
    assert result.validation_identity_count == 320
    assert sorted(path.name for path in request.output_dir.iterdir()) == sorted(
        runner.DECLARED_OUTPUT_ARTIFACTS
    )
    assert len(ruonia_urls) == len(key_rate_urls) == 1
    assert "UniDbQuery.From=01.12.2023" in ruonia_urls[0]
    assert "UniDbQuery.From=03.02.2014" in key_rate_urls[0]
    assert "UniDbQuery.To=" in ruonia_urls[0]
    assert "UniDbQuery.To=" in key_rate_urls[0]

    matrix = pd.read_parquet(
        request.output_dir / "external_pit_acceptance_matrix.parquet"
    )
    assert tuple(matrix.columns) == runner.MATRIX_COLUMNS
    assert len(matrix) == 472
    assert not (set(matrix.columns) & runner.FORBIDDEN_MATRIX_FIELDS)
    assert (
        pd.to_datetime(matrix["ruonia_publication_date"])
        < pd.to_datetime(matrix["target_trade_date"])
    ).all()
    assert (
        pd.to_datetime(matrix["key_rate_effective_date"])
        <= pd.to_datetime(matrix["target_trade_date"])
    ).all()
    assert matrix.loc[0, "key_rate_effective_date"] == matrix.loc[
        0, "target_trade_date"
    ]
    assert matrix.loc[0, "key_rate_age_calendar_days"] == 0
    assert matrix.loc[1, "key_rate_age_calendar_days"] > 0
    assert matrix["key_rate_age_calendar_days"].max() > 0

    manifest = json.loads(
        (request.output_dir / "source_fetch_manifest.json").read_text()
    )
    assert [item["source_id"] for item in manifest["sources"]] == list(
        runner.ACCEPTED_SOURCES
    )
    assert all(len(item["raw_payload_sha256"]) == 64 for item in manifest["sources"])
    assert all(item["exact_requested_route"].startswith("https://") for item in manifest["sources"])
    starts = {
        item["source_id"]: item["requested_start_date"]
        for item in manifest["sources"]
    }
    assert starts == {
        "cbr_ruonia_daily": "2023-12-01",
        "cbr_key_rate_daily": "2014-02-03",
    }
    identity = json.loads(
        (request.output_dir / "input_identity_verification.json").read_text()
    )
    assert identity["phase81_source_contract"]["sha256_computed_at_runtime"] is True
    assert len(identity["phase81_source_contract"]["observed_sha256"]) == 64

    blockers = json.loads(
        (request.output_dir / "source_blocker_register.json").read_text()
    )
    assert {item["source_id"] for item in blockers["blocked_sources"]} == set(
        runner.BLOCKED_SOURCE_STATUSES
    )
    assert all(not item["entered_acceptance_matrix"] for item in blockers["blocked_sources"])
    gates = json.loads((request.output_dir / "gate_results.json").read_text())
    assert all(item["passed"] for item in gates.values())


def test_coverage_staleness_and_diagnostic_spread_are_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(tmp_path, monkeypatch)
    _run(request)
    coverage = pd.read_csv(request.output_dir / "coverage_by_source.csv")
    assert coverage["eligible_covered_count"].tolist() == [472, 472]
    assert coverage["validation_covered_count"].tolist() == [320, 320]
    assert coverage["eligible_coverage_pct"].tolist() == [100.0, 100.0]
    staleness = pd.read_csv(request.output_dir / "staleness_by_source.csv")
    assert len(staleness) == 12
    assert set(staleness["age_basis"]) == {
        "observation_date",
        "publication_date",
        "effective_date",
    }
    matrix = pd.read_parquet(
        request.output_dir / "external_pit_acceptance_matrix.parquet"
    )
    assert (
        matrix["ruonia_minus_key_rate_pp"]
        == matrix["ruonia_rate_pct"] - matrix["key_rate_pct"]
    ).all()


def test_immutable_hash_identity_counts_and_output_reuse_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(tmp_path / "hash", monkeypatch)
    request.modeling_dataset_path.write_bytes(b"tampered")
    with pytest.raises(runner.Phase82AcceptanceMatrixError, match="SHA256 mismatch"):
        _run(request)

    request = _request(
        tmp_path / "eligible",
        monkeypatch,
        dataset=_dataset().iloc[:-1].copy(),
        validation_rows=320,
    )
    with pytest.raises(runner.Phase82AcceptanceMatrixError, match="472"):
        _run(request)

    request = _request(
        tmp_path / "validation",
        monkeypatch,
        validation_rows=319,
    )
    with pytest.raises(runner.Phase82AcceptanceMatrixError, match="320"):
        _run(request)

    request = _request(tmp_path / "reuse", monkeypatch)
    request.output_dir.mkdir()
    with pytest.raises(runner.Phase82AcceptanceMatrixError, match="must not already exist"):
        _run(request)


def test_blocked_source_contract_change_and_source_schema_change_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(tmp_path / "blocked", monkeypatch)
    contract = json.loads(request.experiment_contract_path.read_text())
    contract["accepted_sources"].append("cme_wti_pre_moex")
    request.experiment_contract_path.write_text(json.dumps(contract) + "\n")
    with pytest.raises(runner.Phase82AcceptanceMatrixError, match="accepted source set"):
        _run(request)

    request = _request(tmp_path / "schema", monkeypatch)
    _, key_payload = _payloads()

    def malformed_ruonia(_: str) -> bytes:
        return b"<html><table></table></html>"

    with pytest.raises(
        runner.Phase82AcceptanceMatrixError,
        match="external source acquisition failed",
    ):
        runner.run_acceptance_matrix(
            request,
            ruonia_transport=malformed_ruonia,
            key_rate_transport=lambda _: key_payload,
        )
    assert not request.output_dir.exists()


def test_daily_repeated_key_rate_rows_cannot_pass_source_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(tmp_path, monkeypatch)
    ruonia_payload, _ = _payloads()
    daily_rows = [
        (target.strftime("%d.%m.%Y"), "13.0")
        for target in pd.bdate_range("2024-01-01", periods=20)
    ]
    with pytest.raises(
        runner.Phase82AcceptanceMatrixError,
        match="external source acquisition failed",
    ):
        runner.run_acceptance_matrix(
            request,
            ruonia_transport=lambda _: ruonia_payload,
            key_rate_transport=lambda _: _table(KEY_RATE_HEADERS, daily_rows),
        )
    assert not request.output_dir.exists()


def test_all_zero_key_rate_age_fails_semantic_integrity_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = pd.bdate_range("2024-01-01", periods=472)
    normalized = pd.DataFrame(
        {
            "effective_date": targets.strftime("%Y-%m-%d"),
            "key_rate_pct": [float(index) for index in range(len(targets))],
        }
    )
    matrix = pd.DataFrame(
        {
            "key_rate_effective_date": targets.strftime("%Y-%m-%d"),
            "key_rate_age_calendar_days": 0,
        }
    )
    manifest = {
        "sources": [
            {
                "source_id": "cbr_key_rate_daily",
                "requested_start_date": "2014-02-03",
                "requested_end_date": targets[-1].strftime("%Y-%m-%d"),
            }
        ]
    }
    monkeypatch.setattr(runner, "KEY_RATE_MAX_NORMALIZED_ROW_FRACTION", 1.0)
    assert not runner._key_rate_semantic_integrity(
        matrix,
        normalized,
        source_fetch_manifest=manifest,
    )


def test_selected_key_rate_effective_dates_must_exist_in_change_history() -> None:
    normalized = pd.DataFrame(
        {
            "effective_date": ["2014-02-03", "2024-01-01"],
            "key_rate_pct": [5.5, 13.0],
        }
    )
    matrix = pd.DataFrame(
        {
            "key_rate_effective_date": ["2024-01-02"],
            "key_rate_age_calendar_days": [10],
        }
    )
    manifest = {
        "sources": [
            {
                "source_id": "cbr_key_rate_daily",
                "requested_start_date": "2014-02-03",
                "requested_end_date": "2025-01-01",
            }
        ]
    }
    assert not runner._key_rate_semantic_integrity(
        matrix,
        normalized,
        source_fetch_manifest=manifest,
    )


def test_source_error_does_not_echo_secret_and_no_unapproved_side_effect_code_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(tmp_path, monkeypatch)

    def leaking_transport(_: str) -> bytes:
        raise RuntimeError("Authorization: Bearer should-never-appear")

    with pytest.raises(runner.Phase82AcceptanceMatrixError) as captured:
        runner.run_acceptance_matrix(
            request,
            ruonia_transport=leaking_transport,
            key_rate_transport=leaking_transport,
        )
    assert "Bearer" not in str(captured.value)
    assert not request.output_dir.exists()

    source = Path(runner.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "LogisticRegression",
        "predict_proba(",
        "joblib.dump",
        "pickle.dump",
        "subprocess.",
        "os.system(",
    ):
        assert forbidden not in source


def test_g9_fails_when_any_preceding_gate_fails() -> None:
    gates = {
        f"G{index}_synthetic": {"passed": index != 4}
        for index in range(1, 9)
    }
    finalized = runner._finalize_gate_results(gates)
    assert finalized["G9_final_acceptance"]["passed"] is False


def test_request_parser_rejects_mutable_run_id_bad_sha_and_non_utc_timestamp() -> None:
    with pytest.raises(runner.Phase82AcceptanceMatrixError, match="immutable"):
        runner._validate_run_id("latest")
    with pytest.raises(runner.Phase82AcceptanceMatrixError, match="40 hexadecimal"):
        runner._validate_git_sha("abc")
    with pytest.raises(runner.Phase82AcceptanceMatrixError, match="expressed in UTC"):
        runner._parse_optional_retrieved_at("2026-07-15T09:00:00+03:00")
