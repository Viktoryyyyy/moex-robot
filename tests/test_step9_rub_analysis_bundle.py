from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest
import re

from moex_data import step9_rub_analysis_bundle as bundle


AS_OF = "2026-08-27T12:00:00+00:00"
PAST = "2026-08-26T10:00:00+00:00"
FUTURE = "2026-08-28T10:00:00+00:00"


def _parse_scalar(raw: str) -> object:
    value = raw.strip()
    if value == "true":
        return True
    if value == "false":
        return False
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if value.startswith('"') and value.endswith('"'):
        return json.loads(value)
    return value


def _parse_scalar_mapping(text: str, header: str, child_indent: int) -> dict[str, object]:
    lines = text.splitlines()
    try:
        start = lines.index(header) + 1
    except ValueError as exc:
        raise AssertionError("missing mapping header: " + header) from exc
    values: dict[str, object] = {}
    for line in lines[start:]:
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent < child_indent:
            break
        if indent != child_indent or ":" not in line:
            raise AssertionError("unexpected nested/non-scalar YAML under " + header + ": " + line)
        key, raw = line.strip().split(":", 1)
        values[key] = _parse_scalar(raw)
    return values


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _root_ref(root: Path, path: Path) -> str:
    return "${MOEX_DATA_ROOT}/" + path.relative_to(root).as_posix()


def _row(spec: bundle.PointerSpec, when: str, marker: str) -> dict[str, object]:
    row: dict[str, object] = {
        "instrument_id": spec.instrument_id,
        spec.causal_field: when,
        "marker": marker,
    }
    if spec.timeframe is not None:
        row["timeframe"] = spec.timeframe
        row["period_start_date"] = "2026-08-24"
        row["period_end_date"] = "2026-08-26"
    if spec.dataset_id.startswith("futures_futoi"):
        row["trade_date"] = "2026-08-26"
        row["snapshot_ts_utc"] = when
    if spec.dataset_id in {"futures_raw_5m", "fx_spot_raw_5m", "rub_basis_carry_5m"}:
        row["ts"] = when
    if spec.dataset_id == "futures_open_interest_raw_5m":
        row["ts"] = when
    return row


def _materialize_pointer(root: Path, spec: bundle.PointerSpec, *, include_hashes: bool = True) -> Path:
    safe = spec.block_id.replace(".", "_")
    artifact_dir = root / "accepted" / safe
    artifact_dir.mkdir(parents=True, exist_ok=True)
    partition = artifact_dir / "part.parquet"
    manifest = artifact_dir / "manifest.json"
    quality = artifact_dir / "quality.json"
    pd.DataFrame([_row(spec, PAST, "past"), _row(spec, FUTURE, "future")]).to_parquet(partition, index=False)
    identity = {
        "dataset_id": spec.dataset_id,
        "instrument_id": spec.instrument_id,
        "quality_status": "pass",
    }
    if spec.timeframe is not None:
        identity["timeframe"] = spec.timeframe
    manifest.write_text(json.dumps(identity, sort_keys=True), encoding="utf-8")
    quality.write_text(json.dumps(identity, sort_keys=True), encoding="utf-8")

    pointer = {
        "dataset_id": spec.dataset_id,
        "instrument_id": spec.instrument_id,
        "run_id": "producer_run",
        "acceptance_run_id": "acceptance_run",
        "acceptance_contract_id": "test_acceptance.v1",
        "manifest_ref": _root_ref(root, manifest),
        "quality_report_ref": _root_ref(root, quality),
        "partition_ref": _root_ref(root, partition),
        "quality_status": "pass",
    }
    if spec.timeframe is not None:
        pointer["timeframe"] = spec.timeframe
    if spec.stage == 3:
        pointer["refresh_status"] = "succeeded"
    if include_hashes:
        pointer["manifest_sha256"] = _sha(manifest)
        pointer["quality_report_sha256"] = _sha(quality)
        pointer["partition_sha256"] = _sha(partition)

    path = bundle._pointer_path(root, spec)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(pointer, sort_keys=True), encoding="utf-8")
    return path


def _materialize_scope(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scope: str) -> Path:
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    for spec in bundle.pointer_specs(scope):
        _materialize_pointer(root, spec)
    return root


def _risk_payload(as_of: str = "2026-08-26T09:00:00Z") -> dict[str, object]:
    return {
        "schema_version": "step8_position_risk_input.v1",
        "snapshot_id": "risk_snapshot_1",
        "as_of_ts_utc": as_of,
        "source": {"mode": "manual", "reference": "test fixture"},
        "account": {
            "currency": "RUB",
            "free_funds_rub": "1000",
            "current_initial_margin_rub": "0",
            "variation_margin_rub": "0",
            "liquidity_buffer_rub": "1000",
            "max_total_contracts": 0,
            "max_allowed_loss_rub": "0",
        },
        "positions": [],
        "scenario_pnl_rub": {
            "usd_rub_minus_5": "0",
            "usd_rub_minus_3": "0",
            "usd_rub_minus_1": "0",
            "usd_rub_plus_1": "0",
            "usd_rub_plus_3": "0",
            "usd_rub_plus_5": "0",
            "gap": {"usd_rub_move": "1", "pnl_rub": "0"},
        },
    }


def test_daily_bundle_uses_exact_twenty_blocks_and_excludes_future_rows(tmp_path, monkeypatch):
    _materialize_scope(tmp_path, monkeypatch, "daily")
    result = bundle.build_analysis_bundle(scope="daily", as_of=AS_OF)
    assert result["schema_version"] == "rub_analysis_bundle.v1"
    assert result["identity"] == {"project": "MOEX_Bot", "scope": "daily", "as_of": "2026-08-27T12:00:00+00:00"}
    assert result["server_core"]["status"] == "ready"
    assert result["server_core"]["block_count"] == 20
    assert len(result["server_core"]["blocks"]) == 20
    assert {row["selected_observation"]["marker"] for row in result["server_core"]["blocks"]} == {"past"}
    assert result["position_risk"]["status"] == "not_supplied"
    assert result["readiness"]["analysis_bundle_complete"] is False
    assert result["readiness"]["policy_gaps"] == []
    assert result["quality_gates"]["bundle_generates_trade_recommendation"] is False
    assert result["quality_gates"]["bundle_generates_position_size"] is False
    assert all(
        set(("manifest_sha256", "quality_report_sha256", "partition_sha256")).issubset(row["provenance"])
        for row in result["server_core"]["blocks"]
    )


def test_weekly_bundle_adds_w1_and_preserves_declared_policy_gaps(tmp_path, monkeypatch):
    _materialize_scope(tmp_path, monkeypatch, "weekly")
    result = bundle.build_analysis_bundle(scope="weekly", as_of=AS_OF)
    assert result["server_core"]["block_count"] == 24
    assert len(result["server_core"]["blocks"]) == 24
    assert {row["timeframe"] for row in result["server_core"]["blocks"] if row["stage"] == 7} == {"1D", "1W"}
    assert [row["block_id"] for row in result["readiness"]["policy_gaps"]] == [
        "si_cr_continuous_weekly", "weekly_open_interest", "ema_filter", "realized_volatility",
        "range_percentile", "swing_high_low", "break_of_structure",
    ]
    assert all(row["status"] == "not_ready_policy_gap" for row in result["readiness"]["policy_gaps"])


def test_explicit_stage8_input_is_strictly_validated_and_carried(tmp_path, monkeypatch):
    _materialize_scope(tmp_path, monkeypatch, "daily")
    risk = tmp_path / "risk.json"
    risk.write_text(json.dumps(_risk_payload()), encoding="utf-8")
    result = bundle.build_analysis_bundle(scope="daily", as_of=AS_OF, position_risk_input=str(risk))
    assert result["position_risk"]["status"] == "ready"
    assert result["position_risk"]["state"]["schema_version"] == "step8_position_risk_state.v1"
    assert result["position_risk"]["state"]["snapshot_id"] == "risk_snapshot_1"
    assert result["quality_gates"]["missing_position_risk_blocks_downstream_size_or_add_recommendation"] is False


def test_stage8_state_later_than_bundle_as_of_fails_closed(tmp_path, monkeypatch):
    _materialize_scope(tmp_path, monkeypatch, "daily")
    risk = tmp_path / "risk.json"
    risk.write_text(json.dumps(_risk_payload("2026-08-29T09:00:00Z")), encoding="utf-8")
    with pytest.raises(bundle.Step9AnalysisBundleError, match="later than bundle as_of"):
        bundle.build_analysis_bundle(scope="daily", as_of=AS_OF, position_risk_input=str(risk))


def test_naive_as_of_fails_closed(tmp_path, monkeypatch):
    _materialize_scope(tmp_path, monkeypatch, "daily")
    with pytest.raises(bundle.Step9AnalysisBundleError, match="timezone-aware"):
        bundle.build_analysis_bundle(scope="daily", as_of="2026-08-27T12:00:00")


def test_no_observation_before_as_of_fails_closed(tmp_path, monkeypatch):
    _materialize_scope(tmp_path, monkeypatch, "daily")
    with pytest.raises(bundle.Step9AnalysisBundleError, match="no causal observation"):
        bundle.build_analysis_bundle(scope="daily", as_of="2020-01-01T00:00:00Z")


def test_missing_mandatory_pointer_fails_closed(tmp_path, monkeypatch):
    root = _materialize_scope(tmp_path, monkeypatch, "daily")
    first = bundle.pointer_specs("daily")[0]
    bundle._pointer_path(root, first).unlink()
    with pytest.raises(bundle.Step9AnalysisBundleError, match="accepted pointer missing"):
        bundle.build_analysis_bundle(scope="daily", as_of=AS_OF)


def test_pointer_identity_and_quality_fail_closed(tmp_path, monkeypatch):
    root = _materialize_scope(tmp_path, monkeypatch, "daily")
    first = bundle.pointer_specs("daily")[0]
    path = bundle._pointer_path(root, first)
    values = json.loads(path.read_text(encoding="utf-8"))
    values["instrument_id"] = "wrong"
    path.write_text(json.dumps(values), encoding="utf-8")
    with pytest.raises(bundle.Step9AnalysisBundleError, match="instrument_id mismatch"):
        bundle.build_analysis_bundle(scope="daily", as_of=AS_OF)


def test_missing_trusted_digest_fails_closed(tmp_path, monkeypatch):
    root = _materialize_scope(tmp_path, monkeypatch, "daily")
    first = bundle.pointer_specs("daily")[0]
    path = bundle._pointer_path(root, first)
    values = json.loads(path.read_text(encoding="utf-8"))
    values.pop("partition_sha256")
    path.write_text(json.dumps(values), encoding="utf-8")
    with pytest.raises(bundle.Step9AnalysisBundleError, match="partition_sha256 is required trusted integrity evidence"):
        bundle.build_analysis_bundle(scope="daily", as_of=AS_OF)


def test_pointer_sha_mismatch_fails_closed(tmp_path, monkeypatch):
    root = _materialize_scope(tmp_path, monkeypatch, "daily")
    first = bundle.pointer_specs("daily")[0]
    path = bundle._pointer_path(root, first)
    values = json.loads(path.read_text(encoding="utf-8"))
    values["partition_sha256"] = "0" * 64
    path.write_text(json.dumps(values), encoding="utf-8")
    with pytest.raises(bundle.Step9AnalysisBundleError, match="partition_sha256 mismatch"):
        bundle.build_analysis_bundle(scope="daily", as_of=AS_OF)


def test_traversal_ref_fails_closed(tmp_path, monkeypatch):
    root = _materialize_scope(tmp_path, monkeypatch, "daily")
    first = bundle.pointer_specs("daily")[0]
    path = bundle._pointer_path(root, first)
    values = json.loads(path.read_text(encoding="utf-8"))
    values["partition_ref"] = "${MOEX_DATA_ROOT}/../foreign.parquet"
    path.write_text(json.dumps(values), encoding="utf-8")
    with pytest.raises(bundle.Step9AnalysisBundleError, match="path traversal"):
        bundle.build_analysis_bundle(scope="daily", as_of=AS_OF)


def test_symlink_partition_fails_closed(tmp_path, monkeypatch):
    root = _materialize_scope(tmp_path, monkeypatch, "daily")
    first = bundle.pointer_specs("daily")[0]
    pointer_path = bundle._pointer_path(root, first)
    values = json.loads(pointer_path.read_text(encoding="utf-8"))
    target = root / values["partition_ref"].removeprefix("${MOEX_DATA_ROOT}/")
    real = target.with_name("real.parquet")
    target.replace(real)
    target.symlink_to(real)
    pointer_path.write_text(json.dumps(values), encoding="utf-8")
    with pytest.raises(bundle.Step9AnalysisBundleError, match="symlink"):
        bundle.build_analysis_bundle(scope="daily", as_of=AS_OF)


def test_duplicate_pointer_json_member_fails_closed(tmp_path, monkeypatch):
    root = _materialize_scope(tmp_path, monkeypatch, "daily")
    first = bundle.pointer_specs("daily")[0]
    path = bundle._pointer_path(root, first)
    valid = json.loads(path.read_text(encoding="utf-8"))
    payload = json.dumps(valid)
    payload = payload[:-1] + ',"dataset_id":"duplicate"}'
    path.write_text(payload, encoding="utf-8")
    with pytest.raises(bundle.Step9AnalysisBundleError, match="duplicate JSON object member"):
        bundle.build_analysis_bundle(scope="daily", as_of=AS_OF)


def test_output_is_deterministic_for_identical_inputs(tmp_path, monkeypatch):
    _materialize_scope(tmp_path, monkeypatch, "daily")
    first = bundle.build_analysis_bundle(scope="daily", as_of=AS_OF)
    second = bundle.build_analysis_bundle(scope="daily", as_of=AS_OF)
    assert json.dumps(first, sort_keys=True, separators=(",", ":")) == json.dumps(second, sort_keys=True, separators=(",", ":"))


def test_typed_stage9_config_and_contract_are_exact():
    config_text = Path("configs/datasets/step9_rub_analysis_bundle.v1.yaml").read_text(encoding="utf-8")
    contract_text = Path("contracts/datasets/rub_analysis_bundle.v1.yaml").read_text(encoding="utf-8")

    scope_counts = _parse_scalar_mapping(config_text, "scope_counts:", 2)
    assert scope_counts == {"daily_server_core_blocks": 20, "weekly_server_core_blocks": 24}

    pointer_policy = _parse_scalar_mapping(config_text, "pointer_policy:", 2)
    assert pointer_policy["latest_autodetect_allowed"] is False
    assert pointer_policy["directory_scan_allowed"] is False
    assert pointer_policy["trusted_sha256_required_for_manifest"] is True
    assert pointer_policy["trusted_sha256_required_for_quality_report"] is True
    assert pointer_policy["trusted_sha256_required_for_partition"] is True
    assert pointer_policy["missing_trusted_sha256_fail_closed"] is True

    readiness = _parse_scalar_mapping(config_text, "readiness_flags:", 2)
    assert readiness == {
        "implementation_ready": True,
        "deterministic_pointer_resolution_ready": True,
        "causal_as_of_selection_ready": True,
        "explicit_stage8_integration_ready": True,
        "fastapi_endpoint_ready": False,
        "external_context_cache_ready": False,
        "si_cr_continuous_weekly_ready": False,
        "weekly_oi_ready": False,
        "advanced_technical_policy_ready": False,
        "scheduler_ready": False,
        "research_ready": False,
    }

    safety = _parse_scalar_mapping(contract_text, "safety:", 2)
    assert safety == {
        "network_calls_used": False,
        "broker_write_access_used": False,
        "order_placement_allowed": False,
        "trade_recommendation_generated": False,
        "scenario_probability_generated": False,
        "market_regime_generated": False,
        "position_size_generated": False,
        "stop_or_target_generated": False,
        "price_based_futures_pnl_recomputed": False,
        "participant_group_smart_money_label_generated": False,
    }

    pointer_integrity = _parse_scalar_mapping(contract_text, "  pointer_integrity:", 4)
    assert pointer_integrity["manifest_sha256_required"] is True
    assert pointer_integrity["quality_report_sha256_required"] is True
    assert pointer_integrity["partition_sha256_required"] is True
    assert pointer_integrity["trusted_sha256_revalidated"] is True
    assert pointer_integrity["missing_trusted_sha256"] == "fail_closed"
    assert "  exact_block_count: 20" in contract_text
    assert "  exact_block_count: 24" in contract_text
    assert "  may_be_normalized_to_ready_without_separate_approved_policy: false" in contract_text
