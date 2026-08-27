from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "configs" / "datasets" / "step7_rub_native_d1_w1_technical.v1.yaml"


def _config_text() -> str:
    return CONFIG.read_text(encoding="utf-8")


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
        raise AssertionError(f"missing mapping header: {header}") from exc
    prefix = " " * child_indent
    values: dict[str, object] = {}
    for line in lines[start:]:
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent < child_indent:
            break
        if indent != child_indent or not line.startswith(prefix) or ":" not in line:
            raise AssertionError(f"unexpected nested/non-scalar YAML under {header}: {line}")
        key, raw = line.strip().split(":", 1)
        values[key] = _parse_scalar(raw)
    return values


def test_stage7_status_records_physical_acceptance() -> None:
    text = _config_text()
    top_status = re.search(r"^status:\s+([^\s]+)\s*$", text, re.MULTILINE)
    assert top_status is not None
    assert top_status.group(1) == "rub_native_d1_w1_technical_stage7_accepted"

    evidence = _parse_scalar_mapping(text, "  applied_state_evidence:", 4)
    assert evidence == {
        "status": "accepted",
        "evidence_date": "2026-08-27",
        "run_id": "step7_pilot_20260827_v1",
        "acceptance_contract_id": "step7_rub_native_d1_w1_technical_acceptance.v1",
        "content_attestation_generation_id": "stage2_content_attestation_20260826_v1",
        "stage2_content_attestation_marker_sha256": "03ef2b6d554ce8857af614275ebe6ba699a47cd6e77f9507aa204c6424f789ff",
        "usdrubf_raw_partition_count": 1100,
        "cnyrubf_raw_partition_count": 1100,
        "usdrubf_d1_ohlcv_row_count": 1100,
        "cnyrubf_d1_ohlcv_row_count": 1100,
        "usdrubf_w1_ohlcv_row_count": 224,
        "cnyrubf_w1_ohlcv_row_count": 224,
        "usdrubf_d1_technical_row_count": 1100,
        "cnyrubf_d1_technical_row_count": 1100,
        "usdrubf_w1_technical_row_count": 224,
        "cnyrubf_w1_technical_row_count": 224,
        "accepted_pointer_count": 8,
        "expected_pointer_count": 8,
        "physical_partition_readback_required": True,
        "frozen_raw_physical_revalidation_required": True,
        "current_accepted_raw_scope_match_required": True,
        "independent_d1_w1_oracle_required": True,
        "independent_technical_oracle_required": True,
        "output_single_descriptor_capture_required": True,
        "output_content_sha256_binding_required": True,
        "output_identity_sha256_prewrite_recheck_required": True,
        "promotion_semantics": "serialized_transactional_with_rollback",
    }


def test_stage7_readiness_flags_match_accepted_state_without_expanding_scope() -> None:
    readiness = _parse_scalar_mapping(_config_text(), "readiness_flags:", 2)
    assert readiness == {
        "implementation_ready": True,
        "physical_pilot_passed": True,
        "accepted_pointer_ready": True,
        "scheduler_ready": False,
        "research_ready": False,
        "si_cr_continuous_ready": False,
        "weekly_oi_ready": False,
        "advanced_technical_policy_ready": False,
    }


def test_stage7_accepted_evidence_does_not_normalize_counts_or_policy_gaps() -> None:
    text = _config_text()
    d1_rows = [int(match.group(1)) for match in re.finditer(r"^\s+expected_d1_rows:\s+(\d+)\s*$", text, re.MULTILINE)]
    assert d1_rows == [1100, 1100]

    exact_false_fields = {
        match.group(1): match.group(2) == "true"
        for match in re.finditer(
            r"^\s+(production_dependency_enabled|fixed_expiry_substitution_allowed|fabricated_oi_allowed):\s+(true|false)\s*$",
            text,
            re.MULTILINE,
        )
    }
    assert exact_false_fields == {
        "production_dependency_enabled": False,
        "fixed_expiry_substitution_allowed": False,
        "fabricated_oi_allowed": False,
    }
