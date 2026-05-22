from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "src" / "moex_data" / "futures" / "all_universe_futoi_raw_backfill_slice.py"


def source():
    return MODULE.read_text(encoding="utf-8")


def tree():
    return ast.parse(source())


def literal_assigned_to(name):
    for node in tree().body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError("assignment not found: " + name)


def test_module_exists_at_canonical_path():
    assert MODULE.exists()


def test_no_slice1_whitelist_or_short_history_scope():
    text = source()
    assert "DEFAULT_WHITELIST" not in text
    assert "SHORT_HISTORY_ALLOWED" not in text
    assert "slice1_common" not in text
    assert "--whitelist" not in text
    assert "--excluded" not in text


def test_required_sot_files_include_eligibility_and_futoi_availability_contracts():
    text = source()
    assert "contracts/datasets/futures_all_universe_eligibility_contract.md" in text
    assert "contracts/datasets/futures_futoi_availability_report_contract.md" in text
    assert "configs/datasets/futures_all_universe_eligibility_config.json" in text
    assert "FUTOI_AVAILABILITY_CONTRACT" in text


def test_selection_is_eligibility_snapshot_driven_futoi_true():
    text = source()
    assert "Missing required eligibility snapshot" in text
    assert "futoi_eligible" in text
    assert "classification_status=included and futoi_eligible=true" in text
    assert "eligibility_snapshot_driven_futoi_eligible_true" in text
    assert "derive_futoi_eligibility" in text
    assert "futoi_eligibility_snapshot" in text


def test_fail_closed_on_missing_or_invalid_futoi_availability():
    text = source()
    assert "Missing canonical FUTOI availability report" in text
    assert "FUTOI availability report missing required columns" in text
    assert "missing_futoi_availability_row" in text
    assert "futoi_availability_not_available_completed" in text
    assert "Canonical FUTOI availability validation failed" in text


def test_futoi_storage_stays_separate_raw_zone_no_ohlcv_prejoin():
    text = source()
    assert "futoi.write_partitions" in text
    assert "no_futoi_prejoin_into_ohlcv" in text
    assert "raw_5m_loader.write_partitions" not in text
    assert "derived_d1" not in text
    assert "continuous_5m" not in text


def test_preserves_excluded_deferred_visibility_in_aggregate():
    text = source()
    assert "included_count" in text
    assert "deferred_count" in text
    assert "excluded_count" in text
    assert "classification_visibility_preserved" in text


def test_derived_futoi_eligibility_marks_only_included_available_completed_rows():
    text = source()
    assert 'status != "included"' in text
    assert 'futoi_status.append("not_applicable_not_included")' in text
    assert 'availability_status != "available" or probe_status != "completed"' in text
    assert 'futoi_status.append("pass")' in text
    assert 'futoi_flags.append(True)' in text


def test_futoi_scope_filters_are_real_selection_filters():
    text = source()
    assert "def apply_scope_filters" in text
    assert 'parser.add_argument("--secid"' in text
    assert 'parser.add_argument("--family"' in text
    assert "FUTOI scope filters produced empty selected universe" in text
    assert "selected = apply_scope_filters(selected, secid_filter, family_filter)" in text
    assert '"scope_filters"' in text


def test_exact_contract_mode_avoids_family_first_fallback():
    text = source()
    assert "def fetch_futoi_exact_contract" in text
    assert 'parser.add_argument("--exact-contract-only", action="store_true")' in text
    assert 'if bool(args.exact_contract_only):' in text
    assert 'path = "/iss/analyticalproducts/futoi/securities/" + ticker + ".json"' in text
    assert 'generic = "/iss/analyticalproducts/futoi/securities.json"' not in text
