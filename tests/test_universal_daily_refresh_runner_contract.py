from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "src" / "moex_data" / "futures" / "universal_daily_refresh_runner.py"
SCHEDULER_CONTRACT = ROOT / "contracts" / "datasets" / "futures_daily_refresh_scheduler_contract.md"
MANIFEST_CONTRACT = ROOT / "contracts" / "datasets" / "futures_universal_daily_refresh_manifest_contract.md"

EXPECTED_STAGE_ORDER = [
    "registry_refresh",
    "all_universe_eligibility_snapshot",
    "raw_5m_refresh",
    "futoi_raw_refresh",
    "raw_d1_derivation",
    "continuous_eligibility_refinement",
    "expiration_map",
    "roll_map",
    "continuous_5m",
    "continuous_d1",
    "continuous_w1",
    "quality_reports",
    "unified_manifest",
]


def runner_source():
    return RUNNER.read_text(encoding="utf-8")


def runner_ast():
    return ast.parse(runner_source())


def literal_assigned_to(name):
    for node in runner_ast().body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError("assignment not found: " + name)


def test_canonical_stage_order_is_frozen():
    assert literal_assigned_to("CANONICAL_STAGE_IDS") == EXPECTED_STAGE_ORDER


def test_universal_runner_does_not_import_slice1_whitelist_defaults():
    tree = runner_ast()
    forbidden = {"DEFAULT_WHITELIST", "SHORT_HISTORY_ALLOWED"}
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imported.add(alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name)
    assert forbidden.isdisjoint(imported)
    text = runner_source()
    assert "DEFAULT_WHITELIST" not in text
    assert "SHORT_HISTORY_ALLOWED" not in text


def test_canonical_futoi_stage_is_wired_not_slice1_fallback():
    stages = literal_assigned_to("STAGES")
    futoi = stages["futoi_raw_refresh"]
    assert futoi["kind"] == "command"
    assert futoi["component_id"] == "canonical_all_universe_futoi_raw_refresh"
    assert futoi["script"] == "src/moex_data/futures/all_universe_futoi_raw_backfill_slice.py"


def test_full_run_preflight_keeps_missing_component_guard_for_future_gaps():
    text = runner_source()
    assert "def preflight_planned_stages" in text
    assert "known_missing_canonical_component_detected" in text
    assert "blocked_stage_id" in text
    assert "if not args.stage" in text
    assert "preflight_item = preflight_planned_stages(planned_stage_order)" in text


def test_manifest_records_planned_stage_order_and_no_real_execution_for_preflight_marker():
    text = runner_source()
    assert '"planned_stage_order": planned_stage_order' in text
    assert '"executed_stage_order": executed_stage_ids(items)' in text
    assert 'x.get("stage_id") != "preflight"' in text


def test_utc_timestamp_is_timezone_aware():
    text = runner_source()
    assert "datetime.utcnow()" not in text
    assert "from datetime import datetime, timezone" in text
    assert "datetime.now(timezone.utc)" in text


def test_debug_controls_are_orchestration_only():
    text = runner_source()
    assert "orchestration_only_no_universe_or_eligibility_redefinition" in text
    assert "semantics_effect" in text
    assert "slice1_whitelist_semantics" in text
    assert "forbidden_as_canonical_scope" in text
    assert "eligibility_snapshot_driven_futoi_eligible_true" in text


def test_futoi_stage_receives_snapshot_date_data_root_timeout_and_apim():
    text = runner_source()
    assert '"futoi_raw_refresh"' in text
    assert '{"registry_refresh", "all_universe_eligibility_snapshot", "raw_5m_refresh", "futoi_raw_refresh", "raw_d1_derivation", "expiration_map", "roll_map", "continuous_5m"}' in text
    assert '{"raw_5m_refresh", "futoi_raw_refresh", "raw_d1_derivation", "continuous_5m", "continuous_d1", "continuous_w1"}' in text
    assert '{"all_universe_eligibility_snapshot", "raw_5m_refresh", "futoi_raw_refresh"}' in text
    assert '{"raw_5m_refresh", "futoi_raw_refresh"}' in text


def test_futoi_stage_forwards_small_scope_filters_without_forcing_exact_endpoint():
    text = runner_source()
    assert 'if stage_id == "futoi_raw_refresh":' in text
    assert 'cmd.extend(["--family", args.family])' in text
    assert 'cmd.extend(["--secid", args.secid])' in text
    assert 'cmd.extend(["--secid", args.secid, "--exact-contract-only"])' not in text


def test_scheduler_contract_points_to_universal_runner():
    text = SCHEDULER_CONTRACT.read_text(encoding="utf-8")
    assert "module: moex_data.futures.universal_daily_refresh_runner" in text
    assert "PYTHONPATH=src python -m moex_data.futures.universal_daily_refresh_runner" in text
    assert "module: moex_data.futures.daily_refresh_runner" in text
    assert "compatibility_only" in text


def test_manifest_contract_declares_universal_manifest_and_stage_order():
    text = MANIFEST_CONTRACT.read_text(encoding="utf-8")
    assert "schema_version: futures_universal_daily_refresh_manifest.v1" in text
    assert "producer: src/moex_data/futures/universal_daily_refresh_runner.py" in text
    for stage in EXPECTED_STAGE_ORDER:
        assert stage in text
    assert "Slice 1 whitelist semantics are forbidden as canonical daily refresh scope" in text
