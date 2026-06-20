import ast
import json
from pathlib import Path

from moex_research.publishers.research_run_registration import load_registration_spec


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = REPO_ROOT / "contracts" / "experiments" / "research_run_registration_v1.json"
SPEC_ROOT = REPO_ROOT / "docs" / "sot" / "research" / "registration_specs"
SOURCE_PATHS = (
    REPO_ROOT / "src" / "moex_research" / "registry" / "file_write.py",
    REPO_ROOT / "src" / "moex_research" / "assets" / "run_archive.py",
    REPO_ROOT / "src" / "moex_research" / "publishers" / "research_run_registration.py",
    REPO_ROOT / "src" / "moex_research" / "runners" / "register_existing_research_run.py",
)
SPEC_FILES = (
    "m2_1_usdrubf_ema_d1_baseline_20260618.json",
    "m3_usdrubf_ema_d1_logistic_screen_20260618.json",
    "m4a_usdrubf_ema_d1_indicators_horizons_20260619.json",
)
REQUIRED_REGISTRATION_FIELDS = {
    "schema_version",
    "run_id",
    "strategy_id",
    "strategy_version",
    "test_type",
    "instrument_scope",
    "timeframe_scope",
    "repo_commit",
    "run_status",
    "result_status",
    "canonicality_status",
    "dataset_refs",
    "feature_refs",
    "label_refs",
    "parameter_set",
    "metrics",
    "artifacts",
}
REQUIRED_ARTIFACT_FIELDS = {
    "filename",
    "artifact_id",
    "artifact_role",
    "format",
    "schema_version",
    "required_for_canonical",
}


def test_registration_contract_requires_complete_registration_and_artifact_fields():
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

    assert REQUIRED_REGISTRATION_FIELDS <= set(contract["required"])
    assert REQUIRED_ARTIFACT_FIELDS <= set(contract["$defs"]["artifact"]["required"])
    assert contract["additionalProperties"] is False
    assert contract["$defs"]["artifact"]["additionalProperties"] is False
    assert {"evidence_canonical", "evidence_provisional"} <= set(
        contract["properties"]["result_status"]["enum"]
    )


def test_all_three_backfill_registration_specs_validate_and_are_canonical():
    specs = [load_registration_spec(SPEC_ROOT / name) for name in SPEC_FILES]

    assert [spec.run_id for spec in specs] == [
        "m2_1_usdrubf_ema_d1_baseline_20260618",
        "m3_usdrubf_ema_d1_logistic_screen_20260618",
        "m4a_usdrubf_ema_d1_indicators_horizons_20260619",
    ]
    for spec in specs:
        assert spec.canonicality_status == "canonical"
        assert spec.run_status == "executed"
        assert spec.dataset_refs
        assert spec.feature_refs
        assert spec.parameter_set
        canonical_roles = {
            item.artifact_role
            for item in spec.artifacts
            if item.required_for_canonical
        }
        assert {"run_metadata", "metrics", "primary_result"} <= canonical_roles


def test_backfill_specs_preserve_accepted_run_commits_statuses_metrics_and_primary_results():
    m2 = json.loads((SPEC_ROOT / SPEC_FILES[0]).read_text(encoding="utf-8"))
    m3 = json.loads((SPEC_ROOT / SPEC_FILES[1]).read_text(encoding="utf-8"))
    m4 = json.loads((SPEC_ROOT / SPEC_FILES[2]).read_text(encoding="utf-8"))

    assert m2["repo_commit"] == "9eb16539ee9c11f9a23d02ea858ff97b345c00a8"
    assert m2["result_status"] == "evidence_canonical"
    assert m2["metrics"] == {
        "event_count": 64,
        "label_row_count": 64,
        "d1_ohlc_row_count": 980,
        "baseline_summary_row_count": 8,
    }
    assert _primary_result(m2) == "usdrubf_ema_3_19_raw_baseline_summary.csv"

    assert m3["repo_commit"] == "dbca89179d764afa9972c3ec592c1ca5898b4f66"
    assert m3["result_status"] == "not_supported_canonical"
    assert m3["metrics"] == {
        "primary_oos_rows": 30,
        "roc_auc": 0.39819004524886875,
        "brier_score": 0.3019281918872732,
        "prevalence_baseline_brier": 0.2506863700880736,
        "screening_support": "not_supported_or_hold",
    }
    assert _primary_result(m3) == "m3_oos_predictions.csv"

    assert m4["repo_commit"] == "522bf0600131065d983a3125029d9fafdac17f37"
    assert m4["result_status"] == "evidence_canonical"
    assert m4["metrics"] == {
        "d1_rows": 980,
        "event_rows": 64,
        "indicator_ready_events": 63,
        "h1_mean_signed_return": 0.001551,
        "h2_mean_signed_return": 0.001671,
        "h5_mean_signed_return": -0.000169,
        "h10_mean_signed_return": -0.003034,
    }
    assert _primary_result(m4) == "usdrubf_ema_3_19_horizon_baseline_summary.csv"


def test_cli_is_thin_and_contains_no_registry_archive_or_research_math():
    cli_path = SOURCE_PATHS[-1]
    source = cli_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules = _imported_modules(tree)

    assert imported_modules <= {
        "__future__",
        "argparse",
        "pathlib",
        "publishers.research_run_registration",
    }
    for forbidden in (
        "hashlib",
        "json",
        "zipfile",
        "FileExperimentRegistryWriter",
        "create_deterministic_run_archive",
        "ArtifactManifest",
        "ExperimentRegistryEntry",
        "pandas",
        "numpy",
        "sklearn",
    ):
        assert forbidden not in source
    calls = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "register_existing_research_run" in calls


def test_new_registration_sources_have_no_runtime_live_broker_or_trading_imports():
    forbidden_prefixes = (
        "moex_runtime",
        "src.moex_runtime",
        "broker",
        "brokers",
        "trading",
        "src.strategies",
        "strategies",
    )
    for path in SOURCE_PATHS:
        modules = _imported_modules(ast.parse(path.read_text(encoding="utf-8")))
        assert not any(
            module == prefix or module.startswith(prefix + ".")
            for module in modules
            for prefix in forbidden_prefixes
        )


def _primary_result(payload):
    return next(
        artifact["filename"]
        for artifact in payload["artifacts"]
        if artifact["artifact_role"] == "primary_result"
    )


def _imported_modules(tree):
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                modules.add(node.module or "")
            else:
                modules.add(node.module or "")
    return modules
