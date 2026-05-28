import pytest

from moex_strategy_sdk.artifact_contracts import ArtifactContract
from moex_strategy_sdk.errors import ArtifactContractValidationError, ManifestValidationError
from moex_strategy_sdk.lifecycle import RuntimeLifecycleStatus, StrategyLifecycle
from moex_strategy_sdk.manifest import REQUIRED_MANIFEST_FIELDS, StrategyManifest, validate_strategy_manifest


def _manifest_kwargs():
    return {
        "strategy_id": "d1_tsmom",
        "version": "0.1.0",
        "instrument_scope": ("Si", "USDRUBF"),
        "timeframe": "D1",
        "required_datasets": ("futures_derived_d1",),
        "required_features": ("close_to_close_momentum",),
        "required_labels": ("forward_return_1d",),
        "supports_backtest": True,
        "supports_live": False,
        "report_schema_version": "strategy_report.v1",
        "artifact_contract_version": "strategy_artifacts.v1",
    }


def test_strategy_manifest_requires_canonical_fields():
    manifest = StrategyManifest(**_manifest_kwargs())

    assert REQUIRED_MANIFEST_FIELDS == (
        "strategy_id",
        "version",
        "instrument_scope",
        "timeframe",
        "required_datasets",
        "required_features",
        "required_labels",
        "supports_backtest",
        "supports_live",
        "report_schema_version",
        "artifact_contract_version",
    )
    for field_name in REQUIRED_MANIFEST_FIELDS:
        assert hasattr(manifest, field_name)
    assert validate_strategy_manifest(manifest) is manifest


@pytest.mark.parametrize("field_name", REQUIRED_MANIFEST_FIELDS)
def test_strategy_manifest_rejects_missing_required_fields(field_name):
    kwargs = _manifest_kwargs()
    kwargs.pop(field_name)

    with pytest.raises(TypeError):
        StrategyManifest(**kwargs)


def test_strategy_manifest_rejects_empty_dependencies():
    kwargs = _manifest_kwargs()
    kwargs["required_labels"] = ()

    with pytest.raises(ManifestValidationError):
        StrategyManifest(**kwargs)


def test_artifact_contract_requires_explicit_contract_fields():
    contract = ArtifactContract(
        artifact_id="signals_table",
        artifact_class="experiment_table",
        producer="strategy.d1_tsmom",
        consumer="moex_backtest",
        format="parquet",
        schema_version="signals.v1",
    )

    assert contract.artifact_id == "signals_table"
    assert contract.artifact_class == "experiment_table"
    assert contract.producer == "strategy.d1_tsmom"
    assert contract.consumer == "moex_backtest"
    assert contract.format == "parquet"
    assert contract.schema_version == "signals.v1"


def test_artifact_contract_rejects_empty_explicit_fields():
    with pytest.raises(ArtifactContractValidationError):
        ArtifactContract(
            artifact_id="",
            artifact_class="experiment_table",
            producer="strategy.d1_tsmom",
            consumer="moex_backtest",
            format="parquet",
            schema_version="signals.v1",
        )


def test_lifecycle_defaults_runtime_and_live_to_blocked():
    lifecycle = StrategyLifecycle()

    assert lifecycle.runtime_status == RuntimeLifecycleStatus.BLOCKED
    assert lifecycle.live_status == RuntimeLifecycleStatus.BLOCKED
    assert lifecycle.runtime_blocked is True
    assert lifecycle.live_blocked is True
