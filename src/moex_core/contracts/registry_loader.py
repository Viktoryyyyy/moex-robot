from __future__ import annotations

import importlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from src.moex_strategy_sdk.artifact_contracts import ALLOWED_CONTRACT_CLASSES, ArtifactContract, validate_artifact_contract
from src.moex_strategy_sdk.errors import ArtifactContractValidationError, ConfigValidationError, ManifestValidationError, StrategyRegistrationError, UnsupportedModeError
from src.moex_strategy_sdk.manifest import validate_strategy_manifest

_REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class ResolvedRegisteredBacktest:
    instrument_record: Mapping[str, object]
    dataset_record: Mapping[str, object]
    feature_record: Mapping[str, object]
    strategy_record: Mapping[str, object]
    portfolio_record: Mapping[str, object]
    environment_record: Mapping[str, object]
    default_strategy_config_record: Mapping[str, object]
    manifest: Any
    strategy_config: Any
    dataset_contract: Mapping[str, object]
    feature_contract: Mapping[str, object]
    strategy_artifact_contracts: tuple[ArtifactContract, ...]
    backtest_feature_builder: Any
    backtest_signal_builder: Any
    backtest_request_builder: Any
    backtest_output_contract: ArtifactContract


@dataclass(frozen=True)
class ResolvedRegisteredRuntimeBoundary:
    instrument_record: Mapping[str, object]
    dataset_record: Mapping[str, object]
    feature_record: Mapping[str, object]
    strategy_record: Mapping[str, object]
    portfolio_record: Mapping[str, object]
    environment_record: Mapping[str, object]
    default_strategy_config_record: Mapping[str, object]
    manifest: Any
    strategy_config: Any
    dataset_contract: Mapping[str, object]
    feature_contract: Mapping[str, object]
    strategy_artifact_contracts: tuple[ArtifactContract, ...]
    runtime_state_contract: ArtifactContract
    runtime_trade_log_contract: ArtifactContract
    runtime_feature_builder: Any
    runtime_signal_builder: Any
    runtime_live_decision_builder: Any


def _load_json(repo_relative_path: str) -> Mapping[str, object]:
    path = _REPO_ROOT / repo_relative_path
    if not path.exists():
        raise StrategyRegistrationError("missing registry/config file: " + repo_relative_path)
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise StrategyRegistrationError("registry/config file must contain JSON object: " + repo_relative_path)
    return payload


def _import_ref(ref: str) -> Any:
    if not isinstance(ref, str) or ":" not in ref:
        raise StrategyRegistrationError("invalid import ref: " + repr(ref))
    module_name, attr_name = ref.split(":", 1)
    if not module_name or not attr_name:
        raise StrategyRegistrationError("invalid import ref: " + repr(ref))
    module = importlib.import_module(module_name)
    if not hasattr(module, attr_name):
        raise StrategyRegistrationError("missing imported symbol: " + ref)
    return getattr(module, attr_name)


def _require_exact_keys(record_name: str, payload: Mapping[str, object], required_keys: set[str]) -> None:
    actual_keys = set(payload.keys())
    missing = sorted(required_keys - actual_keys)
    extra = sorted(actual_keys - required_keys)
    if missing:
        raise StrategyRegistrationError(record_name + " missing field(s): " + ", ".join(missing))
    if extra:
        raise StrategyRegistrationError(record_name + " has unexpected field(s): " + ", ".join(extra))


def _validate_registry_artifact_contract(record_name: str, payload: Mapping[str, object]) -> Mapping[str, object]:
    required = {"artifact_id", "artifact_role", "contract_class", "producer_ref", "consumer_refs", "format", "schema_version", "partitioning", "locator_ref"}
    _require_exact_keys(record_name, payload, required)
    contract_class = payload["contract_class"]
    if contract_class not in ALLOWED_CONTRACT_CLASSES:
        raise ArtifactContractValidationError(record_name + " unsupported contract_class")
    if not isinstance(payload["artifact_id"], str) or not payload["artifact_id"]:
        raise ArtifactContractValidationError(record_name + " artifact_id is required")
    if not isinstance(payload["artifact_role"], str) or not payload["artifact_role"]:
        raise ArtifactContractValidationError(record_name + " artifact_role is required")
    if not isinstance(payload["producer_ref"], str) or not payload["producer_ref"]:
        raise ArtifactContractValidationError(record_name + " producer_ref is required")
    consumer_refs = payload["consumer_refs"]
    if not isinstance(consumer_refs, list) or not consumer_refs:
        raise ArtifactContractValidationError(record_name + " consumer_refs must be non-empty list")
    if not isinstance(payload["format"], str) or not payload["format"]:
        raise ArtifactContractValidationError(record_name + " format is required")
    schema_version = payload["schema_version"]
    if isinstance(schema_version, bool) or not isinstance(schema_version, int) or schema_version < 1:
        raise ArtifactContractValidationError(record_name + " schema_version must be >= 1")
    if not isinstance(payload["partitioning"], str) or not payload["partitioning"]:
        raise ArtifactContractValidationError(record_name + " partitioning is required")
    if not isinstance(payload["locator_ref"], str) or not payload["locator_ref"]:
        raise ArtifactContractValidationError(record_name + " locator_ref is required")
    return payload


def _validate_default_strategy_config(payload: Mapping[str, object]) -> None:
    required = {"strategy_id", "version", "params", "artifact_bindings", "runtime_policy_ref", "risk_policy_ref"}
    _require_exact_keys("strategy default config", payload, required)
    if not isinstance(payload["strategy_id"], str) or not payload["strategy_id"]:
        raise ConfigValidationError("strategy default config strategy_id is required")
    if not isinstance(payload["version"], str) or not payload["version"]:
        raise ConfigValidationError("strategy default config version is required")
    if not isinstance(payload["params"], dict):
        raise ConfigValidationError("strategy default config params must be object")
    if not isinstance(payload["artifact_bindings"], dict):
        raise ConfigValidationError("strategy default config artifact_bindings must be object")
    if payload["runtime_policy_ref"] is not None:
        raise ConfigValidationError("runtime_policy_ref must be null")
    if payload["risk_policy_ref"] is not None:
        raise ConfigValidationError("risk_policy_ref must be null")


def _load_strategy_artifact_contracts(import_ref: str) -> tuple[ArtifactContract, ...]:
    loaded = _import_ref(import_ref)
    if not isinstance(loaded, tuple):
        raise ArtifactContractValidationError("strategy artifact contracts must be tuple")
    normalized: list[ArtifactContract] = []
    for item in loaded:
        if not isinstance(item, ArtifactContract):
            raise ArtifactContractValidationError("strategy artifact contracts must contain ArtifactContract items")
        validate_artifact_contract(item)
        normalized.append(item)
    if not normalized:
        raise ArtifactContractValidationError("strategy artifact contracts must be non-empty")
    return tuple(normalized)


def _resolve_strategy_hook_ref(*, strategy_record: Mapping[str, object], module_suffix: str, attr_name: str) -> str:
    package_ref = strategy_record.get("package_ref")
    if not isinstance(package_ref, str) or not package_ref:
        raise StrategyRegistrationError("strategy registry record must declare package_ref")
    return package_ref + "." + module_suffix + ":" + attr_name


def _resolve_runtime_artifact_contract(*, strategy_artifact_contracts: tuple[ArtifactContract, ...], artifact_role: str, producer: str) -> ArtifactContract:
    matches = [contract for contract in strategy_artifact_contracts if contract.artifact_role == artifact_role and contract.producer == producer]
    if len(matches) != 1:
        raise StrategyRegistrationError("expected exactly one runtime artifact contract for role=" + artifact_role + " producer=" + producer)
    return matches[0]


def _load_common_registered_components(*, strategy_id: str, portfolio_id: str, environment_id: str) -> tuple[Mapping[str, object], Mapping[str, object], Mapping[str, object], Mapping[str, object], Mapping[str, object], Mapping[str, object], Mapping[str, object], Mapping[str, object], Mapping[str, object], Any, Any, tuple[ArtifactContract, ...]]:
    strategy_record = _load_json("configs/strategies/" + strategy_id + ".json")
    portfolio_record = _load_json("configs/portfolios/" + portfolio_id + ".json")
    environment_record = _load_json("configs/environments/" + environment_id + ".json")
    dataset_ids = strategy_record.get("required_dataset_ids")
    feature_ids = strategy_record.get("required_feature_set_ids")
    instrument_scope = strategy_record.get("instrument_scope")
    if not isinstance(dataset_ids, list) or len(dataset_ids) != 1:
        raise StrategyRegistrationError("wave-2 requires exactly one required_dataset_id")
    if not isinstance(feature_ids, list) or len(feature_ids) != 1:
        raise StrategyRegistrationError("wave-2 requires exactly one required_feature_set_id")
    if not isinstance(instrument_scope, list) or len(instrument_scope) != 1:
        raise StrategyRegistrationError("wave-2 requires exactly one instrument_scope id")
    dataset_record = _load_json("configs/datasets/" + str(dataset_ids[0]) + ".json")
    feature_record = _load_json("configs/features/" + str(feature_ids[0]) + ".json")
    instrument_record = _load_json("configs/instruments/" + str(instrument_scope[0]) + ".json")
    default_strategy_config_record = _load_json(str(strategy_record.get("default_config_ref")))
    dataset_contract = _validate_registry_artifact_contract("dataset artifact contract", _load_json(str(dataset_record.get("artifact_ref"))))
    feature_contract = _validate_registry_artifact_contract("feature artifact contract", _load_json(str(feature_record.get("artifact_ref"))))
    manifest = _import_ref(str(strategy_record.get("manifest_ref")))
    try:
        manifest = validate_strategy_manifest(manifest)
    except Exception as exc:
        raise ManifestValidationError(str(exc)) from exc
    validate_config = _import_ref(str(strategy_record.get("config_schema_ref").rsplit(":", 1)[0] + ":validate_config"))
    strategy_artifact_contracts = _load_strategy_artifact_contracts(str(strategy_record.get("artifact_contract_ref")))
    _validate_default_strategy_config(default_strategy_config_record)
    raw_config = {"strategy_id": default_strategy_config_record["strategy_id"], "version": default_strategy_config_record["version"]}
    raw_config.update(dict(default_strategy_config_record["params"]))
    strategy_config = validate_config(raw_config)
    if strategy_record.get("strategy_id") != manifest.strategy_id:
        raise ManifestValidationError("strategy registry id does not match manifest.strategy_id")
    if strategy_record.get("version") != manifest.version:
        raise ManifestValidationError("strategy registry version does not match manifest.version")
    if tuple(strategy_record.get("required_dataset_ids", [])) != tuple(manifest.required_datasets):
        raise ManifestValidationError("required_dataset_ids do not match manifest.required_datasets")
    if tuple(strategy_record.get("required_feature_set_ids", [])) != tuple(manifest.required_features):
        raise ManifestValidationError("required_feature_set_ids do not match manifest.required_features")
    if tuple(strategy_record.get("instrument_scope", [])) != tuple(manifest.instrument_scope):
        raise ManifestValidationError("instrument_scope does not match manifest.instrument_scope")
    if strategy_record.get("timeframe") != manifest.timeframe:
        raise ManifestValidationError("timeframe does not match manifest.timeframe")
    if bool(strategy_record.get("supports_backtest")) != bool(manifest.supports_backtest):
        raise ManifestValidationError("supports_backtest does not match manifest")
    if bool(strategy_record.get("supports_live")) != bool(manifest.supports_live):
        raise ManifestValidationError("supports_live does not match manifest")
    if str(strategy_record.get("artifact_contract_version")) != str(manifest.artifact_contract_version):
        raise ManifestValidationError("artifact_contract_version does not match manifest")
    if strategy_record.get("status") != "active":
        raise StrategyRegistrationError("strategy registry record must be active")
    if dataset_record.get("status") != "active":
        raise StrategyRegistrationError("dataset registry record must be active")
    if feature_record.get("status") != "active":
        raise StrategyRegistrationError("feature registry record must be active")
    if not bool(instrument_record.get("is_active")):
        raise StrategyRegistrationError("instrument registry record must be active")
    if feature_record.get("row_semantics") != "finalized_bar_end":
        raise StrategyRegistrationError("feature row_semantics must equal finalized_bar_end")
    if not bool(feature_record.get("lookahead_safe")):
        raise StrategyRegistrationError("feature record must be lookahead_safe")
    enabled_strategy_ids = portfolio_record.get("enabled_strategy_ids")
    if not isinstance(enabled_strategy_ids, list) or not enabled_strategy_ids:
        raise StrategyRegistrationError("portfolio enabled_strategy_ids must be non-empty list")
    if strategy_id not in enabled_strategy_ids:
        raise StrategyRegistrationError("portfolio must explicitly enable the requested strategy id")
    if portfolio_record.get("status") != "active":
        raise StrategyRegistrationError("portfolio registry record must be active")
    return instrument_record, dataset_record, feature_record, strategy_record, portfolio_record, environment_record, default_strategy_config_record, dataset_contract, feature_contract, manifest, strategy_config, strategy_artifact_contracts


def _require_runtime_env_vars(environment_record: Mapping[str, object]) -> None:
    required_env_vars = environment_record.get("required_env_vars")
    if not isinstance(required_env_vars, list) or not required_env_vars:
        raise StrategyRegistrationError("runtime environment must declare required_env_vars")
    for env_name in required_env_vars:
        if not isinstance(env_name, str) or not env_name:
            raise StrategyRegistrationError("runtime environment required_env_vars must be non-empty strings")
        if not os.environ.get(env_name):
            raise StrategyRegistrationError("missing required artifact root env var: " + env_name)


def _resolve_strategy_artifact_contract(strategy_artifact_contracts: tuple[ArtifactContract, ...], artifact_id: str) -> ArtifactContract:
    for contract in strategy_artifact_contracts:
        if contract.artifact_id == artifact_id:
            return contract
    raise StrategyRegistrationError("missing strategy artifact contract: " + artifact_id)


def load_registered_backtest(*, strategy_id: str, portfolio_id: str, environment_id: str) -> ResolvedRegisteredBacktest:
    instrument_record, dataset_record, feature_record, strategy_record, portfolio_record, environment_record, default_strategy_config_record, dataset_contract, feature_contract, manifest, strategy_config, strategy_artifact_contracts = _load_common_registered_components(strategy_id=strategy_id, portfolio_id=portfolio_id, environment_id=environment_id)
    if not bool(environment_record.get("is_backtest")):
        raise UnsupportedModeError("environment must be backtest-enabled")
    if bool(environment_record.get("is_live")):
        raise UnsupportedModeError("backtest environment must not be live-enabled")
    if environment_record.get("status") != "active":
        raise StrategyRegistrationError("environment registry record must be active")
    artifact_bindings = default_strategy_config_record.get("artifact_bindings")
    if not isinstance(artifact_bindings, dict):
        raise ConfigValidationError("strategy default config artifact_bindings must be object")
    output_artifact_id = artifact_bindings.get("output_day_metrics_artifact_id")
    if not isinstance(output_artifact_id, str) or not output_artifact_id:
        raise ConfigValidationError("strategy default config output_day_metrics_artifact_id is required")
    backtest_output_contract = _resolve_strategy_artifact_contract(strategy_artifact_contracts, output_artifact_id)
    if backtest_output_contract.artifact_role != "output" or backtest_output_contract.producer != "moex_backtest":
        raise StrategyRegistrationError("expected exactly one backtest output artifact contract for artifact_id=" + output_artifact_id)
    backtest_feature_builder = _import_ref(str(feature_record.get("producer_ref")))
    backtest_signal_builder = _import_ref(_resolve_strategy_hook_ref(strategy_record=strategy_record, module_suffix="signal_engine", attr_name="generate_signals"))
    backtest_request_builder = _import_ref(_resolve_strategy_hook_ref(strategy_record=strategy_record, module_suffix="backtest_adapter", attr_name="build_backtest_request"))
    return ResolvedRegisteredBacktest(instrument_record=instrument_record, dataset_record=dataset_record, feature_record=feature_record, strategy_record=strategy_record, portfolio_record=portfolio_record, environment_record=environment_record, default_strategy_config_record=default_strategy_config_record, manifest=manifest, strategy_config=strategy_config, dataset_contract=dataset_contract, feature_contract=feature_contract, strategy_artifact_contracts=strategy_artifact_contracts, backtest_feature_builder=backtest_feature_builder, backtest_signal_builder=backtest_signal_builder, backtest_request_builder=backtest_request_builder, backtest_output_contract=backtest_output_contract)


def load_registered_runtime_boundary(*, strategy_id: str, portfolio_id: str, environment_id: str) -> ResolvedRegisteredRuntimeBoundary:
    instrument_record, dataset_record, feature_record, strategy_record, portfolio_record, environment_record, default_strategy_config_record, dataset_contract, feature_contract, manifest, strategy_config, strategy_artifact_contracts = _load_common_registered_components(strategy_id=strategy_id, portfolio_id=portfolio_id, environment_id=environment_id)
    if not bool(manifest.supports_live):
        raise UnsupportedModeError("strategy manifest must support live mode")
    if not bool(strategy_record.get("supports_live")):
        raise UnsupportedModeError("strategy registry record must support live mode")
    if not bool(portfolio_record.get("is_live_allowed")):
        raise UnsupportedModeError("portfolio registry record must allow live mode")
    if environment_record.get("status") != "active":
        raise StrategyRegistrationError("environment registry record must be active")
    if bool(environment_record.get("is_backtest")):
        raise UnsupportedModeError("runtime boundary environment must not be backtest-enabled")
    if not bool(environment_record.get("is_live")):
        raise UnsupportedModeError("runtime boundary environment must be live-enabled")
    _require_runtime_env_vars(environment_record)
    runtime_state_contract = _resolve_runtime_artifact_contract(strategy_artifact_contracts=strategy_artifact_contracts, artifact_role="state", producer="moex_runtime")
    runtime_trade_log_contract = _resolve_runtime_artifact_contract(strategy_artifact_contracts=strategy_artifact_contracts, artifact_role="output", producer="moex_runtime")
    runtime_feature_builder = _import_ref(str(feature_record.get("producer_ref")))
    runtime_signal_builder = _import_ref(_resolve_strategy_hook_ref(strategy_record=strategy_record, module_suffix="signal_engine", attr_name="generate_signals"))
    runtime_live_decision_builder = _import_ref(_resolve_strategy_hook_ref(strategy_record=strategy_record, module_suffix="live_adapter", attr_name="build_live_decision"))
    return ResolvedRegisteredRuntimeBoundary(instrument_record=instrument_record, dataset_record=dataset_record, feature_record=feature_record, strategy_record=strategy_record, portfolio_record=portfolio_record, environment_record=environment_record, default_strategy_config_record=default_strategy_config_record, manifest=manifest, strategy_config=strategy_config, dataset_contract=dataset_contract, feature_contract=feature_contract, strategy_artifact_contracts=strategy_artifact_contracts, runtime_state_contract=runtime_state_contract, runtime_trade_log_contract=runtime_trade_log_contract, runtime_feature_builder=runtime_feature_builder, runtime_signal_builder=runtime_signal_builder, runtime_live_decision_builder=runtime_live_decision_builder)
