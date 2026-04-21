from __future__ import annotations
import json, os
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from src.moex_strategy_sdk.errors import StrategyRegistrationError

def _cfg():
    p = Path(__file__).resolve().parents[3] / "contracts" / "experiments" / "backtest_research_publication.json"
    with p.open("r", encoding="utf-8") as h:
        x = json.load(h)
    if not isinstance(x, dict):
        raise StrategyRegistrationError("publication contract must be object")
    root = os.environ.get(str(x.get("artifact_root_env_var")))
    if not root:
        raise StrategyRegistrationError("missing required artifact root env var: " + str(x.get("artifact_root_env_var")))
    arts = x.get("artifacts")
    if not isinstance(arts, dict):
        raise StrategyRegistrationError("publication contract artifacts must be object")
    return Path(root), int(x.get("publication_contract_version", 1)), arts

def _cfg_dump(v):
    if is_dataclass(v):
        return asdict(v)
    if hasattr(v, "model_dump"):
        return dict(v.model_dump())
    if hasattr(v, "dict"):
        return dict(v.dict())
    if isinstance(v, dict):
        return dict(v)
    raise StrategyRegistrationError("unsupported strategy_config type for publication")

def _summary(df: pd.DataFrame):
    if df.empty:
        return {"rows": 0, "total_pnl_day": 0.0, "max_drawdown_day": 0.0, "num_trades_total": 0.0, "positive_days": 0, "negative_days": 0}
    p = pd.to_numeric(df["pnl_day"], errors="coerce")
    d = pd.to_numeric(df["max_dd_day"], errors="coerce")
    t = pd.to_numeric(df["num_trades_day"], errors="coerce")
    if p.isna().any() or d.isna().any() or t.isna().any():
        raise StrategyRegistrationError("day_metrics contains invalid numeric values")
    return {"rows": int(len(df)), "total_pnl_day": float(p.sum()), "max_drawdown_day": float(d.max()), "num_trades_total": float(t.sum()), "positive_days": int((p > 0.0).sum()), "negative_days": int((p < 0.0).sum())}

def publish_backtest_research_result(*, run_id, strategy_id, portfolio_id, environment_id, resolved, dataset_path, primary_result_path, day_metrics):
    root, ver, arts = _cfg()
    meta = root / str(arts["run_metadata"]["locator_ref"]).format(run_id=run_id)
    mets = root / str(arts["metrics"]["locator_ref"]).format(run_id=run_id)
    reg = root / str(arts["experiment_registry"]["locator_ref"]).format(run_id=run_id)
    meta.parent.mkdir(parents=True, exist_ok=True)
    mets.parent.mkdir(parents=True, exist_ok=True)
    reg.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    labels = resolved.strategy_record.get("required_label_set_ids")
    if not isinstance(labels, list):
        labels = []
    cfg_id = str(resolved.default_strategy_config_record.get("strategy_id")) + ":" + str(resolved.default_strategy_config_record.get("version"))
    params = _cfg_dump(resolved.strategy_config)
    sm = _summary(day_metrics)
    meta_payload = {"run_id": run_id, "run_type": "backtest_research", "run_status": "published", "created_at_utc": ts, "strategy_id": strategy_id, "strategy_version": str(resolved.strategy_record.get("version")), "strategy_config_id": cfg_id, "portfolio_id": portfolio_id, "environment_id": environment_id, "dataset_artifact_id": str(resolved.dataset_contract.get("artifact_id")), "dataset_artifact_path": str(dataset_path), "feature_set_id": str(resolved.feature_record.get("feature_set_id")), "label_set_id": labels[0] if len(labels) == 1 else None, "parameter_snapshot": params, "publication_status": "published", "primary_result_artifact_ref": str(primary_result_path), "publication_contract_version": ver}
    mets_payload = {"run_id": run_id, "metrics_table": day_metrics.to_dict(orient="records"), "summary_metrics": sm}
    rec = {"run_id": run_id, "run_type": "backtest_research", "run_status": "published", "created_at_utc": ts, "strategy_id": strategy_id, "strategy_version": str(resolved.strategy_record.get("version")), "strategy_config_id": cfg_id, "dataset_artifact_id": str(resolved.dataset_contract.get("artifact_id")), "dataset_artifact_path": str(dataset_path), "feature_set_id": str(resolved.feature_record.get("feature_set_id")), "label_set_id": labels[0] if len(labels) == 1 else None, "parameter_snapshot": params, "run_metadata_artifact_ref": str(meta), "metrics_artifact_ref": str(mets), "primary_result_artifact_ref": str(primary_result_path), "summary_metrics": sm, "verdict_status": "unreviewed"}
    with meta.open("w", encoding="utf-8") as h:
        json.dump(meta_payload, h, ensure_ascii=False, indent=2)
    with mets.open("w", encoding="utf-8") as h:
        json.dump(mets_payload, h, ensure_ascii=False, indent=2)
    with reg.open("a", encoding="utf-8") as h:
        h.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return {"run_metadata_artifact_ref": str(meta), "metrics_artifact_ref": str(mets), "primary_result_artifact_ref": str(primary_result_path), "experiment_registry_ref": str(reg)}
