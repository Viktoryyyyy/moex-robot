from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from moex_research.registry.result_storage_contracts import (
    _reject_forbidden_text,
    _require_mapping,
    _require_text,
)


@dataclass(frozen=True)
class ResearchRunRequest:
    request_id: str
    strategy_package_ref: str
    strategy_config_ref: str
    research_runner_ref: str
    canonical_backtest_engine_ref: str
    dataset_refs: Mapping[str, str]
    parameter_snapshot_ref: str
    repo_commit: str
    immutable_inputs: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "request_id",
            _reject_forbidden_text(_require_text(self.request_id, "request_id"), "request_id"),
        )
        object.__setattr__(
            self,
            "strategy_package_ref",
            _reject_forbidden_text(
                _require_text(self.strategy_package_ref, "strategy_package_ref"),
                "strategy_package_ref",
            ),
        )
        object.__setattr__(
            self,
            "strategy_config_ref",
            _reject_forbidden_text(
                _require_text(self.strategy_config_ref, "strategy_config_ref"),
                "strategy_config_ref",
            ),
        )
        object.__setattr__(
            self,
            "research_runner_ref",
            _reject_forbidden_text(
                _require_text(self.research_runner_ref, "research_runner_ref"),
                "research_runner_ref",
            ),
        )
        object.__setattr__(
            self,
            "canonical_backtest_engine_ref",
            _reject_forbidden_text(
                _require_text(self.canonical_backtest_engine_ref, "canonical_backtest_engine_ref"),
                "canonical_backtest_engine_ref",
            ),
        )
        object.__setattr__(
            self,
            "parameter_snapshot_ref",
            _reject_forbidden_text(
                _require_text(self.parameter_snapshot_ref, "parameter_snapshot_ref"),
                "parameter_snapshot_ref",
            ),
        )
        object.__setattr__(self, "repo_commit", _require_text(self.repo_commit, "repo_commit"))
        object.__setattr__(self, "dataset_refs", dict(_require_mapping(self.dataset_refs, "dataset_refs")))
        object.__setattr__(
            self,
            "immutable_inputs",
            dict(_require_mapping(self.immutable_inputs, "immutable_inputs")),
        )


def validate_research_run_request(request: ResearchRunRequest) -> ResearchRunRequest:
    if not isinstance(request, ResearchRunRequest):
        raise TypeError("request must be ResearchRunRequest")
    if not request.dataset_refs:
        raise ValueError("dataset_refs are required")
    return request
