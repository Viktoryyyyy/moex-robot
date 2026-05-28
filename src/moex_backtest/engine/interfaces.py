from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, Sequence

from moex_backtest.semantics.contracts import BacktestSemanticsContract

BacktestRows = Sequence[Mapping[str, object]]
BacktestTable = tuple[dict[str, object], ...]
BacktestArtifacts = Mapping[str, object]


@dataclass(frozen=True)
class BacktestRequest:
    strategy_id: str
    strategy_version: str
    signals: BacktestRows
    market_data: BacktestRows
    semantics: BacktestSemanticsContract
    context: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BacktestResult:
    fills: BacktestTable
    costs: BacktestTable
    position_path: BacktestTable
    metrics: Mapping[str, Any]
    artifacts: BacktestArtifacts


class BacktestEngine(Protocol):
    def simulate_fills(
        self,
        orders: BacktestRows,
        market_data: BacktestRows,
        semantics: BacktestSemanticsContract,
    ) -> BacktestTable:
        ...

    def apply_costs_and_slippage(
        self,
        fills: BacktestRows,
        cost_model_id: str,
        slippage_model_id: str,
        context: Mapping[str, Any],
    ) -> BacktestTable:
        ...

    def build_position_path(
        self,
        fills: BacktestRows,
        initial_state: Mapping[str, Any],
        semantics: BacktestSemanticsContract,
    ) -> BacktestTable:
        ...

    def build_report_artifacts(
        self,
        position_path: BacktestRows,
        metrics: Mapping[str, Any],
        artifact_names: Sequence[str],
    ) -> BacktestArtifacts:
        ...

    def run_backtest(self, request: BacktestRequest) -> BacktestResult:
        ...
