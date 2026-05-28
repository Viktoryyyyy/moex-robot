# Cost Slippage Contract v1

contract_id: cost_slippage_contract.v1
schema_version: v1
artifact_class: cost_slippage_contract
producer: moex_backtest.costs
consumer: moex_backtest.engine / moex_research.runners / PM review

## purpose

Declares transaction cost and slippage assumptions for canonical strategy testing.

## required_fields

- cost_slippage_ref_id
- schema_version
- artifact_class
- producer
- consumer
- commission_rule
- exchange_fee_rule
- broker_fee_rule
- slippage_rule
- spread_rule
- turnover_rule
- gross_net_reporting_rule
- sensitivity_grid

## validation_rules

- commission_rule, exchange_fee_rule, and broker_fee_rule must state whether values are zero, fixed, bps, per-contract, or externally referenced.
- slippage_rule must define the price impact assumption or state gross-only diagnostic status.
- spread_rule must define whether bid/ask, half-spread, or close/open proxy is used.
- turnover_rule must define how entries, exits, reversals, and terminal closes contribute to costs.
- gross_net_reporting_rule must require separate gross and net metrics when costs are modeled.
- sensitivity_grid must define cost scenarios when promotion or fragility review uses cost robustness.

## forbidden_patterns

- Hidden zero-cost assumptions in promotion-facing results.
- Applying costs to only losing trades or only winning trades.
- Mixing gross and net metrics without explicit labels.
- Runtime/live permission from results that have no approved cost sensitivity.
