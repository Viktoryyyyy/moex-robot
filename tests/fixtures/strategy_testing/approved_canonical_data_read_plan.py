from __future__ import annotations

from moex_research.runners.canonical_data_read_plan import (
    CanonicalDataAccessPolicy,
    plan_canonical_data_read,
)
from tests.fixtures.strategy_testing.approved_canonical_data_refs import (
    EMA_3_19_SI_D1_CANONICAL_READ_REQUEST,
)

APPROVED_CANONICAL_DATA_DRY_RUN_POLICY = CanonicalDataAccessPolicy(
    policy_id="canonical.data.access_policy.dry_run.v1",
    allowed_dataset_classes=("canonical_bars",),
    allowed_instruments=("Si", "USDRUBF"),
    allowed_timeframes=("D1", "5m"),
    allow_file_read=False,
    allow_network=False,
    allow_discovery=False,
    access_mode="dry_run_plan_only",
)

EMA_3_19_SI_D1_CANONICAL_READ_PLAN = plan_canonical_data_read(
    EMA_3_19_SI_D1_CANONICAL_READ_REQUEST,
    APPROVED_CANONICAL_DATA_DRY_RUN_POLICY,
)

__all__ = [
    "APPROVED_CANONICAL_DATA_DRY_RUN_POLICY",
    "EMA_3_19_SI_D1_CANONICAL_READ_PLAN",
]
