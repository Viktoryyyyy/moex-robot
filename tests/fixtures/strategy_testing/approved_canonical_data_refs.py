from __future__ import annotations

from moex_research.runners.canonical_data_read import (
    CanonicalDataReadRequest,
    CanonicalDatasetRef,
)

APPROVED_SI_D1_CANONICAL_DATASET_REF = CanonicalDatasetRef(
    dataset_ref_id="canonical.dataset.si.d1.v1",
    dataset_class="canonical_bars",
    instrument_id="Si",
    timeframe="D1",
    schema_ref="canonical.schema.ohlcv.d1.v1",
    storage_ref="canonical.store.si.d1.v1",
    calendar_ref="canonical.calendar.moex.trading_days.v1",
    source_granularity="bar",
    read_mode="dry_run_reference_validation_only",
)

APPROVED_USDRUBF_5M_CANONICAL_DATASET_REF = CanonicalDatasetRef(
    dataset_ref_id="canonical.dataset.usdrubf.5m.v1",
    dataset_class="canonical_bars",
    instrument_id="USDRUBF",
    timeframe="5m",
    schema_ref="canonical.schema.ohlcv.5m.v1",
    storage_ref="canonical.store.usdrubf.5m.v1",
    calendar_ref="canonical.calendar.moex.intraday.v1",
    source_granularity="bar",
    read_mode="dry_run_reference_validation_only",
)

EMA_3_19_SI_D1_CANONICAL_READ_REQUEST = CanonicalDataReadRequest(
    request_id="ema_3_19.canonical_data_read.si_d1.v1",
    strategy_id="ema_3_19",
    strategy_test_id="ema_3_19.strategy_test.canonical_read.v1",
    dataset_ref=APPROVED_SI_D1_CANONICAL_DATASET_REF,
    read_purpose="strategy_testing_planned",
    read_mode="dry_run_reference_validation_only",
)

__all__ = [
    "APPROVED_SI_D1_CANONICAL_DATASET_REF",
    "APPROVED_USDRUBF_5M_CANONICAL_DATASET_REF",
    "EMA_3_19_SI_D1_CANONICAL_READ_REQUEST",
]
