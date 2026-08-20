# DEPRECATED: futures_algopack_tradestats_availability_report_contract

status: deprecated_compatibility_tombstone
canonical_source_contract: contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml
canonical_registry_evidence: configs/instruments/forts_instrument_registry.v1.yaml
canonical_runbook: docs/data/moex_market_data_ingestion_runbook.v1.md

Do not use the historical `${MOEX_DATA_ROOT}/futures/availability/...` report path as current ingestion architecture or coverage authority. Current source identity, proven coverage and bindings are defined by the canonical source contract, instrument registry and data-lake config.
