# DEPRECATED: futures_futoi_availability_report_contract

status: deprecated_compatibility_tombstone
canonical_source_contract: contracts/sources/futures/moex_algopack_futoi.v1.yaml
canonical_registry_evidence: configs/instruments/forts_instrument_registry.v1.yaml
canonical_runbook: docs/data/moex_market_data_ingestion_runbook.v1.md

Do not use the historical `${MOEX_DATA_ROOT}/futures/availability/...` report path as current ingestion architecture or availability authority. Current FUTOI source availability, ticker bindings and probe status are defined by the canonical source contract and instrument registry.
