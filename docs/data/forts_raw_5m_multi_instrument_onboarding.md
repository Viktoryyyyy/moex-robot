# FORTS raw 5m multi-instrument onboarding

Status: superseded navigation note

Canonical operational onboarding is now defined in:

`docs/data/moex_market_data_ingestion_runbook.v1.md`

Machine-readable onboarding contract:

`contracts/datasets/forts_raw_5m_multi_instrument_onboarding.v1.yaml`

Do not use historical artifact-id/SECID pointer paths, family storage identity, or legacy `${MOEX_DATA_ROOT}/forts` / `${MOEX_DATA_ROOT}/futures` layouts for new ingestion.
