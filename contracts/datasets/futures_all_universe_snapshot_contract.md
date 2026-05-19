# futures_all_universe_snapshot_contract

status: implemented_contract
project: MOEX Bot
contract_id: futures_all_universe_snapshot.v1
contract_version: v1
artifact_class: external_pattern
format: parquet
schema_version: futures_all_universe_snapshot.v1

purpose: Define the candidate-only all-universe registry snapshot used by PM L3-2 bounded all-futures backfill slices.

producer: src/moex_data/futures/all_universe_raw_5m_backfill_slice.py

consumers:
- futures_all_universe_eligibility_snapshot.v1 producer
- raw 5m all-universe bounded backfill slice
- PM review
- later data lake refresh stages

path_pattern: ${MOEX_DATA_ROOT}/futures/all_universe/registry_snapshot/snapshot_date={registry_snapshot_date}/registry_snapshot.parquet

required_identity_fields:
- registry_snapshot_id
- registry_snapshot_date
- engine
- market
- board
- secid
- short_code
- family_code
- asset_code
- instrument_type
- expiration_date
- is_perpetual_candidate
- first_seen_date
- last_seen_date
- registry_source
- source_scope
- schema_version

required_lineage_fields:
- build_run_id
- build_ts
- source_artifact_path
- row_status
- row_status_reason

identity_rules:
- registry_snapshot_id is stable for snapshot_date, engine, market, board, and source artifact hash.
- registry_snapshot_date is the explicit business date used for candidate discovery.
- engine must equal futures for PM L3-2.
- market must equal forts for PM L3-2.
- board must preserve the source board and is not silently coerced into RFUD.
- short_code and family_code must be deterministic strings or empty strings with explicit downstream deferral.
- source_scope must identify whether the row came from the canonical normalized registry artifact or another contracted registry source.

candidate_only_invariant:
- Registry snapshot creates candidates only.
- Registry snapshot never grants included status.
- Registry snapshot never grants raw_5m_eligible, futoi_eligible, raw_d1_eligible, continuous_v1_eligible, access_api_eligible, or w1_eligible.
- Included/deferred/excluded classification belongs to the eligibility snapshot, not to this artifact.

first_slice_policy:
- PM L3-2 first executable slice may use one registry_snapshot_date.
- PM L3-2 first executable slice may restrict execution to RFUD, included candidates, raw_5m_eligible=true, one family, max 2 secids, and 3 recent trading dates.
- Deferred and excluded registry candidates remain visible in downstream eligibility and quality reports.

forbidden_scope:
- no all-history backfill
- no daily unattended refresh
- no continuous build
- no W1 implementation
- no strategy, research, runtime trading, Telegram, or notification changes
