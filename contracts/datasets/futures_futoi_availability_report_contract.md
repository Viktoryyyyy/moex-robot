# futures_futoi_availability_report_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_futoi_availability_report.v1

purpose: Availability report for MOEX AlgoPack FUTOI data by instrument and date range.
producer: futures_algopack_availability_probe
consumer:
- futures_slice1_universe_selector
- futures_liquidity_screen_builder
- futures_history_depth_screen_builder
- futures_data_lake_pm_review

path_pattern: ${MOEX_DATA_ROOT}/futures/availability/snapshot_date={snapshot_date}/futures_futoi_availability_report.parquet
primary_key:
- availability_report_id
- snapshot_date
- board
- secid
- endpoint_id

required_fields:
- availability_report_id
- snapshot_date
- board
- secid
- family_code
- endpoint_id
- source_endpoint_url
- probe_from
- probe_till
- availability_status
- schema_version

nullable_fields:
- first_available_date
- last_available_date
- observed_rows
- observed_min_ts
- observed_max_ts
- error_code
- error_message
- review_notes

status_fields:
- availability_status
- probe_status
- validation_status

source_transport_rules:
- FUTOI availability probes must use authenticated `apim.moex.com` transport with `MOEX_API_KEY`.
- public `iss.moex.com` transport is forbidden for FUTOI.
- public ISS fallback is forbidden for FUTOI.
- the `/iss/analyticalproducts/futoi/...` route namespace is allowed only on the authenticated APIM host.
- an `ERROR_MESSAGE` FUTOI block is an unavailable/error result and must never count as an available row.

validation_rules:
- endpoint_id must identify MOEX AlgoPack FUTOI analytical product.
- availability_status must be available, unavailable, partial, error, or not_checked.
- no loader implementation may use this report as completed unless probe_status is completed.
- available status requires the actual FUTOI data schema, including participant-position fields and a supported timestamp representation.

blocking_conditions:
- duplicate primary key.
- selected instrument lacks completed APIM FUTOI availability status.
- public ISS FUTOI transport or fallback is configured.
- error payload is interpreted as FUTOI data.
