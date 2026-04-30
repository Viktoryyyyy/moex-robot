# futures_derived_d1_ohlcv_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_derived_d1_ohlcv.v1

purpose: Canonical derived daily OHLCV bars built from futures_raw_5m for the accepted Slice 1 whitelist. This dataset does not join FUTOI and does not create continuous futures series.
producer: src/moex_data/futures/derived_d1_ohlcv_builder.py
consumer:
- futures_derived_d1_ohlcv_quality_report
- futures_data_lake_pm_review
- later_futures_data_access_layer
- later_d1_feature_builders

path_pattern: ${MOEX_DATA_ROOT}/futures/derived_d1_ohlcv/trade_date={trade_date}/family={family_code}/secid={secid}/part.parquet
partitioning:
- trade_date
- family_code
- secid
primary_key:
- trade_date
- secid

required_fields:
- trade_date
- session_date
- board
- secid
- family_code
- open
- high
- low
- close
- volume
- bar_count
- min_ts
- max_ts
- source_dataset_id
- source_schema_version
- source_partition_count
- source_rows
- ingest_ts
- schema_version
- short_history_flag
- calendar_denominator_status

nullable_fields:
- value
- num_trades

status_fields:
- short_history_flag
- calendar_denominator_status

validation_rules:
- primary_key must be unique inside each written partition and across repeated idempotent builds for the same trade_date/secid.
- schema_version must equal futures_derived_d1_ohlcv.v1.
- source_dataset_id must equal futures_raw_5m.
- source_schema_version must equal futures_raw_5m.v1.
- calendar_denominator_status must equal canonical_apim_futures_xml.
- OHLC fields must be non-null.
- high must be greater than or equal to low.
- open and close must be within low/high inclusive.
- open must equal the first raw 5m open by timestamp for the same secid/trade_date.
- high must equal the max raw 5m high for the same secid/trade_date.
- low must equal the min raw 5m low for the same secid/trade_date.
- close must equal the last raw 5m close by timestamp for the same secid/trade_date.
- volume must equal the sum of raw 5m volume for the same secid/trade_date.
- exactly one D1 row must exist for every secid/trade_date pair present in futures_raw_5m for the accepted whitelist.
- FUTOI fields must not be joined into this dataset.
- continuous series fields must not be present in this Slice 1 dataset.
- Slice 1 D1 builder must write only the accepted whitelist: SiM6, SiU6, SiU7, SiZ6, USDRUBF.
- SiU7 may have short_history_flag=true; all other accepted Slice 1 instruments must have short_history_flag=false.

blocking_conditions:
- missing required field.
- duplicate primary key.
- invalid OHLC ordering.
- non-canonical calendar_denominator_status.
- missing D1 row for a raw 5m secid/trade_date pair.
- partition created for excluded instruments SiH7 or SiM7.
- FUTOI join or continuous series output added in this Slice 1 dataset.
