# futures_continuous_bars_access_api_contract

status: design_contract
project: MOEX Bot
contract_id: futures_continuous_bars_access_api.v1
contract_version: v1
artifact_class: access_contract
format: in_memory_dataframe_or_equivalent
schema_version: futures_continuous_bars_access_api.v1

purpose: Consumer-facing canonical Python data access API for MOEX futures continuous OHLCV bars across the supported timeframes 5m, 15m, 30m, 1h, 4h, and D1.

public_module:
- src/moex_data/futures/continuous_bars_access.py

public_api:
- load_futures_continuous_bars(...) -> pandas.DataFrame

public_error:
- FuturesContinuousBarsAccessError

implementation_status:
- This contract freezes the API and access semantics only.
- It does not implement the module, tests, server execution, or data backfill.

source_dataset_dependencies:
- dataset_id: futures_continuous_5m
  schema_version: futures_continuous_5m.v1
  contract_file: contracts/datasets/futures_continuous_5m_contract.md
- dataset_id: futures_continuous_d1
  schema_version: futures_continuous_d1.v1
  contract_file: contracts/datasets/futures_continuous_d1_contract.md
- contract_id: futures_continuous_htf_ondemand_resampling.v1
  contract_file: contracts/datasets/futures_continuous_htf_ondemand_resampling_contract.md
- contract_id: moex_futures_session_calendar.v1
  contract_file: contracts/datasets/moex_futures_session_calendar_contract.md

producer: later_futures_continuous_bars_access_layer
consumer:
- later_research_consumers
- later_backtest_consumers
- later_strategy_consumers
- later_runtime_consumers
- later_reporting_consumers

api_signature_contract:
```python
def load_futures_continuous_bars(
    *,
    data_root,
    family_code: str,
    roll_policy_id: str,
    adjustment_policy_id: str,
    timeframe: str,
    start,
    end,
    session_calendar_id: str | None = None,
    session_calendar=None,
    columns=None,
) -> pandas.DataFrame:
    ...
```

request_parameters:
- data_root: required path-like value. Root of the server-side MOEX data lake. It must be explicit; environment fallback or latest-file autodetect is not authorized by this API contract.
- family_code: required string. Selects exactly one continuous futures family.
- roll_policy_id: required string. Must match the selected continuous source partitions.
- adjustment_policy_id: required string. Must match the selected continuous source partitions.
- timeframe: required exact string enum. Supported values are exactly 5m, 15m, 30m, 1h, 4h, D1.
- start: required inclusive lower boundary. For 5m and HTF requests this is interpreted against intraday source timestamps and/or trade_date coverage. For D1 requests this is interpreted against trade_date/session_date coverage.
- end: required inclusive upper boundary. For 5m and HTF requests this is interpreted against intraday source timestamps and/or trade_date coverage. For D1 requests this is interpreted against trade_date/session_date coverage.
- session_calendar_id: optional only for 5m and D1 requests. Required for 15m, 30m, 1h, and 4h requests. When required, the only accepted v1 value is moex_futures_session_calendar.v1.
- session_calendar: optional only for 5m and D1 requests. Required as a valid canonical calendar input or binding for 15m, 30m, 1h, and 4h requests unless a later accepted implementation contract defines an equivalent canonical resolver by session_calendar_id.
- columns: optional explicit output column subset. Unknown columns fail closed. Columns required for source validation, HTF validation, timestamp semantics, lineage, and primary-key construction must not be dropped before validation.

supported_timeframes:
- 5m
- 15m
- 30m
- 1h
- 4h
- D1

rejected_timeframe_aliases:
- 1d
- daily
- 60m
- 240m
- any non-exact timeframe value
- any arbitrary pandas, DuckDB, SQL, or date-bin resampling rule string

timeframe_policy:
- 5m is the canonical stored intraday continuous source.
- 15m, 30m, 1h, and 4h are on-demand access-layer outputs only.
- D1 is the canonical stored daily continuous source.
- 15m, 30m, 1h, and 4h must never be materialized as parquet zones, CSV files, or persistent dataset partitions under this contract.

source_routing:
- timeframe=5m must read canonical futures_continuous_5m.v1 partitions only.
- timeframe in 15m, 30m, 1h, 4h must read canonical futures_continuous_5m.v1 partitions and call resample_continuous_htf(...) from src/moex_data/futures/continuous_htf_resampling.py.
- timeframe=D1 must read canonical futures_continuous_d1.v1 partitions only.
- D1 requests must not be served through the HTF resampling layer.
- HTF requests must not read futures_continuous_d1.v1.
- The access layer must not route to raw contract bars, FUTOI raw, strategy artifacts, research CSV files, runtime logs, or any non-continuous dataset.

canonical_path_patterns:
- 5m source pattern: ${data_root}/futures/continuous_5m/roll_policy={roll_policy_id}/adjustment_policy={adjustment_policy_id}/family={family_code}/trade_date={trade_date}/part.parquet
- D1 source pattern: ${data_root}/futures/continuous_d1/roll_policy={roll_policy_id}/adjustment_policy={adjustment_policy_id}/family={family_code}/trade_date={trade_date}/part.parquet
- HTF source pattern: same as 5m source pattern, followed by in-memory resample_continuous_htf(...)

latest_file_policy:
- The API must never infer source data by newest mtime, lexicographic latest filename, broad glob selection, or implicit server directory discovery.
- The source path set must be determined only from explicit request parameters and accepted dataset contracts.

calendar_binding_policy_for_htf:
- session_calendar_id is mandatory for 15m, 30m, 1h, and 4h.
- session_calendar_id=None must fail closed for 15m, 30m, 1h, and 4h.
- session_calendar_id must equal moex_futures_session_calendar.v1 for v1 HTF requests.
- A valid canonical calendar input or binding is mandatory for 15m, 30m, 1h, and 4h.
- The calendar input or binding must satisfy contracts/datasets/moex_futures_session_calendar_contract.md.
- Observed source bars alone are not a valid HTF calendar binding.
- Missing, unavailable, ambiguous, stale, insufficient, inconsistent, or partially covering calendar binding must fail closed.
- The access layer must pass the validated calendar binding and session_calendar_id to resample_continuous_htf(...).

returned_dataframe_schema_policy:
- The return value must be a pandas.DataFrame.
- Returned columns are timeframe-specific and must preserve canonical timestamp and lineage semantics.
- If columns is None, the API must return the full schema for the selected timeframe class.
- If columns is provided, the API must validate full source/schema semantics first, then return exactly the requested subset in requested order.

returned_schema_5m:
- trade_date
- end
- session_date
- continuous_symbol
- family_code
- source_secid
- source_contract
- open
- high
- low
- close
- volume
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- is_roll_boundary
- roll_map_id
- schema_version
- ingest_ts

returned_schema_htf_15m_30m_1h_4h:
- trade_date
- session_date
- bucket_end
- timeframe
- continuous_symbol
- family_code
- source_secid
- source_contract
- open
- high
- low
- close
- volume
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- contains_roll_boundary
- source_bar_count
- first_source_end
- last_source_end
- schema_version

returned_schema_d1:
- trade_date
- session_date
- continuous_symbol
- family_code
- source_contracts
- open
- high
- low
- close
- volume
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- has_roll_boundary
- roll_map_id
- schema_version
- ingest_ts

schema_version_policy:
- 5m output must preserve schema_version=futures_continuous_5m.v1 from the source dataset.
- HTF output must preserve schema_version=futures_continuous_htf_ondemand_resampling.v1 from resample_continuous_htf(...).
- D1 output must preserve schema_version=futures_continuous_d1.v1 from the source dataset.

timestamp_and_sorting_semantics:
- 5m timestamp field is end, representing the right-labeled 5-minute bar close timestamp.
- HTF timestamp field is bucket_end, representing the right-labeled higher-timeframe bucket close timestamp.
- D1 date key is trade_date with session_date preserved from the canonical daily continuous dataset.
- Returned 5m rows must be sorted by continuous_symbol, roll_policy_id, adjustment_policy_id, trade_date, session_date, end.
- Returned HTF rows must be sorted by continuous_symbol, roll_policy_id, adjustment_policy_id, timeframe, trade_date, session_date, bucket_end.
- Returned D1 rows must be sorted by continuous_symbol, roll_policy_id, adjustment_policy_id, trade_date, session_date.
- Output primary keys must be unique.
- The API must not silently coerce left-labeled, left-closed, or custom bucket semantics into canonical output.

validation_rules_common:
- All required request parameters for the selected timeframe must be present and non-empty.
- timeframe must match one supported exact value.
- family_code, roll_policy_id, and adjustment_policy_id must each select exactly one source lineage set.
- start must be less than or equal to end.
- Requested range must resolve to at least one canonical source row unless a later accepted contract explicitly defines empty-range behavior.
- Source schema_version must match the routed source contract.
- Required source columns must be present and non-null.
- OHLC values must be non-null and internally valid: high >= low and open/close inside low/high inclusive.
- Duplicate primary keys are forbidden.
- Non-monotonic timestamps inside the selected lineage are forbidden.
- Mixed family_code, roll_policy_id, adjustment_policy_id, or schema_version in one returned frame are forbidden.

validation_rules_5m:
- Source rows must come only from futures_continuous_5m.v1.
- All returned rows must match the requested family_code, roll_policy_id, and adjustment_policy_id.
- end must be monotonic within continuous_symbol, trade_date, and session_date.
- No local resampling or aggregation is permitted for timeframe=5m.

validation_rules_htf:
- Source rows must come only from futures_continuous_5m.v1.
- resample_continuous_htf(...) must be the only aggregation mechanism.
- The HTF output schema and fail-closed behavior must conform to futures_continuous_htf_ondemand_resampling.v1.
- HTF buckets must never cross trade_date, session_date, session interval, source_secid, source_contract, roll_map_id, roll_policy_id, adjustment_policy_id, or adjustment_factor boundaries.
- Missing expected 5m source bars must fail closed.
- Partial buckets are allowed only when caused by canonical session interval edges and validated by the canonical session calendar.

validation_rules_d1:
- Source rows must come only from futures_continuous_d1.v1.
- D1 must not be recomputed on demand from 5m inside this API.
- All returned rows must match the requested family_code, roll_policy_id, and adjustment_policy_id.
- source_contracts must be non-empty and must preserve D1 lineage.

fail_closed_rules:
- unsupported timeframe or rejected alias.
- missing data_root, family_code, roll_policy_id, adjustment_policy_id, timeframe, start, or end.
- missing adjustment_policy_id.
- missing or empty roll_policy_id.
- missing or empty family_code.
- start greater than end.
- unresolved source partitions for the requested explicit parameters.
- requested range outside available canonical source coverage.
- source schema mismatch.
- missing required source column.
- null required source value.
- invalid OHLC relationship.
- duplicate primary key or duplicate timestamp in the selected lineage.
- non-monotonic timestamps in the selected lineage.
- mixed family_code, roll_policy_id, adjustment_policy_id, or schema_version in returned rows.
- unknown requested output column.
- request attempts latest-file autodetect.
- request attempts direct parquet path override.
- request attempts local or consumer-provided resampling rules.
- request attempts to persist 15m, 30m, 1h, or 4h bars.
- HTF request has session_calendar_id=None.
- HTF request has missing calendar input or binding.
- HTF request has session_calendar_id other than moex_futures_session_calendar.v1.
- HTF calendar input or binding is missing, unavailable, ambiguous, stale, inconsistent, partially covering, or not canonical.
- resample_continuous_htf(...) raises or cannot validate the requested interval.
- D1 request attempts to route through HTF resampling.
- 5m request attempts to route through HTF resampling.

consumer_boundary:
- Research, backtest, strategy, runtime, and reporting consumers must use load_futures_continuous_bars(...) for continuous futures bars.
- Consumers must not read futures_continuous_5m.v1 or futures_continuous_d1.v1 parquet partitions directly.
- Consumers must not implement local pandas resample, DuckDB date_bin, SQL time_bucket, or custom bucket logic for continuous futures 15m, 30m, 1h, or 4h bars.
- Consumers must not infer latest source files from server filesystem state.
- Consumers must not write materialized 15m, 30m, 1h, or 4h datasets under this contract.
- Consumers must preserve returned timestamp semantics: end for 5m, bucket_end for HTF, trade_date/session_date for D1.

forbidden_scope:
- no implementation code.
- no tests.
- no server commands.
- no all-futures expansion.
- no historical backfill.
- no strategy, research, runtime, or trading behavior changes.
- no DB service.
- no materialized HTF parquet zones.
- no new resampling semantics.

blocking_conditions:
- accepted futures_continuous_5m.v1, futures_continuous_d1.v1, futures_continuous_htf_ondemand_resampling.v1, or moex_futures_session_calendar.v1 contracts contradict this access API.
- canonical path pattern for futures_continuous_5m.v1 or futures_continuous_d1.v1 is missing.
- moex_futures_session_calendar.v1 cannot support HTF calendar binding.
- resample_continuous_htf(...) cannot be reused without widening scope.
- implementation decisions beyond access API semantics are required before contract creation.
