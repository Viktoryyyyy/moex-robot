# futures_all_universe_eligibility_contract

status: design_contract
project: MOEX Bot
contract_id: futures_all_universe_eligibility.v1
contract_version: v1
artifact_class: eligibility_contract
format: markdown
schema_version: futures_all_universe_eligibility.v1

purpose: Freeze controlled all-futures universe eligibility before implementation. Registry discovery creates candidates only; final included status requires mandatory downstream checks.

implementation_status:
- policy contract only
- no implementation code
- no tests
- no server work
- no all-futures rollout
- no historical backfill
- no strategy, research, runtime, or trading changes
- no compatibility-file updates in this cycle

scope:
- universe definition
- classification statuses
- eligibility policy
- deferral and exclusion enums
- raw 5m policy
- FUTOI policy
- liquidity policy with pending PM decision
- history-depth policy with pending PM decision
- expiration reliability policy
- perpetual futures policy
- continuous eligibility policy
- partial-chain policy
- failure isolation policy
- daily refresh partial-failure policy
- quality report expansion fields
- whitelist preservation policy
- acceptance criteria
- stop conditions

related_contracts:
- contracts/datasets/futures_continuous_5m_contract.md
- contracts/datasets/futures_continuous_roll_map_contract.md
- contracts/datasets/futures_continuous_d1_contract.md
- contracts/datasets/futures_continuous_htf_ondemand_resampling_contract.md
- contracts/datasets/moex_futures_session_calendar_contract.md

universe_definition:
- v1 universe means current available MOEX futures discovered from the latest canonical registry snapshot.
- Registry discovery is candidate-only and is never sufficient for included status.
- First supported expansion board is RFUD.
- Unsupported boards are deferred with unsupported_board_pending_review unless a structural exclusion reason applies.
- Historical expired universe and full historical backfill are outside this contract.

classification_statuses:
- included
- excluded
- deferred

classification_policy:
- Every discovered RFUD instrument must be classified as exactly one of included, excluded, or deferred.
- included requires all mandatory checks to pass.
- missing, unresolved, partial, failed, ambiguous, or uncontracted mandatory checks result in deferred.
- structural invalidity results in excluded.
- deferred is non-final and must preserve reason detail.
- excluded is reserved for structural invalidity, explicit PM exclusion, corrupted source payload, non-future rows, duplicate identity, unsupported engine/market, or out-of-current-universe rows.

classification_record_required_fields:
- eligibility_snapshot_date
- secid
- short_code
- family_code
- board
- engine
- market
- instrument_type
- classification_status
- classification_reason
- deferral_reason
- exclusion_reason
- registry_snapshot_date
- registry_source
- identity_check_status
- board_check_status
- family_mapping_status
- raw_5m_check_status
- futoi_check_status
- liquidity_check_status
- history_depth_check_status
- expiration_policy_status
- perpetual_policy_status
- calendar_quality_status
- continuous_eligibility_status
- source_scope
- notes
- schema_version

eligibility_policy:
- Mandatory included checks are identity, supported board, deterministic family mapping, raw 5m availability, FUTOI availability, liquidity policy, history-depth policy, expiration/perpetual policy, calendar/quality checks, and continuous eligibility where applicable.
- Missing required identity fields are excluded with missing_required_identity_fields.
- Duplicate registry identity is excluded with duplicate_registry_identity.
- Technical registry rows are excluded with technical_registry_row.
- Non-future registry rows are excluded with non_future_registry_row.
- Unsupported engine or market is excluded with unsupported_engine_market.
- Unsupported board with otherwise valid identity is deferred with unsupported_board_pending_review.
- Ambiguous family mapping is deferred with family_mapping_ambiguous.
- Explicit PM exclusion is excluded with explicit_pm_exclusion.

raw_5m_policy:
- Raw 5m availability is mandatory for included status.
- Probe must verify availability, schema validity, timestamp coverage, and calendar compatibility.
- Missing raw 5m data is deferred with raw_5m_unavailable.
- Failed probe execution is deferred with raw_5m_probe_failed.
- Invalid schema is deferred with raw_5m_schema_invalid.
- Calendar mismatch is deferred with raw_5m_calendar_mismatch.
- Raw 5m checks must not start continuous construction.

futoi_policy:
- FUTOI availability is mandatory for included status.
- Missing FUTOI is deferred with futoi_unavailable.
- Unresolved FUTOI identity or source lookup is deferred with futoi_unresolved.
- Family-level FUTOI is allowed only when deterministic source_scope and family mapping are explicitly contracted.
- Uncontracted family-level source_scope is deferred with futoi_source_scope_uncontracted.
- Ambiguous family-level mapping is deferred with ambiguous_family_futoi_mapping.

liquidity_policy:
- Liquidity validation is mandatory for included status.
- Exact liquidity thresholds are pending PM decision.
- Until thresholds are accepted, threshold-dependent instruments are deferred with liquidity_threshold_pending_pm_decision.
- After thresholds are accepted, instruments below threshold are deferred with liquidity_below_threshold.
- This contract does not invent liquidity thresholds.

history_depth_policy:
- History-depth validation is mandatory for included status.
- Exact history-depth thresholds are pending PM decision.
- Until thresholds are accepted, threshold-dependent instruments are deferred with history_depth_threshold_pending_pm_decision.
- After thresholds are accepted, instruments below threshold are deferred with history_depth_below_threshold.
- This contract does not invent minimum lookback, bar-count, session-count, or date-range thresholds.

expiration_reliability_policy:
- Expiring futures require reliable expiration or roll anchor before continuous eligibility can pass.
- Missing expiration anchor is deferred with expiration_anchor_missing.
- Conflicting expiration anchors are deferred with expiration_anchor_conflicting.
- Far-future or sentinel expiration is not sufficient for perpetual classification.
- Unreviewed sentinel expiration is deferred with expiration_anchor_sentinel_unreviewed.

perpetual_futures_policy:
- USDRUBF is the only accepted perpetual identity case in v1.
- USDRUBF preserves source_secid=USDRUBF, source_contract=USDRUBF, continuous_symbol=USDRUBF where continuous identity output is required.
- Other perpetual candidates are deferred with perpetual_candidate_pending_review until explicit classifier or review rule is accepted.
- Perpetual classification must not be inferred solely from missing, far-future, sentinel, or anomalous expiration.

continuous_eligibility_policy:
- Existing Continuous v1 must be preserved.
- Required values are roll_policy_id=expiration_minus_1_trading_session_v1, adjustment_policy_id=unadjusted_v1, adjustment_factor=1.0.
- Continuous eligibility requires deterministic family mapping, reliable expiration/perpetual policy, canonical calendar checks, and buildable roll-map semantics.
- Non-buildable roll map is deferred with continuous_roll_map_not_buildable.
- No silent gap bridging, inferred replacement contracts, or change to accepted roll/adjustment policy is allowed.

partial_chain_policy:
- Partial chains are explicit gaps.
- Partial chains are never silently bridged.
- If a partial-chain case lacks accepted contract representation, classify as deferred with partial_chain_missing_contract.
- Explicit gaps must not be filled by synthetic bars, fallback contracts, interpolation, or guessed continuity.

calendar_quality_policy:
- Calendar and quality checks are mandatory for included status.
- Calendar quality unresolved is deferred with calendar_quality_unresolved.
- Partial quality probe failure is deferred with quality_probe_partial_failure for the affected instrument only.
- Required session semantics must use the accepted MOEX futures session calendar contract.
- Observed bars alone cannot override canonical calendar constraints.

failure_isolation_policy:
- Instrument-level probe failures must be isolated.
- Failure for one instrument must not prevent classification of other instruments.
- Failed mandatory probe for one instrument must produce deferred or excluded status for that instrument with exact reason.
- Global failure is allowed only when the canonical registry snapshot is unavailable, unreadable, or structurally corrupt enough that candidate discovery cannot be trusted.

daily_refresh_partial_failure_policy:
- Daily refresh must classify every discovered current RFUD candidate when the registry snapshot is usable.
- Partial downstream probe failure must not erase prior valid artifacts or silently promote instruments.
- Included status must not be granted when mandatory probes fail or are unresolved.
- Deferred instruments must remain visible with explicit reason.
- Current whitelist behavior must be preserved unless a contract-valid exclusion or defer reason is produced.

quality_report_expansion_fields:
- eligibility_snapshot_date
- candidate_universe_count
- included_count
- excluded_count
- deferred_count
- classification_status
- classification_reason
- deferral_reason
- exclusion_reason
- identity_check_status
- board_check_status
- family_mapping_status
- raw_5m_check_status
- futoi_check_status
- liquidity_check_status
- history_depth_check_status
- expiration_policy_status
- perpetual_policy_status
- calendar_quality_status
- continuous_eligibility_status
- source_scope
- probe_error_code
- probe_error_message
- compatibility_note

quality_report_compatibility_policy:
- These fields are reserved for compatibility-only updates to quality-report and builder-manifest contracts.
- This contract does not modify those files.

whitelist_preservation_policy:
- Current whitelist outputs must be preserved unless a later accepted PM decision or contract-valid failure changes status:
  - SiM6
  - SiU6
  - SiU7
  - SiZ6
  - USDRUBF
- Existing excluded/deferred behavior must be preserved:
  - SiH7
  - SiM7
- SiH7 and SiM7 must not be silently promoted by all-universe expansion.
- USDRUBF perpetual identity behavior must be preserved.

deferral_reason_enum:
- unsupported_board_pending_review
- raw_5m_unavailable
- raw_5m_probe_failed
- raw_5m_schema_invalid
- raw_5m_calendar_mismatch
- futoi_unavailable
- futoi_unresolved
- futoi_source_scope_uncontracted
- ambiguous_family_futoi_mapping
- family_mapping_ambiguous
- liquidity_threshold_pending_pm_decision
- liquidity_below_threshold
- history_depth_threshold_pending_pm_decision
- history_depth_below_threshold
- expiration_anchor_missing
- expiration_anchor_conflicting
- expiration_anchor_sentinel_unreviewed
- perpetual_candidate_pending_review
- calendar_quality_unresolved
- partial_chain_missing_contract
- continuous_roll_map_not_buildable
- quality_probe_partial_failure

exclusion_reason_enum:
- technical_registry_row
- non_future_registry_row
- duplicate_registry_identity
- missing_required_identity_fields
- unsupported_engine_market
- explicit_pm_exclusion
- corrupt_source_payload
- not_in_current_available_universe

included_status_requirements:
- classification_status=included
- empty deferral_reason
- empty exclusion_reason
- identity_check_status=pass
- board_check_status=pass
- family_mapping_status=pass
- raw_5m_check_status=pass
- futoi_check_status=pass
- liquidity_check_status=pass
- history_depth_check_status=pass
- expiration_policy_status=pass
- perpetual_policy_status=pass or not_applicable
- calendar_quality_status=pass
- continuous_eligibility_status=pass or not_applicable

acceptance_criteria:
- exactly one new contract file is created at contracts/datasets/futures_all_universe_eligibility_contract.md
- accepted Sub-chat 2 policy is preserved
- registry discovery is candidate-only
- every discovered RFUD instrument is classified as exactly one of included, excluded, or deferred
- included requires all mandatory checks to pass
- missing or unresolved mandatory checks result in deferred
- structural invalidity results in excluded
- FUTOI is mandatory for canonical included status
- family-level FUTOI is allowed only with explicit deterministic source_scope and family mapping
- liquidity thresholds are pending PM decision and are not invented
- history-depth thresholds are pending PM decision and are not invented
- USDRUBF remains the only accepted perpetual identity case
- other perpetual candidates remain deferred pending explicit classifier or review rule
- Continuous v1 roll and adjustment semantics are preserved
- partial chains are explicit gaps and are never silently bridged
- current whitelist preservation is explicit
- SiH7 and SiM7 existing excluded/deferred behavior is preserved
- no implementation starts
- no compatibility files are changed in this cycle

stop_conditions:
- existing contracts contradict accepted Sub-chat 2 policy
- target file already exists and requires update rather than create
- exact file scope cannot be preserved
- GitHub commit flow is blocked
- authoring cannot avoid widening into implementation

forbidden_scope:
- no implementation code
- no tests
- no server commands
- no server apply
- no all-futures rollout
- no historical backfill
- no pilot family selection
- no changes to existing accepted continuous v1 contracts
- no compatibility-file update in this cycle
- no strategy, research, runtime, or trading change
