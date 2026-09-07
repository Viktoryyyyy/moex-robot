# CR current-pair acceptance boundary

CR broad factual authority remains blocked. A separate optional
`current_pair_acceptance` entry under CR instrument governance can admit only
`current_intraday_latest_pair_only`. It is disabled until an explicit, hash-bound
acceptance artifact is committed. This implementation alone grants no CR authority.

Each canonical CR materialization freezes a publication audit before its frontier
is returned or rejected. Every source event timestamp is checked with the same
identity, session, maximum revision and exact zero-balance validator. The audit
records all pair outcomes and binds the canonical partition, raw quality report
and manifest by SHA-256. A valid frontier does not accept earlier publications.
Rejected frontier exceptions carry the attempted provenance, and current/previous
refresh records retain it as `failed_attempt_evidence`, separately from old facts.

Consumer admission requires the committed acceptance artifact, all required gates,
a FRESH last attempt, exact equality with the audit's latest fact and intact raw,
quality and manifest bytes. Corruption, a failed latest attempt or an incomplete
frontier cannot be repaired by choosing an old pair. Previous-session and
delta/statistics consumer rights stay false even when the current pair is admitted.

Source event, publication, availability, ingest and receipt must be causally ordered,
not in the future, and at most 1200 seconds old. The bound matches the existing heavy
snapshot lifetime and also limits source age. Reading a cached snapshot can only
revoke this scoped permission; it never refreshes a receipt or promotes stale data.
Expiry means unavailable current context, not a closed session or neutral factor.

The acceptance artifact must record canonical live smoke and negative replay PASS,
the exact policy `latest_source_timestamp_no_fallback_exact_balance_v1`, instrument
`cr_futures_family`, scope, and `historical_authority=false`. Operational evidence
must identify the source partition and rejected-pair policy before this entry is
enabled. Provider root cause, historical/session acceptance, predictive authority,
training and order execution are not granted by this scope.

Deployment acceptance must exercise the real collector and API, verify archived
failed attempts survive a subsequent successful refresh, check corrupt/expired
evidence fails closed, and check previous-session/delta fields remain prohibited.

## Accepted current-pair scope, 2026-09-07

The separate current-pair gate is explicitly enabled by the hash-bound artifact
`contracts/intelligence/futoi_cr_current_pair_acceptance_2026-09-07.json`.
Canonical smoke at 19:39 UTC replayed the 19:35 UTC pair with zero balance; the
187-publication response retained 49 rejected publications. The known historical
imbalance, newer incomplete frontier, tampered fact/receipt, retention and expiry
were rejected. A second real refresh preserved the rejected evidence hashes.
Snapshot attachment granted only current-pair consumer rights; previous-session
and delta/statistics stayed false. This resolves handling of inconsistent earlier
publications for this narrow scope, not the provider root cause or broad CR gates.
