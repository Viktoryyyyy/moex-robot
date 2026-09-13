# Brent daily and weekly factual context

PROJECT=MOEX_Bot
TASK_ID=brent_daily_weekly_context_v1

The existing `oil_brent_factual_acceptance_v1` anchor remains unchanged: three
official requests, dynamic nearest expiry at least seven calendar days away,
native USD/barrel, prior-date history CLOSE, and the existing 1,200-second receipt
recheck policy. This extension neither grants live freshness nor changes that
policy. History failures are local and do not invalidate an otherwise admitted
latest OHLC. There is no continuous series, roll adjustment, historical front
claim, historical PIT availability, forecast, training or model permission.

The slow oil producer adds `daily_weekly_context`. Acquisition starts with the
current Moscow review week's Monday minus seven weeks, bounded by the native
contract start date, and ends at the accepted native history bound. Up to four
pages and 400 source rows are permitted, with exact cursor offsets, stable totals,
complete page sizes, unique source dates, explicit selected SECID/RFUD/BR identity
and the existing unit/expiry evidence. Each request uses the existing official
transport limits; the extension has an eight-second checked collection budget
and no retries. It does not change the fast loop or fetch on read/export.

The first run and first run on a new Moscow date fetch the full bounded range.
Within a date an unchanged validated anchor reuses the prior accepted evidence,
explicitly reporting CACHE_REUSED and zero requests without renewing its receipt.
A newer source bound refreshes an overlapping current-week tail; a changed OHLC
on the same anchor date triggers a full bounded recheck. Each prefix and overlap
retains its own complete page evidence. A failed refresh keeps the original
evidence but refuses history output until a successful retry, with the original
evidence clocks unchanged. An unchanged source revision preserves first acceptance.
Revisions older than the overlap may be discovered only by the next full daily
check; a successful tail check does not claim to have rechecked the older prefix.

Raw bytes and immutable received version manifests are content-addressed under
`MOEX_DATA_ROOT/audit/brent_daily_context/{raw,versions}`. Existing files are
verified, never overwritten. Bounded evidence, including the original raw text,
also travels in the snapshot so frozen replay requires no network or filesystem
lookup. Replay checks the raw digest, parsed native rows, request/receipt/acceptance
chronology, version digest, range, cursor chain and latest-anchor consistency.
Raw evidence is not sent to the compact consumer; `source_revision_id`,
`audit_version_ref` and request/receipt references intentionally survive compacting.

The oil fact's `values.daily_weekly_context` contains up to 30 D1 source-observed
dates and 1/5/20-observation comparisons. Each comparison names both dates and
CLOSEs, SECID, exact ordinal lag and USD/barrel/percent units:
`change_abs = latest - reference`; `change_pct = (latest/reference - 1) * 100`.
Known-date invalid prices remain in the ordinal sequence. A comparison spanning
an invalid row is UNAVAILABLE rather than shifted to a nearby valid date.
Non-finite computed changes are refused. Invalid identities/dates, duplicates or
incomplete pagination refuse the history range. A missing 20-observation basis
does not suppress valid shorter comparisons or daily records.

W1 reuses the repository's Monday–Sunday convention on native trade_date, without
inferring trading dates. Eight review periods are shown, including explicit empty
periods and the actual current review week at the package as_of. Each aggregate
uses the first valid OPEN, maximum HIGH, minimum LOW and last CLOSE within that
single contract. An invalid row refuses its week. Observed dates/count, period
bounds, covered bounds, requested source coverage and calendar interval ending
are separate fields. Neither five rows, an ended calendar week nor pagination
proves session completion or source finality; both permissions remain false.

The release and compact paths replay the same evidence at the caller's single
as_of. Reverse projection checks the entire oil history subtree for omitted,
added or changed values. Existing package coverage and readiness rules are not
changed. Runtime `last_attempt` exposes request count, received bytes, elapsed
seconds, cache/received/failed status and exact refusal. Production measured values,
reviewed/merged/applied SHAs and HTTP/CLI acceptance belong to deployment evidence,
not constants in the implementation. Tests use synthetic source fixtures.
