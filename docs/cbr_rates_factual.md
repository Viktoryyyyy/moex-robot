# Received CBR key rate and RUONIA facts

`cbr_rates_factual.load(root=DATA_ROOT)` returns one evidence-backed data dictionary.
`received_at` and `system_available_at` are equal ISO UTC strings for completion after
both responses. Each observation additionally has its own actual receipt availability.
`reconcile(component, now=...)` replays both raw HTML files and selected observations;
`apply(snapshot, now=...)` handles only `cbr_rates_verified`.

Sources are the existing official HTTPS English RUONIA dynamics table (30 calendar
days) and key-rate change history (3650 days), using the existing parsers. The newest
row is selected before admission; unsupported latest RUONIA calculation status fails
closed. Only the observed `Standard` status is accepted. Legacy collectors and their
historical availability conventions are untouched. Trading volumes are not published
or examined for discrepancies by this wrapper.

Both responses and a manifest containing exact queries, response times, raw hashes and
normalized observations are frozen under `raw/external/cbr_rates`. Read-time checks
require both receipts to be no more than 1200 seconds old, validate all causal times,
hashes and facts, and reject changed, missing or previously failed evidence. RUONIA
also has a conservative seven-calendar-day observation limit. This is a bounded dated
reference policy, not proof of complete release-calendar coverage. An old effective
date does not stale an unchanged key rate when its source was freshly retrieved.

The wrapper validates every key-rate data row before calling the legacy parser.
Only the exact three grouped subheader rows observed in the official response with
SHA-256 `0a7fc373333b9e13f67c28bc4a9eeed1f85546b9a676159da2ad328ec8864cec`
may precede observations for the six-column grouped header. A malformed first date,
changed subheader or unsupported table schema fails closed; no older row is selected.
Every raw key-rate effective date must also be inside the requested interval before
legacy deduplication of unchanged values; equal-valued future or too-old rows cannot
be hidden by normalization.

The official publication timestamp is unknown (`source_publication_time=null`).
RUONIA's explicit source publication date is retained, while key-rate effective date
remains separate. Same-day published RUONIA can be a current fact after actual receipt;
it is never backdated into historical feature availability. Facts grant no historical
PIT acceptance, full macro completeness, forecast use or action authority.
