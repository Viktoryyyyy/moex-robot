# Latest listed Rosstat weekly CPI in the factual API

The `rosstat_cpi` component fetches the official weekly archive at
https://rosstat.gov.ru/compendium/document/50798 using the pinned verified TLS
client, then fetches the uniquely newest dated document within the exact
weekly CPI archive section. Calendar rows, announcements and other topics do
not participate. DOM order and dates embedded in filenames are not selectors.
An ambiguous, future-dated or unsupported newest release blocks admission;
there is no fallback to an older parsable document. All archive row publication
dates must be interpretable before selection.

Both HTTP receipts and raw HTML vintages are frozen under
`MOEX_DATA_ROOT/raw/external/rosstat_weekly_cpi`. Read-time reconciliation
replays the hashes, pinned transport evidence, index selection and numeric
parser; verifies archive/document period agreement and request/receipt order;
and revokes retained, failed, tampered or expired facts. It makes no network
requests. Both receipts expire after 1200 seconds; the listed publication date
may be at most ten Moscow calendar days old. These are bounded factual-context
policies, not proof that an expected release has not been missed.

The component grants only `latest_listed_weekly_estimate_dated_context`.
The archive's listed date is preserved separately from actual receipt-time
availability; the exact publication timestamp remains unknown. The numeric
parser currently supports only same-month weekly estimates. Cross-month latest
releases fail closed pending real-format acceptance. Final monthly CPI,
full Rosstat macro coverage, the publication calendar, historical PIT and
forecast alignment remain unaccepted. Consensus and surprise remain null;
training, evaluation and trading are not enabled.

The canonical producer, final publication reconciliation and API read-time
reconciliation include this component. Failed refreshes revoke previously
accepted values. The factual D1/W1 inventory includes its admitted indices and
source hashes. The source matrix exposes factual usability but keeps the full
`rosstat_macro` forecast blocker. No other missing macro provider is concealed.

Validation includes newest-release refusal without fallback, archive scope,
publication dates, period conflicts, numeric/evidence tampering, receipt expiry,
retained refresh failure, canonical producer wiring and partial source-matrix
semantics. Production acceptance additionally requires live verified capture,
replay, the final-head CI and a new canonical refresh followed by API verification.
