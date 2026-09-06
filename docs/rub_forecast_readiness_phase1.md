# Forecast readiness: first implementation increment

This increment addresses two defects reproduced from the September 6 snapshot:
the reader checked only the file age while preserving source-time freshness gates,
and the publisher labelled collection start as generation/completion time.

## Implemented behavior

- The canonical reader recalculates live instrument ages against the reading clock.
  The 60-second source threshold is separate from the existing 1,200-second file TTL.
  Future source times beyond the existing 5-second clock tolerance and invalid or
  naive timestamps fail closed. A previously failed source gate is never promoted.
- Live price/OI, synchronization, factual authority and dependent basis/carry metrics
  are downgraded in an independent read view. Persisted input and source timestamps
  remain unchanged. Basis values whose legs expire become unavailable, not zero.
- Every refresh entrypoint records collection start and completion separately.
  `generated_at_utc` describes completed collection immediately before atomic write,
  not filesystem publication completion. Successful components' `last_success_at`
  uses this conservative collection-completion upper bound; retained components
  keep their original success time. Source `data_as_of` is not rewritten.
- Broken 15-minute buckets report the exact expected timestamps that are missing
  and observed timestamps in the bucket. Existing historical-clearing rules and
  the prohibition on synthetic filling remain in force.

## Validation and remaining server evidence

Regression tests cover the 60-second boundary including fractional overflow,
malformed/future timestamps, stale spot with usable futures, dependent basis,
immutability, retained data, backwards clocks, publication duration, and exact
15-minute gap diagnostics. The test suite must also pass on Linux because the
existing publisher uses `fcntl`.

Replay the supplied archive at 2026-09-06T12:56:43Z: futures source age is 825
seconds. The reader must remove their live factual authority and reduce available
basis metrics from 14 to 0 at that historical reading time. This is a validation
of expiration, not a claim that market data was repaired or refreshed.

Before declaring the server defect resolved, obtain:

1. Deployed git SHA and service/timer configuration, with secrets excluded.
2. Refresh logs and original 5-minute USDRUBF bars around the reported
   2026-09-06T12:00:00+03:00 bucket, including timestamp semantics.
3. A post-deployment snapshot and repeated successful scheduled refreshes.
4. Evidence that the delivery path refreshes live quotes before forecast issuance
   or collects them frequently enough for the 60-second requirement. The existing
   600-second collection interval is deliberately not relabelled as compliant.

Do not waive the 15-minute gap, turn on FUTOI authority, substitute spot sources,
or claim forecasting readiness on the strength of this increment. Those require
the subsequent data-coverage, governance and out-of-sample validation work.
