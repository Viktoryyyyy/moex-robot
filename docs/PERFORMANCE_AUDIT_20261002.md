# Performance audit, 2026-10-02

## Scope and safe execution plan

Base: GitHub main `60731339`; server initially `c04a2eb1`. Inspect the source tree,
data readers, HTTP delivery, periodic jobs, research loops and live server state.
Prioritize measured waste in active paths over speculative rewrites of financial
calculations. No changes to strategy, authority, dates, freshness or data retention.

1. Capture a read-only production profile and run the unmodified test suite in an
   isolated archive of main. Keep the canonical checkout and data untouched.
2. Remove redundant work while retaining admission, SHA, size and causality gates.
3. Exercise corrupt-input cases and compare complete logical and HTTP output bytes
   at one fixed consumption clock. Run the complete candidate suite.
4. Publish the reviewed patch, merge only with passing checks, fast-forward the
   clean server checkout, restart only the API/MCP processes that import code.
5. Verify actual API responses and periodic services. Keep the prior Git revision
   as rollback; remove only positively identified temporary test artifacts.

## Confirmed findings and fixes

* HTTP snapshot/release/market handlers encoded full responses once for validation
  and again for sending. Send the validated bytes directly. Encoding still finishes
  before success headers; malformed JSON still returns 503. Readiness keeps full
  snapshot validation before returning its small response.
* Rosstat archive selection scanned every HTML node for every archive row. Index
  relevant descendants in one pass using node identity; preserve DOM order and
  all ancestor matches, so nested/duplicate rows remain rejected. Replay parses
  the same receipt-bound HTML only once for selection and calendar validation.
  Nothing is cached across requests; clocks, raw bytes and receipts are rechecked.
* Accepted quote history decoded its current D1 parquet twice and selected the
  same prefix once per field. Reuse the first frame and select each prefix once.
  Keep exact per-field comparisons and all independent evidence revalidation.
* Legacy user units `moex-cmd` and `moex-loop` pointed at absent scripts and each
  accumulated over 153,000 restarts. Add entrypoint file conditions. Preserve
  their commands and enablement; missing entrypoints cause a skipped start.

## Measured evidence

Production cProfile: 97.5 million calls, 62.94s read + 5.47s single encode under
profiling/concurrent load. Nine freshness passes and ten Rosstat replays contribute
substantial repeated work; this is not a normal endpoint latency benchmark.

Controlled same-process comparison on a fixed production snapshot and clock:

| Step | Before | After |
| --- | ---: | ---: |
| Read/reconcile | 15.00s | 13.32s |
| Response encoding | 8.27s | 3.75s |
| Combined measured steps | 23.28s | 17.07s |

One sample under shared server load, about 27% lower combined elapsed time, not
a p95 guarantee. Complete logical output (27,415,840 bytes) and HTTP response
(16,564,619 bytes) matched byte for byte. Response SHA256:
`d1af888d2eb8235f0781a1c567bb8661a683643e4f1baeb1eb1492707c8d348e`.

114 targeted tests passed, including new serialization-count, invalid-JSON,
linear archive-scan, nested-ambiguity and fresh-replay tests. Full-suite and
deployment results are recorded in the task delivery report.

## Remaining opportunities and independent faults

* Repeated freshness/admission across projections and repeated HTML/evidence
  replays still dominate reads. A future request-scoped immutable evidence context
  needs explicit ownership, mutation, byte-bound and clock tests. A global cache
  would risk stale or changed evidence and is not an acceptable shortcut.
* Strategy/research row loops and the legacy MR-1 CSV loop are candidates for
  separate representative benchmarks. Do not vectorize trading arithmetic or
  truncate history based only on pattern matching.
* Universal futures daily refresh is already failing in `futoi_raw_refresh`.
  This is a source/refresh fault, not evidence that this optimization failed;
  retain failure visibility and investigate its component manifest separately.
* The old `completion-multi-review` checkout has three untracked Minfin source,
  documentation and test files. It must not be deleted as a temporary copy.

## Rollback

Revert this optimization commit if regressions appear; fast-forward the resulting
revert on the server and restart the same API/MCP units. For an immediate rollback
use the recorded pre-deploy revision only after checking that no newer changes
or uncommitted work would be overwritten. Remove just the two new condition
drop-ins and reload the user manager to restore former unit behavior (including
the original missing-script failure). No data migration or source rewrite exists.
