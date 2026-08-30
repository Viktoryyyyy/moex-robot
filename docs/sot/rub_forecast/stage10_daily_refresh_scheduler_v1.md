# MOEX Bot — Stage 10 RUB Daily Refresh Scheduler v1

## Status

Stage 10 defines the rolling production refresh route for the server-core evidence consumed by Stage 9.

The historical Stage 2 content attestation, Stage 5 pilot and Stage 7 pilot remain immutable historical proof. Stage 10 does not rewrite their fixed historical expectations or claim that those historical pilots are themselves schedulers.

The canonical service entrypoint is `moex_data.step10_rub_refresh_dispatcher`. The dispatcher reads the repository FUTOI governance contract before choosing the refresh mode. The lower-level `step10_rub_refresh_scheduler` remains the full Stage 5+7 implementation and is entered only when FUTOI promotion is explicitly allowed.

## Execution boundary

The runner requires both:

- an explicit `--through-date`;
- an explicit immutable `--run-id`.

`through_date` must be a completed Moscow calendar date strictly before the runner's current Moscow date. The systemd route resolves the previous Moscow calendar date explicitly at the service boundary.

No implicit latest dataset/path selection is allowed.

## Governance dispatch

The dispatcher reads `contracts/intelligence/usdrubf_futoi_live_acceptance_governance_v1.json` from the repository Source of Truth.

### Full Stage 10 mode

`FUTOI_PROMOTION_ALLOWED_FULL_STAGE10` is allowed only when all required FUTOI gates are `PASS` and `authority.factual_live_authority=true`.

In that mode Stage 10 may refresh and transactionally promote the complete Stage 5/7 derived set.

### Current governed-blocked mode

While FUTOI remains governed blocked, the canonical route is `FUTOI_GOVERNED_BLOCKED_STAGE7_ONLY`:

1. FUTOI Stage 5 materialization is not run.
2. Stage 5 current accepted pointers are not changed.
3. Stage 7 USDRUBF/CNYRUBF D1/W1 and technical data continue to refresh and publish independently.
4. Stage 3 current raw evidence and Stage 4 basis/carry continue to refresh when needed.
5. Stage 9 daily/weekly smoke runs against the resulting state.

This prevents fresh FUTOI from gaining hidden factual authority through Stage 9 while also preventing the FUTOI blocker from freezing unrelated RUB market data.

## Deterministic full-mode refresh order

1. Read the MOEX futures calendar for an explicit bounded date range.
2. Validate current accepted Stage 5 and Stage 7 histories.
3. Catch up all missing MOEX futures trading dates through the requested completed date.
4. For Stage 5, acquire exact-date Si/CR FUTOI raw partitions, freeze exact bytes into the run scope, derive EOD positioning and recompute D1 positioning features.
5. For Stage 7, acquire exact-date USDRUBF/CNYRUBF 5m partitions, freeze exact bytes into the run scope, extend D1, derive only completed W1 periods, and recompute contracted technical features.
6. Refresh/promote Stage 3 current raw evidence for the latest completed trading date when needed.
7. Refresh/promote Stage 4 basis/carry for the same date when needed.
8. Apply governed derived-pointer promotion.
9. Run Stage 9 daily and weekly post-refresh smoke validation.

## Fail-closed semantics

Before any current pointer mutation, Stage 10 snapshots every current accepted pointer consumed by Stage 9.

If the run fails after pointer mutation, Stage 10 restores only pointer values still owned by that run; a concurrent publisher is not overwritten. Run-scoped artifacts from the failed attempt remain immutable evidence and are not treated as current accepted state.

Derived pointer promotion is transactional within the selected dispatcher mode. Stage 3 and Stage 4 continue to use their canonical promotion functions.

A Stage 10 run is successful only when Stage 9 can read the resulting current accepted state for both scopes with the contracted block counts:

- daily: 20 server-core blocks;
- weekly: 24 server-core blocks.

The Stage 9 policy-gap statuses are not converted to ready by Stage 10. Missing external context, position/risk input, continuous Si/CR, weekly OI and advanced technical policies remain explicit downstream gaps.

## Rolling versus historical semantics

Stage 10 builds full-history derived snapshots by extending the applicable current accepted historical baseline. It does not mutate historical baseline run artifacts.

Stage 7 rolling D1 lineage binds the exact accepted base-frame snapshot and the frozen delta manifest. Sunday catch-up uses the completed Sunday boundary for W1 even when the final new D1 row is Friday.

New raw inputs are exact-date and run-frozen before derived computation. Catch-up availability does not create a claim of historical point-in-time research readiness. `historical_pit_research_ready_claimed=false` remains explicit.

Stage 5 remains non-authoritative and non-published in the current FUTOI governed-blocked dispatcher mode. Any future transition to full Stage 5 publication requires the repository governance contract to permit factual live authority and must preserve complete auditable lineage.

## Scheduling

Dedicated units:

- `ops/systemd/moex-rub-stage10-daily-refresh.service`
- `ops/systemd/moex-rub-stage10-daily-refresh.timer`

The timer targets 00:30 Europe/Moscow each day and passes the previous Moscow calendar date to the dispatcher. This is intentionally separate from the pre-existing generic all-universe futures refresh timer.

A single `flock` protects Stage 10 from overlapping runs.

## Prohibited behavior

Stage 10 does not:

- write broker orders;
- infer position size;
- generate stops or targets;
- generate scenario probabilities;
- generate market-regime conclusions;
- silently scan for newest files or directories;
- mutate Stage 2 historical attestation expectations;
- promote FUTOI Stage 5 data while factual live authority is blocked;
- treat failed/partial refreshes as accepted current state.
