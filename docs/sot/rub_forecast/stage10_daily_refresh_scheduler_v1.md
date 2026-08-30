# MOEX Bot — Stage 10 RUB Daily Refresh Scheduler v1

## Status

Stage 10 defines the rolling production refresh route for the server-core evidence consumed by Stage 9.

The historical Stage 2 content attestation, Stage 5 pilot and Stage 7 pilot remain immutable historical proof. Stage 10 does not rewrite their fixed historical expectations or claim that those historical pilots are themselves schedulers.

## Execution boundary

The runner requires both:

- an explicit `--through-date`;
- an explicit immutable `--run-id`.

`through_date` must be a completed Moscow calendar date strictly before the runner's current Moscow date. The systemd route resolves the previous Moscow calendar date explicitly at the service boundary.

No implicit latest dataset/path selection is allowed.

## Deterministic refresh order

1. Read the MOEX futures calendar for an explicit bounded date range.
2. Validate current accepted Stage 5 and Stage 7 histories and require their max trade dates to be aligned.
3. Catch up all missing MOEX futures trading dates through the requested completed date.
4. For Stage 5, acquire exact-date Si/CR FUTOI raw partitions, freeze exact bytes into the run scope, derive EOD positioning and recompute D1 positioning features.
5. For Stage 7, acquire exact-date USDRUBF/CNYRUBF 5m partitions, freeze exact bytes into the run scope, extend D1, derive only completed W1 periods, and recompute contracted technical features.
6. Refresh/promote Stage 3 current raw evidence for the latest completed trading date when needed.
7. Refresh/promote Stage 4 basis/carry for the same date when needed.
8. Transactionally promote the complete Stage 5/7 derived pointer set when catch-up occurred.
9. Run Stage 9 daily and weekly post-refresh smoke validation.

## Fail-closed semantics

Before any current pointer mutation, Stage 10 snapshots every current accepted pointer consumed by Stage 9.

If the run fails after pointer mutation, Stage 10 restores that complete pointer snapshot. Run-scoped artifacts from the failed attempt remain immutable evidence and are not treated as current accepted state.

Stage 5/7 pointer promotion is itself transactional. Stage 3 and Stage 4 continue to use their canonical promotion functions.

A Stage 10 run is successful only when Stage 9 can read the resulting current accepted state for both scopes with the contracted block counts:

- daily: 20 server-core blocks;
- weekly: 24 server-core blocks.

The Stage 9 policy-gap statuses are not converted to ready by Stage 10. Missing external context, position/risk input, continuous Si/CR, weekly OI and advanced technical policies remain explicit downstream gaps.

## Rolling versus historical semantics

Stage 10 builds full-history Stage 5/7 snapshots by extending the current accepted historical baseline. It does not mutate the historical baseline run artifacts.

New raw inputs are exact-date and run-frozen before derived computation. Catch-up availability does not create a claim of historical point-in-time research readiness. `historical_pit_research_ready_claimed=false` remains explicit.

## Scheduling

Dedicated units:

- `ops/systemd/moex-rub-stage10-daily-refresh.service`
- `ops/systemd/moex-rub-stage10-daily-refresh.timer`

The timer targets 00:30 Europe/Moscow each day and passes the previous Moscow calendar date to the runner. This is intentionally separate from the pre-existing generic all-universe futures refresh timer.

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
- treat failed/partial refreshes as accepted current state.
