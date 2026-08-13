# USDRUBF RUB Intelligence — S6.2 Dry Alert Acceptance

PROJECT=MOEX_Bot

Date: 2026-08-13

## Scope

This record accepts only the non-delivering Change Detector alert boundary introduced by PR #345. External notification delivery remains disabled and broker/order execution remains outside v1.

Applied canonical-server code SHA: `00739e3c6e1fde4c85f7ea1ebfdde5780b854e51`.

## Canonical-server evidence

### 1. Live-state suppression path

A bounded two-cycle live shadow scheduler run completed successfully on a temporary proof state root:

- `CYCLE_COUNT=2`
- `SUCCESSFUL_CYCLES=2`
- `FAILED_CYCLES=0`
- cycle 2 restored prior persisted state
- `LAST_SIGNIFICANT_CHANGE=False`
- `LAST_ACTION_CANDIDATE=False`

The dry alert consumer was then invoked twice against that committed live scheduler state. Both invocations returned:

- `ALERT_DELIVERY_STATUS=NO_ALERT`
- `TRANSPORT_ID=dry-run`
- `EXTERNAL_DELIVERY=False`
- `DELIVERY_HISTORY_COUNT=0`

This proves the consumer does not manufacture an alert when the persisted Change Detector reports no significant change.

### 2. Positive IMPORTANT gate

On the same canonical server and applied SHA, the bounded fixture test
`test_important_change_is_recorded_by_non_delivering_dry_transport`
passed. It proves that a persisted `IMPORTANT` Change Detector event is converted to one deterministic dry alert record with:

- `DRY_RUN_RECORDED`
- `external_delivery=False`
- one delivery-history record
- deterministic message content derived from persisted Change Detector fields only.

### 3. Restart-safe duplicate suppression

The bounded fixture test
`test_restart_reuses_delivery_state_and_suppresses_duplicate`
passed on the canonical server. It proves that a second invocation against the same persisted alert state does not re-run the dry transport and returns `DUPLICATE_SUPPRESSED` while delivery-history count remains one.

Server result for the two positive/dedupe tests:

`2 passed in 0.08s`

## Acceptance result

S6.2 dry alert boundary: `COMPLETED`.

Accepted properties:

- consumes only committed scheduler / Change Detector state;
- deterministic `IMPORTANT` / `ACTION` severity gate;
- no alert on empty / non-significant live state;
- bounded deterministic message rendering;
- persisted duplicate suppression across restart;
- explicit delivery status;
- `EXTERNAL_DELIVERY=False` for the accepted transport;
- no mutation of MarketState or DecisionInput;
- no Flowise dependency;
- no broker/order execution.

## Remaining live-delivery gate

External notification delivery is not accepted by this record. Before live Telegram (or another external transport) can be accepted, the implementation must additionally prove:

1. credentials are loaded without logging secrets;
2. the external endpoint and response contract are bounded and validated;
3. the delivery key is durably reserved before the external send so a timeout/crash cannot cause an automatic duplicate resend;
4. uncertain transport outcomes fail closed and require explicit recovery rather than automatic resend;
5. canonical-server external transport smoke succeeds;
6. scheduler wiring is accepted separately after the transport boundary is proven.
