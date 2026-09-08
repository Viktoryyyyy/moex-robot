# Futures calendar acquisition and replay clocks

The default producer uses an advancing live UTC clock for actual request and
receipt times. Snapshot refresh shares that callable with calendar acquisition
and stamps completion after collection. It never freezes receipt time to the
snapshot's starting timestamp. Live clock readings must match wall time within
five seconds; a historical injected clock is rejected before calendar HTTP.

`CalendarClockContext(mode='TEST', now_fn=clock, fetch=test_transport)` explicitly
allows simulated acquisition. The clock must supply successive start, request,
receipt and completion readings when used with `refresh_snapshot`. A separate
`now_fn` must be the same callable. TEST mode requires an explicit transport;
it cannot accidentally fall back to authenticated live HTTP.

`CalendarClockContext(mode='REPLAY', now_fn=clock, archived_component=component)`
reconciles archived raw/manifest evidence at the supplied consumption time and
never calls the loader or HTTP. It preserves original request/receipt times and
hashes. Consumption before receipt, expiry, or modified evidence fails closed.
Use `calendar_context=context` with `build_snapshot` or `refresh_snapshot` to
bind the default futures-calendar producer. Explicit unrelated injected producers
retain their existing behavior.

Receipt schema, source calendar interpretation, D1/W1 applicability, factual
and historical permissions are unchanged. Replay does not grant historical
features, model acceptance, actual session completion or trading authority.
