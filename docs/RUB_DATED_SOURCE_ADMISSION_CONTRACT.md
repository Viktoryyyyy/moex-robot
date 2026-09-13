# Dated source admission contract v1

There are two distinct origins. A missing `origin` or explicit
`previously_accepted_live` means A: an original live acceptance witness.
Its existing capture payload, digest and replay rules are unchanged.
`source_observation_acquired_now` means B: source evidence acquired now,
potentially useful for preparation after independent semantic replay.
Unknown origins are rejected; B never falls through to A.

Step 2 defines only B custody validation in `rub_dated_source_admission`.
**No B purpose is admitted in this step.** Even a valid envelope raises
`unsupported_source_replay` in the dated selector. No producer is wired.

The v1 envelope requires schema `rub_dated_source_evidence.v1`, origin B,
scope `preparation_only`, revision semantics `observed_now_not_historical_pit`,
and explicit false `current_usable`, `historical_pit_usable`, `model_usable`.
`source_id` and `purpose` are nonempty strings. `identity` and `units` are
nonempty string maps. These checks establish shape, not source truth.
`raw_source_payload` is nonempty JSON object/array with a canonical JSON SHA256
`raw_source_digest` (sorted keys, compact separators, no NaN).

All timestamps must be timezone-aware. The ordering is request start <=
receipt <= candidate acceptance <= validation now, and source observation <=
receipt. Source observation age at validation is within [0, 96 hours], which
also bounds its age at candidate acceptance. No timestamp implies historical
availability. Candidate `accepted_at_utc` is not proof of semantic admission.

`revision_id` hashes source ID, purpose, identity, units, scope, revision
semantics, raw digest and source observation normalized to UTC ISO format.
It excludes request/receipt/acceptance timestamps so refetching identical
evidence cannot define a new revision or renew first acceptance. The later
producer must preserve the first acceptance per revision; this helper does
not persist or renew anything. Hashes detect accidental corruption, not
authenticity; a rehashed false claim still requires source semantic replay.

Step 3 must add explicit source adapters for RFUD/CETS and closed H1 before
any B selection: replay raw values, timestamps, source/board/SECID identity,
units, binding and domain invariants. Normalized stale flags must never be
flipped into live admission. Live TTL, current projection, A capture, CR,
FUTOI, news and Minfin are outside this contract change. Tests use labelled
synthetic custody fixtures and do not claim production replay.
