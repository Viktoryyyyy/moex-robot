# Dated source admission contract v1

There are two distinct origins. A missing `origin` or explicit
`previously_accepted_live` means A: an original live acceptance witness.
Its existing capture payload, digest and replay rules are unchanged.
`source_observation_acquired_now` means B: source evidence acquired now,
potentially useful for preparation after independent semantic replay.
Unknown origins are rejected; B never falls through to A.

Step 2 established custody validation in `rub_dated_source_admission`.
Step 3 adds explicitly dispatched RFUD/CETS market replay and same-acquisition
basis replay. Other sources and purposes remain `unsupported_source_replay`.

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

The authenticated APIM adapter has an optional fast-producer evidence sink.
It retains only selected native market rows and security rows, plus bounded
Si/CR reference rows needed to replay the concrete binding. A failed CETS
request cannot prevent successful RFUD evidence capture. Reference proof is
covered by the complete frame digest but excluded from the stable observation
revision: unrelated registry changes and retrieval times cannot renew first
acceptance. The selected native metadata remains part of the raw digest.

Market replay validates exact approved endpoint, source/board/SECID, canonical
units, receipt-time binding, native positive prices, integral nonnegative OI,
numeric domain/range constraints and selected security metadata. It uses the
existing normalizer at real acceptance time; stale flags are never flipped.
Unproven APIM futures WAP remains unavailable. Binding means the concrete
contract selected at receipt, never historical front selection or PIT data.
Generation equals acceptance; request starts before both source requests.

Basis requires independently replayed legs from that same acquisition and
the existing same-Moscow-date/60-second pair skew policy. It calls existing
basis/carry arithmetic with a separate `DATED_96H` synchronization input.
Prices are never labelled live or made artificially fresh. USD spot has no
supported source and its dependent metrics remain unavailable. CNY spot
failure does not suppress futures-only basis. First acceptance is retained
per metric's actual source legs (including expiry dependencies for carry).

The canonical fast store contains both origins and bounded informational
`last_source_admission_refusals`; it is not a second database. Read-time
description and reverse projection acceptance dispatch B explicitly. All
public B entries and basis legs have current/model/historical-PIT use false.
Live source failure remains visible independently of dated preparation data.
Original A payloads and default live basis arithmetic are unchanged.

H1, CR history, FUTOI, news and Minfin are outside this change. Tests use
labelled synthetic native tables and do not claim archived production replay.
