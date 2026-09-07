# FUTOI latest-publication acceptance policy

The source-native factual selector first selects the maximum source event timestamp
in the exact-date partition. That timestamp must contain exactly FIZ and YUR.
An incomplete or unexpected group at the frontier rejects the attempt; the selector
must never search backwards for a usable pair. This applies to both Si and CR.

Within that timestamp each group must have one session and one unambiguous maximum
seqnum revision. The selected groups must share the session. Existing integer,
position identity, zero net balance, total long/short balance, source identity and
publication/availability checks remain mandatory. A bad newest revision cannot be
replaced by an older revision. Seqnum is resolved per group by the existing source
contract; this change introduces no unsupported cross-group version inference.

A valid latest pair describes only that pair. It does not accept earlier intraday
publications, an entire completed session, historical PIT coverage or a predictive
signal. Earlier rejected pairs remain rejected when replayed independently.
Canonical raw partitions, quality reports and manifests remain content-addressed
evidence; retained records following an error remain stale and do not gain current
authority. No balance tolerance or freshness threshold is increased.

## Verification on 2026-09-07

Frozen CR partition e3e0768ef39177bf9e3f8511049472b15ccce668b10a158c0804633ae7254ada
replays the 18:25 UTC pair with zero balance. Appending a newer one-group
publication to a copy rejects the attempt without fallback. Frozen rejected
partition 5dc08b3cf6523d8e56f062c3c1cda4130759ee5a66e941c245ec138f47e1a0ab
still fails with `FIZ/YUR net positions do not balance to zero`.
Both input hashes were verified. Server evidence is
`/home/trader/moex_bot/deploy_backups/futoi_frontier_replay_20260907.json`.

This replay closes the incomplete-frontier selector defect. It is not a new
canonical live smoke or a resolution of the provider's historical imbalance.
CR governance remains blocked pending separate scope-specific acceptance and
rejection evidence policy. Si authority is unchanged. Calendar state remains
UNKNOWN outside proven coverage; training and model evaluation remain paused.
