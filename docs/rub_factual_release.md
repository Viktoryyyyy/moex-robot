# Reproducible factual D1/W1 release

The read-only API exposes `factual_release`, a dated factual inventory with
explicit D1/W1 limitations. It does not reinterpret Stage 9 READY as a validated
forecast. Both horizons remain INCOMPLETE while source, calendar, historical
and model acceptance are unresolved. No target trading date is invented and
no forecast probabilities, recommendations or execution authority are generated.

Current facts are taken from the read-time downgraded snapshot; expired pairs
are excluded. Every displayed fact references its location in the input
snapshot. Slow published prices retain their observation dates and scopes.
The matrix lists missing mandatory factors rather than replacing them with zero.

For a frozen release use `python -m moex_data.rub_factual_release --snapshot PATH
--output DIRECTORY --code-revision EXACT_COMMIT --as-of AWARE_ISO_TIME` with
`PYTHONPATH=.:src`. The exporter writes the input snapshot, release and manifest
inside a SHA-256 directory. Replay requires the original evidence files referenced
by the input, the same commit and consumption time. Missing or changed source
evidence fails the corresponding factual gates. No current network lookup occurs.
Existing artifacts are never overwritten. A simulated missing-factor example
must be labelled as a simulation, not an observed production outage.
