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

Direct `describe()` calls also replay the FRED evidence at the snapshot's
read-time reference (or generation time when no read-time reference is present).
An expired, modified or malformed FRED component cannot appear as an admitted
fact when its matrix row rejects it. Use `build(..., now=...)` to consume an old
snapshot at a new time. DEXCHUS remains a dated `CNY_per_USD` reference; it is
not a synchronous `RUB_per_CNY` basis leg or an accepted D1/W1 forecast input.

Live market and basis timestamps retain their original subsecond precision.
The 60-second synchronization limit is checked before any display rounding;
60.000001 seconds is outside the limit. These gates do not prove completion
of an exchange trading day.

For a frozen release use `python -m moex_data.rub_factual_release --snapshot PATH
--output DIRECTORY --code-revision EXACT_COMMIT --as-of AWARE_ISO_TIME` with
`PYTHONPATH=.:src`. The exporter writes the input snapshot, release and manifest
inside a SHA-256 directory. Replay requires the original evidence files referenced
by the input, the same commit and consumption time. Missing or changed source
evidence fails the corresponding factual gates. No current network lookup occurs.
Existing artifacts are never overwritten. A simulated missing-factor example
must be labelled as a simulation, not an observed production outage.
