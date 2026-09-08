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

CNY/RUB TOM spot projection uses the same admission as its matrix row: the
producer's aggregate spot quality must be true, an explicit per-record refusal
is respected, and its positive finite price and source timestamp must remain
usable at consumption time. An absent optional per-record spot flag is allowed.
Spot facts contain price only; no spot open interest or USD TOM is invented.

`basis_carry` projects each admitted READY metric once, including when the
component is PARTIAL. `values.metrics` contains `{snapshot_path, values}` entries;
each path resolves to the original metric list index, and values retain the
metric's units, formula, legs, timestamps and provenance. Identical duplicates
use their first deterministic path; conflicting copies of one ID are excluded.
Expired or missing legs remove dependent metrics. Zero and negative basis/carry
values remain valid. The acceptance gate checks projection completeness in both
directions against the read-time source, without a fixed expected metric count.
This projection grants no forecast, historical, session-completion or trading
authority and does not change metric formulas.

`macro_evidence_inventory` separates replayed CBR/Rosstat observations from
scheduled publications and unresolved source/series policies. Direct descriptions
also reconcile these macro components before assembling facts and matrix rows.
See [macro facts and scheduled publications](rub_macro_evidence_inventory.md).

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

## Unified acceptance gate

Run from a clean committed checkout. The CLI pins the executing Git HEAD and
performs no training, model evaluation, source refresh or trading operations.
For the installed local API:

```sh
PYTHONPATH=.:src python -m moex_data.rub_factual_release_acceptance \
  --api-url http://127.0.0.1:8765/v1/rub/factual-snapshot \
  --env-file /home/trader/moex_bot/.env \
  --output /home/trader/moex_bot/deploy_backups/factual-release-acceptance
```

The local API token is read in memory, never put in arguments or artifacts.
Requests reject redirects and ignore proxy environment settings. A captured
snapshot can be tested without network access, using its original evidence files:

```sh
PYTHONPATH=.:src python -m moex_data.rub_factual_release_acceptance \
  --snapshot /absolute/path/input_snapshot.json \
  --as-of 2026-09-08T13:27:28+00:00 --output /absolute/path/acceptance-results
```

Use the intended aware consumption time, not the example timestamp. Keep output
outside the checkout. Each invocation creates a new directory containing
`report.json` and two identical frozen exports with snapshot, release and digest
manifest. Replay still requires the original referenced source evidence files.
Exit code 0 means PASS; 1 means a failed acceptance check; CLI input/setup errors
also return nonzero. Failed checks are named in the report.

The profile requires admitted monthly CPI and banking liquidity; an empty
INCOMPLETE response cannot pass. All displayed facts must agree with matrix
admission and resolve to source paths. Checks also cover D1/W1 limitations,
scheduled-versus-actual events, uncertainty, cached projection consistency,
deterministic export/hash replay and input immutability. Labelled in-memory
simulations check receipt expiry, altered values, missing evidence, failed
refreshes and missing inputs. Simulations never edit production evidence.
PASS is acceptance of the factual contract, while release status remains
INCOMPLETE and forecast/training/trading acceptance remains false.

The same gate has an offline CI scenario with frozen source fixtures:

```sh
PYTHONPATH=.:src pytest -q tests/acceptance/test_rub_factual_release_acceptance.py
```
