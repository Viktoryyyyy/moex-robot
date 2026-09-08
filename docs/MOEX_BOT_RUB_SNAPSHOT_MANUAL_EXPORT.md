# MOEX Bot — Manual RUB Factual JSON Export

status: current
project: MOEX_Bot

Run on the canonical server as trader:

```bash
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && PYTHONPATH=.:src python -m moex_data.rub_factual_release --current
```

The command prints one filename in `/home/trader/moex_bot/exports/rub_snapshots`.
Download that JSON and attach it to a new daily or weekly MOEX_Bot analytical chat.
No OpenAI API key is required. The exporter reads only MOEX_DATA_ROOT from the
existing project environment file if that variable is absent from the shell.
Use `--output DIRECTORY` to choose another export directory.

The same compact builder serves authenticated `GET /v1/rub/factual-release`.
Both call the canonical consumer/reader, including the fast market overlay and
read-time source validation. Export does not refresh upstream sources and does
not copy the heavy persisted current.json. One clock is captured before reading;
`as_of_utc`, source dates and slow/fast generation metadata are preserved.
A later export receives a later consumption time, never a backdated one.

The JSON uses `rub_factual_package.v1`. Its status reports mandatory factual
coverage, with each unavailable requirement and its reason. Presentation
integrity and model readiness are separate. Minfin remains a required external
blocker until an accepted latest announcement is available. The file preserves
useful admitted facts even while coverage is PARTIAL; UNKNOWN news impact and
NO_EXPLICIT_USER_INPUT do not become neutral market opinions or implicit FLAT.

D1 covers the current Moscow civil day through consumption time. W1 covers
Monday through consumption time and identifies the next Monday–Sunday preparation
horizon (normally prepared on Sunday). Source bar intervals remain separate.
Calendar coverage describes civil-date plans only; weekend mappings are explicit,
trading-date labels stay within the prospective week, and no schedule proves an
actual or completed session.

Files are created exclusively with owner-only permissions and never overwrite an
existing filename. The filename includes UTC consumption time and a digest prefix.
The exported facts are readable without opening server paths. Full raw evidence,
hashes and manifests remain available through the separate frozen audit workflow:
`python -m moex_data.rub_factual_release --snapshot PATH --output DIRECTORY
--code-revision EXACT_COMMIT --as-of AWARE_TIME`. Audit replay requires original
evidence files; the compact upload does not substitute for that audit bundle.
