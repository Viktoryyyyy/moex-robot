# MOEX Bot — RUB Snapshot Export

PROJECT=MOEX_Bot
status: current quick-reference

Use this file when a chat, PML1, PML2, or operator needs the canonical command to export the latest RUB factual snapshot for manual upload to ChatGPT.

Canonical detailed runbook:

`docs/MOEX_BOT_RUB_SNAPSHOT_MANUAL_EXPORT.md`

Canonical source snapshot:

`/home/trader/moex_bot/data/state/rub_intelligence/chat_analysis_snapshot/current.json`

Canonical export directory:

`/home/trader/moex_bot/exports/rub_snapshots`

Canonical one-line export command:

```bash
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && PYTHONPATH=.:src python -m moex_data.rub_factual_release --current
```

The command reads the canonical snapshot and fast overlay, verifies evidence and
freshness, and writes the current factual package without refreshing sources.
It prints the exclusively created export filename. Large JSON uses the readable
`rub_snapshot_references.v1` representation; follow its in-document references
or expand it with `python -m moex_data.rub_snapshot_serialization --expand FILE`.
Do not use `jq .identity` on raw `current.json`: its lossless storage envelope is
decoded by the canonical reader. See the detailed runbook for full raw fallback.
