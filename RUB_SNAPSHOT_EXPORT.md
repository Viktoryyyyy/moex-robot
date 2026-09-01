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
SRC=/home/trader/moex_bot/data/state/rub_intelligence/chat_analysis_snapshot/current.json && DIR=/home/trader/moex_bot/exports/rub_snapshots && mkdir -p "$DIR" && chmod 700 "$DIR" && TS="$(jq -er '.identity.generated_at_utc' "$SRC")" && NAME="$(TZ=Europe/Moscow date -d "$TS" '+%Y-%m-%d_%H-%M-%S_MSK')_rub_snapshot.json" && install -m 600 "$SRC" "$DIR/$NAME" && printf 'PROJECT=MOEX_Bot\nfile=%s\n' "$DIR/$NAME"
```

The command is fail-fast and does not refresh data; it copies the current canonical snapshot only.
