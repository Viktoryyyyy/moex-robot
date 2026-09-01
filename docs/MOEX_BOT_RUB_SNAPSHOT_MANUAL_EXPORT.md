# MOEX Bot — Manual RUB Snapshot Export

status: current
project: MOEX_Bot
purpose: quick manual export of the canonical RUB factual snapshot for upload to ChatGPT

## Canonical source

```text
/home/trader/moex_bot/data/state/rub_intelligence/chat_analysis_snapshot/current.json
```

## Export directory

```text
/home/trader/moex_bot/exports/rub_snapshots
```

The export directory is Applied State storage for manual transfers only. It is not Source of Truth.

## Canonical one-line export command

Run on the RF server as `trader`:

```bash
SRC=/home/trader/moex_bot/data/state/rub_intelligence/chat_analysis_snapshot/current.json; DIR=/home/trader/moex_bot/exports/rub_snapshots; mkdir -p "$DIR"; chmod 700 "$DIR"; TS="$(jq -r '.identity.generated_at_utc' "$SRC")"; NAME="$(TZ=Europe/Moscow date -d "$TS" '+%Y-%m-%d_%H-%M-%S_MSK')_rub_snapshot.json"; install -m 600 "$SRC" "$DIR/$NAME"; printf 'PROJECT=MOEX_Bot\nfile=%s\n' "$DIR/$NAME"
```

## Result

The exported filename starts with the snapshot generation date and time in Moscow time, for example:

```text
2026-09-01_11-58-47_MSK_rub_snapshot.json
```

Download that file from the RF server and attach it to the MOEX Bot ChatGPT conversation for analysis.

## Rules

- Do not modify `current.json` during export.
- Do not overwrite Source of Truth.
- Do not place manual export files in `/home/trader`.
- Manual export does not refresh source data; it only copies the current canonical snapshot.
- Snapshot readiness such as `PARTIAL` must be preserved and interpreted as-is.
