# Price-only stored-history audit

`moex_data.rub_price_history_audit` independently inspects saved
`rub_exchange_history_research.v1` SiU6/CRU6 files. It does not import the
research builders or calculate features, predictions, fit statistics or
model evaluations. Only explicit OHLC, identity, dates and causal metadata
are projected. Unrelated source fields are neither inspected nor copied.

The audit checks exact-decimal positive finite OHLC; unique, ordered,
five-minute-grid bar timestamps; source identity; timezone-aware source,
acquisition and build times; acquisition no earlier than the bar; and child
evidence available no later than its aggregate. Daily aggregation follows
the file's explicit `source_calendar_dates`. Weekly aggregation checks the
observed daily rows within its declared seven-day bucket. It does not infer
expected sessions from weekdays. Duplicate periods, overlapping daily date
assignments, unassigned observed dates, contradictory completeness flags and
incorrect aggregate OHLC produce a failed validation report.

Gaps are reported as elapsed intervals between stored bars, with unknown
missing-bar counts and session state. Overnight breaks, empty source
intervals and exchange closures cannot be distinguished from this file
alone. The original calendar witness and declared lineage input hashes are
not replayed. Even a PASS therefore has `coverage_acceptance=UNVERIFIED`,
`calendar_acceptance_granted=false`, `historical_pit_ready=false`,
`model_acceptance_granted=false` and `accepted_pointer_promotion=false`.

For the verified Si merged artifact, from the repository directory:

```sh
PYTHONPATH=.:src /home/trader/moex_bot/venv/bin/python -m moex_data.rub_price_history_audit \
  --dataset /home/trader/moex_bot/deploy_backups/history_si_exchange_research_merged_20260906.json \
  --expected-sha256 9b3018f1923ff6ebcce86e7151813f12e600e97593de22aafa480045109a071b \
  --as-of 2026-09-06T20:41:52+03:00 \
  --output /home/trader/moex_bot/deploy_backups/history_si_price_audit_20260908.json
```

For CR use `history_cr_exchange_research_merged_20260906.json` and SHA
`47ddd715ff368cbf426abc6b61519bfc3bb89f5edbaa1347503fb44d6b980c5f`.
Its archived as-of is also `2026-09-06T20:41:52+03:00`; use a different output
path. The mandatory SHA gate binds the exact input bytes. Reports include
the input path/hash and validator hash and are created exclusively; existing
reports and datasets are never overwritten. A failed data audit writes its
diagnostics and exits nonzero. Input hash failures write no report.

This closes only structural and price-consistency checks. Further coverage
acceptance requires independently verified, effective-dated session evidence,
capture edges, source revisions and contract/roll mapping.
