# PROJECT=MOEX_Bot

Task ID: `usd_zero_trades_v1`. Primary action: change. Owner request: correct
USD classification for zero trades, without repeated historical acquisition.
Base: `933db19c9158ecbec4b2fabb0ef0c0ee4f904fa4`.
Branch: `codex/usd-zero-trades-v1`. Mutation owner: this Codex session.

The September 30 runtime response was HTTP 200 with NUMTRADES=0, LAST=null,
TRADINGSTATUS=T and native SYSTIME. The old positive-count precondition discarded
the valid source row and caused SOURCE_UNAVAILABLE. A nonnegative integer count
now passes structural validation; zero retains the native identity, status,
counter, row/table dates, response hash and source/receipt clocks. It never
constructs a last-trade timestamp from TIME or admits current price/quote/basis.
Missing, negative, boolean, fractional and string counts remain source errors.

Presentation reports NO_TRADES_OBSERVED for zero USD executions, including when
LAST is absent. Explicit native N still reports NOT_TRADING_OBSERVED, with a
separate NO_TRADES_OBSERVED activity and reported counter. Native T remains T;
it is not inferred to prove active trading or session completion. A retained
LAST, if supplied, is only the last received observation, never a current fact.

Scope is this document, `src/moex_data/rub_usd_cets_reference.py`,
`src/moex_data/rub_currency_market_state.py`,
`src/moex_data/rub_snapshot_read_freshness.py`,
`src/moex_data/rub_analysis_bundle_v2.py`,
`src/moex_data/rub_market_factual_delivery.py` and
`tests/unit/test_usd_cets_reference.py`. The existing admission contract and TTL
remain unchanged: zero trades still denies current use. Historical contracts,
bytes, accepted pointers, collectors, FUTOI policy and HTTP/MCP are unchanged.
No historical reacquisition or backfill is part of validation or server apply.

Synthetic/reconstructed regressions exercise native T/A/N, absent/retained LAST,
present/absent native date, malformed counts, the real fast collector and saved
JSON reader, plus heavy publication and final canonical consumer. They check
independent Si/CR prices and basis metrics survive the USD current refusal.
Full CI and independent read-only review of the exact published head are
required before the separately authorized merge and exact merged-SHA apply.
Actual test, merge and runtime results are recorded in the PR after each gate.

Pre-publication validation: `PYTHONPATH=.:src python -m pytest -q
tests/unit/test_usd_cets_reference.py tests/unit/test_currency_market_state.py
tests/test_stage9_analysis_bundle_v2.py tests/test_market_factual_delivery.py`
passed: **89 tests in 140.39 seconds**, using the existing Python venv and real
Parquet engine in an isolated complete GitHub checkout. No production history
was requested by this run. Full exact-head CI remains a pre-merge requirement.

Review correction (GitHub comment 4141522619): a stored counter changed from a
positive value to zero could pass through presentation despite failing original
evidence admission. The canonical reader and final delivery now explicitly
validate the full original envelope and normalized row before classification;
copied status/reason/validation fields cannot authorize an observation. An
integrity failure shows SOURCE_UNAVAILABLE with no asserted trade count or last
observation. The canonical reader, Stage9 bundle and final delivery are the
three exact presentation callers added to scope for explicit validation.
New regressions persist hash-consistent altered rows and damaged zero-trade
evidence, then enter the actual saved fast/heavy readers and final consumer.
The correcting head requires repeat independent review and full CI.
Final correction validation: the same four-file pytest command passed **93
tests in 148.21 seconds**, including explicit daily/weekly agreement and rejection
of copied counter/reason/validation markers in the saved canonical snapshot.
