# PROJECT=MOEX_Bot

Task ID: `cny_basis_calendar_repair_v1`. Primary action: change.
Owner request: correct eight historical CNY spot basis/carry comparisons whose
previous target incorrectly used Sunday 2026-09-27 from the futures witness.
This is a follow-up to merged #555 / #553, not a replacement of that implementation.
Base: `dfa05c25f4ad749a8ccf4564e61d06ffe0a93cac`.
Branch: `codex/cny-basis-calendar-v1`. Mutation owner: current Codex session.
GitHub is Source of Truth; server is applied state.

## Scope

- `contracts/intelligence/cny_basis_calendar_2026_v1.json`
- `contracts/intelligence/exact_comparison_sources_v2.json`
- `src/moex_data/rub_cny_basis_calendar.py`
- `src/moex_data/rub_exact_comparison_sources.py`
- `src/moex_data/rub_exact_comparisons.py`
- `src/moex_data/rub_historical_basis_carry_context.py`
- `tests/unit/test_cny_basis_calendar.py`
- `tests/unit/test_exact_comparison_sources.py`
- `docs/data/cny_basis_calendar_repair_v1.md`

## Result and selection contract

The existing heavy refresh captures a v2 supplemental envelope. The eight CNY
metrics using CNYRUB_TOM select their own anchor and lags 1/5 from the intersection
of retained futures observations with the explicitly enumerated CETS trading
dates, before testing price quality or archive availability. On 2026-09-28 their
targets are 2026-09-25 and 2026-09-21; futures-only metrics retain 27 and 23 September.
On a futures-only weekend the CNY spot anchor remains the latest eligible date,
and each metric exposes that date instead of representing it as the futures anchor.

The bounded 2026 policy is based on official MOEX publications
[n94172](https://www.moex.com/n94172),
[n96571](https://www.moex.com/n96571) and the
[currency trading calendar](https://www.moex.com/ru/tradingcalendar?market=currency-market&type=trading).
It includes TOM trading on January 5/6/8/9, March 9 and May 11; the futures-only
holidays February 23, May 1, June 12 and November 4 remain excluded for CETS.
This is a reviewed published plan for selection, not proof of actual trading or
session completion. No extrapolation beyond 2026: unknown calendar coverage
refuses the CNY spot comparisons. An exchange schedule amendment requires a new
reviewed policy version. There is no new network calendar collector.

Each selected date still requires exact same-date, same-five-minute endpoint
evidence for both original legs. EMPTY, timeout, HTTP/auth failure, malformed or
rejected evidence on an eligible date never select an earlier successful day.
The separate previous-comparable diagnostic does not substitute for lag 1/5.
Hash-checked v2 envelopes retain the calendar identity, excluded dates, source
bytes and original clocks. Corrupt/expired v2 cannot restore the v1 date policy.
Original v1 contracts, stored envelopes, cache bytes and accepted pointers remain
unchanged and replayable with their original semantics.

The existing exact-source collector/cache is reused; at most three CNY target
dates require their four existing own legs. The v2 bound is 10 distinct selected
dates / 46 date-source combinations (v1 remains 7/37). The 16 MB total byte bound,
per-source bounds and 345600-second dated TTL do not increase. Price/OI 1/5/20,
FUTOI, current basis/carry, USD spot exclusion, Stage5 and trading authority are
unchanged. Repeated identical capture preserves the first acceptance clock.

## Verification and delivery gates

Synthetic/reconstructed tests cover the September 28 plan, futures-only weekends,
currency holidays, unknown calendar coverage, exact trading-date failures,
corrupt outer policy identity, expiry, v1 replay and independent futures metrics.
The real heavy builder, disk JSON, canonical reader, Stage9 daily/weekly and
compact/export paths are tested without replacing selection or admission.

Targeted command:
`PYTHONPATH=.:src pytest -q tests/unit/test_cny_basis_calendar.py tests/unit/test_exact_comparison_sources.py tests/unit/test_historical_basis_carry_context.py tests/unit/test_contract_price_market_oi_observed.py`

Required full CI: `python -m compileall src tests` and `PYTHONPATH=.:src pytest -q`.
Exact-head independent review and CI precede merge. Owner has authorized merge
and separate apply of the exact confirmed merged SHA after these gates in the
ongoing snapshot delivery task. Runtime acceptance must read the new saved JSON
and report actual dates and coverage; USD-dependent gaps remain separate.
Final test, review, merge and applied-state evidence is recorded in the PR.

## Independent review correction

Review of initial head `7f22173d659f20a5a82f8fb8af35e14906ad6dcd` found that the
existing publication-expiry mask removed the selected envelope entirely. With
the new calendar this could wrongly revoke independent legacy CNY metrics for
v1 and relabel v2 supplemental metadata as v1. The mask now preserves the original
versioned envelope and explicitly refuses its admission. Regression for both
versions checks the full prepared/published JSON against subsequent canonical
projection and the independent oracle, including version and admission reference.
The weekend-anchor fixture also uses the actual retained-witness constructor.

Local Python compilation and `git diff --check` passed. The targeted server
regressions use the repository venv, ordinary imports and real PyArrow in an
isolated full checkout. Full test counts, CI run URLs, final exact-head review and
production acceptance are attached to [PR #557](https://github.com/Viktoryyyyy/moex-robot/pull/557)
to avoid claiming a future CI or deployment result in its own untested head.
