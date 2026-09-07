# Session timestamps and historical CR FUTOI verification (updated September 7)

## Findings

Authenticated APIM calendar `/iss/calendars/futures.json?from=2026-09-04&till=2026-09-07` returns September 5 and 6 with `is_traded=1`, reason `W`, and `trade_session_date=2026-09-07`. Weekend calendar dates cannot be treated as closed days or as the exchange trading date. The currency calendar's empty `off_days` result alone does not prove instrument-level opening hours or that CNYRUB_TOM traded on Sunday.

On September 6, SiU6 marketdata reported `TIME=18:59:59`, `TRADEDATE=2026-09-06`, and `SYSTIME=2026-09-06 21:38:32` (Moscow). CNYRUB_TOM reported `TIME=18:59:54`, `SYSTIME=2026-09-04 23:50:03`, and `TRADINGSTATUS=N`; TRADEDATE was absent. These are different clocks, not proof of a new price at SYSTIME.

The APIM collector now requests optional TIME, TRADEDATE and TRADINGSTATUS fields. The normalized record preserves them as `last_trade_time_moscow`, `source_trade_date` and `source_trading_status`, and explicitly labels its existing timestamp as source-row update time. Missing trade dates remain null; combining TIME with SYSTIME's date could invent a last-trade timestamp. Read-time freshness remains downgrade-only and explicitly does not attest last-trade freshness or session-calendar acceptance.

References: https://www.moex.com/ru/tradingcalendar and https://www.moex.com/files/4rkd3yjkghfhqz4h7g8rewetx4 (ISS calendar methods). Instrument-level session classification still needs a verified timetable and exception policy; no closed-market freshness upgrade is implemented here.

## CR acceptance evidence

Both Si and CR source-native manual refresh artifacts from `step10_manual_20260906T155933Z` were rechecked. For each instrument, all four hashes match: frozen raw partition, quality report, refresh manifest and complete archived run. The archived run reproduces current.json excluding its two archive-reference fields. Both 218-row raw datasets reproduce their factual payload exactly through `latest_aligned_factual`.

The September 5 CR candidate also passed the canonical snapshot component loader while retaining GOVERNED_BLOCKED and consumer_factual_use_allowed=false. Historical evidence is recorded in `contracts/intelligence/futoi_cr_historical_smoke_verification_2026-09-06.json`. It does not resolve later intraday failures and grants no current acceptance. No raw position history is published in this evidence document.

On September 7 the server advanced to b03c275355847712e1542491f7ff7087cf40c45c (PRs 470 and 471). The 00:30 scheduled run completed successfully at 00:34:34 Moscow. PR 471 already accepted recurring evidence and Si factual authority, while keeping CR blocked for missing current smoke acceptance and an unresolved intraday FIZ/YUR balance failure. This change preserves that decision and the quote-quality fixes from PR 470. The older successful CR replay must not override the later failure. Next scheduled run: September 8 at 00:30 Moscow.

Training and model evaluation remain paused; volume discrepancy investigation remains excluded. Neither session freshness nor full FUTOI live acceptance is declared complete by this change.
