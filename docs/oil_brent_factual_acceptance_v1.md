# Brent factual acceptance — published trading results only

PROJECT=MOEX_Bot
TASK_ID=oil_brent_factual_acceptance_v1

## Scope and activation

This policy permits only the current selected MOEX BR contract's **latest published official trading-results CLOSE** in the existing `components.oil` of `rub_chat_analysis_snapshot.v1`.

The source policy becomes effective through the approved, reviewed merge. A branch, successful source probe, or passing test alone is not production acceptance. Server apply and verification of the canonical persisted snapshot remain separate. No server apply is performed by this repository change.

This is narrower than a live quote and narrower than proof of the most recently completed exchange session. `latest_completed_session_proven=false` is intentional: native history bounds prove the latest published result available through this source, not the absence of a newer completed but unpublished session. Consumers must display the price date and must not relabel this record as intraday.

Source identity remains `moex_brent_futures_daily`; the existing historical pilot and historical model eligibility are not promoted.

## Source and deterministic contract selection

Three requests, no historical backfill:

1. `/iss/engines/futures/markets/forts/securities.json?assetcode=BR&iss.only=securities` supplies current RFUD/BR candidates and native last-trade dates.
2. `/iss/securities/{selected_SECID}.json?iss.only=description,boards` verifies identity, quote unit, contract size, lifecycle and native RFUD `history_till`.
3. `/iss/history/engines/futures/markets/forts/boards/RFUD/securities/{selected_SECID}.json` requests exactly that `history_till` date with `from=till`, `start=0`, `limit=100`, and `history,history.cursor`.

All requests use HTTPS on `iss.moex.com`. Redirects, oversized bodies and invalid source structures fail closed. There is no authenticated feed fallback.

Selection reuses `moex_brent_history.select_nearest_contract`: the unique nearest expiry at least seven calendar days after the Moscow evaluation date. Candidate expiry is native `LASTTRADEDATE`, verified against selected contract `LSTTRADE`; delivery date is separately recorded and cross-checked. No ticker-derived expiry, fixed BRV6 binding, liquidity rule, calendar API, weekday inference or continuous-roll construction is introduced.

Missing expiry, duplicate identities, a tied nearest expiry, inconsistent metadata, missing unit, missing/duplicate history rows, incomplete cursor, invalid prices and a selection-date transition during collection are failures. A native history bound on the current evaluation date is deliberately not accepted by this prior-date policy.

## Price, units and availability

`price` is **CLOSE**, never `LAST`, `PREVPRICE` or `SETTLEPRICE`. OHLC must be positive, finite, numeric and internally consistent. The source's explicit `UNIT` must be `в долларах США за 1 баррель`. Output is `quote_currency=USD`, `price_unit=USD/barrel`; native LOTSIZE must agree with LOTVOLUME and is exposed as `contract_size_barrels`.

`source_trade_date` and `source_history_till` retain the date of the published result. `source_event_time` and `source_published_at` remain null: the responses do not establish the timestamp of the closing trade or first publication.

`received_at` is recorded after the history response is received. Component and data `data_as_of` equal that receipt, with explicit semantics `receipt_of_current_revision_not_price_event_time`. This proves availability to this collector at receipt only. It is not a reconstructed historical availability timestamp, an assertion of price freshness, or historical PIT/training eligibility.

Provenance records each of the three request URLs, request/receipt timestamps and SHA-256 of the received raw body. The normal collector does not claim to retain a separate immutable raw-body archive.

## Freshness, failures and consumer authority

A successful source collection can expose `READY` and factual consumer authority only under the above dated-result semantics. Rechecking an unchanged published result does not create a new trade or change its source date.

The receipt recheck expires after 1,200 seconds, matching the existing snapshot lifetime, or when the Moscow selection date changes. This timeout measures cache/recheck age, **not** the age of the price. Intraday freshness is always false; current market session status remains UNKNOWN.

Persisted acceptance also requires the exact source/contract identity, positive integer contract size matching retained native identity evidence, and exactly three ordered provenance records with matching official routes, selected SECID, published date, valid SHA-256 fields and receipt chronology. These are structural/internal-consistency gates, not a claim of independent raw-body replay or cryptographic authenticity. Missing or corrupt metadata/provenance withdraw factual authority at the reader and matrix boundaries.

The collector samples `collection_completed_at` after response parsing and validation; it rejects a selection-date transition or budget violation at that point, not merely at response receipt. This completion stamp does not replace the original history receipt.

Read-time evaluation and final publication evaluation only downgrade. A new snapshot generation time or `last_success_at` does not renew the underlying receipt. The shared reader performs no source fetch and does not overwrite the persisted snapshot.

A refresh failure without a previously accepted record is `UNAVAILABLE`. An old governance placeholder cannot become a retained price. A previously accepted record may be displayed as `RETAINED_PREVIOUS` with its original value and receipt, but factual consumer authority is false until a new valid collection. Repeated failure does not renew that record. Explicit producer injection without oil preserves the prior governance-blocked test/offline mode; production defaults include the Brent producer.

Directional, action, standalone buy/sell, historical PIT, Stage5 full mode and pointer-promotion permissions remain false. No forecast, scenario, trading automation, broker operation, WTI, Urals or EIA source is added.

## Production matrix

The existing Brent row derives collection presence from the canonical oil record and use from explicit acceptance plus receipt/selection/price gates. READY alone is insufficient. Null UNAVAILABLE data, retained data and stale receipts do not pass. A null/malformed optional read-freshness block or invalid supplied read timestamp makes Brent unavailable without aborting the matrix or changing unrelated rows. An absent optional block still uses the snapshot-generation reference.

The legacy row field `usable_for_full_forecast` denotes admission of this **dated factual input**, not full product readiness. The row additionally exposes `factual_context_usable`, `price_context_scope=latest_published_history_only`, and `intraday_fresh=false`. Root `data_acceptance_complete`, `analysis_ready`, `model_validated` and `training_authorized` remain false. Other source acceptance decisions are unchanged.

The matrix is snapshot-bound: it evaluates the supplied read reference, or the snapshot generation reference when no read reference is present. A historical matrix is not proof of current freshness. Consumers of manually exported raw snapshots must check the source receipt age at consumption time.

## Operator evidence, 7 September 2026

Preflight main and reported server SHA: `b03c275355847712e1542491f7ff7087cf40c45c`.

The operator supplied two read-only console probes. Their normalized results, rather than a separately frozen raw-body archive, are the evidence available to this task:

| Probe | Evidence |
| --- | --- |
| Identity/candles, received around 14:41:24 UTC | Selected BRV6, RFUD, expiry and delivery 2026-10-01, explicit USD/barrel unit, 10-barrel size. Five prior-date OHLC rows later matched history exactly. Current-day candle was partial; the LAST trade was about 903 seconds old and was not accepted as intraday fresh. |
| History semantics, received 14:58:58.110714 UTC | Native history bounds ended 2026-09-04. Six unique rows, 2026-08-28 through 2026-09-04, complete cursor and valid OHLC. Latest row: OPEN 95.97, HIGH 96.59, LOW 93.26, CLOSE 95.88; SETTLEPRICE 95.70 was distinct. Native CLOSE definition was the last transaction price in trading results. |

Input console-file SHA-256:
- identity probe: `48e078fd5eb5964894b5824448a8985ffca53d53b10613dec25575c6030c5c23`;
- history probe: `70f00314a804db7ccde44c0146174e0ea41349145be3edec532292ff45c7d775`.

Reported source-body SHA-256:
- selected contract description/boards: `6bb8f2af3ef38f79a3449659147666b6f034ee48d219477511cfa159cffbc0e3`;
- history field definitions: `08a21635c2e47088c2efc187dfca66f7d1102ac19d9966df89a8321eac117b9a`;
- six-row history response: `c5081dde2a3ddb0f6676bd3ec0509a04bcf17d3170456a7e717906f2ed2f515e`.

These are operator-reported source hashes, not a claim that their original raw response bytes were independently replayed here. The two probe durations were 0.954 and 0.771 seconds; these do not measure the new production collector or whole snapshot refresh.

## Validation and remaining operational gates

`tests/unit/test_moex_brent_factual.py` contains offline source, selection, failure, clock, metadata, price, canonical snapshot, retained-state, reader and matrix regressions. Existing source selector and unrelated snapshot regressions remain applicable. CI and independent review must be tied to the exact final PR head; results are recorded in the PR, not inferred from this document.

There are at most three source attempts and no retries. Each request uses a two-second socket timeout and a one-megabyte response cap. A six-second collection budget is checked around requests; it is not a claim of a hard operating-system deadline for DNS/socket operations. No timer, service, persistent cache, accepted pointer or second snapshot is added.

After a separately authorized merge/apply, verify the exact server SHA, refresh the canonical snapshot, inspect oil source/unit/selected contract/date/receipt/authority, run the matrix on the read-freshness view, and measure total refresh duration plus Brent's collection duration. Production runtime performance and applied oil status remain unverified until that evidence is returned.
