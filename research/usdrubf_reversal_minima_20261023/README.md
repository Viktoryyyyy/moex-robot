# USDRUBF L3/L4/L5 minimum reversal study

## Scope

This study mirrors the earlier H3/H4/H5 maximum analysis on completed
five-minute USDRUBF sessions. It uses the frozen daily panel through
2026-10-06, whose nested retrospective labels include `USDRUBF_L_order`, and
exact-date MOEX ALGOPACK TradeStats plus optional FUTOI.

The sample contains 14 L3+ events, including five L4+ and two L5+ events. Each
event is paired with two distinct L1/L2 controls matched on information known
before the event session: prior five-session return divided by ATR20, relative
volume, ATR percentage and weekday/weekend type. The resulting control sample
contains 28 sessions. D-1 and D+1 mean previous and next observed sessions.

Retrospective L labels and alignment to the final session low use future or
end-of-session information. They are research labels, never live inputs.

## Main findings

The useful distinction is a completed-session combination, not a standalone
FUTOI pattern:

- L3+ median relative volume was 1.81x versus 1.21x for controls; volume versus
  the previous session was 1.29x versus 1.09x.
- L3+ median close location was 65.8% of the daily range and low-to-close
  distance was 0.68 ATR20, versus 59.8% and 0.50 ATR20 for controls.
- A six-bar resistance break with positive bar delta occurred within 60 minutes
  of the final low in 12/14 L3+ events and 17/28 controls. It is common and must
  be combined with rejection and volume.
- Daily signed flow remained negative at L3+ minima: median -2.64% versus
  -1.98% for controls. L4+ median daily OI change was -3.76% and L5+ was -7.56%.
  This supports a capitulation/liquidation interpretation rather than requiring
  positive flow or rising OI.
- FIZ long change was available for 11/14 events and 22/28 controls. Its median
  was -0.30 percentage points for events and +0.58 for controls. No universal
  bullish FUTOI confirmation was found, so FUTOI is context only for the
  bullish rule.
- Next-session median return was +0.28% for L3+ events and +0.45% for controls.
  The completed-session alert therefore indicates a candidate exhaustion low;
  it does not establish a profitable next-session long entry.

## Frozen operational diagnostic

All three mandatory gates plus score >=10 identify 4/14 L3+ events and 1/28
controls. They identify 2/5 L4+ and both L5+ events. On the post-2023-06 subset
the counts are 2/11 events and 1/22 controls. These are in-sample descriptive
counts, not calibrated probabilities or out-of-sample performance.

Thresholds below 10 materially increase false alerts. Thresholds 10 and 11
produce the same 4/14 versus 1/28 split; 10 is retained to match the bearish
monitor and avoid creating direction-dependent alert semantics.

## Reproduction

Run `analyze_minima.py` inside the project environment with the frozen daily
panel, project repository, environment file, a raw-cache destination and an
output directory. The script selects the sample, fetches exact-date read-only
ALGOPACK data when the cache is absent, and writes `protocol.json`,
`events.json`, `summary.json`, `rule_diagnostics.json` and `quality.json`.

Frozen input and output hashes from the accepted run:

- daily panel: `afcf779ae6cf528e985662400c250a1ccd560c62d019f2f7e3679d2d25fa9a38`;
- raw TradeStats/FUTOI cache: `91a4a9869542540c3db6a3c29a91f843bf079220470e08a78490f8fd948e5b73`;
- protocol: `02e2221a05bd5a1954f4d7a5f50ffafa98b6c896bcd646501f7ff445b9261ead`;
- summary: `de4bc71da8eb2ce38f7064a1e163fc9ecb4cda75f9ddd004546268b0805d879b`;
- rule diagnostics: `090498442a1d27febd5df8f53ceb21abef72591412b0ca29862d1b420660c707`.

The 47.6 MB raw cache is retained on the research host and is not committed.
The script, methodology, aggregate counts and checksums are versioned here.
