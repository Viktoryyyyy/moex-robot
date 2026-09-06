# Price-only research profile

On September 6, 2026 the user explicitly directed work to continue without
investigating daily volume discrepancies. The price_only_research profile
excludes volume reconciliation from its dependencies. This is a scope decision,
not a claim that conflicting volume data were corrected. Volume, turnover,
trade count, OI, VWAP and volume-dependent features are not consumed.
Other accepted datasets and the Stage 9 accepted-pointer contract are unchanged.

`python -m moex_features.rub_price_only_research --dataset EXCHANGE_RESEARCH_JSON --as-of AWARE_TIMESTAMP --output NEW_JSON`

Inputs must be explicit SiU6/CRU6 research history. D1 requires complete session
coverage and verified daily prices. W1 requires complete coverage and all five
underlying daily price checks. Both closed-period time and actual availability
must be no later than as_of. Missing or rejected periods reset indicator warmup.
The capture period has no weekday holidays; the contiguous-period rule handles
weekends only and requires a calendar extension before use in another regime.

Versioned formulas, identical for prefix and full-history calculation:

- Return 1/5: close ratio minus one over 1/5 periods of the selected timeframe.
- EMA20: seed is the first 20 closes' mean, then alpha=2/21.
- ATR14: true range includes prior close; first range uses high-low. Seed is
  the first 14 ranges' mean, followed by Wilder smoothing with alpha=1/14.
- Realized volatility20: sample standard deviation of 20 log returns, multiplied
  by sqrt(252) for D1 or sqrt(52) for W1; output is a fraction, not percent.
- Range percentile20: current high-low ranked against the previous 20 ranges;
  smaller ranges plus half the ties, divided by 20, scaled to 0–100.
- Swing high/low: strict extremum with two bars on each side. Pivot period and
  confirmation period are separate; availability is never backdated to the pivot.
- Break of structure: close crosses the latest confirmed swing level from the
  other side. Null means no confirmed crossing, not a directional prediction.

Server run: 34 D1 feature rows and six W1 rows per contract. Latest D1 indicators
have enough observations. W1 EMA20, ATR14, volatility20 and range percentile20
remain null with explicit insufficient-history fields. No window is shortened
to make the result appear ready. Backfilled data retain actual September 6
availability, so they cannot be treated as historically available training data.

Eight tests cover causal append invariance, delayed pivots, formula seeds,
as-of gating, invalid prices, duplicates, gap resets and independence from
arbitrary volume-field changes. Outputs use exclusive creation and bind the
source file SHA-256. Model acceptance, historical PIT readiness and accepted
pointer promotion remain false.

Next work: extend price history and contract-roll provenance; finish weekly
warmup and OI/FUTOI dependencies where separately permitted; then validate the
forecast method out of sample. Daily-volume investigation is not a pending
requirement of this user-selected profile.
