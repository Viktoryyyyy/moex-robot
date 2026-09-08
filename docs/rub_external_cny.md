# Dated external CNY reference

The heavy snapshot collects FRED DEXCHUS into `components.external_cny`.
It is a New York noon buying reference in CNY per USD, distributed in weekly
H.10 releases with daily observation dates. It is not an intraday CNH quote.
Sources: https://fred.stlouisfed.org/series/DEXCHUS and
https://www.federalreserve.gov/releases/h10/about.htm.

Each successful response and receipt manifest is frozen by SHA-256 beneath
`raw/external/fred_dexchus` in the configured data root. The exact CSV series,
ordered unique dates and positive finite rates are checked. Official missing
markers are skipped without forward filling. A received vintage is replayed at
consumption time; changed facts, missing evidence, failed refreshes or retained
records cannot retain factual admission. API reads do not fetch data.

Scope: latest published dated reference. Receipt must be no older than 1200
seconds; the observation must be no more than 14 New York calendar days old.
This is an explicit maximum staleness policy allowing weekly distribution and
holiday delays, not a guarantee of a timely release. The original observation
date remains visible. The system availability bound is the actual receipt;
exact source publication and source event timestamps remain unknown. Subsequent
receipts create separate manifests, including revised values. No backdating,
historical PIT, CNH, synchronized basis or directional authority is granted.
Full D1/W1 forecast alignment remains pending in the source matrix.
