# Rosstat weekly CPI publication schedule and document formats

The weekly CPI component now replays the explicit current-year publication
schedule from the same verified and frozen official archive receipt:
https://rosstat.gov.ru/compendium/document/50798 . Only the titled first/second
half-year schedule tables supply calendar rows; archive links and unrelated
announcements do not. The observed period and listed publication date must agree
with one schedule row. The next row bounds freshness: the old release is rejected
after the end of that scheduled publication date in Europe/Moscow. The source
does not supply a release hour, so `scheduled_release_time` remains null.
No weekday inference, invented timestamp, or extrapolation beyond the last
scheduled row is allowed. Publication postponements require updated official
evidence; an inconsistent archive/calendar fails closed.

`weekly_release_calendar_accepted` describes only this bounded dated schedule.
`calendar_accepted`, `full_rosstat_macro_accepted`, historical PIT admission,
forecast alignment and action authority remain false. The schedule is not proof
that a release has actually been published, or proof of trading session closure.

The parser supports explicit same-year adjacent-month periods, `с`/`со`, and
holiday periods up to fourteen inclusive dates. Real documents verified on
2026-09-08 with the pinned host-scoped HTTPS client include:

- https://rosstat.gov.ru/storage/mediabank/118_05-08-2026.html : 28 July–3 August;
  previous-registration 99.98, month-start 100.00, year-start 104.84.
- https://rosstat.gov.ru/storage/mediabank/103_08-07-2026.html : 30 June–6 July;
  100.31, 100.26, 104.49.
- https://rosstat.gov.ru/storage/mediabank/82_03-06-2026.html : 26 May–1 June;
  100.15, 100.03, 103.37.
- https://rosstat.gov.ru/storage/mediabank/1_14-01-2026.html : 1–12 January;
  the summary/table provide a month-start index of 101.26 only. The separate
  previous-registration and year-start fields, and weekly change, remain null.

All indices use percent, base 100. Cross-month month-start values belong to the
ending month, verified against the table heading. The January branch requires
the explicit one-index summary and corresponding five-cell table row; it does
not invent absent bases even when their numerical equality seems plausible.
Cross-year wording and unknown table formats remain unsupported. Minimal
fixtures preserve the source summary/table values and identify their URLs.
