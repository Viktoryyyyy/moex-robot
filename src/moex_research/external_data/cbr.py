from __future__ import annotations

from datetime import date, datetime
from html.parser import HTMLParser
from urllib.parse import urlencode

from .models import (
    ExternalDataError,
    HttpTransport,
    fetch_bytes,
    parse_date,
    parse_integer,
    parse_number,
    provenance,
)


RUONIA_ROUTE = "https://www.cbr.ru/eng/hd_base/ruonia/dynamics/"
KEY_RATE_ROUTE = "https://www.cbr.ru/eng/hd_base/ProcStav/IR_CHG_MPO/"
LIQUIDITY_ROUTE = "https://www.cbr.ru/eng/hd_base/bliquidity/"

RUONIA_HEADERS = (
    "Date of rate",
    "RUONIA, % p.a.",
    "Transactions volume in the RUONIA, bln. rubles",
    "Number of transactions, units",
    "Number of the RUONIA participants who conducted transactions in the given day, units",
    "Minimum rate, % p.a.",
    "25th percentile of rates, % p.a.",
    "75th percentile of rates, % p.a.",
    "Maximum rate, % p.a.",
    "Status of calculation",
    "Date of publication",
)
KEY_RATE_HEADERS = ("Date effective", "Key rate")
DAILY_KEY_RATE_HEADERS = ("Date", "Rate")
LIQUIDITY_NUMBER_HEADERS = (
    "1",
    "2 = 4 - 9 + 13 - 14 + 15",
    "3 = 4 - 9 + 13",
    "4 = 5 + 6 + 7 + 8",
    "5",
    "6",
    "7",
    "8",
    "9 = 10 + 11 + 12",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
)
LIQUIDITY_REQUIRED_MARKERS = (
    "Liquidity deficit (+)/surplus (-)",
    "Correspondent account balances of credit institutions with the Bank of Russia",
    "Required Reserves to be Averaged on Correspondent Accounts",
)


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[tuple[str, ...]] = []
        self.tables: list[tuple[tuple[str, ...], ...]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._table_depth = 0
        self._table_rows: list[tuple[str, ...]] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            if self._table_depth == 0:
                self._table_rows = []
            self._table_depth += 1
        elif tag == "tr":
            self._row = []
        elif tag in {"th", "td"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"th", "td"} and self._row is not None and self._cell is not None:
            self._row.append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "tr" and self._row:
            row = tuple(self._row)
            self.rows.append(row)
            if self._table_rows is not None:
                self._table_rows.append(row)
            self._row = None
        elif tag == "table" and self._table_depth:
            self._table_depth -= 1
            if self._table_depth == 0 and self._table_rows is not None:
                self.tables.append(tuple(self._table_rows))
                self._table_rows = None


def _html_rows(payload: bytes) -> tuple[tuple[str, ...], ...]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ExternalDataError("CBR response is not UTF-8 HTML") from exc
    parser = _TableParser()
    parser.feed(text)
    if not parser.rows:
        raise ExternalDataError("CBR response contains no table rows")
    return tuple(parser.rows)


def _key_rate_change_rows(payload: bytes) -> tuple[tuple[str, ...], ...]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ExternalDataError("CBR response is not UTF-8 HTML") from exc
    parser = _TableParser()
    parser.feed(text)
    if not parser.tables:
        raise ExternalDataError("CBR response contains no table rows")
    change_matches = [
        (table, index, row)
        for table in parser.tables
        for index, row in enumerate(table)
        if row[: len(KEY_RATE_HEADERS)] == KEY_RATE_HEADERS
    ]
    if not change_matches:
        if any(
            row[: len(DAILY_KEY_RATE_HEADERS)] == DAILY_KEY_RATE_HEADERS
            for table in parser.tables
            for row in table
        ):
            raise ExternalDataError(
                "daily Date / Rate table cannot provide key-rate effective dates"
            )
        raise ExternalDataError("CBR response columns differ from expected schema")
    if len(change_matches) != 1:
        raise ExternalDataError("CBR key-rate change-history table is ambiguous")
    table, header_index, header = change_matches[0]
    data = table[header_index + 1 :]
    if not data:
        raise ExternalDataError("requested non-empty CBR interval returned no rows")
    result: list[tuple[str, ...]] = []
    data_started = False
    for row in data:
        try:
            parse_date(row[0], field="effective_date")
        except (ExternalDataError, IndexError):
            if not data_started:
                continue
            raise ExternalDataError("malformed effective_date in CBR data row")
        data_started = True
        if len(row) < len(KEY_RATE_HEADERS):
            raise ExternalDataError("CBR data row lacks required key-rate columns")
        result.append(row[: len(KEY_RATE_HEADERS)])
    if not result:
        raise ExternalDataError("requested non-empty CBR interval returned no rows")
    return tuple(result)


def _url(route: str, start: date, end: date) -> str:
    if start > end:
        raise ExternalDataError("requested CBR interval is reversed")
    query = urlencode(
        {
            "UniDbQuery.Posted": "True",
            "UniDbQuery.From": start.strftime("%d.%m.%Y"),
            "UniDbQuery.To": end.strftime("%d.%m.%Y"),
        }
    )
    return route + "?" + query


def _data_rows(
    rows: tuple[tuple[str, ...], ...],
    *,
    headers: tuple[str, ...],
    date_field: str,
) -> tuple[tuple[str, ...], ...]:
    if headers not in rows:
        raise ExternalDataError("CBR response columns differ from expected schema")
    result: list[tuple[str, ...]] = []
    for row in rows:
        try:
            parse_date(row[0], field=date_field)
        except (ExternalDataError, IndexError):
            continue
        if len(row) != len(headers):
            raise ExternalDataError("CBR data row width differs from expected schema")
        result.append(row)
    if not result:
        raise ExternalDataError("requested non-empty CBR interval returned no rows")
    return tuple(result)


def parse_ruonia_html(
    payload: bytes,
    *,
    retrieved_at_utc: datetime,
    source_route: str = RUONIA_ROUTE,
) -> list[dict[str, object]]:
    rows = _data_rows(_html_rows(payload), headers=RUONIA_HEADERS, date_field="observation_date")
    base = provenance(
        source_id="cbr_ruonia_daily",
        source_route=source_route,
        payload=payload,
        retrieved_at_utc=retrieved_at_utc,
        source_revision_status="official_published_history",
        historical_model_use_status="candidate_for_phase8_2",
    )
    records: list[dict[str, object]] = []
    identities: set[str] = set()
    for row in rows:
        observation = parse_date(row[0], field="observation_date")
        publication = parse_date(row[10], field="publication_date")
        if publication < observation:
            raise ExternalDataError("RUONIA publication_date precedes observation_date")
        identity = observation.isoformat()
        if identity in identities:
            raise ExternalDataError("duplicate RUONIA observation identity")
        identities.add(identity)
        records.append(
            {
                "observation_date": identity,
                "publication_date": publication.isoformat(),
                "ruonia_rate_pct": parse_number(row[1], field="ruonia_rate_pct"),
                "transaction_volume_rub_bn": parse_number(row[2], field="transaction_volume_rub_bn"),
                "transaction_count": parse_integer(row[3], field="transaction_count"),
                "participant_count": parse_integer(row[4], field="participant_count"),
                "minimum_rate_pct": parse_number(row[5], field="minimum_rate_pct"),
                "percentile_25_rate_pct": parse_number(row[6], field="percentile_25_rate_pct"),
                "percentile_75_rate_pct": parse_number(row[7], field="percentile_75_rate_pct"),
                "maximum_rate_pct": parse_number(row[8], field="maximum_rate_pct"),
                "calculation_status": row[9],
                **base,
            }
        )
    return records


def parse_key_rate_html(
    payload: bytes,
    *,
    retrieved_at_utc: datetime,
    source_route: str = KEY_RATE_ROUTE,
) -> list[dict[str, object]]:
    rows = _key_rate_change_rows(payload)
    base = provenance(
        source_id="cbr_key_rate_daily",
        source_route=source_route,
        payload=payload,
        retrieved_at_utc=retrieved_at_utc,
        source_revision_status="official_change_date_history",
        historical_model_use_status="candidate_for_phase8_2",
    )
    points: list[tuple[date, float]] = []
    for row in rows:
        points.append(
            (
                parse_date(row[0], field="effective_date"),
                parse_number(row[1], field="key_rate_pct"),
            )
        )
    points.sort(key=lambda item: item[0])
    records: list[dict[str, object]] = []
    previous_date: date | None = None
    previous_rate: float | None = None
    for effective, rate in points:
        if effective == previous_date:
            raise ExternalDataError("duplicate key-rate effective_date")
        if previous_rate is not None and rate == previous_rate:
            previous_date = effective
            continue
        records.append(
            {
                "effective_date": effective.isoformat(),
                "key_rate_pct": rate,
                **base,
            }
        )
        previous_date = effective
        previous_rate = rate
    return records


def parse_banking_liquidity_html(
    payload: bytes,
    *,
    retrieved_at_utc: datetime,
    source_route: str = LIQUIDITY_ROUTE,
) -> list[dict[str, object]]:
    rows = _html_rows(payload)
    if LIQUIDITY_NUMBER_HEADERS not in rows:
        raise ExternalDataError("CBR liquidity numbered columns differ from expected schema")
    flattened = " | ".join(cell for row in rows for cell in row)
    if any(marker not in flattened for marker in LIQUIDITY_REQUIRED_MARKERS):
        raise ExternalDataError("CBR liquidity required response block is absent")
    data = _data_rows(
        rows,
        headers=LIQUIDITY_NUMBER_HEADERS,
        date_field="observation_date",
    )
    base = provenance(
        source_id="cbr_banking_liquidity_daily",
        source_route=source_route,
        payload=payload,
        retrieved_at_utc=retrieved_at_utc,
        source_revision_status="latest_revised",
        historical_model_use_status="blocked_pending_vintage_policy",
    )
    records: list[dict[str, object]] = []
    identities: set[str] = set()
    for row in data:
        identity = parse_date(row[0], field="observation_date").isoformat()
        if identity in identities:
            raise ExternalDataError("duplicate banking-liquidity observation identity")
        identities.add(identity)
        records.append(
            {
                "observation_date": identity,
                "liquidity_deficit_surplus_rub_bn": parse_number(
                    row[1], field="liquidity_deficit_surplus_rub_bn"
                ),
                "liquidity_deficit_surplus_ex_correspondent_accounts_rub_bn": parse_number(
                    row[2],
                    field="liquidity_deficit_surplus_ex_correspondent_accounts_rub_bn",
                ),
                "bank_correspondent_accounts_rub_bn": parse_number(
                    row[13], field="bank_correspondent_accounts_rub_bn"
                ),
                "required_reserves_averaging_rub_bn": parse_number(
                    row[14], field="required_reserves_averaging_rub_bn"
                ),
                **base,
            }
        )
    return records


def load_ruonia_daily(
    start: date,
    end: date,
    *,
    retrieved_at_utc: datetime,
    transport: HttpTransport = fetch_bytes,
) -> list[dict[str, object]]:
    source_route = _url(RUONIA_ROUTE, start, end)
    return parse_ruonia_html(
        transport(source_route),
        retrieved_at_utc=retrieved_at_utc,
        source_route=source_route,
    )


def load_key_rate_daily(
    start: date,
    end: date,
    *,
    retrieved_at_utc: datetime,
    transport: HttpTransport = fetch_bytes,
) -> list[dict[str, object]]:
    source_route = _url(KEY_RATE_ROUTE, start, end)
    return parse_key_rate_html(
        transport(source_route),
        retrieved_at_utc=retrieved_at_utc,
        source_route=source_route,
    )


def load_banking_liquidity_daily(
    start: date,
    end: date,
    *,
    retrieved_at_utc: datetime,
    transport: HttpTransport = fetch_bytes,
) -> list[dict[str, object]]:
    source_route = _url(LIQUIDITY_ROUTE, start, end)
    return parse_banking_liquidity_html(
        transport(source_route),
        retrieved_at_utc=retrieved_at_utc,
        source_route=source_route,
    )
