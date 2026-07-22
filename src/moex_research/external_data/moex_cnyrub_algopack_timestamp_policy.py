from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta

from . import moex_cnyrub_algopack_history as source

TIMESTAMP_POLICY_ID = "algopack_tradetime_is_five_minute_interval_end_v1"


def parse_tradestats_page_response(
    payload: bytes,
    *,
    from_date: date,
    till_date: date,
    start: int,
    route: str,
    retrieved_at_utc: datetime,
) -> tuple[
    list[source.AlgoPackTradeStat],
    tuple[str, ...],
    source.AlgoPackCursor,
    str,
]:
    """Parse TradeStats with provider ``tradetime`` treated as bucket end.

    Live CNYRUB_TOM evidence shows ``SYSTIME`` follows ``tradetime`` by
    seconds, not by a further five minutes. Therefore the provider timestamp
    identifies the completed interval end, and the interval begin is derived
    by subtracting the declared five-minute width.
    """

    source._official_algopack_route(
        route,
        from_date=from_date,
        till_date=till_date,
        start=start,
    )
    try:
        root = source.parse_json_object(payload)
    except source.ExternalDataError as exc:
        raise source.CnyrubAlgoPackError(
            "AlgoPack response is not valid UTF-8 JSON",
            blocker="algopack_schema_not_stable",
        ) from exc

    rows, columns = source._block(root, "data", source._TRADESTAT_COLUMNS)
    cursor_rows, _ = source._block(
        root,
        "data.cursor",
        source._CURSOR_COLUMNS,
    )
    if len(cursor_rows) != 1:
        raise source.CnyrubAlgoPackError(
            "AlgoPack cursor must contain exactly one row",
            blocker="algopack_schema_not_stable",
        )

    cursor_row = cursor_rows[0]
    cursor = source.AlgoPackCursor(
        index=source._integer(cursor_row["INDEX"], "cursor INDEX"),
        total=source._integer(cursor_row["TOTAL"], "cursor TOTAL"),
        page_size=source._integer(cursor_row["PAGESIZE"], "cursor PAGESIZE"),
    )
    if cursor.index != start or cursor.page_size <= 0 or start > cursor.total:
        raise source.CnyrubAlgoPackError(
            "AlgoPack cursor is inconsistent with requested page",
            blocker="algopack_schema_not_stable",
        )
    remaining = cursor.total - start
    if len(rows) > remaining or len(rows) > cursor.page_size:
        raise source.CnyrubAlgoPackError(
            "AlgoPack page row count exceeds cursor bounds",
            blocker="algopack_schema_not_stable",
        )

    source._utc(retrieved_at_utc)
    result: list[source.AlgoPackTradeStat] = []
    previous_end: datetime | None = None
    identities: set[tuple[date, datetime, str]] = set()

    for row in rows:
        if str(row["secid"]).strip() != source.SECURITY_ID:
            raise source.CnyrubAlgoPackError(
                "AlgoPack response contains a substituted security",
                blocker="security_identity_not_reproducible",
            )

        bucket_end = source._bucket_datetime(
            row["tradedate"],
            row["tradetime"],
        )
        bucket_begin = bucket_end - timedelta(
            minutes=source.ALGOPACK_BUCKET_MINUTES
        )
        if not from_date <= bucket_end.date() <= till_date:
            raise source.CnyrubAlgoPackError(
                "AlgoPack bucket is outside requested range",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if previous_end is not None and bucket_end <= previous_end:
            raise source.CnyrubAlgoPackError(
                "AlgoPack buckets are duplicated or not chronological",
                blocker="numerical_or_chronology_integrity_failure",
            )

        identity = (bucket_end.date(), bucket_end, source.SECURITY_ID)
        if identity in identities:
            raise source.CnyrubAlgoPackError(
                "AlgoPack provider row identity is duplicated",
                blocker="algopack_schema_not_stable",
            )
        identities.add(identity)

        source_available_at = source._provider_timestamp(row["SYSTIME"])
        if source_available_at < bucket_end:
            raise source.CnyrubAlgoPackError(
                "AlgoPack SYSTIME precedes its completed provider bucket",
                blocker="point_in_time_cutoff_not_provable",
            )

        source._validate_directional_totals(row)
        open_ = source._number(row["pr_open"], "pr_open")
        high = source._number(row["pr_high"], "pr_high")
        low = source._number(row["pr_low"], "pr_low")
        close = source._number(row["pr_close"], "pr_close")
        if high < max(open_, close, low) or low > min(open_, close, high):
            raise source.CnyrubAlgoPackError(
                "AlgoPack bucket OHLC values are inconsistent",
                blocker="numerical_or_chronology_integrity_failure",
            )

        result.append(
            source.AlgoPackTradeStat(
                trade_date=bucket_end.date(),
                bucket_begin=bucket_begin,
                source_available_at=source_available_at,
                security_id=source.SECURITY_ID,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=source._number(row["vol"], "vol", nonnegative=True),
                value=source._number(row["val"], "val", nonnegative=True),
                trades=source._integer(row["trades"], "trades"),
                trades_buy=source._integer(row["trades_b"], "trades_b"),
                trades_sell=source._integer(row["trades_s"], "trades_s"),
                value_buy=source._number(
                    row["val_b"], "val_b", nonnegative=True
                ),
                value_sell=source._number(
                    row["val_s"], "val_s", nonnegative=True
                ),
                volume_buy=source._number(
                    row["vol_b"], "vol_b", nonnegative=True
                ),
                volume_sell=source._number(
                    row["vol_s"], "vol_s", nonnegative=True
                ),
            )
        )
        previous_end = bucket_end

    return (
        result,
        columns,
        cursor,
        source.raw_payload_sha256(payload),
    )


def install_timestamp_policy() -> None:
    """Install the corrected parser into the canonical AlgoPack source module."""

    source.parse_tradestats_page_response = parse_tradestats_page_response


__all__ = [
    "TIMESTAMP_POLICY_ID",
    "install_timestamp_policy",
    "parse_tradestats_page_response",
]
