from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone

from src.moex_research.external_data.cbr import load_key_rate_daily, load_ruonia_daily
from src.moex_research.external_data.models import ExternalDataError
from src.moex_research.intelligence.usdrubf_macro_live_cbr import (
    CbrMacroAdapterError,
    KEY_RATE_SOURCE_ID,
    MOSCOW_TZ,
    RUONIA_SOURCE_ID,
    build_current_cbr_macro_observations,
)
from src.moex_research.intelligence.usdrubf_news_macro import MacroObservation


Loader = Callable[..., list[dict[str, object]]]
Clock = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _restamp_retrieved_at(
    records: list[dict[str, object]],
    *,
    retrieved_at_utc: datetime,
) -> list[dict[str, object]]:
    stamp = _as_utc(retrieved_at_utc, "retrieved_at_utc").isoformat().replace("+00:00", "Z")
    return [{**record, "retrieved_at_utc": stamp} for record in records]


def run_current_cbr_macro_smoke(
    *,
    now_utc: datetime | None = None,
    ruonia_lookback_days: int = 30,
    key_rate_lookback_days: int = 3650,
    ruonia_loader: Loader = load_ruonia_daily,
    key_rate_loader: Loader = load_key_rate_daily,
    clock_utc: Clock = _utc_now,
) -> tuple[MacroObservation, MacroObservation]:
    """Acquire current governed CBR observations with causal ingestion timestamps.

    ``now_utc`` is a deterministic fixed-time mode for tests/replay. In live mode
    it is omitted: each loader result is restamped only after that response has
    returned, and final eligibility is evaluated at a fresh post-acquisition
    timestamp. This prevents response content from being backdated to cycle start.
    """

    if ruonia_lookback_days <= 0 or key_rate_lookback_days <= 0:
        raise ValueError("lookback days must be positive")

    fixed_time = now_utc is not None
    anchor = _as_utc(now_utc, "now_utc") if fixed_time else _as_utc(clock_utc(), "clock_utc")
    end_date = anchor.astimezone(MOSCOW_TZ).date()

    ruonia_records = ruonia_loader(
        end_date - timedelta(days=ruonia_lookback_days),
        end_date,
        retrieved_at_utc=anchor,
    )
    if not fixed_time:
        ruonia_records = _restamp_retrieved_at(
            ruonia_records,
            retrieved_at_utc=_as_utc(clock_utc(), "clock_utc"),
        )

    key_rate_records = key_rate_loader(
        end_date - timedelta(days=key_rate_lookback_days),
        end_date,
        retrieved_at_utc=anchor,
    )
    if not fixed_time:
        key_rate_records = _restamp_retrieved_at(
            key_rate_records,
            retrieved_at_utc=_as_utc(clock_utc(), "clock_utc"),
        )

    as_of = anchor if fixed_time else _as_utc(clock_utc(), "clock_utc")
    if as_of.astimezone(MOSCOW_TZ).date() != end_date:
        raise CbrMacroAdapterError(
            "CBR live acquisition crossed the Moscow calendar-date boundary; retry the cycle"
        )

    observations = build_current_cbr_macro_observations(
        ruonia_records=ruonia_records,
        key_rate_records=key_rate_records,
        as_of_timestamp=as_of,
    )
    for observation in observations:
        if observation.quality_status != "OK":
            raise CbrMacroAdapterError(
                f"{observation.source_id} current observation is not quality OK"
            )
        if observation.available_at > as_of or observation.ingested_at > as_of:
            raise CbrMacroAdapterError(
                f"{observation.source_id} current observation exceeds as-of boundary"
            )
    return observations


def _print_observation(observation: MacroObservation) -> None:
    print(f"SOURCE={observation.source_id}")
    print(f"QUALITY={observation.quality_status}")
    print(f"METRIC_ID={observation.metric_id}")
    print(f"VALUE={observation.value}")
    print(f"UNIT={observation.unit}")
    print(f"OBSERVED_OR_EFFECTIVE_AT={observation.observed_or_effective_at.isoformat()}")
    print(f"PUBLISHED_AT={observation.published_at.isoformat()}")
    print(f"AVAILABLE_AT={observation.available_at.isoformat()}")
    print(f"INGESTED_AT={observation.ingested_at.isoformat()}")
    print(f"SOURCE_REFERENCE={observation.source_reference}")


def main() -> int:
    print("PROJECT=MOEX_Bot")
    print("MODE=cbr_macro_live_acceptance_smoke")
    try:
        observations = run_current_cbr_macro_smoke()
    except (ExternalDataError, CbrMacroAdapterError, ValueError) as exc:
        print("STATUS=BLOCKED")
        print(f"ERROR={exc}")
        return 1

    print("STATUS=COMPLETED")
    print(f"SOURCE_COUNT={len(observations)}")
    print(f"OK_SOURCE_COUNT={sum(item.quality_status == 'OK' for item in observations)}")
    by_source = {item.source_id: item for item in observations}
    for source_id in (RUONIA_SOURCE_ID, KEY_RATE_SOURCE_ID):
        _print_observation(by_source[source_id])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
