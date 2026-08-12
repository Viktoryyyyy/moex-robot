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


def run_current_cbr_macro_smoke(
    *,
    now_utc: datetime | None = None,
    ruonia_lookback_days: int = 30,
    key_rate_lookback_days: int = 3650,
    ruonia_loader: Loader = load_ruonia_daily,
    key_rate_loader: Loader = load_key_rate_daily,
) -> tuple[MacroObservation, MacroObservation]:
    if ruonia_lookback_days <= 0 or key_rate_lookback_days <= 0:
        raise ValueError("lookback days must be positive")

    anchor = now_utc or datetime.now(timezone.utc)
    if anchor.tzinfo is None or anchor.utcoffset() is None:
        raise ValueError("now_utc must be timezone-aware")
    anchor = anchor.astimezone(timezone.utc)
    end_date = anchor.astimezone(MOSCOW_TZ).date()

    ruonia_retrieved_at = anchor
    ruonia_records = ruonia_loader(
        end_date - timedelta(days=ruonia_lookback_days),
        end_date,
        retrieved_at_utc=ruonia_retrieved_at,
    )

    key_rate_retrieved_at = anchor
    key_rate_records = key_rate_loader(
        end_date - timedelta(days=key_rate_lookback_days),
        end_date,
        retrieved_at_utc=key_rate_retrieved_at,
    )

    as_of = anchor if now_utc is not None else datetime.now(timezone.utc)
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
