from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.moex_research.intelligence.usdrubf_macro_live_cbr import (
    CbrMacroAdapterError,
    KEY_RATE_METRIC_ID,
    RUONIA_METRIC_ID,
    build_current_cbr_macro_observations,
    latest_key_rate_macro_observation,
    latest_ruonia_macro_observation,
)


AS_OF = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
RETRIEVED = "2026-07-15T11:00:00Z"


def _ruonia(
    observation_date: str,
    publication_date: str,
    rate: float,
    *,
    retrieved_at: str = RETRIEVED,
) -> dict[str, object]:
    return {
        "source_id": "cbr_ruonia_daily",
        "source_route": "https://www.cbr.ru/eng/hd_base/ruonia/dynamics/?sample=1",
        "retrieved_at_utc": retrieved_at,
        "historical_model_use_status": "candidate_for_phase8_2",
        "observation_date": observation_date,
        "publication_date": publication_date,
        "ruonia_rate_pct": rate,
    }


def _key_rate(
    effective_date: str,
    rate: float,
    *,
    retrieved_at: str = RETRIEVED,
) -> dict[str, object]:
    return {
        "source_id": "cbr_key_rate_daily",
        "source_route": "https://www.cbr.ru/eng/hd_base/ProcStav/IR_CHG_MPO/?sample=1",
        "retrieved_at_utc": retrieved_at,
        "historical_model_use_status": "candidate_for_phase8_2",
        "effective_date": effective_date,
        "key_rate_pct": rate,
    }


def test_ruonia_preserves_strict_prior_publication_rule() -> None:
    observation = latest_ruonia_macro_observation(
        [
            _ruonia("2026-07-13", "2026-07-14", 12.0),
            _ruonia("2026-07-14", "2026-07-15", 99.0),
        ],
        as_of_timestamp=AS_OF,
    )

    assert observation.metric_id == RUONIA_METRIC_ID
    assert observation.value == 12.0
    assert observation.observed_or_effective_at.isoformat() == "2026-07-13T00:00:00+03:00"
    assert observation.published_at.isoformat() == "2026-07-14T23:59:59.999999+03:00"
    assert observation.available_at.isoformat() == "2026-07-15T00:00:00+03:00"
    assert observation.ingested_at.isoformat() == "2026-07-15T11:00:00+00:00"
    assert observation.quality_status == "OK"


def test_ruonia_same_day_only_fails_closed() -> None:
    with pytest.raises(CbrMacroAdapterError, match="no causally eligible RUONIA"):
        latest_ruonia_macro_observation(
            [_ruonia("2026-07-14", "2026-07-15", 13.0)],
            as_of_timestamp=AS_OF,
        )


def test_ruonia_retrieval_before_causal_boundary_is_not_eligible() -> None:
    with pytest.raises(CbrMacroAdapterError, match="no causally eligible RUONIA"):
        latest_ruonia_macro_observation(
            [
                _ruonia(
                    "2026-07-13",
                    "2026-07-14",
                    12.0,
                    retrieved_at="2026-07-14T20:59:59Z",
                )
            ],
            as_of_timestamp=AS_OF,
        )


def test_key_rate_includes_effective_date_and_excludes_future() -> None:
    observation = latest_key_rate_macro_observation(
        [
            _key_rate("2026-06-01", 12.0),
            _key_rate("2026-07-15", 13.0),
            _key_rate("2026-07-16", 99.0),
        ],
        as_of_timestamp=AS_OF,
    )

    assert observation.metric_id == KEY_RATE_METRIC_ID
    assert observation.value == 13.0
    assert observation.observed_or_effective_at.isoformat() == "2026-07-15T00:00:00+03:00"
    assert observation.published_at == observation.observed_or_effective_at
    assert observation.available_at == observation.observed_or_effective_at
    assert observation.quality_status == "OK"


def test_key_rate_visible_before_effective_date_is_not_usable_early() -> None:
    with pytest.raises(CbrMacroAdapterError, match="no causally eligible key-rate"):
        latest_key_rate_macro_observation(
            [
                _key_rate(
                    "2026-07-15",
                    13.0,
                    retrieved_at="2026-07-14T10:00:00Z",
                )
            ],
            as_of_timestamp="2026-07-14T12:00:00+00:00",
        )


def test_future_retrieval_cannot_enter_as_of_state() -> None:
    with pytest.raises(CbrMacroAdapterError, match="no causally eligible key-rate"):
        latest_key_rate_macro_observation(
            [_key_rate("2026-06-01", 12.0, retrieved_at="2026-07-15T13:00:00Z")],
            as_of_timestamp=AS_OF,
        )


def test_source_policy_and_identity_fail_closed() -> None:
    wrong_source = _ruonia("2026-07-13", "2026-07-14", 12.0)
    wrong_source["source_id"] = "not_cbr"
    with pytest.raises(CbrMacroAdapterError, match="unexpected source_id"):
        latest_ruonia_macro_observation([wrong_source], as_of_timestamp=AS_OF)

    blocked = _key_rate("2026-06-01", 12.0)
    blocked["historical_model_use_status"] = "blocked_pending_vintage_policy"
    with pytest.raises(CbrMacroAdapterError, match="not governed"):
        latest_key_rate_macro_observation([blocked], as_of_timestamp=AS_OF)


def test_pair_builder_returns_both_governed_metrics() -> None:
    observations = build_current_cbr_macro_observations(
        ruonia_records=[_ruonia("2026-07-13", "2026-07-14", 12.0)],
        key_rate_records=[_key_rate("2026-07-15", 13.0)],
        as_of_timestamp=AS_OF,
    )

    assert tuple(item.metric_id for item in observations) == (
        RUONIA_METRIC_ID,
        KEY_RATE_METRIC_ID,
    )
    assert all(item.source_reference.startswith("https://www.cbr.ru/") for item in observations)
