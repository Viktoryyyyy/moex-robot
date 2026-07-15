from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from moex_research.external_data.pit_alignment import (
    MATRIX_COLUMNS,
    PITAlignmentError,
    align_key_rate,
    align_ruonia,
    build_external_pit_matrix,
)


def _identities(*dates: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "target_trade_date": list(dates),
            "target_instrument_id": "forts.usdrubf",
        }
    )


def _ruonia(
    observation_date: str,
    publication_date: str,
    *,
    rate: float = 14.0,
) -> dict[str, object]:
    return {
        "observation_date": observation_date,
        "publication_date": publication_date,
        "ruonia_rate_pct": rate,
        "transaction_volume_rub_bn": 500.0,
        "transaction_count": 50,
        "participant_count": 20,
        "minimum_rate_pct": rate - 0.5,
        "percentile_25_rate_pct": rate - 0.25,
        "percentile_75_rate_pct": rate + 0.25,
        "maximum_rate_pct": rate + 0.5,
        "calculation_status": "Standard",
    }


def _key_rate(effective_date: str, rate: float = 13.0) -> dict[str, object]:
    return {"effective_date": effective_date, "key_rate_pct": rate}


def test_ruonia_accepts_only_latest_strictly_prior_publication() -> None:
    identities = _identities("2026-07-15")
    records = [
        _ruonia("2026-07-11", "2026-07-12", rate=11.0),
        _ruonia("2026-07-13", "2026-07-14", rate=12.0),
        _ruonia("2026-07-14", "2026-07-15", rate=13.0),
        _ruonia("2026-07-15", "2026-07-16", rate=14.0),
    ]

    aligned = align_ruonia(identities, records)

    assert aligned.loc[0, "ruonia_observation_date"] == "2026-07-13"
    assert aligned.loc[0, "ruonia_publication_date"] == "2026-07-14"
    assert aligned.loc[0, "ruonia_rate_pct"] == 12.0


def test_weekend_ruonia_carry_forward_uses_prior_publication_only() -> None:
    aligned = align_ruonia(
        _identities("2026-07-20"),
        [
            _ruonia("2026-07-16", "2026-07-17", rate=10.0),
            _ruonia("2026-07-17", "2026-07-20", rate=99.0),
        ],
    )

    assert aligned.loc[0, "ruonia_rate_pct"] == 10.0
    assert aligned.loc[0, "ruonia_publication_age_calendar_days"] == 3


def test_ruonia_duplicate_observation_or_publication_tie_fails_closed() -> None:
    with pytest.raises(PITAlignmentError, match="duplicate RUONIA observation"):
        align_ruonia(
            _identities("2026-07-15"),
            [
                _ruonia("2026-07-12", "2026-07-13"),
                _ruonia("2026-07-12", "2026-07-14"),
            ],
        )
    with pytest.raises(PITAlignmentError, match="publication-date tie"):
        align_ruonia(
            _identities("2026-07-15"),
            [
                _ruonia("2026-07-11", "2026-07-14"),
                _ruonia("2026-07-12", "2026-07-14"),
            ],
        )


def test_ruonia_missing_first_coverage_and_invalid_chronology_fail_closed() -> None:
    with pytest.raises(PITAlignmentError, match="first eligible identity"):
        align_ruonia(
            _identities("2026-07-15"),
            [_ruonia("2026-07-14", "2026-07-15")],
        )
    with pytest.raises(PITAlignmentError, match="precedes observation"):
        align_ruonia(
            _identities("2026-07-15"),
            [_ruonia("2026-07-14", "2026-07-13")],
        )


def test_key_rate_accepts_target_date_excludes_future_and_selects_latest() -> None:
    aligned = align_key_rate(
        _identities("2026-07-15"),
        [
            _key_rate("2026-06-01", 12.0),
            _key_rate("2026-07-15", 13.0),
            _key_rate("2026-07-16", 99.0),
        ],
    )

    assert aligned.loc[0, "key_rate_effective_date"] == "2026-07-15"
    assert aligned.loc[0, "key_rate_pct"] == 13.0
    assert aligned.loc[0, "key_rate_age_calendar_days"] == 0


def test_key_rate_duplicate_and_missing_first_coverage_fail_closed() -> None:
    with pytest.raises(PITAlignmentError, match="duplicate key-rate"):
        align_key_rate(
            _identities("2026-07-15"),
            [_key_rate("2026-07-01"), _key_rate("2026-07-01")],
        )
    with pytest.raises(PITAlignmentError, match="first eligible identity"):
        align_key_rate(
            _identities("2026-07-15"),
            [_key_rate("2026-07-16")],
        )


def test_identity_order_ages_and_diagnostic_spread_are_exact() -> None:
    targets = _identities("2026-07-16", "2026-07-15")
    matrix = build_external_pit_matrix(
        targets,
        ruonia_records=[_ruonia("2026-07-12", "2026-07-14", rate=14.5)],
        key_rate_records=[_key_rate("2026-07-01", rate=13.0)],
    )

    assert tuple(matrix.columns) == MATRIX_COLUMNS
    assert matrix["target_trade_date"].tolist() == ["2026-07-15", "2026-07-16"]
    assert matrix["ruonia_observation_age_calendar_days"].tolist() == [3, 4]
    assert matrix["ruonia_publication_age_calendar_days"].tolist() == [1, 2]
    assert matrix["key_rate_age_calendar_days"].tolist() == [14, 15]
    assert matrix["ruonia_minus_key_rate_pp"].tolist() == [1.5, 1.5]


def test_daily_prior_publications_never_select_a_future_row() -> None:
    target_dates = pd.bdate_range("2026-01-01", periods=20)
    records = []
    for target in target_dates:
        publication = target.date() - timedelta(days=1)
        observation = publication - timedelta(days=1)
        records.append(_ruonia(observation.isoformat(), publication.isoformat()))
    aligned = align_ruonia(
        _identities(*target_dates.strftime("%Y-%m-%d").tolist()), records
    )
    assert (
        pd.to_datetime(aligned["ruonia_publication_date"])
        < pd.Series(target_dates).reset_index(drop=True)
    ).all()
