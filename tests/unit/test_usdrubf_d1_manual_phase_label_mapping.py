import sys
from datetime import date
from pathlib import Path

SRC_PATH = Path(__file__).resolve().parents[2] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from moex_research.labels.usdrubf_d1_manual_phase_labels import (  # noqa: E402
    NON_RUNTIME_FIELDS,
    RawPhaseInterval,
    assert_single_primary_label_per_session,
    materialize_phase_label_dicts,
    runtime_feature_fields,
)


def interval(interval_id, start_date, end_date, label):
    return RawPhaseInterval(
        interval_id=interval_id,
        start_date=date.fromisoformat(start_date),
        end_date=date.fromisoformat(end_date),
        label=label,
    )


def test_materializer_uses_inclusive_boundaries():
    rows = materialize_phase_label_dicts(
        session_dates=["2025-02-25", "2025-02-26", "2025-03-05", "2025-03-06"],
        intervals=[interval("i1", "2025-02-26", "2025-03-05", "B")],
    )

    assert [row["session_date"] for row in rows] == ["2025-02-26", "2025-03-05"]
    assert {row["phase_label"] for row in rows} == {"B"}
    assert rows[-1]["transition_exit_day"] is True


def test_materializer_maps_non_trading_start_and_end_to_supplied_sessions_only():
    rows = materialize_phase_label_dicts(
        session_dates=["2024-12-31", "2025-01-03", "2025-01-08", "2025-01-13"],
        intervals=[interval("i1", "2025-01-01", "2025-01-10", "OUT")],
    )

    assert [row["session_date"] for row in rows] == ["2025-01-03", "2025-01-08"]
    assert all(row["phase_label"] == "OUT" for row in rows)
    assert rows[-1]["transition_exit_day"] is True


def test_materializer_applies_previous_interval_wins_on_2025_09_11_overlap():
    rows = materialize_phase_label_dicts(
        session_dates=["2025-09-10", "2025-09-11", "2025-09-12"],
        intervals=[
            interval("b", "2025-09-03", "2025-09-11", "B"),
            interval("out", "2025-09-11", "2025-09-23", "OUT"),
        ],
    )

    by_date = {row["session_date"]: row for row in rows}
    assert by_date["2025-09-11"]["phase_label"] == "B"
    assert by_date["2025-09-11"]["transition_exit_day"] is True
    assert by_date["2025-09-12"]["phase_label"] == "OUT"


def test_materializer_emits_no_double_primary_label_per_session():
    rows = materialize_phase_label_dicts(
        session_dates=["2025-09-11", "2025-09-12"],
        intervals=[
            interval("b", "2025-09-03", "2025-09-11", "B"),
            interval("out", "2025-09-11", "2025-09-23", "OUT"),
        ],
    )

    assert len(rows) == len({row["session_date"] for row in rows})
    assert_single_primary_label_per_session(rows)


def test_materializer_creates_no_synthetic_rows():
    rows = materialize_phase_label_dicts(
        session_dates=["2025-01-01", "2025-01-15"],
        intervals=[interval("i1", "2025-01-03", "2025-01-10", "S")],
    )

    assert rows == []


def test_runtime_feature_filter_excludes_manual_labels_and_future_targets():
    candidate_fields = {
        "ema_3",
        "ema_19",
        "phase_label",
        "phase_label_meaning",
        "source_interval_id",
        "interval_start_date",
        "interval_end_date",
        "transition_exit_day",
        "phase_remaining_sessions",
        "current_regime_ends_within_1d",
        "current_regime_ends_within_3d",
        "current_regime_ends_within_5d",
        "next_regime_if_current_ends",
    }

    runtime_fields = runtime_feature_fields(candidate_fields)

    assert runtime_fields == {"ema_3", "ema_19"}
    assert {
        "phase_label",
        "phase_label_meaning",
        "source_interval_id",
        "interval_start_date",
        "interval_end_date",
    }.issubset(NON_RUNTIME_FIELDS)


def test_runtime_feature_filter_excludes_all_normalized_label_row_metadata():
    rows = materialize_phase_label_dicts(
        session_dates=["2025-02-26"],
        intervals=[interval("i1", "2025-02-26", "2025-02-26", "B")],
    )

    runtime_fields = runtime_feature_fields(rows[0].keys())

    assert runtime_fields == set()
    assert {
        "session_date",
        "phase_label",
        "phase_label_meaning",
        "source_interval_id",
        "interval_start_date",
        "interval_end_date",
        "transition_exit_day",
        "phase_remaining_sessions",
        "current_regime_ends_within_1d",
        "current_regime_ends_within_3d",
        "current_regime_ends_within_5d",
        "next_regime_if_current_ends",
    }.issubset(NON_RUNTIME_FIELDS)
