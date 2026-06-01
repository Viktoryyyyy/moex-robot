import pytest

from moex_research.runners.real_read_readiness_snapshot import (
    BLOCKED_STATUS,
    SNAPSHOT_FIELDS,
    RealReadReadinessSnapshot,
    RealReadReadinessSnapshotError,
    make_repo_only_real_read_readiness_snapshot,
    validate_real_read_readiness_snapshot,
)


def _snapshot(**overrides: object) -> RealReadReadinessSnapshot:
    values = make_repo_only_real_read_readiness_snapshot().__dict__.copy()
    values.update(overrides)
    return RealReadReadinessSnapshot(**values)


def test_repo_only_snapshot_is_valid():
    snapshot = make_repo_only_real_read_readiness_snapshot()

    assert validate_real_read_readiness_snapshot(snapshot) is snapshot
    assert frozenset(snapshot.__dict__) == frozenset(SNAPSHOT_FIELDS)
    assert snapshot.snapshot_status == "repo_only_closed"
    assert snapshot.metadata_only is True


def test_all_downstream_statuses_remain_blocked():
    snapshot = make_repo_only_real_read_readiness_snapshot()

    assert snapshot.actual_data_lake_read_status == BLOCKED_STATUS
    assert snapshot.real_market_data_loading_status == BLOCKED_STATUS
    assert snapshot.registry_write_status == BLOCKED_STATUS
    assert snapshot.runtime_live_status == BLOCKED_STATUS
    assert snapshot.promotion_status == BLOCKED_STATUS


@pytest.mark.parametrize(
    "field_name",
    (
        "actual_data_lake_read_status",
        "real_market_data_loading_status",
        "registry_write_status",
        "runtime_live_status",
        "promotion_status",
    ),
)
def test_downstream_statuses_cannot_be_opened(field_name: str):
    with pytest.raises(RealReadReadinessSnapshotError):
        _snapshot(**{field_name: "open"})


def test_snapshot_status_is_limited_to_repo_only_closed():
    with pytest.raises(RealReadReadinessSnapshotError):
        _snapshot(snapshot_status="accepted")


def test_snapshot_rejects_metadata_false():
    with pytest.raises(RealReadReadinessSnapshotError):
        _snapshot(metadata_only=False)


def test_snapshot_rejects_dynamic_markers():
    with pytest.raises(RealReadReadinessSnapshotError):
        _snapshot(export_ref="latest")
    with pytest.raises(RealReadReadinessSnapshotError):
        _snapshot(export_ref="current")
    with pytest.raises(RealReadReadinessSnapshotError):
        _snapshot(export_ref="autodetect")
