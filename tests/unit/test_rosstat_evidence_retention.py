import json
from pathlib import Path

from moex_research.external_data import rosstat_cpi_factual as weekly
from moex_research.external_data import rosstat_monthly_cpi as monthly


def _write_current(root, components):
    path = root / weekly.CURRENT_SNAPSHOT_RELATIVE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'components': components}), encoding='utf-8')
    return path


def test_current_weekly_index_manifest_is_protected(tmp_path):
    manifest = tmp_path / weekly.EVIDENCE_RELATIVE_DIR / ('a' * 64 + '.json')
    _write_current(tmp_path, {
        weekly.COMPONENT: {'data': {'index_manifest_path': str(manifest)}}
    })
    assert weekly._current_index_manifests(tmp_path) == (str(manifest),)


def test_current_monthly_index_manifest_is_protected_independently(tmp_path):
    weekly_manifest = tmp_path / weekly.EVIDENCE_RELATIVE_DIR / ('a' * 64 + '.json')
    monthly_manifest = tmp_path / monthly.EVIDENCE_RELATIVE_DIR / ('b' * 64 + '.json')
    _write_current(tmp_path, {
        weekly.COMPONENT: {'data': {'index_manifest_path': str(weekly_manifest)}},
        monthly.COMPONENT: {'data': {'index_manifest_path': str(monthly_manifest)}},
    })
    assert weekly._current_index_manifests(tmp_path) == (str(weekly_manifest),)
    assert weekly._current_index_manifests(tmp_path, monthly.COMPONENT) == (str(monthly_manifest),)


def test_missing_current_snapshot_allows_first_run_cleanup(tmp_path):
    assert weekly._current_index_manifests(tmp_path) == ()


def test_malformed_current_snapshot_disables_cleanup(tmp_path):
    path = tmp_path / weekly.CURRENT_SNAPSHOT_RELATIVE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{broken', encoding='utf-8')
    assert weekly._current_index_manifests(tmp_path) is None


def test_nonregular_current_snapshot_disables_cleanup(tmp_path):
    path = tmp_path / weekly.CURRENT_SNAPSHOT_RELATIVE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.mkdir()
    assert weekly._current_index_manifests(tmp_path) is None
