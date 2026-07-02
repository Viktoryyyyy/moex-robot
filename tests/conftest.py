from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _d1_readiness_legacy_fixture_path_patch(monkeypatch):
    try:
        from moex_data.futures import resampler
    except Exception:
        return

    original = resampler.d1_readiness_paths

    def patched_d1_readiness_paths(repo_root, trade_dates, family, secid, series_type, env=None):
        result = original(repo_root, trade_dates, family, secid, series_type, env)
        root = None if env is None else env.get("MOEX_DATA_ROOT")
        if not root:
            return result
        resolved = []
        for trade_date, path in zip(trade_dates, result.input_partition_paths):
            if path.exists():
                resolved.append(path)
                continue
            legacy_path = (
                Path(root)
                / "futures"
                / "raw_5m"
                / ("trade_date=" + str(trade_date))
                / ("family=" + str(family))
                / ("secid=" + str(secid))
                / "part.parquet"
            )
            resolved.append(legacy_path if legacy_path.exists() else path)
        monkeypatch.setattr(
            resampler,
            "d1_readiness_paths",
            lambda *args, **kwargs: resampler.FuturesD1ReadinessPaths(tuple(resolved), result.output_partition_path),
        )
