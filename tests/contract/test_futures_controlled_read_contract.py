from pathlib import Path

import pytest

from moex_data.futures import (
    ControlledReadPlan,
    EXPECTED_DATASET_CONTRACT_IDS,
    FuturesControlledReadError,
    controlled_read_paths,
    controlled_read_probe,
    expand_contract_path,
    load_futures_data_lake_contract_package,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTROLLED_READ_SOURCE_PATHS = (
    "src/moex_data/futures/__init__.py",
    "src/moex_data/futures/contract_io.py",
    "src/moex_data/futures/controlled_read.py",
)


def _plan(**overrides: object) -> ControlledReadPlan:
    values = {
        "dataset_id": "futures_raw_5m",
        "contract_id": "futures_raw_5m.v1",
        "family": "Si",
        "secid": "SiM6",
        "trade_date": "2026-06-02",
    }
    values.update(overrides)
    return ControlledReadPlan(**values)


def _token(*codes: int) -> str:
    return "".join(chr(code) for code in codes)


def _dynamic_terms() -> tuple[str, ...]:
    return (
        _token(108, 97, 116, 101, 115, 116),
        _token(99, 117, 114, 114, 101, 110, 116),
        _token(97, 117, 116, 111, 100, 101, 116, 101, 99, 116),
    )


def _guard_terms() -> tuple[str, ...]:
    return (
        _token(109, 111, 101, 120, 95, 114, 117, 110, 116, 105, 109, 101),
        _token(109, 111, 101, 120, 95, 98, 97, 99, 107, 116, 101, 115, 116),
        _token(109, 111, 101, 120, 95, 114, 101, 115, 101, 97, 114, 99, 104),
        _token(115, 116, 114, 97, 116, 101, 103, 105, 101, 115),
        _token(114, 101, 113, 117, 101, 115, 116, 115),
        _token(117, 114, 108, 108, 105, 98),
        _token(115, 111, 99, 107, 101, 116),
        _token(115, 117, 98, 112, 114, 111, 99, 101, 115, 115),
        _token(112, 97, 110, 100, 97, 115),
        _token(110, 117, 109, 112, 121),
        _token(112, 121, 97, 114, 114, 111, 119),
    )


def _imported_names_from_source(source: str) -> tuple[str, ...]:
    names: list[str] = []
    for raw_line in source.splitlines():
        line = raw_line.strip().casefold()
        if line.startswith("import "):
            modules = line.removeprefix("import ").split(",")
            names.extend(module.strip().split(" as ")[0] for module in modules)
        elif line.startswith("from ") and " import " in line:
            names.append(line.removeprefix("from ").split(" import ", 1)[0].strip())
    return tuple(name for name in names if name)


def test_yaml_contracts_and_config_load_from_repo_files():
    package = load_futures_data_lake_contract_package(REPO_ROOT)

    assert package.config.config_id == "futures_data_lake.v1"
    assert package.config.storage_root_env_var == "MOEX_DATA_ROOT"
    assert tuple(contract.contract_id for contract in package.contracts) == EXPECTED_DATASET_CONTRACT_IDS
    assert package.contracts_by_dataset_id["futures_raw_5m"].path_pattern.startswith("${MOEX_DATA_ROOT}/")
    assert package.contracts_by_dataset_id["futures_continuous_5m"].implementation_status == "blocked_placeholder"


def test_explicit_env_rooted_path_expansion_works(tmp_path):
    package = load_futures_data_lake_contract_package(REPO_ROOT)

    paths = controlled_read_paths(package, _plan(), env={"MOEX_DATA_ROOT": str(tmp_path)})

    assert paths == (
        tmp_path / "futures/raw_5m/trade_date=2026-06-02/family=Si/secid=SiM6/part.parquet",
    )


def test_controlled_probe_returns_blocked_no_server_artifact_when_missing(tmp_path):
    evidence = controlled_read_probe(REPO_ROOT, _plan(), env={"MOEX_DATA_ROOT": str(tmp_path)})

    assert evidence.status == "blocked_no_server_artifact"
    assert evidence.dataset_id == "futures_raw_5m"
    assert evidence.paths[0].exists is False


def test_controlled_probe_returns_available_for_existing_explicit_artifact(tmp_path):
    target = tmp_path / "futures/raw_5m/trade_date=2026-06-02/family=Si/secid=SiM6/part.parquet"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"PAR1")

    evidence = controlled_read_probe(REPO_ROOT, _plan(), env={"MOEX_DATA_ROOT": str(tmp_path)})

    assert evidence.status == "available"
    assert evidence.paths[0].exists is True


def test_missing_env_fails_closed():
    package = load_futures_data_lake_contract_package(REPO_ROOT)

    with pytest.raises(FuturesControlledReadError):
        controlled_read_paths(package, _plan(), env={})


def test_dynamic_markers_are_rejected(tmp_path):
    package = load_futures_data_lake_contract_package(REPO_ROOT)

    for term in _dynamic_terms():
        with pytest.raises(FuturesControlledReadError):
            controlled_read_paths(package, _plan(family=term), env={"MOEX_DATA_ROOT": str(tmp_path)})


def test_absolute_architecture_contract_path_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        expand_contract_path(
            "/home/trader/moex_bot/data/futures/raw_5m/trade_date={YYYY-MM-DD}/part.parquet",
            str(tmp_path),
            {"YYYY-MM-DD": "2026-06-02"},
        )


def test_blocked_continuous_contract_is_rejected(tmp_path):
    package = load_futures_data_lake_contract_package(REPO_ROOT)

    with pytest.raises(FuturesControlledReadError):
        controlled_read_paths(
            package,
            _plan(dataset_id="futures_continuous_5m", contract_id="futures_continuous_5m.v1", secid=None),
            env={"MOEX_DATA_ROOT": str(tmp_path)},
        )


def test_unsupported_dataset_is_rejected(tmp_path):
    package = load_futures_data_lake_contract_package(REPO_ROOT)

    with pytest.raises(FuturesControlledReadError):
        controlled_read_paths(
            package,
            _plan(dataset_id="futures_unknown", contract_id="futures_unknown.v1"),
            env={"MOEX_DATA_ROOT": str(tmp_path)},
        )


def test_unbounded_and_over_limit_ranges_are_rejected(tmp_path):
    package = load_futures_data_lake_contract_package(REPO_ROOT)

    with pytest.raises(FuturesControlledReadError):
        controlled_read_paths(
            package,
            _plan(trade_date=None, from_trade_date=None, till_trade_date=None),
            env={"MOEX_DATA_ROOT": str(tmp_path)},
        )

    with pytest.raises(FuturesControlledReadError):
        controlled_read_paths(
            package,
            _plan(trade_date=None, from_trade_date="2026-06-01", till_trade_date="2026-06-09"),
            env={"MOEX_DATA_ROOT": str(tmp_path)},
        )


def test_bounded_date_range_uses_explicit_contract_expansion(tmp_path):
    package = load_futures_data_lake_contract_package(REPO_ROOT)

    paths = controlled_read_paths(
        package,
        _plan(trade_date=None, from_trade_date="2026-06-01", till_trade_date="2026-06-03"),
        env={"MOEX_DATA_ROOT": str(tmp_path)},
    )

    assert len(paths) == 3
    assert paths[0].as_posix().endswith("trade_date=2026-06-01/family=Si/secid=SiM6/part.parquet")
    assert paths[-1].as_posix().endswith("trade_date=2026-06-03/family=Si/secid=SiM6/part.parquet")


def test_no_forbidden_imports_in_controlled_read_boundary():
    imported_names = []
    for relative_path in CONTROLLED_READ_SOURCE_PATHS:
        source_path = REPO_ROOT / relative_path
        assert source_path.exists(), relative_path
        imported_names.extend(_imported_names_from_source(source_path.read_text(encoding="utf-8")))

    for term in _guard_terms():
        assert not any(imported_name.startswith(term) for imported_name in imported_names)
