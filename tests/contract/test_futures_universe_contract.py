import pytest

from moex_data.futures.universe import validate_futures_universe_values
from moex_data.futures.validation import FuturesValidationError


def _universe(**overrides):
    values = {
        "universe_id": "futures_universe.v1",
        "artifact_class": "repo_relative",
        "repo_path": "configs/instruments/futures_universe.v1.yaml",
        "dynamic_scan_allowed": False,
        "instruments": (
            {
                "FAMILY": "TEST_FAMILY_A",
                "SECID": "TEST_CONTRACT_A",
                "BOARD": "RFUD",
                "MARKET": "FORTS",
                "SERIES_TYPE": "native",
            },
        ),
    }
    values.update(overrides)
    return values


def test_universe_accepts_explicit_config_only():
    universe = validate_futures_universe_values(_universe())

    assert universe.universe_id == "futures_universe.v1"
    assert len(universe.instruments.instruments) == 1


def test_universe_rejects_dynamic_scan_autodiscovery():
    with pytest.raises(FuturesValidationError):
        validate_futures_universe_values(_universe(dynamic_scan_allowed=True))


@pytest.mark.parametrize("marker", ("latest", "current", "autodetect"))
def test_universe_rejects_dynamic_path_markers(marker):
    with pytest.raises(FuturesValidationError):
        validate_futures_universe_values(_universe(repo_path="configs/instruments/" + marker + ".yaml"))


def test_universe_rejects_missing_instruments():
    with pytest.raises(FuturesValidationError):
        validate_futures_universe_values(_universe(instruments=()))
