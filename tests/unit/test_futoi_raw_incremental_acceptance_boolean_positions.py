from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import futoi_raw_incremental_acceptance as acceptance


@pytest.mark.parametrize(
    "field",
    ("pos", "pos_long", "pos_short", "pos_long_num", "pos_short_num"),
)
def test_rejects_boolean_typed_position_field_after_parquet_round_trip(
    tmp_path: Path,
    field: str,
) -> None:
    frame = pd.DataFrame(
        {
            "pos": [20],
            "pos_long": [100],
            "pos_short": [-80],
            "pos_long_num": [10],
            "pos_short_num": [9],
        }
    )
    frame[field] = pd.Series([True], dtype="bool")
    path = tmp_path / "part.parquet"
    frame.to_parquet(path, index=False)
    round_tripped = pd.read_parquet(path)

    assert pd.api.types.is_bool_dtype(round_tripped[field].dtype)
    with pytest.raises(
        acceptance.FutoiIncrementalAcceptanceError,
        match="position/count field must not contain boolean values",
    ):
        acceptance._validate_position_invariants(round_tripped)
