from __future__ import annotations

from collections.abc import Sequence
from typing import Final

import pandas as pd

from moex_research.external_data import moex_cnyrubf_algopack_history as source
from moex_research.runners import (
    usdrubf_phase8_6a_algopack_cnyrubf_source_validation as runner,
)


OPTIONAL_INITIAL_MARGIN_COLUMNS: Final[frozenset[str]] = frozenset(
    {"initial_margin_close", "cnyrubf_initial_margin_close"}
)

_ORIGINAL_NUMBER = source._number
_ORIGINAL_FINITE = runner._finite
_INSTALLED = False


def _number_allowing_missing_initial_margin(
    value: object,
    field: str,
    *,
    nonnegative: bool = False,
) -> float | None:
    if field == "im" and (value is None or str(value).strip() == ""):
        return None
    return _ORIGINAL_NUMBER(value, field, nonnegative=nonnegative)


def _coverage_without_optional_initial_margin(
    matrix: pd.DataFrame,
    validation: pd.DataFrame,
) -> pd.DataFrame:
    mask = pd.MultiIndex.from_frame(
        matrix.loc[:, runner.IDENTITY_COLUMNS]
    ).isin(pd.MultiIndex.from_frame(validation))
    required_columns = tuple(
        column
        for column in runner.ACCEPTANCE_MATRIX_COLUMNS[3:]
        if column not in OPTIONAL_INITIAL_MARGIN_COLUMNS
    )
    complete = matrix.loc[:, required_columns].notna().all(axis=1)
    eligible_covered = int(complete.sum())
    validation_count = int(mask.sum())
    validation_covered = int(complete.to_numpy()[mask].sum())
    return pd.DataFrame(
        [
            {
                "source_id": runner.SOURCE_ID,
                "eligible_identity_count": len(matrix),
                "eligible_covered_count": eligible_covered,
                "eligible_missing_count": len(matrix) - eligible_covered,
                "eligible_coverage_pct": (
                    eligible_covered / len(matrix) * 100 if len(matrix) else 0.0
                ),
                "validation_identity_count": validation_count,
                "validation_covered_count": validation_covered,
                "validation_missing_count": validation_count - validation_covered,
                "validation_coverage_pct": (
                    validation_covered / validation_count * 100
                    if validation_count
                    else 0.0
                ),
            }
        ]
    )


def _finite_without_optional_initial_margin(
    frame: pd.DataFrame,
    columns: Sequence[str],
) -> bool:
    required_columns = tuple(
        column
        for column in columns
        if column not in OPTIONAL_INITIAL_MARGIN_COLUMNS
    )
    return _ORIGINAL_FINITE(frame, required_columns)


def install_nullable_initial_margin_policy() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    source._number = _number_allowing_missing_initial_margin
    runner._coverage = _coverage_without_optional_initial_margin
    runner._finite = _finite_without_optional_initial_margin
    _INSTALLED = True


__all__ = [
    "OPTIONAL_INITIAL_MARGIN_COLUMNS",
    "install_nullable_initial_margin_policy",
]
