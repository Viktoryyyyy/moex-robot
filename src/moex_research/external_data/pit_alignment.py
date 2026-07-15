from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final

import numpy as np
import pandas as pd


IDENTITY_COLUMNS: Final[tuple[str, str]] = (
    "target_trade_date",
    "target_instrument_id",
)
RUONIA_SOURCE_COLUMNS: Final[tuple[str, ...]] = (
    "observation_date",
    "publication_date",
    "ruonia_rate_pct",
    "transaction_volume_rub_bn",
    "transaction_count",
    "participant_count",
    "minimum_rate_pct",
    "percentile_25_rate_pct",
    "percentile_75_rate_pct",
    "maximum_rate_pct",
    "calculation_status",
)
RUONIA_MATRIX_COLUMNS: Final[tuple[str, ...]] = (
    "ruonia_observation_date",
    "ruonia_publication_date",
    "ruonia_rate_pct",
    "ruonia_transaction_volume_rub_bn",
    "ruonia_transaction_count",
    "ruonia_participant_count",
    "ruonia_minimum_rate_pct",
    "ruonia_percentile_25_rate_pct",
    "ruonia_percentile_75_rate_pct",
    "ruonia_maximum_rate_pct",
    "ruonia_calculation_status",
    "ruonia_observation_age_calendar_days",
    "ruonia_publication_age_calendar_days",
)
KEY_RATE_SOURCE_COLUMNS: Final[tuple[str, str]] = (
    "effective_date",
    "key_rate_pct",
)
KEY_RATE_MATRIX_COLUMNS: Final[tuple[str, str, str]] = (
    "key_rate_effective_date",
    "key_rate_pct",
    "key_rate_age_calendar_days",
)
MATRIX_COLUMNS: Final[tuple[str, ...]] = (
    *IDENTITY_COLUMNS,
    *RUONIA_MATRIX_COLUMNS,
    *KEY_RATE_MATRIX_COLUMNS,
    "ruonia_minus_key_rate_pp",
)

_RUONIA_FLOAT_COLUMNS: Final[tuple[str, ...]] = (
    "ruonia_rate_pct",
    "transaction_volume_rub_bn",
    "minimum_rate_pct",
    "percentile_25_rate_pct",
    "percentile_75_rate_pct",
    "maximum_rate_pct",
)
_RUONIA_INTEGER_COLUMNS: Final[tuple[str, ...]] = (
    "transaction_count",
    "participant_count",
)


class PITAlignmentError(ValueError):
    """Raised when a point-in-time source alignment must fail closed."""


def align_ruonia(
    identities: pd.DataFrame,
    records: Iterable[Mapping[str, object]],
) -> pd.DataFrame:
    targets = _prepare_identities(identities)
    source = _prepare_ruonia(records)
    publication_dates = source["_publication_date"].to_numpy(dtype="datetime64[ns]")

    rows: list[dict[str, object]] = []
    for target in targets.itertuples(index=False):
        target_date = pd.Timestamp(target.target_trade_date)
        position = int(
            np.searchsorted(
                publication_dates,
                target_date.to_datetime64(),
                side="left",
            )
        ) - 1
        if position < 0:
            raise PITAlignmentError(
                "first eligible identity has no prior-published RUONIA coverage"
            )
        selected = source.iloc[position]
        publication_date = pd.Timestamp(selected["_publication_date"])
        observation_date = pd.Timestamp(selected["_observation_date"])
        if publication_date >= target_date:
            raise PITAlignmentError(
                "same-day or future RUONIA publication cannot enter the matrix"
            )
        rows.append(
            {
                "ruonia_observation_date": observation_date.strftime("%Y-%m-%d"),
                "ruonia_publication_date": publication_date.strftime("%Y-%m-%d"),
                "ruonia_rate_pct": float(selected["ruonia_rate_pct"]),
                "ruonia_transaction_volume_rub_bn": float(
                    selected["transaction_volume_rub_bn"]
                ),
                "ruonia_transaction_count": int(selected["transaction_count"]),
                "ruonia_participant_count": int(selected["participant_count"]),
                "ruonia_minimum_rate_pct": float(selected["minimum_rate_pct"]),
                "ruonia_percentile_25_rate_pct": float(
                    selected["percentile_25_rate_pct"]
                ),
                "ruonia_percentile_75_rate_pct": float(
                    selected["percentile_75_rate_pct"]
                ),
                "ruonia_maximum_rate_pct": float(selected["maximum_rate_pct"]),
                "ruonia_calculation_status": str(selected["calculation_status"]),
                "ruonia_observation_age_calendar_days": int(
                    (target_date - observation_date).days
                ),
                "ruonia_publication_age_calendar_days": int(
                    (target_date - publication_date).days
                ),
            }
        )
    return pd.DataFrame(rows, columns=RUONIA_MATRIX_COLUMNS)


def align_key_rate(
    identities: pd.DataFrame,
    records: Iterable[Mapping[str, object]],
) -> pd.DataFrame:
    targets = _prepare_identities(identities)
    source = _prepare_key_rate(records)
    effective_dates = source["_effective_date"].to_numpy(dtype="datetime64[ns]")

    rows: list[dict[str, object]] = []
    for target in targets.itertuples(index=False):
        target_date = pd.Timestamp(target.target_trade_date)
        position = int(
            np.searchsorted(
                effective_dates,
                target_date.to_datetime64(),
                side="right",
            )
        ) - 1
        if position < 0:
            raise PITAlignmentError(
                "first eligible identity has no effective key-rate coverage"
            )
        selected = source.iloc[position]
        effective_date = pd.Timestamp(selected["_effective_date"])
        if effective_date > target_date:
            raise PITAlignmentError(
                "future key-rate effective date cannot enter the matrix"
            )
        rows.append(
            {
                "key_rate_effective_date": effective_date.strftime("%Y-%m-%d"),
                "key_rate_pct": float(selected["key_rate_pct"]),
                "key_rate_age_calendar_days": int(
                    (target_date - effective_date).days
                ),
            }
        )
    return pd.DataFrame(rows, columns=KEY_RATE_MATRIX_COLUMNS)


def build_external_pit_matrix(
    identities: pd.DataFrame,
    *,
    ruonia_records: Iterable[Mapping[str, object]],
    key_rate_records: Iterable[Mapping[str, object]],
) -> pd.DataFrame:
    targets = _prepare_identities(identities)
    ruonia = align_ruonia(targets, ruonia_records)
    key_rate = align_key_rate(targets, key_rate_records)
    matrix = pd.concat(
        [targets.reset_index(drop=True), ruonia, key_rate],
        axis=1,
    )
    matrix["ruonia_minus_key_rate_pp"] = (
        matrix["ruonia_rate_pct"] - matrix["key_rate_pct"]
    )
    matrix = matrix.loc[:, MATRIX_COLUMNS]
    if matrix.isna().any().any():
        raise PITAlignmentError("acceptance matrix contains a missing required value")
    return matrix


def _prepare_identities(identities: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in IDENTITY_COLUMNS if column not in identities.columns]
    if missing:
        raise PITAlignmentError(
            "target identities missing required columns: " + ", ".join(missing)
        )
    targets = identities.loc[:, IDENTITY_COLUMNS].copy()
    dates = pd.to_datetime(targets["target_trade_date"], errors="coerce")
    instruments = targets["target_instrument_id"].astype("string").str.strip()
    if dates.isna().any():
        raise PITAlignmentError("target_trade_date contains an invalid date")
    if instruments.isna().any() or instruments.eq("").any():
        raise PITAlignmentError("target_instrument_id contains an empty value")
    targets["target_trade_date"] = dates.dt.strftime("%Y-%m-%d")
    targets["target_instrument_id"] = instruments.astype(str)
    targets = targets.sort_values(list(IDENTITY_COLUMNS), kind="mergesort").reset_index(
        drop=True
    )
    if targets.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise PITAlignmentError("duplicate target identity")
    if targets.empty:
        raise PITAlignmentError("target identity set is empty")
    return targets


def _prepare_ruonia(records: Iterable[Mapping[str, object]]) -> pd.DataFrame:
    source = pd.DataFrame(list(records))
    missing = [column for column in RUONIA_SOURCE_COLUMNS if column not in source.columns]
    if source.empty:
        raise PITAlignmentError("normalized RUONIA source is empty")
    if missing:
        raise PITAlignmentError(
            "normalized RUONIA source missing fields: " + ", ".join(missing)
        )
    source = source.copy()
    source["_observation_date"] = pd.to_datetime(
        source["observation_date"], errors="coerce"
    )
    source["_publication_date"] = pd.to_datetime(
        source["publication_date"], errors="coerce"
    )
    if source[["_observation_date", "_publication_date"]].isna().any().any():
        raise PITAlignmentError("RUONIA source contains an invalid date")
    if (source["_publication_date"] < source["_observation_date"]).any():
        raise PITAlignmentError("RUONIA publication date precedes observation date")
    if source["_observation_date"].duplicated(keep=False).any():
        raise PITAlignmentError("duplicate RUONIA observation identity")
    if source["_publication_date"].duplicated(keep=False).any():
        raise PITAlignmentError("ambiguous RUONIA publication-date tie")

    _require_finite_columns(source, _RUONIA_FLOAT_COLUMNS, source_name="RUONIA")
    for column in _RUONIA_INTEGER_COLUMNS:
        numeric = pd.to_numeric(source[column], errors="coerce")
        if numeric.isna().any() or not np.isfinite(numeric.to_numpy(float)).all():
            raise PITAlignmentError(f"RUONIA {column} is not finite")
        if (numeric < 0).any() or not np.equal(numeric, np.floor(numeric)).all():
            raise PITAlignmentError(f"RUONIA {column} must be a non-negative integer")
        source[column] = numeric.astype("int64")
    status = source["calculation_status"].astype("string").str.strip()
    if status.isna().any() or status.eq("").any():
        raise PITAlignmentError("RUONIA calculation_status is empty")
    source["calculation_status"] = status.astype(str)
    return source.sort_values("_publication_date", kind="mergesort").reset_index(
        drop=True
    )


def _prepare_key_rate(records: Iterable[Mapping[str, object]]) -> pd.DataFrame:
    source = pd.DataFrame(list(records))
    missing = [column for column in KEY_RATE_SOURCE_COLUMNS if column not in source.columns]
    if source.empty:
        raise PITAlignmentError("normalized key-rate source is empty")
    if missing:
        raise PITAlignmentError(
            "normalized key-rate source missing fields: " + ", ".join(missing)
        )
    source = source.copy()
    source["_effective_date"] = pd.to_datetime(source["effective_date"], errors="coerce")
    if source["_effective_date"].isna().any():
        raise PITAlignmentError("key-rate source contains an invalid effective date")
    if source["_effective_date"].duplicated(keep=False).any():
        raise PITAlignmentError("duplicate key-rate effective date")
    _require_finite_columns(source, ("key_rate_pct",), source_name="key rate")
    return source.sort_values("_effective_date", kind="mergesort").reset_index(
        drop=True
    )


def _require_finite_columns(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
    *,
    source_name: str,
) -> None:
    for column in columns:
        numeric = pd.to_numeric(frame[column], errors="coerce")
        if numeric.isna().any() or not np.isfinite(numeric.to_numpy(float)).all():
            raise PITAlignmentError(f"{source_name} {column} is not finite")
        frame[column] = numeric.astype(float)
