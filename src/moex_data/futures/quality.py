from collections.abc import Mapping, Sequence
from typing import Final

from .contracts import _guard_text, _require_text
from .schemas import EXPECTED_DATASET_CONTRACT_IDS, FuturesQualityReport, FuturesQualityRow


class FuturesQualityValidationError(ValueError):
    pass


_QUALITY_ROW_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "run_id",
    "dataset_id",
    "family",
    "secid",
    "trade_date",
    "rows",
    "duplicate_key_count",
    "gap_count",
    "null_ohlc_count",
    "invalid_ohlc_count",
    "futoi_missing_count",
    "calendar_status",
    "quality_status",
)
_ALLOWED_QUALITY_STATUS: Final[frozenset[str]] = frozenset({"pass", "warn", "fail"})


def _require_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise FuturesQualityValidationError(f"{field_name} must be a mapping")
    return value


def _require_int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise FuturesQualityValidationError(f"{field_name} must be int")
    if value < 0:
        raise FuturesQualityValidationError(f"{field_name} must be non-negative")
    return value


def validate_quality_row_values(values: Mapping[str, object]) -> FuturesQualityRow:
    values = _require_mapping(values, "quality_row")
    missing = tuple(field for field in _QUALITY_ROW_REQUIRED_FIELDS if field not in values)
    if missing:
        raise FuturesQualityValidationError("quality row is missing required fields")

    dataset_id = _guard_text(_require_text(values["dataset_id"], "dataset_id"), "dataset_id")
    if f"{dataset_id}.v1" not in EXPECTED_DATASET_CONTRACT_IDS:
        raise FuturesQualityValidationError("dataset_id is not part of futures data lake contract set")
    quality_status = _require_text(values["quality_status"], "quality_status")
    if quality_status not in _ALLOWED_QUALITY_STATUS:
        raise FuturesQualityValidationError("quality_status is unsupported")

    return FuturesQualityRow(
        run_id=_require_text(values["run_id"], "run_id"),
        dataset_id=dataset_id,
        family=_require_text(values["family"], "family"),
        secid=_require_text(values["secid"], "secid"),
        trade_date=_require_text(values["trade_date"], "trade_date"),
        rows=_require_int(values["rows"], "rows"),
        duplicate_key_count=_require_int(values["duplicate_key_count"], "duplicate_key_count"),
        gap_count=_require_int(values["gap_count"], "gap_count"),
        null_ohlc_count=_require_int(values["null_ohlc_count"], "null_ohlc_count"),
        invalid_ohlc_count=_require_int(values["invalid_ohlc_count"], "invalid_ohlc_count"),
        futoi_missing_count=_require_int(values["futoi_missing_count"], "futoi_missing_count"),
        calendar_status=_require_text(values["calendar_status"], "calendar_status"),
        quality_status=quality_status,
    )


def validate_quality_report_rows(rows: Sequence[Mapping[str, object]]) -> FuturesQualityReport:
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
        raise FuturesQualityValidationError("quality rows must be a sequence")
    validated_rows = tuple(validate_quality_row_values(row) for row in rows)
    if not validated_rows:
        raise FuturesQualityValidationError("quality rows must be non-empty")
    run_ids = {row.run_id for row in validated_rows}
    if len(run_ids) != 1:
        raise FuturesQualityValidationError("quality rows must belong to one run_id")
    return FuturesQualityReport(run_id=validated_rows[0].run_id, rows=validated_rows)
