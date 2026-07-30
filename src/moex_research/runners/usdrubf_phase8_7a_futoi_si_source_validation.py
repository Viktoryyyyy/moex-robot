from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Final
from urllib.parse import parse_qsl, urlencode, urlsplit
from zoneinfo import ZoneInfo

import pandas as pd

from moex_data.futures.futoi_raw_loader import SCHEMA_FUTOI_RAW, normalize_futoi
from moex_research.external_data import moex_algopack_http as algopack_http

PROJECT: Final[str] = "MOEX_Bot"
PHASE: Final[str] = "8.7A"
TASK_ID: Final[str] = (
    "ema_3_19_ai_phase_8_7a_futoi_si_source_validation_implementation_v1"
)
SOURCE_ID: Final[str] = "moex_algopack_futoi_si_participant_positioning"
SOURCE_TICKER: Final[str] = "Si"
TARGET_SECURITY_ID: Final[str] = "USDRUBF"
TARGET_INSTRUMENT_ID: Final[str] = "forts.usdrubf"
STORAGE_FAMILY_CODE: Final[str] = "USDRUBF"
BOARD_ID: Final[str] = "RFUD"
ENGINE: Final[str] = "futures"
MARKET: Final[str] = "forts"
ALGOPACK_HOST: Final[str] = "apim.moex.com"
FUTOI_PATH: Final[str] = "/iss/analyticalproducts/futoi/securities/si.json"
FUTOI_ROUTE: Final[str] = f"https://{ALGOPACK_HOST}{FUTOI_PATH}"
MOSCOW: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")
FORECAST_ANCHOR: Final[time] = time(6, 0)
SOURCE_REVISION_STATUS: Final[str] = "algopack_futoi_current_revision"
EXPECTED_ELIGIBLE_IDENTITIES: Final[int] = 472
EXPECTED_VALIDATION_IDENTITIES: Final[int] = 320
EXPECTED_LICENSE_PROVIDER: Final[str] = "MOEX AlgoPack FUTOI"
EXPECTED_LICENSE_PRODUCT: Final[str] = "AlgoPack FUTOI"
PARTICIPANT_GROUPS: Final[tuple[str, str]] = ("FIZ", "YUR")
RAW_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "sess_id",
    "ticker",
    "clgroup",
    "pos",
    "pos_long",
    "pos_short",
    "pos_long_num",
    "pos_short_num",
    "seqnum",
    "moment",
    "systime",
)
FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "target_phase_label",
        "target_is_labeled",
        "target_source",
        "fold_id",
        "y_true",
        "candidate_y_pred",
        "prediction",
        "probability_B",
        "probability_S",
        "probability_OUT",
    }
)
BLOCKER_CLASSIFICATIONS: Final[frozenset[str]] = frozenset(
    {
        "token_env_not_configured",
        "algopack_authentication_failed",
        "algopack_subscription_not_entitled",
        "official_route_not_reproducible",
        "futoi_si_not_available",
        "algopack_rate_limit_blocked",
        "algopack_futoi_not_available",
        "provider_license_and_access_terms_not_documented",
        "official_schema_not_stable",
        "point_in_time_cutoff_not_provable",
        "incomplete_identity_coverage",
        "numerical_or_chronology_integrity_failure",
        "provenance_not_sufficient",
        "target_derived_field_leakage",
        "other_fail_closed_with_exact_reason",
    }
)
REQUIRED_RUNTIME_ARTIFACTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "official_route_validation.json",
    "futoi_si_license_access_validation.json",
    "futoi_si_schema_profile.json",
    "futoi_si_daily_positioning.parquet",
    "futoi_si_pit_acceptance_matrix.parquet",
    "coverage_by_source.csv",
    "session_alignment_diagnostics.csv",
    "source_blocker_register.json",
    "gate_results.json",
)

FutoiTransport = Callable[[str, str], bytes]
TokenLoader = Callable[[], str]
UtcClock = Callable[[], datetime]


class FutoiSiSourceValidationError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        blocker: str = "other_fail_closed_with_exact_reason",
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.blocker = (
            blocker
            if blocker in BLOCKER_CLASSIFICATIONS
            else "other_fail_closed_with_exact_reason"
        )
        self.retryable = bool(retryable)


@dataclass(frozen=True)
class FutoiParticipantRow:
    trade_date: date
    moment: datetime
    source_available_at: datetime
    sess_id: str
    ticker: str
    clgroup: str
    pos: float
    pos_long: float
    pos_short: float
    pos_long_num: int
    pos_short_num: int
    seqnum: int


@dataclass(frozen=True)
class FutoiDailyPair:
    source_id: str
    source_ticker: str
    target_security_id: str
    target_instrument_id: str
    storage_family_code: str
    board_id: str
    engine: str
    market: str
    trade_date: date
    moment: datetime
    sess_id: str
    source_available_at: datetime
    fiz_pos: float
    fiz_pos_long: float
    fiz_pos_short: float
    fiz_pos_long_num: int
    fiz_pos_short_num: int
    fiz_seqnum: int
    fiz_systime: datetime
    yur_pos: float
    yur_pos_long: float
    yur_pos_short: float
    yur_pos_long_num: int
    yur_pos_short_num: int
    yur_seqnum: int
    yur_systime: datetime
    source_route: str
    retrieved_at_utc: datetime
    raw_payload_sha256: str
    source_revision_status: str

    def as_record(self) -> dict[str, object]:
        return asdict(self)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc(value: datetime) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise FutoiSiSourceValidationError(
            "retrieval timestamp must be timezone-aware",
            blocker="provenance_not_sufficient",
        )
    return value.astimezone(timezone.utc)


def build_futoi_url(trade_date: date) -> str:
    return FUTOI_ROUTE + "?" + urlencode(
        {"from": trade_date.isoformat(), "till": trade_date.isoformat(), "latest": 1}
    )


def _exact_futoi_route_date(url: str) -> date:
    parsed = urlsplit(str(url))
    if (
        parsed.scheme != "https"
        or parsed.hostname != ALGOPACK_HOST
        or parsed.port not in (None, 443)
        or parsed.username
        or parsed.password
        or parsed.path != FUTOI_PATH
        or parsed.fragment
    ):
        raise FutoiSiSourceValidationError(
            "route is not the exact allowlisted AlgoPack FUTOI Si route",
            blocker="provenance_not_sufficient",
        )
    try:
        pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise FutoiSiSourceValidationError(
            "FUTOI route query is malformed",
            blocker="provenance_not_sufficient",
        ) from exc
    if len(pairs) != 3 or len({key for key, _ in pairs}) != 3:
        raise FutoiSiSourceValidationError(
            "FUTOI route query is not exact",
            blocker="provenance_not_sufficient",
        )
    query = dict(pairs)
    if set(query) != {"from", "till", "latest"} or query["latest"] != "1":
        raise FutoiSiSourceValidationError(
            "FUTOI route query is not exact",
            blocker="provenance_not_sufficient",
        )
    try:
        found_from = date.fromisoformat(query["from"])
        found_till = date.fromisoformat(query["till"])
    except ValueError as exc:
        raise FutoiSiSourceValidationError(
            "FUTOI route date is invalid",
            blocker="provenance_not_sufficient",
        ) from exc
    if found_from != found_till or str(url) != build_futoi_url(found_from):
        raise FutoiSiSourceValidationError(
            "FUTOI request must pin one exact trade date",
            blocker="provenance_not_sufficient",
        )
    return found_from


def _marker_text(error: algopack_http.AlgoPackHttpError) -> str:
    return " ".join(error.sanitized_header_markers).lower()


def _map_transport_error(
    error: algopack_http.AlgoPackHttpError,
) -> FutoiSiSourceValidationError:
    outcome = error.transport_outcome
    if outcome in {
        "token_env_not_configured",
        "algopack_authentication_failed",
        "algopack_subscription_not_entitled",
        "algopack_rate_limit_blocked",
    }:
        blocker = outcome
    elif outcome == "algopack_http_not_found":
        markers = _marker_text(error)
        blocker = (
            "futoi_si_not_available"
            if any(word in markers for word in ("ticker", "security", "source"))
            else "official_route_not_reproducible"
        )
    elif outcome == "algopack_http_redirect_refused":
        blocker = "provenance_not_sufficient"
    else:
        blocker = "algopack_futoi_not_available"
    return FutoiSiSourceValidationError(
        str(error), blocker=blocker, retryable=error.retryable
    )


def fetch_futoi_bytes(
    url: str,
    bearer_token: str,
    *,
    opener: algopack_http.HttpOpener = algopack_http.open_without_redirects,
) -> bytes:
    try:
        return algopack_http.fetch_algopack_bytes_with_retry(
            url,
            bearer_token,
            route_validator=_exact_futoi_route_date,
            opener=opener,
            user_agent="moex-robot-algopack-futoi-si/1.0",
        )
    except algopack_http.AlgoPackHttpError as exc:
        raise _map_transport_error(exc) from None


def _json_object(payload: bytes) -> dict[str, Any]:
    try:
        root = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FutoiSiSourceValidationError(
            "FUTOI response is not valid UTF-8 JSON",
            blocker="official_schema_not_stable",
        ) from exc
    if not isinstance(root, dict):
        raise FutoiSiSourceValidationError(
            "FUTOI response root must be an object",
            blocker="official_schema_not_stable",
        )
    return root


def _data_rows(payload: bytes) -> tuple[list[dict[str, object]], tuple[str, ...]]:
    block = _json_object(payload).get("data")
    if not isinstance(block, Mapping):
        raise FutoiSiSourceValidationError(
            "FUTOI response lacks data block",
            blocker="official_schema_not_stable",
        )
    columns = block.get("columns")
    data = block.get("data")
    if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
        raise FutoiSiSourceValidationError(
            "FUTOI columns are malformed",
            blocker="official_schema_not_stable",
        )
    if not set(RAW_REQUIRED_FIELDS).issubset(columns):
        missing = sorted(set(RAW_REQUIRED_FIELDS).difference(columns))
        raise FutoiSiSourceValidationError(
            "FUTOI schema is missing required fields: " + ",".join(missing),
            blocker=(
                "point_in_time_cutoff_not_provable"
                if "systime" in missing
                else "official_schema_not_stable"
            ),
        )
    if not isinstance(data, list):
        raise FutoiSiSourceValidationError(
            "FUTOI data rows are malformed",
            blocker="official_schema_not_stable",
        )
    rows: list[dict[str, object]] = []
    for raw in data:
        if not isinstance(raw, list) or len(raw) != len(columns):
            raise FutoiSiSourceValidationError(
                "FUTOI row width mismatch",
                blocker="official_schema_not_stable",
            )
        rows.append(dict(zip(columns, raw, strict=True)))
    return rows, tuple(columns)


def _validate_raw_si_ticker_identity(
    raw_rows: Sequence[Mapping[str, object]],
) -> None:
    if not raw_rows:
        raise FutoiSiSourceValidationError(
            "FUTOI provider payload is empty",
            blocker="incomplete_identity_coverage",
        )
    tickers = [str(row.get("ticker") or "").strip() for row in raw_rows]
    if any(ticker != SOURCE_TICKER for ticker in tickers):
        raise FutoiSiSourceValidationError(
            "FUTOI provider rows do not all carry exact ticker Si",
            blocker="provenance_not_sufficient",
        )


def _canonical_normalized_rows(
    raw_rows: Sequence[Mapping[str, object]],
    *,
    route: str,
    retrieved_at_utc: datetime,
) -> list[dict[str, object]]:
    _validate_raw_si_ticker_identity(raw_rows)
    normalized, metadata = normalize_futoi(
        pd.DataFrame(raw_rows),
        secid=TARGET_SECURITY_ID,
        family_code=STORAGE_FAMILY_CODE,
        board=BOARD_ID,
        source_url=route,
        source_ticker=SOURCE_TICKER,
        ingest_ts=_utc(retrieved_at_utc).isoformat().replace("+00:00", "Z"),
        short_history_flag=False,
        calendar_status="phase8_7a_source_validation",
    )
    error = str(metadata.get("error") or "").strip()
    if error or normalized.empty or len(normalized) != len(raw_rows):
        raise FutoiSiSourceValidationError(
            "canonical FUTOI normalizer rejected or filtered provider rows"
            + (f": {error}" if error else ""),
            blocker="official_schema_not_stable",
        )
    required = {
        "trade_date",
        "moment",
        "systime",
        "secid",
        "family_code",
        "source_ticker",
        "source_scope",
        "clgroup",
        "pos",
        "pos_long",
        "pos_short",
        "pos_long_num",
        "pos_short_num",
        "sess_id",
        "seqnum",
        "schema_version",
    }
    if not required.issubset(normalized.columns):
        raise FutoiSiSourceValidationError(
            "canonical FUTOI normalized schema is incomplete",
            blocker="official_schema_not_stable",
        )
    if (
        not normalized["secid"].astype(str).eq(TARGET_SECURITY_ID).all()
        or not normalized["family_code"].astype(str).eq(STORAGE_FAMILY_CODE).all()
        or not normalized["source_ticker"].astype(str).str.upper().eq("SI").all()
        or not normalized["source_scope"].astype(str).eq("family_aggregate_futoi").all()
        or not normalized["schema_version"].astype(str).eq(SCHEMA_FUTOI_RAW).all()
    ):
        raise FutoiSiSourceValidationError(
            "canonical FUTOI storage or schema identity mismatch",
            blocker="provenance_not_sufficient",
        )
    return normalized.to_dict(orient="records")


def _number(value: object, field: str) -> float:
    if isinstance(value, bool):
        raise FutoiSiSourceValidationError(
            f"FUTOI {field} is malformed",
            blocker="numerical_or_chronology_integrity_failure",
        )
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise FutoiSiSourceValidationError(
            f"FUTOI {field} is malformed",
            blocker="numerical_or_chronology_integrity_failure",
        ) from exc
    if not math.isfinite(result):
        raise FutoiSiSourceValidationError(
            f"FUTOI {field} must be finite",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return result


def _integer(value: object, field: str, *, nonnegative: bool = True) -> int:
    number = _number(value, field)
    if not number.is_integer() or (nonnegative and number < 0):
        raise FutoiSiSourceValidationError(
            f"FUTOI {field} must be a non-negative integer",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return int(number)


def _required_identifier(value: object, field: str) -> str:
    if value is None or pd.isna(value) or isinstance(value, bool):
        raise FutoiSiSourceValidationError(
            f"FUTOI {field} identity is missing or malformed",
            blocker="official_schema_not_stable",
        )
    if isinstance(value, (int, float)):
        number = float(value)
        if not math.isfinite(number) or not number.is_integer():
            raise FutoiSiSourceValidationError(
                f"FUTOI {field} identity is malformed",
                blocker="official_schema_not_stable",
            )
        text = str(int(number))
    else:
        text = str(value).strip()
    if not text:
        raise FutoiSiSourceValidationError(
            f"FUTOI {field} identity is missing",
            blocker="official_schema_not_stable",
        )
    return text


def _provider_timestamp(value: object, field: str) -> datetime:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise FutoiSiSourceValidationError(
            f"FUTOI {field} timestamp is malformed",
            blocker=(
                "point_in_time_cutoff_not_provable"
                if field == "systime"
                else "numerical_or_chronology_integrity_failure"
            ),
        ) from exc
    if pd.isna(parsed):
        raise FutoiSiSourceValidationError(
            f"FUTOI {field} timestamp is missing",
            blocker=(
                "point_in_time_cutoff_not_provable"
                if field == "systime"
                else "numerical_or_chronology_integrity_failure"
            ),
        )
    parsed = parsed.tz_localize(MOSCOW) if parsed.tzinfo is None else parsed.tz_convert(MOSCOW)
    return parsed.to_pydatetime()


def _participant(
    row: Mapping[str, object],
    requested_date: date,
) -> FutoiParticipantRow:
    trade_date = date.fromisoformat(str(row.get("trade_date")))
    group = str(row.get("clgroup") or "").strip().upper()
    if trade_date != requested_date:
        raise FutoiSiSourceValidationError(
            "canonical FUTOI trade date differs from the requested date",
            blocker="numerical_or_chronology_integrity_failure",
        )
    if group not in PARTICIPANT_GROUPS:
        raise FutoiSiSourceValidationError(
            "FUTOI response contains an unexpected participant group",
            blocker="official_schema_not_stable",
        )
    moment = _provider_timestamp(row.get("moment"), "moment")
    systime = _provider_timestamp(row.get("systime"), "systime")
    if moment.date() != requested_date:
        raise FutoiSiSourceValidationError(
            "FUTOI moment is outside the requested trade date",
            blocker="numerical_or_chronology_integrity_failure",
        )
    if systime < moment:
        raise FutoiSiSourceValidationError(
            "FUTOI systime precedes the observation moment",
            blocker="point_in_time_cutoff_not_provable",
        )
    pos = _number(row.get("pos"), "pos")
    pos_long = _number(row.get("pos_long"), "pos_long")
    pos_short = _number(row.get("pos_short"), "pos_short")
    if pos_long < 0 or pos_short > 0:
        raise FutoiSiSourceValidationError(
            "FUTOI long/short signs violate the provider contract",
            blocker="numerical_or_chronology_integrity_failure",
        )
    if not math.isclose(pos, pos_long + pos_short, rel_tol=0.0, abs_tol=1e-9):
        raise FutoiSiSourceValidationError(
            "FUTOI net position identity failed",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return FutoiParticipantRow(
        trade_date=requested_date,
        moment=moment,
        source_available_at=systime,
        sess_id=_required_identifier(row.get("sess_id"), "sess_id"),
        ticker=SOURCE_TICKER,
        clgroup=group,
        pos=pos,
        pos_long=pos_long,
        pos_short=pos_short,
        pos_long_num=_integer(row.get("pos_long_num"), "pos_long_num"),
        pos_short_num=_integer(row.get("pos_short_num"), "pos_short_num"),
        seqnum=_integer(row.get("seqnum"), "seqnum"),
    )


def parse_futoi_daily_response(
    payload: bytes,
    *,
    trade_date: date,
    route: str,
    retrieved_at_utc: datetime,
) -> tuple[FutoiDailyPair, tuple[str, ...]]:
    if _exact_futoi_route_date(route) != trade_date:
        raise FutoiSiSourceValidationError(
            "FUTOI route date differs from the requested date",
            blocker="provenance_not_sufficient",
        )
    raw_rows, columns = _data_rows(payload)
    normalized_rows = _canonical_normalized_rows(
        raw_rows, route=route, retrieved_at_utc=retrieved_at_utc
    )
    participants = [_participant(row, trade_date) for row in normalized_rows]
    identities: set[tuple[date, datetime, str, str, int]] = set()
    by_pair: dict[tuple[date, datetime, str], dict[str, FutoiParticipantRow]] = {}
    for row in participants:
        identity = (row.trade_date, row.moment, row.sess_id, row.clgroup, row.seqnum)
        if identity in identities:
            raise FutoiSiSourceValidationError(
                "FUTOI provider row identity is duplicated",
                blocker="official_schema_not_stable",
            )
        identities.add(identity)
        key = (row.trade_date, row.moment, row.sess_id)
        groups = by_pair.setdefault(key, {})
        if row.clgroup in groups:
            raise FutoiSiSourceValidationError(
                "FUTOI pair contains duplicate participant group rows",
                blocker="official_schema_not_stable",
            )
        groups[row.clgroup] = row
    complete = [
        (key, groups)
        for key, groups in by_pair.items()
        if set(groups) == set(PARTICIPANT_GROUPS)
    ]
    if not complete:
        raise FutoiSiSourceValidationError(
            "FUTOI response lacks one aligned FIZ/YUR pair",
            blocker="incomplete_identity_coverage",
        )
    latest_moment = max(key[1] for key, _groups in complete)
    latest = [(key, groups) for key, groups in complete if key[1] == latest_moment]
    if len(latest) != 1:
        raise FutoiSiSourceValidationError(
            "FUTOI latest common pair is ambiguous",
            blocker="official_schema_not_stable",
        )
    key, groups = latest[0]
    fiz, yur = groups["FIZ"], groups["YUR"]
    if not math.isclose(fiz.pos + yur.pos, 0.0, rel_tol=0.0, abs_tol=1e-9):
        raise FutoiSiSourceValidationError(
            "FUTOI FIZ/YUR zero-sum identity failed",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return (
        FutoiDailyPair(
            source_id=SOURCE_ID,
            source_ticker=SOURCE_TICKER,
            target_security_id=TARGET_SECURITY_ID,
            target_instrument_id=TARGET_INSTRUMENT_ID,
            storage_family_code=STORAGE_FAMILY_CODE,
            board_id=BOARD_ID,
            engine=ENGINE,
            market=MARKET,
            trade_date=trade_date,
            moment=key[1],
            sess_id=key[2],
            source_available_at=max(fiz.source_available_at, yur.source_available_at),
            fiz_pos=fiz.pos,
            fiz_pos_long=fiz.pos_long,
            fiz_pos_short=fiz.pos_short,
            fiz_pos_long_num=fiz.pos_long_num,
            fiz_pos_short_num=fiz.pos_short_num,
            fiz_seqnum=fiz.seqnum,
            fiz_systime=fiz.source_available_at,
            yur_pos=yur.pos,
            yur_pos_long=yur.pos_long,
            yur_pos_short=yur.pos_short,
            yur_pos_long_num=yur.pos_long_num,
            yur_pos_short_num=yur.pos_short_num,
            yur_seqnum=yur.seqnum,
            yur_systime=yur.source_available_at,
            source_route=route,
            retrieved_at_utc=_utc(retrieved_at_utc),
            raw_payload_sha256=hashlib.sha256(payload).hexdigest(),
            source_revision_status=SOURCE_REVISION_STATUS,
        ),
        columns,
    )


def load_futoi_daily_pair(
    trade_date: date,
    *,
    bearer_token: str | None = None,
    transport: FutoiTransport = fetch_futoi_bytes,
    token_loader: TokenLoader = algopack_http.load_algopack_token,
    clock: UtcClock = utc_now,
) -> tuple[FutoiDailyPair, tuple[str, ...]]:
    token = str(bearer_token).strip() if bearer_token is not None else token_loader()
    if not token:
        raise FutoiSiSourceValidationError(
            "AlgoPack token environment variable is not configured",
            blocker="token_env_not_configured",
        )
    route = build_futoi_url(trade_date)
    return parse_futoi_daily_response(
        transport(route, token),
        trade_date=trade_date,
        route=route,
        retrieved_at_utc=clock(),
    )


def validate_prior_session_pair(
    pair: FutoiDailyPair,
    *,
    target_trade_date: date,
    prior_trade_date: date,
) -> None:
    anchor = datetime.combine(target_trade_date, FORECAST_ANCHOR, tzinfo=MOSCOW)
    if (
        pair.trade_date != prior_trade_date
        or pair.moment.date() != prior_trade_date
        or pair.source_available_at < pair.moment
        or pair.moment >= anchor
        or pair.source_available_at > anchor
    ):
        raise FutoiSiSourceValidationError(
            "selected FUTOI Si pair cannot prove exact prior-session availability before or at the forecast anchor",
            blocker="point_in_time_cutoff_not_provable",
        )


DAILY_POSITIONING_COLUMNS: Final[tuple[str, ...]] = tuple(
    FutoiDailyPair.__dataclass_fields__
)
ACCEPTANCE_MATRIX_COLUMNS: Final[tuple[str, ...]] = (
    "target_trade_date",
    "target_instrument_id",
    "prior_trade_date",
    "futoi_trade_date",
    "futoi_moment",
    "futoi_sess_id",
    "futoi_source_available_at",
    "fiz_pos",
    "fiz_pos_long",
    "fiz_pos_short",
    "fiz_pos_long_num",
    "fiz_pos_short_num",
    "fiz_seqnum",
    "yur_pos",
    "yur_pos_long",
    "yur_pos_short",
    "yur_pos_long_num",
    "yur_pos_short_num",
    "yur_seqnum",
    "source_route",
    "raw_payload_sha256",
    "source_revision_status",
)
DIAGNOSTIC_COLUMNS: Final[tuple[str, ...]] = (
    "target_trade_date",
    "prior_trade_date",
    "candidate_trade_date",
    "accepted",
    "reason",
    "blocker_classification",
    "same_day_or_future_used",
    "forward_fill_used",
    "backward_fill_used",
    "nearest_date_substitution_used",
    "arbitrary_last_row_used",
    "source_substitution_used",
    "target_derived_field_used",
)


def _empty_acceptance_source() -> dict[str, object | None]:
    return {name: None for name in ACCEPTANCE_MATRIX_COLUMNS[3:]}


def _accepted_pair_record(pair: FutoiDailyPair) -> dict[str, object]:
    return {
        "futoi_trade_date": pair.trade_date.isoformat(),
        "futoi_moment": pair.moment.isoformat(),
        "futoi_sess_id": pair.sess_id,
        "futoi_source_available_at": pair.source_available_at.isoformat(),
        "fiz_pos": pair.fiz_pos,
        "fiz_pos_long": pair.fiz_pos_long,
        "fiz_pos_short": pair.fiz_pos_short,
        "fiz_pos_long_num": pair.fiz_pos_long_num,
        "fiz_pos_short_num": pair.fiz_pos_short_num,
        "fiz_seqnum": pair.fiz_seqnum,
        "yur_pos": pair.yur_pos,
        "yur_pos_long": pair.yur_pos_long,
        "yur_pos_short": pair.yur_pos_short,
        "yur_pos_long_num": pair.yur_pos_long_num,
        "yur_pos_short_num": pair.yur_pos_short_num,
        "yur_seqnum": pair.yur_seqnum,
        "source_route": pair.source_route,
        "raw_payload_sha256": pair.raw_payload_sha256,
        "source_revision_status": pair.source_revision_status,
    }


def build_futoi_pit_acceptance_matrix(
    eligible: pd.DataFrame,
    pairs: Sequence[FutoiDailyPair],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = {"target_trade_date", "target_instrument_id", "prior_trade_date"}
    if not required.issubset(eligible.columns):
        raise FutoiSiSourceValidationError(
            "eligible identity frame lacks required columns",
            blocker="provenance_not_sufficient",
        )
    keyed: dict[date, FutoiDailyPair] = {}
    for pair in pairs:
        if pair.trade_date in keyed:
            raise FutoiSiSourceValidationError(
                "duplicate FUTOI daily source date",
                blocker="numerical_or_chronology_integrity_failure",
            )
        keyed[pair.trade_date] = pair
    rows: list[dict[str, object | None]] = []
    diagnostics: list[dict[str, object | None]] = []
    for identity in eligible.itertuples(index=False):
        target = date.fromisoformat(str(identity.target_trade_date))
        prior = date.fromisoformat(str(identity.prior_trade_date))
        pair = keyed.get(prior)
        accepted = False
        blocker: str | None = None
        candidate = None if pair is None else pair.trade_date.isoformat()
        if pair is None:
            source_row = _empty_acceptance_source()
            reason = "missing_exact_prior_trade_date_futoi_pair"
            blocker = "incomplete_identity_coverage"
        else:
            try:
                validate_prior_session_pair(
                    pair, target_trade_date=target, prior_trade_date=prior
                )
            except FutoiSiSourceValidationError as exc:
                source_row = _empty_acceptance_source()
                reason, blocker = str(exc), exc.blocker
            else:
                source_row = _accepted_pair_record(pair)
                accepted = True
                reason = "accepted_exact_prior_trade_date_futoi_pair"
        rows.append(
            {
                "target_trade_date": str(identity.target_trade_date),
                "target_instrument_id": str(identity.target_instrument_id),
                "prior_trade_date": str(identity.prior_trade_date),
                **source_row,
            }
        )
        diagnostics.append(
            {
                "target_trade_date": str(identity.target_trade_date),
                "prior_trade_date": str(identity.prior_trade_date),
                "candidate_trade_date": candidate,
                "accepted": accepted,
                "reason": reason,
                "blocker_classification": blocker,
                "same_day_or_future_used": False,
                "forward_fill_used": False,
                "backward_fill_used": False,
                "nearest_date_substitution_used": False,
                "arbitrary_last_row_used": False,
                "source_substitution_used": False,
                "target_derived_field_used": False,
            }
        )
    matrix = pd.DataFrame(rows, columns=ACCEPTANCE_MATRIX_COLUMNS)
    diagnostic_frame = pd.DataFrame(diagnostics, columns=DIAGNOSTIC_COLUMNS)
    if set(matrix.columns) & FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS:
        raise FutoiSiSourceValidationError(
            "acceptance matrix contains target-derived fields",
            blocker="target_derived_field_leakage",
        )
    return matrix, diagnostic_frame


def coverage_by_source(
    matrix: pd.DataFrame,
    validation_identities: pd.DataFrame,
) -> pd.DataFrame:
    identity_columns = ["target_trade_date", "target_instrument_id"]
    validation_index = pd.MultiIndex.from_frame(
        validation_identities.loc[:, identity_columns].astype(str)
    )
    matrix_index = pd.MultiIndex.from_frame(matrix.loc[:, identity_columns].astype(str))
    validation_mask = matrix_index.isin(validation_index)
    complete = matrix.futoi_trade_date.notna()
    validation_count = int(validation_mask.sum())
    validation_covered = int(complete.to_numpy()[validation_mask].sum())
    return pd.DataFrame(
        [
            {
                "source_id": SOURCE_ID,
                "eligible_identity_count": len(matrix),
                "eligible_covered_count": int(complete.sum()),
                "eligible_missing_count": int((~complete).sum()),
                "eligible_coverage_pct": float(complete.mean() * 100.0) if len(matrix) else 0.0,
                "validation_identity_count": validation_count,
                "validation_covered_count": validation_covered,
                "validation_missing_count": validation_count - validation_covered,
                "validation_coverage_pct": (
                    validation_covered / validation_count * 100.0
                    if validation_count
                    else 0.0
                ),
            }
        ]
    )


def _aware_iso_timestamp(value: object) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def validate_license_access_evidence(
    evidence: Mapping[str, object],
) -> tuple[bool, dict[str, object]]:
    provider_identity = bool(
        evidence.get("provider") == EXPECTED_LICENSE_PROVIDER
        and evidence.get("product") == EXPECTED_LICENSE_PRODUCT
    )
    permissions = all(
        evidence.get(name) is True
        for name in (
            "account_entitlement",
            "permitted_research_use",
            "permitted_local_raw_storage",
            "permitted_derived_feature_use",
        )
    )
    redistribution = str(evidence.get("redistribution_policy") or "").strip()
    evidence_source = str(evidence.get("evidence_source") or "").strip()
    verified_at = str(evidence.get("verified_at") or "").strip()
    passed = bool(
        provider_identity
        and permissions
        and redistribution
        and evidence_source
        and _aware_iso_timestamp(verified_at)
    )
    normalized = {
        "provider": str(evidence.get("provider") or ""),
        "product": str(evidence.get("product") or ""),
        "expected_provider": EXPECTED_LICENSE_PROVIDER,
        "expected_product": EXPECTED_LICENSE_PRODUCT,
        "provider_identity_verified": provider_identity,
        "account_entitlement": evidence.get("account_entitlement") is True,
        "permitted_research_use": evidence.get("permitted_research_use") is True,
        "permitted_local_raw_storage": evidence.get("permitted_local_raw_storage") is True,
        "permitted_derived_feature_use": evidence.get("permitted_derived_feature_use") is True,
        "redistribution_policy": redistribution,
        "evidence_source": evidence_source,
        "verified_at": verified_at or None,
        "status": "passed" if passed else "blocked",
        "blocker": None if passed else "provider_license_and_access_terms_not_documented",
    }
    return passed, normalized


def evaluate_gates(
    *,
    immutable_inputs_verified: bool,
    eligible_identity_count: int,
    validation_identity_count: int,
    route_validated: bool,
    license_access_passed: bool,
    schema_stable: bool,
    pit_semantics_verified: bool,
    matrix: pd.DataFrame,
    coverage: pd.DataFrame,
    diagnostics: pd.DataFrame,
    numerical_integrity_passed: bool,
    provenance_passed: bool,
) -> dict[str, dict[str, object]]:
    coverage_row = coverage.iloc[0]
    g1 = (
        immutable_inputs_verified
        and eligible_identity_count == EXPECTED_ELIGIBLE_IDENTITIES
        and validation_identity_count == EXPECTED_VALIDATION_IDENTITIES
    )
    g2 = route_validated
    g3 = license_access_passed
    g4 = schema_stable
    g5 = pit_semantics_verified and not diagnostics.blocker_classification.eq(
        "point_in_time_cutoff_not_provable"
    ).any()
    g6 = (
        len(matrix) == EXPECTED_ELIGIBLE_IDENTITIES
        and int(coverage_row.eligible_covered_count) == EXPECTED_ELIGIBLE_IDENTITIES
        and int(coverage_row.validation_covered_count) == EXPECTED_VALIDATION_IDENTITIES
    )
    g7 = numerical_integrity_passed
    g8 = provenance_passed and not diagnostics[
        [
            "same_day_or_future_used",
            "forward_fill_used",
            "backward_fill_used",
            "nearest_date_substitution_used",
            "arbitrary_last_row_used",
            "source_substitution_used",
            "target_derived_field_used",
        ]
    ].any().any()
    values = [g1, g2, g3, g4, g5, g6, g7, g8]
    statuses = values + [all(values)]
    names = (
        "G1_immutable_inputs",
        "G2_exact_route_and_transport",
        "G3_license_and_access",
        "G4_schema_and_pairing",
        "G5_pit_publication_semantics",
        "G6_exact_coverage",
        "G7_numerical_and_chronology",
        "G8_provenance_and_no_leakage",
        "G9_final_acceptance",
    )
    return {
        name: {"passed": bool(status), "status": "passed" if status else "failed"}
        for name, status in zip(names, statuses, strict=True)
    }


def write_validation_artifacts(
    output_dir: Path,
    *,
    input_identity_verification: Mapping[str, object],
    route_validation: Mapping[str, object],
    license_validation: Mapping[str, object],
    schema_profile: Mapping[str, object],
    pairs: Sequence[FutoiDailyPair],
    matrix: pd.DataFrame,
    coverage: pd.DataFrame,
    diagnostics: pd.DataFrame,
    blockers: Mapping[str, object],
    gates: Mapping[str, object],
) -> tuple[str, ...]:
    if output_dir.exists():
        raise FutoiSiSourceValidationError(
            "output directory must not pre-exist",
            blocker="provenance_not_sufficient",
        )
    output_dir.mkdir(parents=True)
    json_payloads = {
        "input_identity_verification.json": input_identity_verification,
        "official_route_validation.json": route_validation,
        "futoi_si_license_access_validation.json": license_validation,
        "futoi_si_schema_profile.json": schema_profile,
        "source_blocker_register.json": blockers,
        "gate_results.json": gates,
    }
    for name, payload in json_payloads.items():
        (output_dir / name).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    pd.DataFrame(
        [pair.as_record() for pair in pairs], columns=DAILY_POSITIONING_COLUMNS
    ).to_parquet(output_dir / "futoi_si_daily_positioning.parquet", index=False)
    matrix.to_parquet(output_dir / "futoi_si_pit_acceptance_matrix.parquet", index=False)
    coverage.to_csv(output_dir / "coverage_by_source.csv", index=False)
    diagnostics.to_csv(output_dir / "session_alignment_diagnostics.csv", index=False)
    names = tuple(sorted(path.name for path in output_dir.iterdir() if path.is_file()))
    if set(names) != set(REQUIRED_RUNTIME_ARTIFACTS):
        raise FutoiSiSourceValidationError(
            "runtime artifact inventory mismatch",
            blocker="provenance_not_sufficient",
        )
    return names


__all__ = [
    "ACCEPTANCE_MATRIX_COLUMNS",
    "ALGOPACK_HOST",
    "BLOCKER_CLASSIFICATIONS",
    "DAILY_POSITIONING_COLUMNS",
    "DIAGNOSTIC_COLUMNS",
    "EXPECTED_ELIGIBLE_IDENTITIES",
    "EXPECTED_LICENSE_PRODUCT",
    "EXPECTED_LICENSE_PROVIDER",
    "EXPECTED_VALIDATION_IDENTITIES",
    "FUTOI_PATH",
    "FUTOI_ROUTE",
    "FORECAST_ANCHOR",
    "FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS",
    "FutoiDailyPair",
    "FutoiParticipantRow",
    "FutoiSiSourceValidationError",
    "FutoiTransport",
    "PARTICIPANT_GROUPS",
    "PROJECT",
    "RAW_REQUIRED_FIELDS",
    "REQUIRED_RUNTIME_ARTIFACTS",
    "SOURCE_ID",
    "SOURCE_REVISION_STATUS",
    "SOURCE_TICKER",
    "STORAGE_FAMILY_CODE",
    "TARGET_INSTRUMENT_ID",
    "TARGET_SECURITY_ID",
    "TASK_ID",
    "build_futoi_pit_acceptance_matrix",
    "build_futoi_url",
    "coverage_by_source",
    "evaluate_gates",
    "fetch_futoi_bytes",
    "load_futoi_daily_pair",
    "parse_futoi_daily_response",
    "utc_now",
    "validate_license_access_evidence",
    "validate_prior_session_pair",
    "write_validation_artifacts",
]
