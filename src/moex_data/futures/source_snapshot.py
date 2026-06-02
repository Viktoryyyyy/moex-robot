from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Final
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


EXPECTED_SOURCE_CONTRACT_ID: Final[str] = "moex_algopack_fo_tradestats_snapshot.v1"
EXPECTED_ENDPOINT: Final[str] = "https://apim.moex.com/iss/datashop/algopack/fo/tradestats/{secid}.json"
EXPECTED_SECID: Final[str] = "SiM6"
EXPECTED_FAMILY: Final[str] = "Si"
EXPECTED_TRADE_DATE: Final[str] = "2026-06-02"
EXPECTED_LATEST: Final[int] = 0
EXPECTED_METHOD: Final[str] = "GET"
EXPECTED_PRIMARY_TABLE: Final[str] = "data"
EXPECTED_SOURCE_VALUE: Final[str] = "algopack.fo.tradestats.v1"
EXPECTED_BOARD: Final[str] = "RFUD"

REQUIRED_SOURCE_COLUMNS: Final[tuple[str, ...]] = (
    "tradedate",
    "tradetime",
    "secid",
    "asset_code",
    "pr_open",
    "pr_high",
    "pr_low",
    "pr_close",
    "vol",
    "val",
    "trades",
    "SYSTIME",
)

RAW_5M_OUTPUT_COLUMNS: Final[tuple[str, ...]] = (
    "trade_date",
    "ts",
    "session_date",
    "secid",
    "family",
    "board",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value",
    "num_trades",
    "source",
    "ingest_ts",
)

_OHLC_SOURCE_COLUMNS: Final[tuple[str, ...]] = ("pr_open", "pr_high", "pr_low", "pr_close")
_NON_NEGATIVE_SOURCE_COLUMNS: Final[tuple[str, ...]] = ("vol", "val", "trades")
_DYNAMIC_MARKERS: Final[tuple[str, ...]] = ("latest", "current", "autodetect")
_BEARER_TOKEN_ENV_NAMES: Final[tuple[str, ...]] = (
    "MOEX_APIM_BEARER_TOKEN",
    "MOEX_APIM_TOKEN",
    "MOEX_ALGOPACK_TOKEN",
    "MOEX_DATASHOP_TOKEN",
    "MOEX_ISS_TOKEN",
    "MOEX_TOKEN",
)
_SUBSCRIPTION_KEY_ENV_NAMES: Final[tuple[str, ...]] = (
    "MOEX_APIM_SUBSCRIPTION_KEY",
    "OCP_APIM_SUBSCRIPTION_KEY",
    "MOEX_SUBSCRIPTION_KEY",
)


class SourceSnapshotError(ValueError):
    pass


@dataclass(frozen=True)
class SourceSnapshotResult:
    source_artifact_path: str
    row_count: int
    source_sha256: str
    endpoint: str
    query_parameters: dict[str, Any]
    fetched_at: str
    required_columns: tuple[str, ...]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def source_contract_path() -> Path:
    return repo_root() / "contracts" / "datasets" / "moex_algopack_fo_tradestats_snapshot.v1.yaml"


def _parse_scalar(value: str) -> object:
    stripped = value.strip().strip('"')
    if stripped.isdigit():
        return int(stripped)
    return stripped


def _yaml_items(text: str) -> tuple[tuple[int, str], ...]:
    rows: list[tuple[int, str]] = []
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        if indent % 2:
            raise SourceSnapshotError("invalid contract indentation")
        rows.append((indent, raw_line.strip()))
    return tuple(rows)


def _parse_list(items: tuple[tuple[int, str], ...], index: int, indent: int) -> tuple[list[object], int]:
    values: list[object] = []
    while index < len(items):
        item_indent, content = items[index]
        if item_indent < indent or not content.startswith("- "):
            break
        if item_indent != indent:
            raise SourceSnapshotError("invalid contract list indentation")
        values.append(_parse_scalar(content[2:]))
        index += 1
    return values, index


def _parse_mapping(items: tuple[tuple[int, str], ...], index: int, indent: int) -> tuple[dict[str, object], int]:
    values: dict[str, object] = {}
    while index < len(items):
        item_indent, content = items[index]
        if item_indent < indent or content.startswith("- "):
            break
        if item_indent != indent:
            raise SourceSnapshotError("invalid contract mapping indentation")
        key, separator, value = content.partition(":")
        if separator != ":":
            raise SourceSnapshotError("invalid contract mapping row")
        index += 1
        if value.strip():
            values[key.strip()] = _parse_scalar(value)
            continue
        if index >= len(items):
            values[key.strip()] = {}
            continue
        child_indent, child_content = items[index]
        if child_indent != indent + 2:
            raise SourceSnapshotError("invalid contract child indentation")
        if child_content.startswith("- "):
            child, index = _parse_list(items, index, indent + 2)
        else:
            child, index = _parse_mapping(items, index, indent + 2)
        values[key.strip()] = child
    return values, index


def load_simple_yaml_mapping(path: Path) -> dict[str, object]:
    items = _yaml_items(path.read_text(encoding="utf-8"))
    values, next_index = _parse_mapping(items, 0, 0)
    if next_index != len(items):
        raise SourceSnapshotError("contract parse did not consume all rows")
    return values


def _require_exact(value: object, expected: object, field_name: str) -> None:
    if value != expected:
        raise SourceSnapshotError(field_name + " does not match approved source contract")


def validate_source_contract_values(
    contract: dict[str, object],
    expected_source_contract_id: str = EXPECTED_SOURCE_CONTRACT_ID,
) -> dict[str, object]:
    _require_exact(contract.get("source_contract_id"), expected_source_contract_id, "source_contract_id")
    _require_exact(contract.get("method"), EXPECTED_METHOD, "method")
    _require_exact(contract.get("endpoint"), EXPECTED_ENDPOINT, "endpoint")
    _require_exact(contract.get("primary_table"), EXPECTED_PRIMARY_TABLE, "primary_table")
    _require_exact(contract.get("target_dataset_id"), "futures_raw_5m", "target_dataset_id")
    _require_exact(contract.get("target_contract_id"), "futures_raw_5m.v1", "target_contract_id")
    _require_exact(
        contract.get("target_partition"),
        {"trade_date": EXPECTED_TRADE_DATE, "family": EXPECTED_FAMILY, "secid": EXPECTED_SECID},
        "target_partition",
    )
    _require_exact(
        contract.get("query_parameters"),
        {"secid": EXPECTED_SECID, "from": EXPECTED_TRADE_DATE, "till": EXPECTED_TRADE_DATE, "latest": EXPECTED_LATEST},
        "query_parameters",
    )
    if tuple(contract.get("required_source_columns", ())) != REQUIRED_SOURCE_COLUMNS:
        raise SourceSnapshotError("required_source_columns do not match approved source contract")
    return contract


def load_source_contract(expected_source_contract_id: str = EXPECTED_SOURCE_CONTRACT_ID) -> dict[str, object]:
    return validate_source_contract_values(load_simple_yaml_mapping(source_contract_path()), expected_source_contract_id)


def reject_dynamic_markers(value: str, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        raise SourceSnapshotError(field_name + " is required")
    parts = tuple(part.casefold() for part in Path(text).parts)
    name = Path(text).name.casefold()
    stem = Path(text).stem.casefold()
    for marker in _DYNAMIC_MARKERS:
        if marker in parts or marker == name or marker == stem:
            raise SourceSnapshotError(field_name + " contains forbidden dynamic marker")
    return text


def require_output_path(output_path: str | os.PathLike[str] | None) -> Path:
    checked = reject_dynamic_markers("" if output_path is None else str(output_path), "output_path")
    path = Path(checked)
    if path.suffix.casefold() != ".csv":
        raise SourceSnapshotError("output_path must point to a .csv file")
    if path.name in ("", ".", ".."):
        raise SourceSnapshotError("output_path must point to a file")
    forbidden_fragment = ("futures", "raw_5m", "trade_date=" + EXPECTED_TRADE_DATE, "family=" + EXPECTED_FAMILY, "secid=" + EXPECTED_SECID)
    path_text = path.as_posix()
    if all(fragment in path_text for fragment in forbidden_fragment):
        raise SourceSnapshotError("output_path must not be the futures_raw_5m target partition path")
    return path


def endpoint_and_query(contract: dict[str, object]) -> tuple[str, dict[str, Any], str]:
    query = contract["query_parameters"]
    if not isinstance(query, dict):
        raise SourceSnapshotError("query_parameters must be a mapping")
    _require_exact(query, {"secid": EXPECTED_SECID, "from": EXPECTED_TRADE_DATE, "till": EXPECTED_TRADE_DATE, "latest": EXPECTED_LATEST}, "query_parameters")
    endpoint_template = str(contract["endpoint"])
    endpoint = endpoint_template.replace("{secid}", EXPECTED_SECID)
    if endpoint == endpoint_template:
        raise SourceSnapshotError("endpoint must contain secid path placeholder")
    url = endpoint + "?" + urlencode(query)
    return endpoint, dict(query), url


def auth_headers_from_env(env: dict[str, str] | None = None) -> dict[str, str]:
    active_env = os.environ if env is None else env
    headers: dict[str, str] = {}
    custom_header_name = active_env.get("MOEX_APIM_AUTH_HEADER_NAME", "").strip()
    custom_header_value = active_env.get("MOEX_APIM_AUTH_HEADER_VALUE", "").strip()
    if custom_header_name or custom_header_value:
        if not custom_header_name or not custom_header_value:
            raise SourceSnapshotError("both MOEX_APIM_AUTH_HEADER_NAME and MOEX_APIM_AUTH_HEADER_VALUE are required")
        headers[custom_header_name] = custom_header_value
    direct_authorization = active_env.get("MOEX_APIM_AUTHORIZATION", "").strip()
    if direct_authorization:
        headers["Authorization"] = direct_authorization
    for env_name in _BEARER_TOKEN_ENV_NAMES:
        token = active_env.get(env_name, "").strip()
        if token:
            if token.casefold().startswith("bearer "):
                headers["Authorization"] = token
            else:
                headers["Authorization"] = "Bearer " + token
            break
    for env_name in _SUBSCRIPTION_KEY_ENV_NAMES:
        subscription_key = active_env.get(env_name, "").strip()
        if subscription_key:
            headers["Ocp-Apim-Subscription-Key"] = subscription_key
            break
    return headers


def fetch_iss_json(url: str) -> tuple[dict[str, Any], str]:
    headers = {"Accept": "application/json", **auth_headers_from_env()}
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=30) as response:
            status = int(getattr(response, "status", response.getcode()))
            if status != 200:
                raise SourceSnapshotError("HTTP status is not 200: " + str(status))
            content_type = response.headers.get("Content-Type", "")
            if "json" not in content_type.casefold():
                raise SourceSnapshotError("response content type is not JSON")
            body = response.read()
    except HTTPError as exc:
        raise SourceSnapshotError("HTTP status is not 200: " + str(exc.code)) from exc
    try:
        payload = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise SourceSnapshotError("response body is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise SourceSnapshotError("response JSON root must be an object")
    return payload, body.decode("utf-8")


def _table_rows(payload: dict[str, Any], table_name: str) -> list[dict[str, Any]]:
    table = payload.get(table_name)
    if not isinstance(table, dict):
        raise SourceSnapshotError("primary table is missing")
    columns = table.get("columns")
    data = table.get("data")
    if not isinstance(columns, list) or not all(isinstance(column, str) for column in columns):
        raise SourceSnapshotError("primary table columns are missing")
    if not isinstance(data, list):
        raise SourceSnapshotError("primary table data is missing")
    missing = tuple(column for column in REQUIRED_SOURCE_COLUMNS if column not in columns)
    if missing:
        raise SourceSnapshotError("primary table is missing required source columns")
    rows: list[dict[str, Any]] = []
    for raw_row in data:
        if not isinstance(raw_row, list):
            raise SourceSnapshotError("primary table row is not a list")
        if len(raw_row) != len(columns):
            raise SourceSnapshotError("primary table row width does not match columns")
        rows.append(dict(zip(columns, raw_row, strict=True)))
    if not rows:
        raise SourceSnapshotError("primary table row_count is zero")
    return rows


def _cursor_values(payload: dict[str, Any]) -> None:
    cursor = payload.get("data.cursor")
    if cursor is None:
        return
    if not isinstance(cursor, dict):
        raise SourceSnapshotError("data.cursor must be a table object")
    columns = cursor.get("columns")
    data = cursor.get("data")
    if not isinstance(columns, list) or not isinstance(data, list):
        raise SourceSnapshotError("data.cursor table is malformed")
    if not data:
        return
    row = data[0]
    if not isinstance(row, list) or len(row) != len(columns):
        raise SourceSnapshotError("data.cursor row is malformed")
    values = dict(zip(columns, row, strict=True))
    normalized = {str(key).casefold(): value for key, value in values.items()}
    if {"index", "total", "pagesize"}.issubset(normalized):
        index = int(normalized["index"])
        total = int(normalized["total"])
        page_size = int(normalized["pagesize"])
        if index + page_size < total:
            raise SourceSnapshotError("data.cursor indicates incomplete pagination")


def _decimal(value: Any, field_name: str) -> Decimal:
    if value is None or value == "":
        raise SourceSnapshotError(field_name + " is null")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SourceSnapshotError(field_name + " is not numeric") from exc


def _timestamp(trade_date: Any, trade_time: Any) -> datetime:
    if trade_date is None or trade_time is None:
        raise SourceSnapshotError("tradedate/tradetime is null")
    try:
        return datetime.fromisoformat(str(trade_date) + " " + str(trade_time))
    except ValueError as exc:
        raise SourceSnapshotError("tradedate/tradetime is invalid") from exc


def validate_source_rows(rows: list[dict[str, Any]]) -> None:
    seen: set[tuple[str, str]] = set()
    previous_ts: datetime | None = None
    for row in rows:
        if row.get("secid") != EXPECTED_SECID:
            raise SourceSnapshotError("secid does not match approved source contract")
        if row.get("asset_code") != EXPECTED_FAMILY:
            raise SourceSnapshotError("asset_code does not match approved source contract")
        if row.get("tradedate") != EXPECTED_TRADE_DATE:
            raise SourceSnapshotError("tradedate does not match approved source contract")

        key = (str(row.get("tradedate")), str(row.get("tradetime")))
        if key in seen:
            raise SourceSnapshotError("duplicate tradedate + tradetime")
        seen.add(key)

        ts = _timestamp(row.get("tradedate"), row.get("tradetime"))
        if previous_ts is not None and ts <= previous_ts:
            raise SourceSnapshotError("non-monotonic timestamps")
        previous_ts = ts

        open_price = _decimal(row.get("pr_open"), "pr_open")
        high_price = _decimal(row.get("pr_high"), "pr_high")
        low_price = _decimal(row.get("pr_low"), "pr_low")
        close_price = _decimal(row.get("pr_close"), "pr_close")
        if min(open_price, high_price, low_price, close_price) <= 0:
            raise SourceSnapshotError("OHLC values must be positive")
        if high_price < low_price:
            raise SourceSnapshotError("invalid OHLC high-low range")
        if open_price < low_price or open_price > high_price or close_price < low_price or close_price > high_price:
            raise SourceSnapshotError("invalid OHLC open/close range")

        for column in _NON_NEGATIVE_SOURCE_COLUMNS:
            if _decimal(row.get(column), column) < 0:
                raise SourceSnapshotError(column + " is negative")


def normalize_rows(rows: list[dict[str, Any]], ingest_ts: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        ts = _timestamp(row["tradedate"], row["tradetime"]).isoformat(sep=" ")
        normalized.append(
            {
                "trade_date": row["tradedate"],
                "ts": ts,
                "session_date": row["tradedate"],
                "secid": row["secid"],
                "family": row["asset_code"],
                "board": EXPECTED_BOARD,
                "open": row["pr_open"],
                "high": row["pr_high"],
                "low": row["pr_low"],
                "close": row["pr_close"],
                "volume": row["vol"],
                "value": row["val"],
                "num_trades": row["trades"],
                "source": EXPECTED_SOURCE_VALUE,
                "ingest_ts": ingest_ts,
            }
        )
    return normalized


def write_csv_atomic(path: Path, rows: list[dict[str, Any]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=path.name + ".tmp.",
            suffix=".csv",
            delete=False,
        ) as handle:
            temp_name = handle.name
            writer = csv.DictWriter(handle, fieldnames=RAW_5M_OUTPUT_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temp_name, path)
        temp_name = None
    finally:
        if temp_name is not None:
            try:
                Path(temp_name).unlink()
            except FileNotFoundError:
                pass
    return hashlib.sha256(path.read_bytes()).hexdigest()


def create_source_snapshot(
    output_path: str | os.PathLike[str],
    expected_source_contract_id: str = EXPECTED_SOURCE_CONTRACT_ID,
) -> SourceSnapshotResult:
    output = require_output_path(output_path)
    contract = load_source_contract(expected_source_contract_id)
    endpoint, query, url = endpoint_and_query(contract)
    fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    payload, _raw_body = fetch_iss_json(url)
    _cursor_values(payload)
    rows = _table_rows(payload, EXPECTED_PRIMARY_TABLE)
    validate_source_rows(rows)
    ingest_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    normalized = normalize_rows(rows, ingest_ts)
    digest = write_csv_atomic(output, normalized)
    return SourceSnapshotResult(
        source_artifact_path=str(output),
        row_count=len(normalized),
        source_sha256=digest,
        endpoint=endpoint,
        query_parameters=query,
        fetched_at=fetched_at,
        required_columns=RAW_5M_OUTPUT_COLUMNS,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create one ALGOPACK FO TradeStats source snapshot artifact.")
    parser.add_argument("--source-contract-id", required=True)
    parser.add_argument("--output-path", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        result = create_source_snapshot(
            output_path=args.output_path,
            expected_source_contract_id=args.source_contract_id,
        )
    except SourceSnapshotError as exc:
        print(json.dumps({"status": "failed_validation", "error": str(exc)}, ensure_ascii=False), flush=True)
        return 1
    print(json.dumps({"status": "succeeded", **asdict(result)}, ensure_ascii=False, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
