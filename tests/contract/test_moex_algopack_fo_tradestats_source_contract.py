from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_CONTRACT_PATH = REPO_ROOT / "contracts/datasets/moex_algopack_fo_tradestats_snapshot.v1.yaml"
RAW_5M_CONTRACT_PATH = REPO_ROOT / "contracts/datasets/futures_raw_5m.v1.yaml"


EXPECTED_SOURCE_COLUMNS = {
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
}
EXPECTED_FIELD_MAPPING = {
    "trade_date": "tradedate",
    "ts": "tradedate + tradetime",
    "session_date": "tradedate",
    "secid": "secid",
    "family": "asset_code",
    "board": "RFUD",
    "open": "pr_open",
    "high": "pr_high",
    "low": "pr_low",
    "close": "pr_close",
    "volume": "vol",
    "value": "val",
    "num_trades": "trades",
    "source": "algopack.fo.tradestats.v1",
    "ingest_ts": "source snapshot creation timestamp",
}


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
        assert indent % 2 == 0
        rows.append((indent, raw_line.strip()))
    return tuple(rows)


def _parse_list(items: tuple[tuple[int, str], ...], index: int, indent: int) -> tuple[list[object], int]:
    values: list[object] = []
    while index < len(items):
        item_indent, content = items[index]
        if item_indent < indent or not content.startswith("- "):
            break
        assert item_indent == indent
        values.append(_parse_scalar(content[2:]))
        index += 1
    return values, index


def _parse_mapping(items: tuple[tuple[int, str], ...], index: int, indent: int) -> tuple[dict[str, object], int]:
    values: dict[str, object] = {}
    while index < len(items):
        item_indent, content = items[index]
        if item_indent < indent or content.startswith("- "):
            break
        assert item_indent == indent
        key, separator, value = content.partition(":")
        assert separator == ":"
        index += 1
        if value.strip():
            values[key.strip()] = _parse_scalar(value)
            continue
        child_indent, child_content = items[index]
        assert child_indent == indent + 2
        if child_content.startswith("- "):
            child, index = _parse_list(items, index, indent + 2)
        else:
            child, index = _parse_mapping(items, index, indent + 2)
        values[key.strip()] = child
    return values, index


def _load_contract(path: Path) -> dict[str, object]:
    items = _yaml_items(path.read_text(encoding="utf-8"))
    values, next_index = _parse_mapping(items, 0, 0)
    assert next_index == len(items)
    return values


def _source_contract() -> dict[str, object]:
    return _load_contract(SOURCE_CONTRACT_PATH)


def test_contract_file_exists_and_is_loadable():
    assert SOURCE_CONTRACT_PATH.exists()
    contract = _source_contract()
    assert contract["source_contract_id"] == "moex_algopack_fo_tradestats_snapshot.v1"
    assert contract["artifact_class"] == "external_pattern"
    assert contract["producer"] == "future owner-run source snapshot step"
    assert contract["consumer"] == "existing single-partition materializer"


def test_target_partition_endpoint_and_query_are_exact():
    contract = _source_contract()

    assert contract["target_dataset_id"] == "futures_raw_5m"
    assert contract["target_contract_id"] == "futures_raw_5m.v1"
    assert contract["target_partition"] == {
        "trade_date": "2026-06-02",
        "family": "Si",
        "secid": "SiM6",
    }
    assert contract["method"] == "GET"
    assert contract["endpoint"] == "https://apim.moex.com/iss/datashop/algopack/fo/tradestats/{secid}.json"
    assert "{secid}" in contract["endpoint"]
    assert contract["query_parameters"] == {
        "secid": "SiM6",
        "from": "2026-06-02",
        "till": "2026-06-02",
        "latest": 0,
    }
    assert contract["response_format"] == "ISS-style JSON"
    assert contract["primary_table"] == "data"
    assert contract["pagination_table"] == "data.cursor"
    assert contract["confirmed_row_count"] == 96


def test_required_source_columns_and_mapping_cover_raw_5m_contract():
    contract = _source_contract()
    raw_contract = _load_contract(RAW_5M_CONTRACT_PATH)

    assert set(contract["required_source_columns"]) == EXPECTED_SOURCE_COLUMNS
    assert contract["field_mapping_to_futures_raw_5m"] == EXPECTED_FIELD_MAPPING
    assert set(raw_contract["required_columns"]).issubset(contract["field_mapping_to_futures_raw_5m"].keys())


def test_timestamp_and_pagination_semantics_are_contract_only():
    contract = _source_contract()

    timestamp_semantics = "\n".join(contract["timestamp_semantics"])
    assert "MOEX/MSK exchange-local 5-minute bucket timestamp" in timestamp_semantics
    assert "no UTC conversion" in timestamp_semantics
    assert "session_date equals tradedate" in timestamp_semantics
    assert contract["pagination_rule"] == ["fail closed if data.cursor indicates incomplete pagination"]


def test_fail_closed_conditions_include_required_guards():
    conditions = set(_source_contract()["fail_closed_conditions"])

    for required_condition in (
        "HTTP status != 200",
        "non-JSON response",
        "missing required columns",
        "row_count = 0",
        "secid != SiM6",
        "asset_code != Si",
        "tradedate != 2026-06-02",
        "duplicate tradedate + tradetime",
        "non-monotonic timestamps",
        "null OHLC",
        "invalid OHLC",
        "negative vol/val/trades",
        "incomplete cursor/pagination",
        "fallback to ISS candles/RFUD marketdata",
        "synthetic/manual row construction",
        "latest/current/autodetect dynamic selection",
    ):
        assert required_condition in conditions


def test_scope_exclusions_block_artifact_creation_materialization_and_runtime_permissions():
    contract = _source_contract()
    exclusions = set(contract["scope_exclusions"])
    guards = set(contract["execution_guards"])

    assert "this contract does not authorize source artifact creation" in exclusions
    assert "this contract does not authorize data lake write" in exclusions
    assert "this contract does not authorize materialization" in exclusions
    assert "this contract does not authorize strategy/research/runtime/live" in exclusions
    assert "no loader implementation is created by this contract" in guards
    assert "no network call is made by this contract" in guards
    assert "no source artifact is created by this contract" in guards
    assert "no data lake write is performed by this contract" in guards
    assert "no materialization is run by this contract" in guards
    assert "no backfill is run by this contract" in guards


def test_contract_does_not_declare_output_paths_or_permissioned_execution():
    contract_text = SOURCE_CONTRACT_PATH.read_text(encoding="utf-8")

    forbidden_contract_markers = (
        "path_pattern:",
        "source_path:",
        "materialize_single_raw_5m_partition",
        "requests.",
        "urllib",
        "httpx",
        "aiohttp",
        "socket",
    )
    for marker in forbidden_contract_markers:
        assert marker not in contract_text

    for line in contract_text.splitlines():
        normalized = line.strip()
        if "authorize strategy/research/runtime/live" in normalized:
            assert normalized == "- this contract does not authorize strategy/research/runtime/live"
