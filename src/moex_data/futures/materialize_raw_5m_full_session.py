from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Final

import pandas as pd
import requests

from . import materialize_raw_5m as materializer


MAX_APIM_PAGES: Final[int] = 500


def _request_params(request: materializer.Raw5mMaterializationRequest, start: int) -> dict[str, object]:
    return {
        "date": request.trade_date,
        "from": request.trade_date,
        "till": request.trade_date,
        "secid": request.secid,
        "start": start,
        "iss.meta": "off",
        "iss.only": "tradestats",
    }


def _page_signature(frame: pd.DataFrame) -> tuple[object, ...]:
    if frame.empty:
        return (0,)
    first_row = tuple(str(value) for value in frame.iloc[0].tolist())
    last_row = tuple(str(value) for value in frame.iloc[-1].tolist())
    return (int(len(frame.index)), first_row, last_row)


def _fetch_apim_tradestats_full_session_frame(
    request: materializer.Raw5mMaterializationRequest,
    timeout: float,
    apim_base_url: str | None,
    env: Mapping[str, str] | None,
) -> tuple[pd.DataFrame, str]:
    base_url = materializer._apim_base_url(apim_base_url, env)
    url = materializer._source_url(base_url, request.source_endpoint)
    headers = materializer._auth_headers(env)
    frames: list[pd.DataFrame] = []
    seen_signatures: set[tuple[object, ...]] = set()
    source_url = url
    start = 0

    for _ in range(MAX_APIM_PAGES):
        params = _request_params(request, start)
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, Mapping):
            materializer._fail("APIM tradestats response JSON root is not an object")
        frame = materializer._block_to_frame(data)
        source_url = str(getattr(response, "url", url))
        if frame.empty:
            if not frames:
                materializer._fail("APIM tradestats response returned no rows")
            break

        signature = _page_signature(frame)
        if signature in seen_signatures:
            materializer._fail("APIM pagination did not advance")
        seen_signatures.add(signature)
        frames.append(frame)
        start += int(len(frame.index))
    else:
        materializer._fail("APIM pagination exceeded max_pages guard")

    return pd.concat(frames, ignore_index=True), source_url


def materialize_single_raw_5m_full_session_partition(
    repo_root: str,
    dataset_id: str,
    contract_id: str,
    trade_date: str,
    family: str,
    secid: str,
    source_path: str | None,
    run_id: str,
    env: Mapping[str, str] | None = None,
    *,
    source_candidate: str | None = None,
    source_endpoint: str | None = None,
    market: str | None = None,
    board: str | None = None,
    series_type: str | None = None,
    granularity: str | None = None,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
) -> materializer.Raw5mMaterializationResult:
    original_fetcher = materializer._fetch_apim_tradestats_frame
    try:
        materializer._fetch_apim_tradestats_frame = _fetch_apim_tradestats_full_session_frame
        return materializer.materialize_single_raw_5m_partition(
            repo_root=repo_root,
            dataset_id=dataset_id,
            contract_id=contract_id,
            trade_date=trade_date,
            family=family,
            secid=secid,
            source_path=source_path,
            run_id=run_id,
            env=env,
            source_candidate=source_candidate,
            source_endpoint=source_endpoint,
            market=market,
            board=board,
            series_type=series_type,
            granularity=granularity,
            timeout=timeout,
            apim_base_url=apim_base_url,
        )
    finally:
        materializer._fetch_apim_tradestats_frame = original_fetcher


def main(argv: Sequence[str] | None = None) -> int:
    args = materializer.parse_args(argv)
    try:
        result = materialize_single_raw_5m_full_session_partition(
            repo_root=args.repo_root,
            dataset_id=args.dataset_id,
            contract_id=args.contract_id,
            trade_date=args.trade_date,
            family=args.family,
            secid=args.secid,
            source_path=args.source_path,
            run_id=args.run_id,
            source_candidate=args.source_candidate,
            source_endpoint=args.source_endpoint,
            market=args.market,
            board=args.board,
            series_type=args.series_type,
            granularity=args.granularity,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
        )
    except materializer.FuturesRaw5mMaterializationError as exc:
        print(json.dumps(materializer._error_payload(exc), ensure_ascii=False, sort_keys=True))
        if exc.status == materializer.BLOCKED_NO_SOURCE_STATUS:
            return 2
        return 1
    print(json.dumps(materializer._result_payload(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
