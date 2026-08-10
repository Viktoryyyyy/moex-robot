from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
from typing import Mapping

from dotenv import load_dotenv

from src.moex_research.intelligence.usdrubf_flowise_decision_agent import (
    stage11_shadow_decision_agent,
)
from src.moex_research.intelligence.usdrubf_live_shadow_bridge import (
    MOSCOW,
    SECID_KEY,
    build_live_decision_input,
    closed_bars,
    find_prior_session,
    load_futoi_context,
    safe_wait_decision_agent,
)
from src.moex_research.intelligence.usdrubf_news_macro_runtime import (
    FlowiseJsonAdapter,
    FlowiseTransportConfig,
)
from src.moex_research.intelligence.usdrubf_shadow_runtime import ShadowJsonStore, ShadowRuntime


PROJECT = "MOEX_Bot"
MODE = "short_live_shadow_input_bridge"
PROJECT_ENV_PATH = Path(__file__).resolve().parents[3] / ".env"


def _load_project_env() -> None:
    load_dotenv(PROJECT_ENV_PATH, override=False)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="One-shot USDRUBF live shadow input-bridge smoke")
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--max-prior-lookback-days", type=int, default=7)
    parser.add_argument("--enable-futoi", action="store_true")
    parser.add_argument("--safe-wait-agent", action="store_true")
    parser.add_argument(
        "--flowise-endpoint",
        default=os.getenv("MOEX_RUB_INTELLIGENCE_FLOWISE_ENDPOINT"),
    )
    parser.add_argument(
        "--flowise-request-field",
        default=os.getenv("MOEX_RUB_INTELLIGENCE_FLOWISE_REQUEST_FIELD"),
    )
    parser.add_argument(
        "--flowise-response-field",
        default=os.getenv("MOEX_RUB_INTELLIGENCE_FLOWISE_RESPONSE_FIELD"),
    )
    parser.add_argument(
        "--flowise-timeout-seconds",
        type=float,
        default=float(os.getenv("MOEX_RUB_INTELLIGENCE_FLOWISE_TIMEOUT_SECONDS", "20")),
    )
    return parser


def _decision_agent(args: argparse.Namespace):
    if args.safe_wait_agent:
        return safe_wait_decision_agent, "SAFE_WAIT"
    if not args.flowise_endpoint or not args.flowise_request_field or not args.flowise_response_field:
        raise RuntimeError(
            "explicit Flowise endpoint/request/response configuration is required unless --safe-wait-agent is selected"
        )
    adapter = FlowiseJsonAdapter(
        FlowiseTransportConfig(
            endpoint=args.flowise_endpoint,
            request_field=args.flowise_request_field,
            response_field=args.flowise_response_field,
            timeout_seconds=args.flowise_timeout_seconds,
        )
    )
    return stage11_shadow_decision_agent(adapter), "FLOWISE"


def _load_bars(secid: str, trade_date):
    _load_project_env()
    from src.api.futures.fo_feed_intraday import load_fo_5m_day

    return load_fo_5m_day(secid=secid, trade_date=trade_date)


def run_once(args: argparse.Namespace) -> Mapping[str, object]:
    wall_clock = datetime.now(MOSCOW).replace(microsecond=0)
    current_trade_date = wall_clock.date()
    current_raw = tuple(_load_bars(SECID_KEY, current_trade_date))
    if not current_raw:
        raise RuntimeError("current Moscow trade date has no USDRUBF/Si 5m bars")
    current_closed = closed_bars(current_raw, as_of_timestamp=wall_clock)
    decision_as_of = current_closed[-1]["end"]

    prior_trade_date, prior_bars = find_prior_session(
        current_trade_date,
        loader=_load_bars,
        max_lookback_days=args.max_prior_lookback_days,
    )
    futoi = load_futoi_context(
        prior_trade_date=prior_trade_date,
        current_trade_date=current_trade_date,
        fallback_available_at=decision_as_of,
        enabled=bool(args.enable_futoi),
    )
    decision_input = build_live_decision_input(
        current_session_bars=current_closed,
        prior_session_bars=prior_bars,
        wall_clock_as_of=wall_clock,
        futoi_context=futoi,
        news_events=(),
        macro_state=None,
    )
    decision_agent, decision_agent_mode = _decision_agent(args)
    runtime = ShadowRuntime(ShadowJsonStore(Path(args.state_root)))
    result = runtime.run_cycle(decision_input, decision_agent=decision_agent)

    return {
        "project": PROJECT,
        "mode": MODE,
        "status": "COMPLETED",
        "wall_clock": wall_clock.isoformat(),
        "current_trade_date": current_trade_date.isoformat(),
        "prior_trade_date": prior_trade_date.isoformat(),
        "as_of_timestamp": decision_input.as_of_timestamp.isoformat(),
        "current_bar_count": len(current_closed),
        "prior_bar_count": len(prior_bars),
        "price": decision_input.price,
        "market_regime": decision_input.market_regime,
        "ema_direction": decision_input.ema_3_19_ai.direction,
        "ema_quality": decision_input.ema_3_19_ai.quality_status,
        "futoi_direction": decision_input.futoi.direction,
        "futoi_quality": decision_input.futoi.quality_status,
        "news_event_count": len(decision_input.news_events),
        "macro_observation_count": len(decision_input.macro_state.observations),
        "decision_agent_mode": decision_agent_mode,
        "final_bias": result.market_state.final_bias,
        "trade_state": result.market_state.trade_state,
        "confidence": result.market_state.confidence,
        "significant_change": result.significant_change,
        "action_candidate": result.action_candidate,
        "market_state_path": str(result.market_state_path),
        "change_detection_path": str(result.change_detection_path),
    }


def _print_result(result: Mapping[str, object]) -> None:
    ordered = (
        "project",
        "mode",
        "status",
        "wall_clock",
        "current_trade_date",
        "prior_trade_date",
        "as_of_timestamp",
        "current_bar_count",
        "prior_bar_count",
        "price",
        "market_regime",
        "ema_direction",
        "ema_quality",
        "futoi_direction",
        "futoi_quality",
        "news_event_count",
        "macro_observation_count",
        "decision_agent_mode",
        "final_bias",
        "trade_state",
        "confidence",
        "significant_change",
        "action_candidate",
        "market_state_path",
        "change_detection_path",
    )
    for key in ordered:
        print(f"{key.upper()}={result[key]}")


def main(argv: list[str] | None = None) -> int:
    _load_project_env()
    args = _parser().parse_args(argv)
    try:
        result = run_once(args)
    except Exception as exc:
        print(f"PROJECT={PROJECT}")
        print(f"MODE={MODE}")
        print("STATUS=BLOCKED")
        print(f"ERROR_CLASS={exc.__class__.__name__}")
        print(f"ERROR={exc}")
        return 2
    _print_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
