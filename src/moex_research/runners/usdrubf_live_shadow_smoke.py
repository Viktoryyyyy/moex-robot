from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
from typing import Mapping

from dotenv import load_dotenv

from src.moex_research.intelligence.usdrubf_decision_engine import DecisionInput
from src.moex_research.intelligence.usdrubf_flowise_auth import (
    FLOWISE_API_KEY_ENV,
    flowise_bearer_opener,
)
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
from src.moex_research.intelligence.usdrubf_news_macro import MacroState, build_macro_state
from src.moex_research.intelligence.usdrubf_news_macro_runtime import (
    FlowiseJsonAdapter,
    FlowiseTransportConfig,
)
from src.moex_research.intelligence.usdrubf_shadow_runtime import ShadowJsonStore, ShadowRuntime
from src.moex_research.runners.usdrubf_macro_live_cbr_smoke import (
    run_current_cbr_macro_smoke,
)


PROJECT = "MOEX_Bot"
MODE = "short_live_shadow_input_bridge"
PROJECT_ENV_PATH = Path(__file__).resolve().parents[3] / ".env"
_REQUIRED_CBR_MACRO_METRICS = frozenset({"cbr_ruonia_rate_pct", "cbr_key_rate_pct"})


def _load_project_env() -> None:
    load_dotenv(PROJECT_ENV_PATH, override=False)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="One-shot USDRUBF live shadow input-bridge smoke")
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--max-prior-lookback-days", type=int, default=7)
    parser.add_argument("--enable-futoi", action="store_true")
    parser.add_argument("--safe-wait-agent", action="store_true")
    parser.add_argument(
        "--cbr-macro",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable accepted CBR key-rate/RUONIA MacroState composition (default: enabled)",
    )
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
    api_key = os.getenv(FLOWISE_API_KEY_ENV, "").strip()
    if not api_key:
        raise RuntimeError(f"required Flowise API key env missing: {FLOWISE_API_KEY_ENV}")
    adapter = FlowiseJsonAdapter(
        FlowiseTransportConfig(
            endpoint=args.flowise_endpoint,
            request_field=args.flowise_request_field,
            response_field=args.flowise_response_field,
            timeout_seconds=args.flowise_timeout_seconds,
        ),
        opener=flowise_bearer_opener(api_key),
    )
    return stage11_shadow_decision_agent(adapter), "FLOWISE"


def _load_bars(secid: str, trade_date):
    _load_project_env()
    from src.api.futures.fo_feed_intraday import load_fo_5m_day

    return load_fo_5m_day(secid=secid, trade_date=trade_date)


def _load_current_cbr_macro_state(*, as_of_timestamp: datetime) -> MacroState:
    observations = run_current_cbr_macro_smoke(now_utc=as_of_timestamp)
    macro_state = build_macro_state(observations, as_of_timestamp=as_of_timestamp)
    metric_ids = {item.metric_id for item in macro_state.observations}
    if metric_ids != _REQUIRED_CBR_MACRO_METRICS:
        raise RuntimeError(
            "current CBR MacroState must contain exactly key rate and RUONIA observations"
        )
    if any(item.quality_status != "OK" for item in macro_state.observations):
        raise RuntimeError("current CBR MacroState contains a non-OK observation")
    return macro_state


def _compose_wall_clock_decision_input(
    *,
    market_input: DecisionInput,
    wall_clock: datetime,
    macro_state: MacroState,
) -> DecisionInput:
    """Compose asynchronous factual inputs at the actual decision wall clock.

    Market/EMA/level facts remain based only on the latest closed market bars.
    The DecisionInput as-of moves to wall clock so a CBR observation retrieved
    after the last 5m bar is never backdated into the market-bar timestamp.
    """

    if wall_clock.tzinfo is None or wall_clock.utcoffset() is None:
        raise RuntimeError("wall_clock must be timezone-aware")
    if market_input.as_of_timestamp > wall_clock:
        raise RuntimeError("market input is from the future relative to wall clock")
    return DecisionInput(
        as_of_timestamp=wall_clock,
        price=market_input.price,
        trend=market_input.trend,
        market_regime=market_input.market_regime,
        active_levels=market_input.active_levels,
        level_interactions=market_input.level_interactions,
        ema_3_19_ai=market_input.ema_3_19_ai,
        futoi=market_input.futoi,
        news_events=market_input.news_events,
        macro_state=macro_state,
    )


def run_once(args: argparse.Namespace) -> Mapping[str, object]:
    wall_clock = datetime.now(MOSCOW).replace(microsecond=0)
    current_trade_date = wall_clock.date()
    current_raw = tuple(_load_bars(SECID_KEY, current_trade_date))
    if not current_raw:
        raise RuntimeError("current Moscow trade date has no USDRUBF/Si 5m bars")
    current_closed = closed_bars(current_raw, as_of_timestamp=wall_clock)
    market_data_as_of = current_closed[-1]["end"]
    if not isinstance(market_data_as_of, datetime):
        raise RuntimeError("latest closed market bar timestamp is malformed")

    prior_trade_date, prior_bars = find_prior_session(
        current_trade_date,
        loader=_load_bars,
        max_lookback_days=args.max_prior_lookback_days,
    )
    futoi = load_futoi_context(
        prior_trade_date=prior_trade_date,
        current_trade_date=current_trade_date,
        fallback_available_at=market_data_as_of,
        enabled=bool(args.enable_futoi),
    )
    market_input = build_live_decision_input(
        current_session_bars=current_closed,
        prior_session_bars=prior_bars,
        wall_clock_as_of=wall_clock,
        futoi_context=futoi,
        news_events=(),
        macro_state=None,
    )

    # Parser-generated CLI args always include cbr_macro=True by default. A missing
    # attribute is treated as legacy-disabled only for older direct run_once callers.
    cbr_macro_enabled = bool(getattr(args, "cbr_macro", False))
    if cbr_macro_enabled:
        macro_state = _load_current_cbr_macro_state(as_of_timestamp=wall_clock)
        macro_mode = "LIVE_CBR"
    else:
        macro_state = market_input.macro_state
        macro_mode = "DISABLED"

    decision_input = _compose_wall_clock_decision_input(
        market_input=market_input,
        wall_clock=wall_clock,
        macro_state=macro_state,
    )
    decision_agent, decision_agent_mode = _decision_agent(args)
    runtime = ShadowRuntime(ShadowJsonStore(Path(args.state_root)))
    result = runtime.run_cycle(decision_input, decision_agent=decision_agent)

    macro_metric_ids = tuple(item.metric_id for item in decision_input.macro_state.observations)
    return {
        "project": PROJECT,
        "mode": MODE,
        "status": "COMPLETED",
        "wall_clock": wall_clock.isoformat(),
        "current_trade_date": current_trade_date.isoformat(),
        "prior_trade_date": prior_trade_date.isoformat(),
        "market_data_as_of_timestamp": market_data_as_of.isoformat(),
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
        "macro_mode": macro_mode,
        "macro_observation_count": len(decision_input.macro_state.observations),
        "macro_metric_ids": ",".join(macro_metric_ids),
        "macro_direction": decision_input.macro_state.overall_direction,
        "macro_confidence": decision_input.macro_state.confidence,
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
        "market_data_as_of_timestamp",
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
        "macro_mode",
        "macro_observation_count",
        "macro_metric_ids",
        "macro_direction",
        "macro_confidence",
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
