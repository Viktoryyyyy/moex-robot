from __future__ import annotations

import argparse
from pathlib import Path

from src.moex_research.intelligence.usdrubf_shadow_alert_delivery import (
    MODE,
    PROJECT,
    STATE_FILENAME,
    AlertTransportError,
    ShadowAlertError,
    dry_run_transport,
    process_persisted_alert,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consume persisted RUB Intelligence Change Detector output with dry-run alert transport"
    )
    parser.add_argument("--state-root", required=True)
    return parser


def _print_result(result: dict[str, object], *, state_path: Path) -> None:
    print(f"PROJECT={PROJECT}")
    print(f"MODE={MODE}")
    print(f"ALERT_DELIVERY_STATUS={result.get('last_delivery_status')}")
    print(f"TRANSPORT_ID={result.get('last_transport_id')}")
    print(f"ALERT_ID={result.get('last_alert_id') or 'NONE'}")
    print(f"CHANGE_AS_OF_TIMESTAMP={result.get('last_change_as_of_timestamp') or 'NONE'}")
    print(f"EXTERNAL_DELIVERY={result.get('last_external_delivery')}")
    print(f"TRANSPORT_REFERENCE={result.get('last_transport_reference') or 'NONE'}")
    print(f"DELIVERY_HISTORY_COUNT={len(result.get('delivered', []))}")
    print(f"ALERT_STATE_PATH={state_path}")
    if result.get("last_error_class"):
        print(f"ERROR_CLASS={result['last_error_class']}")
        print(f"ERROR={result['last_error']}")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = Path(args.state_root).expanduser()
    state_path = root / STATE_FILENAME
    try:
        result = dict(
            process_persisted_alert(
                root,
                transport_id="dry-run",
                transport=dry_run_transport,
            )
        )
    except (ShadowAlertError, AlertTransportError) as exc:
        print(f"PROJECT={PROJECT}")
        print(f"MODE={MODE}")
        print("ALERT_DELIVERY_STATUS=BLOCKED")
        print("TRANSPORT_ID=dry-run")
        print("EXTERNAL_DELIVERY=False")
        print(f"ALERT_STATE_PATH={state_path}")
        print(f"ERROR_CLASS={exc.__class__.__name__}")
        print(f"ERROR={exc}")
        return 2

    _print_result(result, state_path=state_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
