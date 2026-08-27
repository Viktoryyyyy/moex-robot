from __future__ import annotations

import json
from collections.abc import Sequence

from moex_data import step7_rub_native_d1_w1_acceptance_base as base

# Canonical Stage 7 entrypoint is intentionally thin: all validation and
# promotion invariants live in the shared hardened base, so imported base,
# wrapper, and CLI paths cannot diverge.
for _name in dir(base):
    if _name not in globals():
        globals()[_name] = getattr(base, _name)


def parse_args(argv: Sequence[str] | None = None):
    return base.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    base.load_env_file(args.env_file)
    try:
        result = base.promote(run_id=args.run_id, repo_root=args.repo_root)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 7, "status": "acceptance_failed", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
