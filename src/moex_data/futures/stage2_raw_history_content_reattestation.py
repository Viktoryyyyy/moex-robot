from __future__ import annotations

import sys
from pathlib import Path

from . import stage2_raw_history_content_reattestation_durability as _durability

# Execute the reviewed implementation body under the canonical import name so
# dataclasses and other module-aware machinery can resolve cls.__module__ via
# sys.modules.  When invoked with `python -m`, __name__ starts as __main__;
# register the same module object under its canonical name before exec so the
# implementation's own __main__ guard cannot run before the durability override.
_IMPL_PATH = Path(__file__).with_name("stage2_raw_history_content_reattestation_impl.inc")
_REAL_NAME = __name__
_CANONICAL_NAME = "moex_data.futures.stage2_raw_history_content_reattestation"
_current_module = sys.modules[_REAL_NAME]
_existing_canonical = sys.modules.get(_CANONICAL_NAME)
if _existing_canonical is not None and _existing_canonical is not _current_module:
    raise RuntimeError("canonical content-reattestation module already loaded as a different object")
sys.modules[_CANONICAL_NAME] = _current_module
globals()["__name__"] = _CANONICAL_NAME
try:
    exec(compile(_IMPL_PATH.read_text(encoding="utf-8"), _IMPL_PATH.as_posix(), "exec"), globals(), globals())
finally:
    globals()["__name__"] = _REAL_NAME


def _publish_marker(marker):
    """Durably publish one already-validated immutable generation."""
    generation_id = _safe_token(marker.get("generation_id"), "generation_id")
    generation_root = _generation_root(generation_id)
    _durability.fsync_generation(generation_root)
    return _durability.durable_replace_json(_current_marker_path(), marker)


if _REAL_NAME == "__main__":
    raise SystemExit(main())
