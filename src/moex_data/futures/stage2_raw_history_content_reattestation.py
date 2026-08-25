from __future__ import annotations

from pathlib import Path

from . import stage2_raw_history_content_reattestation_durability as _durability

# Keep the large, already-reviewed implementation body non-executable.  Execute it
# into this canonical module namespace with a non-__main__ name so its historical
# CLI guard cannot run before the durability override below is installed.
_IMPL_PATH = Path(__file__).with_name("stage2_raw_history_content_reattestation_impl.inc")
_REAL_NAME = __name__
globals()["__name__"] = "moex_data.futures.stage2_raw_history_content_reattestation._impl"
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
