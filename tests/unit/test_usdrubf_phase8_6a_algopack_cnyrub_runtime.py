from __future__ import annotations

from pathlib import Path

from moex_research.runners import usdrubf_phase8_6a_algopack_cnyrub_runtime as runtime


def test_runtime_loads_parent_project_env_and_installs_timestamp_policy_before_validation(
    monkeypatch,
) -> None:
    calls: list[tuple[object, ...]] = []

    def fake_load_dotenv(path: Path, *, override: bool) -> bool:
        calls.append(("load_dotenv", path, override))
        return True

    def fake_install_timestamp_policy() -> None:
        calls.append(("install_timestamp_policy",))

    def fake_validation_main(argv: list[str] | None) -> int:
        calls.append(("validation_main", argv))
        return 0

    monkeypatch.setattr(runtime, "load_dotenv", fake_load_dotenv)
    monkeypatch.setattr(
        runtime,
        "install_timestamp_policy",
        fake_install_timestamp_policy,
    )
    monkeypatch.setattr(runtime, "validation_main", fake_validation_main)

    argv = ["--run-id", "phase8_6a_algopack_cnyrub_source_validation_20260722_v2"]
    expected_project_env = Path(__file__).resolve().parents[3] / ".env"

    assert runtime.main(argv) == 0
    assert calls == [
        ("load_dotenv", expected_project_env, False),
        ("install_timestamp_policy",),
        ("validation_main", argv),
    ]
    assert runtime.PROJECT_ENV_PATH == expected_project_env
