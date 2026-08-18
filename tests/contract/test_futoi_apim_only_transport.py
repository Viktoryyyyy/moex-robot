from pathlib import Path

from moex_data.futures import algopack_availability_probe as availability


ROOT = Path(__file__).resolve().parents[2]
TEXT_SUFFIXES = {".py", ".yaml", ".yml", ".json", ".md"}
FORBIDDEN_FUTOI_FILE_TOKENS = (
    "https://" + "iss.moex.com",
    "MOEX_" + "ISS_BASE_URL",
    "iss_" + "base_url",
    "--iss-" + "base-url",
)
OLD_SOURCE_ID = "moex_" + "iss_futoi"


def _futoi_specific_files():
    for path in ROOT.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        rel = path.relative_to(ROOT).as_posix().lower()
        if "futoi" in rel:
            yield path


def test_futoi_specific_files_do_not_expose_public_iss_transport() -> None:
    offenders = []
    for path in _futoi_specific_files():
        text = path.read_text(encoding="utf-8", errors="replace")
        found = [token for token in FORBIDDEN_FUTOI_FILE_TOKENS if token in text]
        if found:
            offenders.append((path.relative_to(ROOT).as_posix(), found))
    assert offenders == []


def test_old_public_iss_futoi_source_identity_is_absent_repo_wide() -> None:
    offenders = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if OLD_SOURCE_ID in text:
            offenders.append(path.relative_to(ROOT).as_posix())
    assert offenders == []


def test_futoi_availability_candidates_are_single_apim_route() -> None:
    candidates = availability.endpoint_probe_candidates(
        "moex_futoi", "USDRUBF", "USDRUBF", "/iss/analyticalproducts/futoi/securities.json"
    )
    assert len(candidates) == 1
    path, params, use_apim = candidates[0]
    assert path == "/iss/analyticalproducts/futoi/securities/usdrubf.json"
    assert params == {"latest": 1}
    assert use_apim is True


def test_futoi_error_message_payload_is_not_available() -> None:
    frame = availability.pd.DataFrame([{"ERROR_MESSAGE": "not available"}])
    assert availability._futoi_schema_error(frame) == "ERROR_MESSAGE payload"


def test_canonical_futoi_source_contract_is_apim_only() -> None:
    source = (ROOT / "contracts/sources/futures/moex_algopack_futoi.v1.yaml").read_text(encoding="utf-8")
    assert "default_base_url: https://apim.moex.com" in source
    assert "token_env: MOEX_API_KEY" in source
    assert "public_iss_transport_allowed: false" in source
    assert not (ROOT / "contracts/sources/futures/moex_iss_futoi.v1.yaml").exists()
