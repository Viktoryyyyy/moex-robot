from pathlib import Path

from moex_data.futures import algopack_availability_probe as availability


ROOT = Path(__file__).resolve().parents[2]
TEXT_SUFFIXES = {".py", ".yaml", ".yml", ".json", ".md"}
FORBIDDEN_FUTOI_FILE_TOKENS = (
    "https://iss.moex.com",
    "MOEX_ISS_BASE_URL",
    "iss_base_url",
    "--iss-base-url",
)


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


def test_futoi_availability_candidates_are_apim_only() -> None:
    candidates = availability.endpoint_probe_candidates(
        "moex_futoi", "USDRUBF", "USDRUBF", "/iss/analyticalproducts/futoi/securities.json"
    )
    assert candidates
    assert all(use_apim is True for _, _, use_apim in candidates)


def test_canonical_futoi_source_contract_is_apim_only() -> None:
    source = (ROOT / "contracts/sources/futures/moex_algopack_futoi.v1.yaml").read_text(encoding="utf-8")
    assert "default_base_url: https://apim.moex.com" in source
    assert "token_env: MOEX_API_KEY" in source
    assert "public_iss_transport_allowed: false" in source
    assert not (ROOT / "contracts/sources/futures/moex_iss_futoi.v1.yaml").exists()
