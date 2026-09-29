"""CR current-pair-only admission; never extends to a session or history."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from . import futoi_publication_audit as audit
from . import futoi_pair_balance as balance

SCOPE = "current_intraday_latest_pair_only"
MAX_AGE_SECONDS = 1200  # Existing heavy snapshot lifetime; source event also expires.
AMENDMENT_REF = "contracts/intelligence/futoi_cr_current_pair_balance_amendment_v1.json"
ORIGINAL_GRANT_REF = "contracts/intelligence/futoi_cr_current_pair_acceptance_2026-09-07.json"
ORIGINAL_GRANT_SHA256 = "6ee9abc2f44c61b325a165f966cead981b9f68e64ba3a4574bb95f974e8d46ab"


def _amendment_content():
    return dict(schema_version="futoi_cr_current_pair_balance_amendment.v1",
        instrument_id="cr_futures_family", source_id="moex_algopack_futoi",
        source_ticker="cr", source_identity_scope="source_ticker_root", raw_schema_version="v2",
        scope=SCOPE, original_grant_ref=ORIGINAL_GRANT_REF,
        original_grant_sha256=ORIGINAL_GRANT_SHA256, original_audit_policy=audit.POLICY,
        audit_policy=audit.RELATIVE_POLICY, pair_balance_policy=balance.RELATIVE,
        balance_contract_ref=balance.CONTRACT, maximum_age_seconds=MAX_AGE_SECONDS,
        historical_authority=False, completed_session_authority=False,
        dated_statistics_authority=False, stage5_authority=False, trading_authority=False)


def validate_policy_binding(policy, entry, amendment=None):
    """Bind a verified audit to its grant, including immutable retained evidence."""
    if policy == audit.POLICY:
        if amendment is not None:
            raise ValueError("strict audit must retain its original policy admission")
        return
    if policy != audit.RELATIVE_POLICY:
        raise ValueError("unsupported current pair audit policy")
    if (entry.get("evidence_ref") != ORIGINAL_GRANT_REF
            or entry.get("evidence_sha256") != ORIGINAL_GRANT_SHA256):
        raise ValueError("balance amendment original grant mismatch")
    if not isinstance(amendment, dict) or set(amendment) != {"ref", "text", "sha256"}:
        raise ValueError("balance policy amendment missing or malformed")
    if (amendment["ref"] != AMENDMENT_REF or not isinstance(amendment["text"], str)
            or hashlib.sha256(amendment["text"].encode()).hexdigest() != amendment["sha256"]):
        raise ValueError("balance policy amendment reference or SHA mismatch")
    content = json.loads(amendment["text"])
    # Compare typed content, not merely a digest an altered snapshot can recompute.
    if json.dumps(content, sort_keys=True) != json.dumps(_amendment_content(), sort_keys=True):
        raise ValueError("balance policy amendment does not prove the required policy and scope")


def _utc(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("current pair timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


def check_time(record, now):
    now = _utc(now)
    if not isinstance(record, dict) or not isinstance(record.get("factual"), dict):
        raise ValueError("current pair record or fact is missing")
    if record.get("status") != "FRESH" or record.get("failed_attempt_at"):
        raise ValueError("latest pair attempt is not fresh")
    fact = record["factual"]
    event = _utc(fact["snapshot_ts"])
    publication = _utc(fact["source_publication_time"])
    availability = _utc(fact["availability_ts_utc"])
    ingest = _utc(fact["ingest_ts_utc"])
    receipt = _utc(record["last_success_at"])
    if not event <= publication <= availability <= ingest <= receipt:
        raise ValueError("current pair causal timestamps are inconsistent")
    for value in (event, publication, availability, ingest, receipt):
        if not 0 <= (now - value).total_seconds() <= MAX_AGE_SECONDS:
            raise ValueError("current pair timestamp is future or expired")


def admit(values, record, *, root, repo_root, now):
    try:
        if root is None:
            root = audit.source._data_root()
        entry = values["instrument_acceptance"]["cr_futures_family"]["current_pair_acceptance"]
        if entry.get("accepted") is not True or entry.get("scope") != SCOPE:
            raise ValueError("current pair scope not accepted")
        gates = [g for g in values["gates"] if g.get("required") is True]
        if not gates or any(g.get("status") != "PASS" for g in gates):
            raise ValueError("required governance gate not passed")
        path = Path(repo_root) / entry["evidence_ref"]
        if path.is_symlink() or not path.resolve().is_relative_to(Path(repo_root).resolve()):
            raise ValueError("invalid acceptance evidence path")
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != entry["evidence_sha256"]:
            raise ValueError("acceptance evidence SHA mismatch")
        evidence = json.loads(content)
        if (evidence.get("scope") != SCOPE or evidence.get("policy") != audit.POLICY
                or evidence.get("instrument_id") != "cr_futures_family"
                or evidence.get("canonical_live_smoke") != "PASS"
                or evidence.get("negative_replay") != "PASS"
                or evidence.get("historical_authority") is not False):
            raise ValueError("acceptance evidence does not prove the required scope")
        check_time(record, now)
        result = audit.verify_current(root, record)
        amendment = None
        if result["policy"] == audit.RELATIVE_POLICY:
            path = Path(repo_root) / AMENDMENT_REF
            if path.is_symlink() or not path.resolve().is_relative_to(Path(repo_root).resolve()):
                raise ValueError("invalid balance policy amendment path")
            content = path.read_bytes()
            amendment = dict(ref=AMENDMENT_REF, text=content.decode(), sha256=hashlib.sha256(content).hexdigest())
        validate_policy_binding(result["policy"], entry, amendment)
        if amendment is not None:
            result["policy_amendment"] = amendment
        return dict(result, allowed=True, error=None)
    except (ValueError, KeyError, TypeError, OSError) as exc:
        return {"scope": SCOPE, "allowed": False, "error": str(exc)}


def apply_read_freshness(snapshot, *, now):
    component = snapshot.get("components", {}).get("futoi_live_cr", {})
    data = component.get("data") or {}
    if data.get("factual_authority_scope") != SCOPE:
        return
    try:
        if data.get("consumer_factual_use_allowed") is not True:
            raise ValueError("persisted current pair admission is blocked")
        check_time(data["current_intraday"], now)
    except (ValueError, KeyError, TypeError) as exc:
        component["status"] = "UNAVAILABLE"
        data["factual_authority"] = data["consumer_factual_use_allowed"] = False
        if isinstance(data.get("current_intraday"), dict):
            data["current_intraday"]["consumer_factual_use_allowed"] = False
        data["current_pair_admission"] = dict(allowed=False, scope=SCOPE, error=str(exc))
        snapshot.get("authority", {}).get("futoi_by_instrument", {}).get("cr_futures_family", {})["factual_authority"] = False
