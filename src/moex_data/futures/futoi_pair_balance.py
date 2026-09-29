"""Versioned FIZ/YUR pair tolerance; source values are never adjusted."""
from collections.abc import Mapping
from decimal import Decimal, localcontext
import json

STRICT = "exact_zero_v1"
RELATIVE = "max_one_side_contracts_1pct_v1"
CONTRACT = "contracts/intelligence/futoi_pair_balance_tolerance_v1.json"


def evaluate(fiz, yur):
    values = [side.get(key) for side in (fiz, yur) for key in ("long", "short", "net")]
    if any(type(value) is not int for value in values):
        raise ValueError("FUTOI balance requires exact integer positions")
    for side in (fiz, yur):
        if min(side['long'], side['short']) < 0 or side['long'] - side['short'] != side['net']:
            raise ValueError("FUTOI balance requires exact per-side net identity and signs")
    long = fiz['long'] + yur['long']
    short = fiz['short'] + yur['short']
    residual = fiz['net'] + yur['net']
    denominator = max(long, short)
    # Integer cross multiplication keeps the inclusive 1% boundary exact,
    # including contract counts beyond binary floating-point precision.
    accepted = 100 * abs(residual) <= denominator
    with localcontext() as context:
        context.prec = 40
        percent = format(Decimal(abs(residual)) * 100 / Decimal(denominator), 'f') if denominator else None
    return {'schema_version':'futoi_pair_balance.v1', 'policy':RELATIVE,
            'contract_ref':CONTRACT, 'total_long':long, 'total_short':short,
            'signed_net_imbalance':residual, 'absolute_net_imbalance':abs(residual),
            'denominator_contracts':denominator, 'denominator_basis':'max_total_long_total_short',
            'limit_percent':1, 'comparison':'100 * absolute_net_imbalance <= denominator_contracts',
            'imbalance_percent_decimal':percent, 'accepted':accepted}


def admit(fiz, yur, policy):
    check = evaluate(fiz, yur)
    if policy == STRICT:
        if check['signed_net_imbalance'] != 0:
            raise ValueError("FIZ/YUR net positions do not balance to zero")
        return None
    if policy != RELATIVE:
        raise ValueError("unsupported FUTOI pair balance policy")
    if not check['accepted']:
        raise ValueError("FIZ/YUR net imbalance exceeds 1% of total contracts")
    return check


def validate_factual(factual):
    """Absent metadata means legacy exact-zero, never an implicit upgrade."""
    check = factual.get('balance_check')
    if 'balance_check' not in factual:
        admit(factual['fiz'], factual['yur'], STRICT)
        return STRICT
    if (factual.get('raw_schema_version') != 'v2'
            or factual.get('source_identity_scope') != 'source_ticker_root'
            or not isinstance(check, Mapping)
            or json.dumps(check, sort_keys=True, allow_nan=False) != json.dumps(
                admit(factual['fiz'], factual['yur'], RELATIVE), sort_keys=True, allow_nan=False)):
        raise ValueError("FUTOI balance policy or arithmetic evidence mismatch")
    return RELATIVE


def from_provenance(provenance):
    policy = provenance.get('pair_balance_policy', STRICT)
    if policy not in (STRICT, RELATIVE):
        raise ValueError("unsupported frozen FUTOI pair balance policy")
    return policy
