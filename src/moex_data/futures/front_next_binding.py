from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable


class FrontNextBindingError(ValueError):
    pass


@dataclass(frozen=True)
class ContractIdentity:
    root: str
    secid: str
    expiration_date: date
    last_trade_date: date
    available_from: date


@dataclass(frozen=True)
class FrontNextBinding:
    root: str
    as_of: date
    front: ContractIdentity
    next: ContractIdentity


def _as_date(value: date | str, field: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise FrontNextBindingError(f"{field} must be YYYY-MM-DD") from exc


def contract_identity(*, root: str, secid: str, expiration_date: date | str, last_trade_date: date | str, available_from: date | str) -> ContractIdentity:
    checked_root = str(root).strip()
    checked_secid = str(secid).strip()
    if not checked_root or not checked_secid:
        raise FrontNextBindingError("root and secid are required")
    expiry = _as_date(expiration_date, "expiration_date")
    last_trade = _as_date(last_trade_date, "last_trade_date")
    available = _as_date(available_from, "available_from")
    if last_trade > expiry:
        raise FrontNextBindingError("last_trade_date cannot be after expiration_date")
    return ContractIdentity(checked_root, checked_secid, expiry, last_trade, available)


def bind_front_next(root: str, as_of: date | str, contracts: Iterable[ContractIdentity]) -> FrontNextBinding:
    checked_root = str(root).strip()
    if not checked_root:
        raise FrontNextBindingError("root is required")
    anchor = _as_date(as_of, "as_of")
    eligible = [
        contract
        for contract in contracts
        if contract.root == checked_root
        and contract.available_from <= anchor
        and contract.last_trade_date >= anchor
    ]
    if len({contract.secid for contract in eligible}) != len(eligible):
        raise FrontNextBindingError("duplicate secid in eligible contract metadata")
    eligible.sort(key=lambda item: (item.expiration_date, item.last_trade_date, item.secid))
    if len(eligible) < 2:
        raise FrontNextBindingError("front and next require at least two eligible explicit contracts")
    if eligible[0].expiration_date == eligible[1].expiration_date:
        raise FrontNextBindingError("ambiguous front/next expiry")
    return FrontNextBinding(checked_root, anchor, eligible[0], eligible[1])
