from datetime import date

import pytest

from moex_data.futures.front_next_binding import FrontNextBindingError, bind_front_next, contract_identity


def test_bind_front_next_is_expiry_ordered_and_point_in_time_safe():
    rows = [
        contract_identity(root="Si", secid="SiZ6", expiration_date="2026-12-17", last_trade_date="2026-12-17", available_from="2025-12-01"),
        contract_identity(root="Si", secid="SiU6", expiration_date="2026-09-17", last_trade_date="2026-09-17", available_from="2025-09-01"),
        contract_identity(root="Si", secid="SiH7", expiration_date="2027-03-18", last_trade_date="2027-03-18", available_from="2026-10-01"),
    ]
    binding = bind_front_next("Si", date(2026, 8, 24), rows)
    assert binding.front.secid == "SiU6"
    assert binding.next.secid == "SiZ6"


def test_bind_front_next_excludes_expired_contracts():
    rows = [
        contract_identity(root="CR", secid="CRM6", expiration_date="2026-06-18", last_trade_date="2026-06-18", available_from="2024-12-01"),
        contract_identity(root="CR", secid="CRU6", expiration_date="2026-09-17", last_trade_date="2026-09-17", available_from="2025-03-07"),
        contract_identity(root="CR", secid="CRZ6", expiration_date="2026-12-17", last_trade_date="2026-12-17", available_from="2025-06-05"),
    ]
    binding = bind_front_next("CR", "2026-08-24", rows)
    assert (binding.front.secid, binding.next.secid) == ("CRU6", "CRZ6")


def test_bind_front_next_fails_closed_without_two_known_contracts():
    rows = [contract_identity(root="Si", secid="SiU6", expiration_date="2026-09-17", last_trade_date="2026-09-17", available_from="2025-09-01")]
    with pytest.raises(FrontNextBindingError, match="at least two"):
        bind_front_next("Si", "2026-08-24", rows)
