from decimal import Decimal, localcontext

from moex_data.step8_position_risk_state import _sum_exact


def test_sum_exact_is_independent_of_ambient_decimal_exponent_bounds() -> None:
    with localcontext() as context:
        context.prec = 6
        context.Emax = 9
        context.Emin = -9
        result = _sum_exact((Decimal("1e1000000"), Decimal("1e1000000")))

    assert result == Decimal("2e1000000")


def test_sum_exact_preserves_exact_cancellation_at_large_exponent() -> None:
    with localcontext() as context:
        context.prec = 6
        context.Emax = 9
        context.Emin = -9
        result = _sum_exact((Decimal("1e1000000"), Decimal("-1e1000000")))

    assert result == Decimal("0")
