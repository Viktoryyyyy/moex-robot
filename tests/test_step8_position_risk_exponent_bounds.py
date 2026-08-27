from decimal import MAX_EMAX, MIN_EMIN, Decimal, Rounded, Subnormal, localcontext

import pytest

from moex_data.step8_position_risk_state import (
    Step8PositionRiskError,
    _decimal_text,
    _safe_token,
    _sum_exact,
)


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


def test_sum_exact_rejects_unrepresentable_absolute_boundary_fail_closed() -> None:
    value = Decimal(f"9e{MAX_EMAX}")
    with pytest.raises(Step8PositionRiskError, match="exact Decimal representability"):
        _sum_exact((value, value))


def test_sum_exact_preserves_small_exact_subnormal_with_sufficient_precision() -> None:
    value = Decimal(f"1e{MIN_EMIN - 3}")
    assert _sum_exact((value,)) == value


def test_sum_exact_ignores_ambient_rounded_trap_for_exact_result() -> None:
    value = Decimal("1.00E+5")
    with localcontext() as context:
        context.traps[Rounded] = True
        assert _sum_exact((value,)) == value


def test_sum_exact_ignores_ambient_subnormal_trap_for_exact_result() -> None:
    value = Decimal(f"1e{MIN_EMIN - 3}")
    with localcontext() as context:
        context.traps[Subnormal] = True
        assert _sum_exact((value,)) == value


def test_sum_exact_rejects_extreme_subnormal_before_silent_underflow() -> None:
    value = Decimal("1e-1999999999999999997")
    with pytest.raises(Step8PositionRiskError, match="resource-safety precision bound"):
        _sum_exact((value,))


def test_sum_exact_rejects_pathological_exponent_span_before_allocation() -> None:
    with pytest.raises(Step8PositionRiskError, match="resource-safety precision bound"):
        _sum_exact((Decimal("1e1000000000"), Decimal("1")))


def test_decimal_text_keeps_large_positive_exponent_compact_and_exact() -> None:
    rendered = _decimal_text(Decimal("1e1000000000"))
    assert rendered == "1E+1000000000"
    assert Decimal(rendered) == Decimal("1e1000000000")
    assert len(rendered) < 32


def test_decimal_text_preserves_normal_canonical_values() -> None:
    assert _decimal_text(Decimal("120.50")) == "120.5"
    assert _decimal_text(Decimal("1000")) == "1000"
    assert _decimal_text(Decimal("0.00100")) == "0.001"


def test_safe_token_rejects_surrounding_whitespace_without_normalization() -> None:
    with pytest.raises(Step8PositionRiskError, match="surrounding whitespace"):
        _safe_token(" risk_20260827_v1 ", "snapshot_id")
