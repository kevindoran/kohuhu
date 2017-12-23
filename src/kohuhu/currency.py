import decimal

ONE_SATOSHI = decimal.Decimal("0.00000001")
ONE_CENT = decimal.Decimal("0.01")

def round_to_satoshi(unrounded):
    """Round a given Decimal to the nearest satoshi.

    Rounding is done in ROUND_HALF_UP mode:
        0-4 -> 0
        5-9 -> 1
    """
    return unrounded.quantize(ONE_SATOSHI, decimal.ROUND_HALF_UP)


def round_to_cents(unrounded):
    """Round a given Decimal to the nearest 2 decimal places.

    Rounding is done in ROUND_HALF_UP mode:
        0-4 -> 0
        5-9 -> 1
    """
    return unrounded.quantize(ONE_CENT, decimal.ROUND_HALF_UP)


def round_down_to_cents(unrounded):
    """Round a given Decimal down to the nearest 2 decimal places."""
    return unrounded.quantize(ONE_CENT, decimal.ROUND_DOWN)
