from decimal import Decimal
from kohuhu import currency
import decimal

def test_btc_rounding():
    one_satoshi = Decimal("0.00000001")
    half_satoshi = one_satoshi / Decimal("2")
    third_satoshi = one_satoshi / Decimal("3")
    rounded_half = currency.round_to_satoshi(half_satoshi)
    print(rounded_half)
    rounded_third = currency.round_to_cents(third_satoshi)
    print(rounded_third)
    assert rounded_half == one_satoshi
    assert rounded_third == Decimal(0)
