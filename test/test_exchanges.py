from kohuhu.exchanges import *


def test_sorted_quotes_bids():
    """Test that the SortedQuotes data structure correctly orders bid quotes"""
    # -- Setup --
    bids = SortedQuotes(Side.BID)
    bid_high = Quote(price=Decimal(10000), quantity=Decimal(3.14))
    bid_medium = Quote(price=Decimal(800), quantity=Decimal(7))
    bid_low = Quote(price=Decimal(200.2), quantity=Decimal(5))

    # -- Action --
    bids.set_quote(bid_medium)
    bids.set_quote(bid_high)
    bids.set_quote(bid_low)

    # -- Check --
    assert bids[0] == bid_high
    assert bids[2] == bid_low
    assert len(bids) == 3

    # -- Setup --
    remove_bids_high = Quote(price=bid_high.price, quantity=Decimal(0))

    # -- Action --
    bids.set_quote(remove_bids_high)

    # -- Check --
    assert len(bids) == 2
    assert bids[0] == bid_medium


def test_sorted_quotes_asks():
    """Test that the SortedQuotes data structure correctly orders ask quotes"""
    # -- Setup --
    asks = SortedQuotes(Side.ASK)
    ask_high = Quote(price=Decimal(10000), quantity=Decimal(3.14))
    ask_medium = Quote(price=Decimal(800), quantity=Decimal(7))
    ask_low = Quote(price=Decimal(200.2), quantity=Decimal(5))

    # -- Action --
    asks.set_quote(ask_medium)
    asks.set_quote(ask_high)
    asks.set_quote(ask_low)

    # -- Check --
    assert asks[0] == ask_low
    assert asks[2] == ask_high
    assert len(asks) == 3

    # -- Setup --
    remove_asks_low = Quote(price=ask_low.price, quantity=Decimal(0))

    # -- Action --
    asks.set_quote(remove_asks_low)

    # -- Check --
    assert len(asks) == 2
    assert asks[0] == ask_medium
