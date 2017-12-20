import pytest
from kohuhu import exchanges
from kohuhu import credentials
import decimal
from decimal import Decimal


@pytest.fixture(autouse=True)
def load_creds():
    credentials.load_credentials()

def test_fee_as_factor():
    # Setup
    transaction_size = Decimal(100)
    fee = Decimal(0.01)

    # Test
    # A percentage fee of 1% on a transaction of 100 should result in a total
    # fee of 0.990099 (6sf) being charged, leaving the the transaction with a
    # value of 99.0099 (6sf). Thus, the fee factor of a 1% fee is 99.0099 (6sf).
    #
    # Another perspective: 99.0099 (6sf) * (1.00 + 0.01 fee) = 100.
    #
    # We use the context here to make it easy to compare the two decimals at
    # a precision. Alternatively, we could just use quantize instead.
    factor = exchanges.fee_as_factor(fee)
    expected_factor = Decimal(1) / Decimal(1.01)
    with decimal.localcontext(decimal.BasicContext):
        # Round the decimals to the precision of this context.
        factor = +factor
        expected_factor = +expected_factor
        assert factor == expected_factor


def test_fee_as_percentage():
    # Setup
    transaction_size = Decimal(100)
    fee_factor = Decimal(0.9)

    # Test
    # A fee factor of 0.9 on a transaction of 100 means that a 10 unit fee will
    # be charged leaving 90 units remaining. As a percentage of the final
    # transaction value (90), the fee percentage is 10/90 = 1/9 = 0.1111 (4dp).
    fee = exchanges.fee_as_percentage(fee_factor)
    expected_fee = Decimal(1) / Decimal(9)
    with decimal.localcontext(decimal.BasicContext):
        # Round the decimals to the precision of this context.
        fee = +fee
        expected_fee = +expected_fee
        assert fee == expected_fee


def test_fees():
    fees = exchanges.fees('gdax')
    # This is a bit of a stupid test, as we might as well hard-code the values.
    # However, I'm doing this as I would like to be notified if the fees for
    # gdax change.
    assert fees.maker == 0.0
    assert fees.taker == 0.0025

    # Make sure it fails for fees that we don't know.
    with pytest.raises(Exception):
        fees = exchanges.fees('cryptopedia')


def test_btc_market_spread():
    market_spread = exchanges.btc_market_spread('gdax')
    # These isn't much that can be tested.
    # I think we can safely assume that there should always be at least one
    # bid and one ask.
    assert market_spread.lowest_ask is not None
    assert market_spread.highest_bid is not None
    # Can we assert this? Do the orders stay in the order book even after they
    # have been matched?
    assert market_spread.lowest_ask.price > market_spread.highest_bid.price


# @pytest.fixture
# @pytest.mark.skip(reason="Gdax sandbox seems to be down.")
# @pytest.mark.parameterize("exchange_id", ["gdax_sandbox, gemini_sandbox"])

@pytest.mark.skip(reason="Gdax sandbox seems to be down.")
def test_get_balance():
    gdax_sandbox = exchanges.load_exchange("gdax_sandbox")
    # I'm not sure when the sandbox balance will change from zero.
    balance_json = gdax_sandbox.fetch_balance()
    assert Decimal(balance_json['BTC']['free']) == Decimal(0)
    assert Decimal(balance_json['GBP']['free']) == Decimal(0)

#@pytest.mark.skip(reason="Gdax sandbox seems to be down.")
#@pytest.mark.parametrize("exchange_id", [#"gdax_sandbox",
#                                         "gemini_sandbox"])
def test_make_limit_buy_order_gemini():
    exchange = exchanges.load_exchange("gemini_sandbox",
                                       with_authorization=True)
    amount_in_btc = 0.2
    price = 5000
    res = exchange.create_limit_buy_order('BTC/USD', amount_in_btc, 5000)
    # Example result:
    # {'order_id': '88040204', 'id': '88040204', 'symbol': 'btcusd',
    #  'exchange': 'gemini', 'avg_execution_price': '0.00', 'side': 'buy',
    #  'type': 'exchange limit', 'timestamp': '1513748090',
    #  'timestampms': 1513748090914, 'is_live': True, 'is_cancelled': False,
    #  'is_hidden': False, 'was_forced': False, 'executed_amount': '0',
    #  'remaining_amount': '1.1', 'client_order_id': '1513748090',
    #  'options': [], 'price': '10.00',
    #  'original_amount': '1.1'}, 'id': '88040204'}
    order_id = res['info']['order_id']
    order_status = request_gemini_order_status(exchange, order_id)


def request_gemini_order_status(exchange, order_id):
    res = exchange.request('order/status', api='private', method='POST',
                     params={'order_id':order_id})
    # Example result:
    # {'order_id': '88040204', 'id': '88040204', 'symbol': 'btcusd',
    #  'exchange': 'gemini', 'avg_execution_price': '0.00', 'side': 'buy',
    #  'type': 'exchange limit', 'timestamp': '1513748090',
    #  'timestampms': 1513748090914, 'is_live': True, 'is_cancelled': False,
    #  'is_hidden': False, 'was_forced': False, 'executed_amount': '0',
    #  'remaining_amount': '1.1', 'client_order_id': '1513748090', 'options': [],
    #  'price': '10.00', 'original_amount': '1.1'}







