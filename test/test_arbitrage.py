import pytest
from kohuhu.arbitrage import OneWayPairArbitrage
import kohuhu.trader as trader
from kohuhu.trader import CreateOrder
import decimal
from decimal import Decimal


# It doesn't actually used these exchanges. The exchanges just have to be
# something for which we have fee data.
bid_on_exchange = 'gdax_sandbox'
ask_on_exchange = 'gemini_sandbox'


class _TestData:
    """Some common test attributes collected here.

    This class name is prefixed with _ so that pytest doesn't try treat it as
    a class containing tests.
    """
    def __init__(self):
        self.algorithm = None
        self.trader = None
        self.slice = None
        self.exch_1_slice = None
        self.exch_2_slice = None


@pytest.fixture
def empty_data():
    """Setup a trader with empty data and the OneWayPairArbitrage algo.

    Returns:
        (TestDat): initialized test data.
    """
    td = _TestData()
    td.algorithm = OneWayPairArbitrage(bid_on_exchange, ask_on_exchange)
    td.trader = trader.Trader(td.algorithm, [bid_on_exchange, ask_on_exchange])
    td.trader.initialize()
    td.slice = trader.Slice()
    td.trader.next_slice = td.slice
    td.exch_1_slice = trader.ExchangeSlice(bid_on_exchange, fetcher=None)
    td.exch_2_slice = trader.ExchangeSlice(ask_on_exchange, fetcher=None)
    td.slice.set_slice(bid_on_exchange, td.exch_1_slice)
    td.slice.set_slice(ask_on_exchange, td.exch_2_slice)
    return td


@pytest.fixture
def exch_2_order_book_data(empty_data):
    """Builds on empty_data() by populating the order_book for exchange 2.

    Does the following:
        * Populates the order book for the exchange to ask on with some bids
          and some asks.
        * Puts USD in the balance for the exchange to bid on.
        * Sets the bid amount to 0.1.
        * Sets the profit target to 10 %.
    """
    # Renamed variable to give it a semantic name.
    td = empty_data
    td.exch_2_slice.order_book = {
        'bids': [[20000,  5.0],
                 [1600,   5.0]],
        'asks': [[21000,  2.3],
                 [21300,  0.7]],
        'timestamp': 0
    }
    # Give us lots of money so that balance isn't an issue by default.
    one_million = 1000000
    full_balance = {
        'free': {
            'BTC': 0.00,
            'USD': one_million
        }
    }
    td.exch_1_slice.balance = full_balance
    # Assign a bid amount.
    td.bid_amount = Decimal(1.0)
    td.algorithm.bid_amount_in_btc = td.bid_amount
    # Target 10% profit.
    td.profit_target = Decimal(0.1)
    td.algorithm.profit_target = td.profit_target
    return td

@pytest.fixture
def state_after_one_bid_order(exch_2_order_book_data):
    """Builds on exch_2_order_book_data(): makes the algo create a bid order.

     1) Steps the trader so that the algo creates a bid order,
     2) Populates the order data and sets the action status to SUCCESS.
    """
    td = exch_2_order_book_data

    # Setup.
    # 1: make the bid limit order.
    actions = td.trader.step()
    # Note: is it okay to have asserts in the fixture?
    assert_single_limit_bid(actions, td.bid_amount)
    # 2: mark the bid limit order as succeeded.
    td.bid_order = actions[0]
    td.order_id = '5'
    td.bid_order.order_id = td.order_id
    td.bid_order.status = CreateOrder.Status.SUCCESS
    order_info = {
        'id': td.order_id,
        'amount': td.bid_order.amount,
        'filled': 0,
        'remaining': td.bid_order.amount
    }
    td.exch_1_slice.set_order(td.order_id, order_info)
    return td


def assert_single_limit_bid(actions, amount=None):
    """Asserts that the only action is a limit bid order.

    This is a convenience method to save having to type these asserts multiple
    times.
    """
    assert len(actions) == 1
    bid_order_action = actions[0]
    assert bid_order_action.type == CreateOrder.Type.LIMIT
    assert bid_order_action.side == CreateOrder.Side.BID
    assert bid_order_action.exchange == bid_on_exchange
    if amount:
        assert bid_order_action.amount == amount


def assert_single_market_ask(actions, amount=None):
    """Asserts that the only action is a market ask order.

    This is a convenience method to save having to type these asserts multiple
    times.
    """
    assert len(actions) == 1
    ask_order_action = actions[0]
    assert ask_order_action.type == CreateOrder.Type.MARKET
    assert ask_order_action.side == CreateOrder.Side.ASK
    assert ask_order_action.exchange == ask_on_exchange
    if amount:
        assert ask_order_action.amount == amount


def test_makes_limit_order(exch_2_order_book_data):
    """Tests that the algorithm makes a limit buy order as it's first action.

    In addition the limit buy order should:
        * be on the bid_on_exchange exchange.
        * be lower than the market bid price on ask_on_exchange.
        * should be the amount: max(balance_in_usd, algo.bid_amount_in_usd)
        * shouldn't fail if balance_in_usd is < algo.bid_amount_in_usd
        * TODO: should there be a lower limit for the bid amount? Other than
                zero, when we have run out of USD.
    """
    # Keep the data variable short, as it is used a lot.
    td = exch_2_order_book_data

    # Setup
    exch_2_market_price = Decimal(td.exch_2_slice.order_book['bids'][0][0])

    # Action
    actions = td.trader.step()

    # Check
    # Check that:
    #   * there is one bid limit action.
    #   * the bid amount is correct.
    #   * the bid price is at least profit_target % lower than the market price
    #     on exch_2.
    assert_single_limit_bid(actions, td.bid_amount)
    bid_action = actions[0]
    assert bid_action.price < (exch_2_market_price / (Decimal(1) +
                                                     td.profit_target))


def test_bid_is_balance_aware(exch_2_order_book_data):
    """Tests that the algorithm doesn't bid more than our balance allows."""
    td = exch_2_order_book_data

    # Setup
    #  * Set the balance info to have a small amount of USD (much less than the
    #    current bid_amount).
    #  * Make an estimate of how many BTC we can buy. Use this + a buffer to
    #    create an upper bound for how much we should be spending.
    small_balance = Decimal(5000)
    td.exch_1_slice.balance['free']['USD'] = small_balance
    # Try calculate how many BTC we can buy with our limited balance.
    exch_2_market_price = td.exch_2_slice.order_book['bids'][0][0]
    bid_price_estimate = small_balance / exch_2_market_price
    # Accounting for our profit target and any fee buffer.
    arbitary_buffer = Decimal(0.05) # Make smaller to make the test stricter.
    bid_price_estimate = bid_price_estimate * (Decimal(1) - td.profit_target -
                                               arbitary_buffer)
    max_can_afford = exch_2_market_price / bid_price_estimate

    # Action
    # Run a step of the algorithm.
    actions = td.trader.step()

    # Check
    # Check that:
    #   * the basic order details are correct (same as other tests).
    #   * the bid amount has been reduced due to the limited USD balance.
    # TODO: insure that, accounting for fees, we are able to execute the order
    #       for the reduced bid amount.
    assert_single_limit_bid(actions, td.bid_amount)
    bid_action = actions[0]
    assert bid_action.amount < max_can_afford


def test_no_action_while_pending_ask(state_after_one_bid_order):
    """Tests that nothing happens while the bid order is pending."""
    td = state_after_one_bid_order

    # Setup.
    # Switch the status of the order to pending.
    td.bid_order.status = CreateOrder.Status.PENDING

    # Action
    # Run a step of the algorithm.
    td.slice.timestamp += td.algorithm.poll_period
    actions = td.trader.step()

    # Check
    assert len(actions) == 0


def test_reset_on_failed_ask(state_after_one_bid_order):
    """Tests that another bid limit is placed if one fails."""
    td = state_after_one_bid_order

    # Setup
    # Switch the status of the order to failed.
    td.bid_order.status = CreateOrder.Status.FAILED

    # Action 1
    td.slice.timestamp += td.algorithm.poll_period
    actions = td.trader.step()

    # Check
    assert len(actions) == 0

    # Action 2
    td.slice.timestamp += td.algorithm.poll_period
    actions = td.trader.step()

    # Check
    # In the next step should try another bid order.
    assert len(actions) == 1
    assert_single_limit_bid(actions, td.bid_amount)


def test_makes_market_order(state_after_one_bid_order):
    """Tests that the algorithm places a market order correctly."""
    td = state_after_one_bid_order

    # Action
    td.slice.timestamp += td.algorithm.poll_period
    actions = td.trader.step()

    # Check
    # The order is 0 filled, so there should be no market ask.
    assert len(actions) == 0

    # Setup
    # Half-fill the bid order.
    bid_order = td.exch_1_slice.order(td.order_id)
    bid_amount = Decimal(bid_order['amount'])
    three_dp = Decimal("0.001")
    # Round the filled about to 3dp for ease of reading.
    filled = (bid_amount / Decimal(2)).quantize(three_dp, decimal.ROUND_HALF_UP)
    bid_order['filled'] = filled
    bid_order['remaining'] = bid_amount - filled

    # Action
    # Step the algorithm again.
    td.slice.timestamp += td.algorithm.poll_period
    actions = td.trader.step()

    # Check
    # An ask market order should be made for the filled amount of the bid order.
    assert_single_market_ask(actions, amount=filled)
