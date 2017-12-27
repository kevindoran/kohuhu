import pytest
from kohuhu.arbitrage import OneWayPairArbitrage
import kohuhu.trader as trader
from kohuhu.exchanges import CreateOrder
from kohuhu.exchanges import Order
from kohuhu.exchanges import ExchangeState
import decimal
import kohuhu.currency as currency
from decimal import Decimal
from queue import Queue

# It doesn't actually used these exchanges. The exchanges just have to be
# something for which we have fee data.
bid_on_exchange = 'gdax_sandbox'
ask_on_exchange = 'gemini_sandbox'


class DummyTimer:
    def do_every(self, delta, callback):
        pass


def create_bid_limit_order(id, price, amount):
    order = Order()
    order.order_id = id
    order.price = price
    order.amount = amount
    order.side = Order.Side.BID
    order.type = Order.Type.LIMIT
    order.filled = Decimal(0)
    order.remaining = amount
    order.status = Order.Status.OPEN
    return order


def create_market_ask_order(id, amount):
    order = Order()
    order.id = id
    order.type = Order.Type.MARKET
    order.side = Order.Side.BID
    order.filled = Decimal(0)
    order.remaining = amount
    order.status = Order.Status.OPEN
    return order

class _TestData:
    """Some common test attributes collected here.

    This class name is prefixed with _ so that pytest doesn't try treat it as
    a class containing tests.
    """

    def __init__(self):
        self.algorithm = None
        self.trader = None
        self.state = None
        self.exch_1_state = None
        self.exch_2_state = None
        self.action_queue = None
        self.bid_action = None

    def step_algorithm(self):
        self.algorithm.on_data()
        actions = list(self.action_queue.queue)
        self.action_queue.queue.clear()
        return actions


@pytest.fixture
def empty_data():
    """Setup a trader with empty data and the OneWayPairArbitrage algo.

    Returns:
        (TestDat): initialized test data.
    """
    td = _TestData()
    td.algorithm = OneWayPairArbitrage(bid_on_exchange, ask_on_exchange)
    td.trader = trader.Trader(td.algorithm, [])
    td.state = td.trader.state
    td.exch_1_state = ExchangeState(bid_on_exchange, exchange_client=None)
    td.exch_2_state = ExchangeState(ask_on_exchange, exchange_client=None)
    td.state.add_exchange(td.exch_1_state)
    td.state.add_exchange(td.exch_2_state)
    td.action_queue = Queue()
    td.algorithm.initialize(td.state, DummyTimer(), td.action_queue)
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
    # Initialize the order book of the exchange to sell on.
    td.exch_2_state.order_book().set_bids_remaining(at_price=Decimal(20000),
                                                    remaining=Decimal(5.0))
    td.exch_2_state.order_book().set_bids_remaining(at_price=Decimal(1600),
                                                    remaining=Decimal(5.0))
    # Give us lots of money so that balance isn't an issue by default.
    one_million = Decimal(1000000)
    td.exch_1_state.balance().set_free("USD", one_million)
    # Assign a bid amount.
    td.bid_amount = Decimal("1.0")
    td.algorithm.bid_amount_in_btc = td.bid_amount
    # Target 10% profit.
    td.profit_target = Decimal("0.1")
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
    actions = td.step_algorithm()
    # Note: is it okay to have asserts in the fixture?
    assert_single_limit_bid(actions, td.bid_amount)
    # 2: mark the bid limit order as succeeded.
    td.bid_action = actions[0]
    td.order_id = '5'
    price = Decimal(10000)
    td.bid_action.status = CreateOrder.Status.SUCCESS
    order = create_bid_limit_order(td.order_id, price, td.bid_action.amount)
    td.bid_action.order = order
    td.exch_1_state.set_order(td.order_id, order)
    return td


def assert_single_limit_bid(actions, amount=None):
    """Asserts that the only action is a limit bid order.

    This is a convenience method to save having to type these asserts multiple
    times.
    """
    assert len(actions) == 1
    bid_order_action = actions[0]
    assert bid_order_action.type == Order.Type.LIMIT
    assert bid_order_action.side == Order.Side.BID
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
    assert ask_order_action.type == Order.Type.MARKET
    assert ask_order_action.side == Order.Side.ASK
    assert ask_order_action.exchange == ask_on_exchange
    if amount:
        # Only compare to about 9 dp (whatever the BasicContext dp limit is).
        # TODO: figure out how to compare Decimals.
        rounded = ask_order_action.amount.quantize(Decimal(10) ** -10)
        assert amount == rounded


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
    exch_2_market_price = td.exch_2_state.order_book().bid_prices()[0]

    # Action
    actions = td.step_algorithm()

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
    td.exch_1_state.balance().set_free('USD', small_balance)
    # Try calculate how many BTC we can buy with our limited balance.
    exch_2_market_price = td.exch_2_state.order_book().bid_prices()[0]
    bid_price_estimate = small_balance / exch_2_market_price
    # Accounting for our profit target and any fee buffer.
    arbitary_buffer = Decimal(0.05)  # Make smaller to make the test stricter.
    bid_price_estimate = bid_price_estimate * (Decimal(1) - td.profit_target -
                                               arbitary_buffer)
    max_can_afford = exch_2_market_price / bid_price_estimate

    # Action
    # Run a step of the algorithm.
    actions = td.step_algorithm()

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
    td.bid_action.status = CreateOrder.Status.PENDING

    # Action
    # Run a step of the algorithm.
    actions = td.step_algorithm()

    # Check
    assert len(actions) == 0


def test_reset_on_failed_ask(state_after_one_bid_order):
    """Tests that another bid limit is placed if one fails."""
    td = state_after_one_bid_order

    # Setup
    # Switch the status of the order to failed.
    td.bid_action.status = CreateOrder.Status.FAILED

    # Action 1
    print("A")
    actions = td.step_algorithm()
    print("B")

    # Check
    assert len(actions) == 0

    # Action 2
    print("C")
    actions = td.step_algorithm()
    print("D")

    # Check
    # In the next step should try another bid order.
    assert len(actions) == 1
    assert_single_limit_bid(actions, td.bid_amount)


def test_makes_market_order(state_after_one_bid_order):
    """Tests that the algorithm places a market order correctly."""
    td = state_after_one_bid_order

    # Action
    actions = td.step_algorithm()

    # Check
    # The order is 0 filled, so there should be no market ask.
    assert len(actions) == 0

    # Setup
    # Half-fill the bid order.
    bid_order = td.exch_1_state.order(td.order_id)
    bid_amount = Decimal(bid_order.amount)
    three_dp = Decimal("0.001")
    # Round the filled about to 3dp for ease of reading.
    filled = (bid_amount / Decimal(2)).quantize(three_dp, decimal.ROUND_HALF_UP)
    bid_order.filled = filled
    bid_order.remaining = bid_amount - filled

    # Action
    # Step the algorithm again.
    actions = td.step_algorithm()

    # Check
    # An ask market order should be made for the filled amount of the bid order.
    assert_single_market_ask(actions, amount=filled)


def fill_limit_bid(order, by_amount):
    # To makes the numbers easier to read, keep by_amount to 4 dp.
    three_dp = Decimal("0.0001")
    rounded = by_amount.quantize(three_dp, decimal.ROUND_DOWN)
    if by_amount != rounded:
        raise Exception("When testing, try to keep by_amount to 4 dp.")

    # Remaining and filled should add to the total.
    # Note: is this actually the case in all instances?
    # This is an error in the testing code, if the assert is false.
    assert currency.round_to_satoshi(order.amount - order.filled -
                                     order.remaining) \
           == 0
    # The order can't be filled for more than it's total amount
    fully_filled = (order.filled + by_amount) >= order.amount
    order.filled = min(order.amount, (order.filled + by_amount))
    order.remaining = max(0, (order.remaining - by_amount))
    print(
        "Filled: {}, remaining: {}".format(order.filled, order.remaining))
    return fully_filled


def test_make_multiple_bids(state_after_one_bid_order):
    """Tests that the algorithm makes more bids as they are filled.

    This test keeps filling the limit bid orders and making sure that the
    algorithm makes market asks and limit bids when appropriate.
    """
    td = state_after_one_bid_order

    # Start by filling the order by increment.
    increment = Decimal("0.15")
    bid_order = td.exch_1_state.order(td.order_id)
    is_filled = fill_limit_bid(bid_order, increment)

    for i in range(0, 100):
        # Step the algorithm.
        print("Order ID: {}".format(bid_order.order_id))
        actions = td.step_algorithm()

        # If we have filled the order, make sure there is both a market order
        # and a new bid limit order, else there should be just a market order.
        if is_filled:
            print("filled")
            assert len(actions) == 2
            bid_action, ask_action = actions
            if bid_action.side == Order.Side.ASK:
                ask_action, bid_action = actions
            assert_single_limit_bid([bid_action], td.bid_amount)
            assert_single_market_ask([ask_action])
            # Reset the order details.
            bid_action.status = CreateOrder.Status.SUCCESS
            bid_action.order = bid_order
            bid_order.filled = 0
            bid_order.remaining = td.bid_amount
        else:
            assert_single_market_ask(actions, increment)
        is_filled = fill_limit_bid(bid_order, increment)


# TODO: test_updates_bid_price()