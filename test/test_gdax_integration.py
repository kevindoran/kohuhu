import pytest
from kohuhu.gdax import GdaxExchange
from kohuhu import credentials
import kohuhu.exchanges as exchanges
from decimal import Decimal
import asyncio
from kohuhu.custom_exceptions import MockError
import logging
from test.common import wait_until

# Disable websockets debug logging for more comprehensible logs when using -s
logger = logging.getLogger('websockets')
logger.setLevel(logging.ERROR)

credentials.load_credentials('api_credentials.json')


@pytest.yield_fixture(scope='module')  # This scope needs to be >= any async fixtures.
def event_loop():
    """Yield the default event loop."""
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='module')
@pytest.mark.timeout(5)  # Give it 5 seconds to connect
async def gdax_exchange():
    """Sets up the real Gdax exchange"""
    creds = credentials.credentials_for('gdax', owner="tim")
    gdax = GdaxExchange(api_credentials=creds, sandbox=True)
    run_gdax_task = asyncio.ensure_future(gdax.run())
    await gdax.order_book_ready.wait()
    yield gdax

    # Clean up
    await gdax.stop()
    await run_gdax_task  # This will propagate any exceptions.


@pytest.fixture(scope='module')
@pytest.mark.timeout(5)  # Give it 5 seconds to connect
async def gdax_sandbox_exchange():
    """Sets up the sandbox Gdax exchange"""
    sandbox_url = 'wss://ws-feed-public.sandbox.gdax.com'
    creds = credentials.credentials_for('gdax_sandbox', owner="tim")
    gdax = GdaxExchange(api_credentials=creds, sandbox=True)
    run_gdax_task = asyncio.ensure_future(gdax.run())
    await gdax.order_book_ready.wait()
    yield gdax

    # Clean up
    await gdax.stop()
    await run_gdax_task  # This will propagate any exceptions.


@pytest.mark.timeout(5)  # Add a timeout to assert failure if the exception is not thrown.
def test_gdax_callback_error_propagation():
    """Tests that errors raised in the callback are propagated from the gdax.run()
    method and cause the run_gdax_task to end and raise the same error"""
    def raise_test_error():
        raise MockError

    with pytest.raises(MockError):
        loop = asyncio.get_event_loop()
        gdax = GdaxExchange(credentials.credentials_for("gdax_sandbox"),
                            sandbox=True)
        gdax.set_on_change_callback(raise_test_error)
        run_gdax_task = asyncio.ensure_future(gdax.run_task())
        loop.run_until_complete(run_gdax_task)


def test_valid_orderbook(gdax_exchange):
    """This tests that the orderbook from the real Gdax exchange has values that we would expect for
    bitcoin. If this test fails it doesn't guarantee that our program has a bug, but it is very likely."""
    # -- Setup --
    min_expected_quotes_per_side = 100
    min_expected_best_bid_price = 1000
    max_expected_best_ask_price = 100000
    max_expected_quote_quantity = 10000

    bids = gdax_exchange.exchange_state.order_book().bids()
    asks = gdax_exchange.exchange_state.order_book().asks()
    best_bid = bids[0]
    best_ask = asks[0]

    # -- Check --
    assert best_ask.price > best_bid.price, \
        f"best_ask ({best_ask.price}) should always be greater than the best_bid ({best_bid.price})"

    bid_ask_spread = best_ask.price - best_bid.price
    relative_bid_ask_spread = bid_ask_spread / best_bid.price
    assert relative_bid_ask_spread < 0.1, \
        "The bid-ask spread is >10% of the the current price. best_bid: {best_bid.price}, best_ask:{best_ask.price}"

    assert len(bids) > min_expected_quotes_per_side, \
        f"There were {len(bids)} bids on gdax which is lower than the minimum of {min_expected_quotes_per_side} " \
        f"expected."
    assert len(asks) > min_expected_quotes_per_side, \
        f"There were {len(asks)} asks on gdax which is lower than the minimum of {min_expected_quotes_per_side} " \
        f"expected."

    assert best_bid.price > min_expected_best_bid_price, \
        f"Expected best_bid to be > {min_expected_best_bid_price}. Actual: {best_bid.price}"
    assert best_ask.price < max_expected_best_ask_price, \
        f"Expected best_ask to be < {max_expected_best_ask_price}. Actual: {best_ask.price}"

    assert best_bid.quantity > 0, "best_bid had zero quantity"
    assert best_ask.quantity > 0, "best_ask had zero quantity"
    assert best_bid.quantity < max_expected_quote_quantity, \
        f"best_bid had quantity {best_bid.quantity} which is > than expected {max_expected_quote_quantity}"
    assert best_ask.quantity < max_expected_quote_quantity, \
        f"best_ask had quantity {best_ask.quantity} which is > than expected {max_expected_quote_quantity}"



@pytest.mark.asyncio
async def test_execute_action(gdax_sandbox_exchange):
    gdax = gdax_sandbox_exchange
    lowest_ask_quote = gdax.exchange_state.order_book().asks()[0]
    max_amount = Decimal("0.000001")
    bid_amount = min(lowest_ask_quote.quantity, max_amount)
    bid_limit_action = exchanges.CreateOrder("gdax_sandbox",
                                             exchanges.Side.BID,
                                             exchanges.Order.Type.LIMIT,
                                             bid_amount,
                                             price=lowest_ask_quote.price)
    order_count = len(gdax.exchange_state._orders)
    assert order_count == 0
    gdax.execute_action(bid_limit_action)
    success = await wait_until(lambda: len(gdax.exchange_state._orders))
    assert success
