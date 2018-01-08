import pytest
from kohuhu.gdax import GdaxExchange
from kohuhu import credentials
import asyncio
from kohuhu.custom_exceptions import MockError
import logging

# Disable debug logging for more comprehensible logs when using -s
logging.disable(logging.DEBUG)

credentials.load_credentials('api_credentials.json')


@pytest.yield_fixture(scope='module')  # Only create once for all tests
@pytest.mark.timeout(5)  # Give it 5 seconds to connect
async def gdax_exchange(event_loop):
    """Sets up the real Gdax exchange"""
    creds = credentials.credentials_for('gdax', owner="tim")
    gdax = GdaxExchange(api_credentials=creds)
    gdax.set_on_change_callback(lambda: None)
    run_gdax_task = asyncio.ensure_future(gdax.run(), loop=event_loop)
    await gdax.order_book_ready.wait()
    yield gdax

    # Clean up
    await gdax.stop()
    await run_gdax_task  # This will propagate any exceptions.


@pytest.yield_fixture(scope='module')  # Only create once for all tests
@pytest.mark.timeout(5)  # Give it 5 seconds to connect
async def gdax_sandbox_exchange(event_loop):
    """Sets up the sandbox Gdax exchange"""
    sandbox_url = 'wss://ws-feed-public.sandbox.gdax.com'
    creds = credentials.credentials_for('gdax_sandbox', owner="tim")
    gdax = GdaxExchange(api_credentials=creds, websocket_url=sandbox_url)
    gdax.set_on_change_callback(lambda: None)
    run_gdax_task = asyncio.ensure_future(gdax.run(), loop=event_loop)
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
        gdax = GdaxExchange()
        gdax.set_on_change_callback(raise_test_error)
        run_gdax_task = asyncio.ensure_future(gdax.run())
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



