import pytest
from kohuhu.gdax import GdaxExchange
from kohuhu import credentials
import asyncio
from kohuhu.custom_exceptions import MockError

import logging
logging.disable(logging.DEBUG)

credentials.load_credentials('api_credentials.json')

@pytest.yield_fixture
@pytest.mark.timeout(5)
async def gdax_exchange(event_loop):
    creds = credentials.credentials_for('gdax', owner="tim")
    gdax = GdaxExchange(api_credentials=creds)
    gdax.set_on_change_callback(lambda: None)
    run_gdax_task = asyncio.ensure_future(gdax.run(), loop=event_loop)
    await gdax.order_book_ready.wait()
    yield gdax

    # Clean up
    await gdax.stop()
    await run_gdax_task  # This will propagate any exceptions.


@pytest.fixture
async def gdax_sandbox_exchange():
    sandbox_url = 'wss://ws-feed-public.sandbox.gdax.com'
    creds = credentials.credentials_for('gdax_sandbox', owner="tim")
    gdax = GdaxExchange(api_credentials=creds, websocket_url=sandbox_url)
    asyncio.ensure_future(gdax.run())
    await gdax.order_book_ready.wait()
    yield gdax
    await gdax.stop()


async def send_orders_when_ready(gdax):
    print("Order book not yet ready")
    await gdax.order_book_ready.wait()
    print("Order book is now ready")


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


# def test_valid_order_book(gdax_exchange):
#     bids = gdax_exchange.exchange_state().order_book().bids()
#     asks = gdax_exchange.exchange_state().order_book().asks()
#     best_bid = bids[0]
#     print(best_bid)
#     print("success")

