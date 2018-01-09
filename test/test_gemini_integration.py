from kohuhu.gemini import GeminiExchange
import kohuhu.exchanges as exchanges
from test.common import wait_until
import pytest
import asyncio
import os
import kohuhu.credentials as credentials
from decimal import Decimal
import logging

credentials.load_credentials()

use_proxy = False
if use_proxy:
    os.environ["HTTP_PROXY"] = "http://127.0.0.1:8080"
    os.environ["HTTPS_PROXY"] = "https://127.0.0.1:8080"
    # For use with BurpSuite.
    # Taken from here: https://www.th3r3p0.com/random/python-requests-and-burp-suite.html
    os.environ["REQUESTS_CA_BUNDLE"] = "/home/k/.ssh/burpsuite_cert.pem"


@pytest.fixture
def sandbox_exchange():
    gemini = GeminiExchange(sandbox=True)
    return gemini


def test_get_balance(sandbox_exchange):
    """Get the balance and check that some BTC and USD are present."""
    sandbox_exchange.update_balance()
    balance = sandbox_exchange.exchange_state.balance()
    assert balance
    assert balance.free("USD") > Decimal(0)
    assert balance.free("BTC") > Decimal(0)


# Using pytest-asyncio to inject an event loop.
# Uncomment the following to use the default event loop in tests.
# The default event loop might be needed if tasks are created from code other
# than the test code.
#@pytest.yield_fixture()
#def event_loop():
#    """Yield the default event loop."""
#    loop = asyncio.get_event_loop()
#    loop.set_debug(False)
#    yield loop
#    loop.close()


@pytest.fixture
async def live_sandbox_exchange(event_loop):
    gemini = GeminiExchange(sandbox=True)
    run_task = gemini.run_task()
    asyncio.ensure_future(run_task, loop=event_loop)
    yield gemini
    # From the cancel() docs:
    #     "This arranges for a CancelledError to be thrown into the wrapped
    #     coroutine on the next cycle through the event loop."
    # So we add a sleep call after calling cancel()
    run_task.cancel()
    await asyncio.sleep(1)
    try:
        run_task.result()
    except asyncio.CancelledError as ex:
        logging.exception(ex)
    # I'm not sure if we need to call one or both of these.
    #event_loop.stop()
    #event_loop.close()


@pytest.mark.asyncio
async def test_market_buy(live_sandbox_exchange):
    """Executes a market bid and checks that the order is registered."""
    gemini = live_sandbox_exchange
    await gemini.setup_event()
    #await asyncio.sleep(5)
    exchange_state = gemini.exchange_state

    bid_amount = Decimal("0.00001")
    bid_action = exchanges.CreateOrder("gemini_sandbox",
                                       exchanges.Side.BID,
                                       exchanges.Order.Type.MARKET,
                                       amount=bid_amount)
    gemini.execute_action(bid_action)
    success = await wait_until(lambda: len(exchange_state._orders))
    assert success


@pytest.mark.asyncio
async def test_order_book(live_sandbox_exchange):
    """Insures the order book is populated after the gemini client starts up."""
    gemini = live_sandbox_exchange
    await gemini.setup_event()
    exchange_state = gemini.exchange_state
    success = await wait_until(lambda: len(exchange_state.order_book().asks()))
    assert success
