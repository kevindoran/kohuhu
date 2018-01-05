from kohuhu.gemini import GeminiExchange
import kohuhu.exchanges as exchanges
import pytest
import asyncio
import datetime
import os
import kohuhu.credentials as credentials
from decimal import Decimal


import logging
# TODO: is there a way to do this from the command line?
# When something fails, something is spamming the INFO log.
#logging.disable(logging.WARNING)

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


# Using pytest-asyncio to inject an event loop.
# Uncomment the following to use the default event loop in tests.
# The default event loop might be needed if tasks are created from code other
# than the test code.
@pytest.yield_fixture()
def event_loop():
    """Yield the default event loop."""
    loop = asyncio.get_event_loop()
    loop.set_debug(False)
    yield loop
    loop.close()

@pytest.fixture
async def live_sandbox_exchange(event_loop):
    gemini = GeminiExchange(sandbox=True)
    await gemini.open_orders_websocket()
    coroutines = gemini.coroutines()
    tasks = []
    for c in coroutines:
        t = asyncio.ensure_future(c, loop=event_loop)
        tasks.append(t)
    yield gemini
    for t in tasks:
        # From the cancel() docs:
        #     "This arranges for a CancelledError to be thrown into the wrapped
        #     coroutine on the next cycle through the event loop."
        # So we add a sleep call after calling cancel()
        t.cancel()
    await asyncio.sleep(1)
    try:
        for t in tasks:
            t.result()
    except asyncio.CancelledError as ex:
        logging.exception(ex)
    await gemini.close_orders_websocket()
    # I'm not sure if we need to call one or both of these.
    #event_loop.stop()
    #event_loop.close()


def test_get_balance(sandbox_exchange):
    """Get the balance and check that some BTC and USD are present."""
    sandbox_exchange.update_balance()
    balance = sandbox_exchange.exchange_state().balance()
    assert balance
    assert balance.free("USD") > Decimal(0)
    assert balance.free("BTC") > Decimal(0)


async def wait_until(test, max_wait=datetime.timedelta(seconds=3)):
    start_time = datetime.datetime.now()
    max_wait = datetime.timedelta(seconds=30)
    while not test():
        await asyncio.sleep(1)
        if (datetime.datetime.now() - start_time) > max_wait:
            return False
    return True


@pytest.mark.asyncio
#@pytest.mark.skip(reason="Haven't figured out why our orders aren't being "
#                         "updated.")
async def test_market_buy(live_sandbox_exchange):
    gemini = live_sandbox_exchange
    exchange_state = gemini.exchange_state()
    #success = await wait_until(lambda: len(exchange_state.order_book().asks()))
    #assert success
    #lowest_ask = exchange_state.order_book().asks()[0]
    #ask_amount = exchange_state.order_book().asks_remaining(lowest_ask)
    ## Lets make a market bid at this price.
    ## Make the bid small so we don't use up all our funds.
    #bid_amount = Decimal(min(Decimal("0.00001"), ask_amount))
    bid_amount = Decimal("0.00001")
    bid_action = exchanges.CreateOrder("gemini_sandbox",
                                       exchanges.Order.Side.BID,
                                       exchanges.Order.Type.MARKET,
                                       amount=bid_amount)
    gemini.execute_action(bid_action)
    success = await wait_until(lambda: len(exchange_state._orders))
    assert success


# Works, but is a manual process to setup the test. See above for the correct
# approach.
@pytest.mark.skip(reason="This is the manual version.")
def test_market_buy_manual(event_loop):
    gemini = GeminiExchange(sandbox=True)
    coroutines = gemini.coroutines()
    for c in coroutines:
        asyncio.ensure_future(c)
    async def _test():
        #for t in tasks:
        #    await t
        print("Finished awaiting")
        exchange_state = gemini.exchange_state()
        #def has_asks():
        #    return len(exchange_state.order_book().asks())
        await wait_until(lambda: len(exchange_state.order_book().asks()))
        lowest_ask = exchange_state.order_book().asks()[0]
        ask_amount = exchange_state.order_book().asks_remaining(lowest_ask)
        # Lets make a market bid at this price.
        bid_amount = Decimal(min(Decimal("0.5"), ask_amount))
        bid_action = exchanges.CreateOrder("gemini_sandbox",
                                           exchanges.Order.Side.BID,
                                           exchanges.Order.Type.MARKET,
                                           amount=bid_amount)
    # FIXME: how to close down the background tasks so that they don't
    # print long exception messages?
    event_loop.run_until_complete(_test())
    assert len(gemini.exchange_state().order_book().asks())
