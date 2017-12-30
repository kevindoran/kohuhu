from kohuhu.gemini import GeminiExchange
import kohuhu.exchanges as exchanges
import pytest
import asyncio
import datetime
import os
import kohuhu.credentials as credentials
from decimal import Decimal

credentials.load_credentials()

use_proxy = True
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
#@pytest.yield_fixture()
#def event_loop():
#    """Yield the default event loop."""
#    loop = asyncio.get_event_loop()
#    yield loop
#    loop.close()

@pytest.fixture
async def live_sandbox_exchange(event_loop):
    gemini = GeminiExchange(sandbox=True)
    coroutines = gemini.coroutines()
    for c in coroutines:
        asyncio.ensure_future(c, loop=event_loop)
    return gemini


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
        await asyncio.sleep(0.5)
        if (datetime.datetime.now() - start_time) > max_wait:
            assert False


@pytest.mark.asyncio
@pytest.mark.skip(reason="Haven't figured out why our orders aren't being "
                         "updated.")
async def test_market_buy(live_sandbox_exchange):
    gemini = live_sandbox_exchange
    exchange_state = gemini.exchange_state()
    await wait_until(lambda: len(exchange_state.order_book().asks()))
    lowest_ask = exchange_state.order_book().asks()[0]
    ask_amount = exchange_state.order_book().asks_remaining(lowest_ask)
    # Lets make a market bid at this price.
    bid_amount = Decimal(min(Decimal("0.0001"), ask_amount))
    bid_action = exchanges.CreateOrder("gemini_sandbox",
                                       exchanges.Order.Side.BID,
                                       exchanges.Order.Type.MARKET,
                                       amount=bid_amount)
    gemini.execute_action(bid_action)
    await wait_until(lambda: len(exchange_state._orders))
    import pdb
    pdb.set_trace()
    assert len(gemini.exchange_state().order_book().asks())


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
