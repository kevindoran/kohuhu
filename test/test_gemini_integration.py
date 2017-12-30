from kohuhu.gemini import GeminiExchange
import kohuhu.exchanges as exchanges
import pytest
import asyncio
import datetime
import os
import kohuhu.credentials as credentials
from decimal import Decimal

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
@pytest.yield_fixture()
def event_loop():
    """Yield the default event loop."""
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def live_sandbox_exchange(event_loop):
    gemini = GeminiExchange(sandbox=True)
    tasks = gemini.initialize()
    #for t in tasks:
    #    await t
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
    max_wait = datetime.timedelta(seconds=60)
    while not test():
        await asyncio.sleep(0.5)
        if (datetime.datetime.now() - start_time) > max_wait:
            assert False

@pytest.mark.skip(reason="FIXME: get tests working with async loops.")
def test_market_buy(event_loop):
    gemini = GeminiExchange(sandbox=True)
    tasks = gemini.initialize()
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
        bid_amount = Decimal(min(0.5, ask_amount))
        bid_action = exchanges.CreateOrder("gemini_sandbox",
                                           exchanges.Order.Side.BID,
                                           exchanges.Order.Type.MARKET,
                                           amount=bid_amount)
        tasks.append(event_loop.create_task(_test()))
    finished, unfinished = event_loop.run_until_complete(
            asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED))
    #event_loop.run_until_complete(_test())
    assert len(gemini.exchange_state().order_book().asks())


#@pytest.mark.asyncio
#async def test_market_buy(live_sandbox_exchange):
#    gemini = live_sandbox_exchange
#    exchange_state = gemini.exchange_state()
#    #def has_asks():
#    #    return len(exchange_state.order_book().asks())
#    await wait_until(lambda: len(exchange_state.order_book().asks()))
#    lowest_ask = exchange_state.order_book().asks()[0]
#    ask_amount = exchange_state.order_book().asks_remaining(lowest_ask)
#    # Lets make a market bid at this price.
#    bid_amount = Decimal(min(0.5, ask_amount))
#    bid_action = exchanges.CreateOrder("gemini_sandbox",
#                                       exchanges.Order.Side.BID,
#                                       exchanges.Order.Type.MARKET,
#                                       amount=bid_amount)
#    gemini.execute_action(bid_action)
#    await wait_until(lambda: bid_action.order)
#    order = bid_action.order
#    order = exchanges.Order()
#    assert order.amount == bid_amount
#    assert order.side == exchanges.Order.Side.BID
#    # Gemini only supports limit orders via there API.
#    assert order.type == exchanges.Order.Type.LIMIT

# calculate roughly how much it will cost to make a market buy
# make a market buy
# calculate how much I will get for a market sell.
# make a market sell.
# make a limit buy. Match our own limit.
# make a limit sell. Match our own limit.

