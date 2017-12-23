import asyncio
import json
import datetime
import websockets
import sortedcontainers
from sortedcontainers import SortedDict
import operator
from decimal import Decimal
import logging

log = logging.getLogger(__name__)

# bids is a dict of {price:quantity}
class OrderBook:
    """Represents an order book

    Attributes:
        timestamp (datetime): UTC time of when the order book was last updated.
        bids ({price:quantity, price:quantity, ...}): a sorted dictionary of price:quantity
            key:value pairs. The first element contains the highest bid. Both price and
            quantity are Decimals.
        asks ({price:quantity, price:quantity, ...}): a sorted dictionary of price:quantity
            key:value pairs. The first element contains the lowest ask. Both price and
            quantity are Decimals.
    """
    def __init__(self, timestamp, bids, asks):
        self.timestamp = timestamp
        self.bids = bids
        self.asks = asks


class Exchange:
    def __init__(self):
        self.order_book = self.order_book = OrderBook(
            datetime.datetime.utcnow(),
            bids=sortedcontainers.SortedDict(operator.neg),
            asks=SortedDict())


class GdaxExchange(Exchange):

    # If we have this many pending updates to add to the orderbook, our processing may not
    # be keeping up with the incoming websocket updates.
    UPDATE_QUEUE_WARNING_LIMIT = 100

    def __init__(self, order_book_update_callback):
        """Creates a new Exchange"""
        super().__init__()
        self.channels = ['heartbeat', 'level2']
        self.symbols = ['BTC-USD']  # Only on symbol is currently supported
        self.message_queue = asyncio.Queue()
        self.order_book_update_callback = order_book_update_callback

    async def initialize(self):
        """TODO"""
        async with websockets.connect('wss://ws-feed.gdax.com') as websocket:
            subscribe_message = {
                'type': 'subscribe',
                'product_ids': self.symbols,
                'channels': self.channels
            }

            # We must send a subscribe message within 5 seconds of opening the websocket
            await websocket.send(json.dumps(subscribe_message))

            # This blocks waiting for a new websocket message
            async for message in websocket:
                if self.message_queue.qsize() >= GdaxExchange.UPDATE_QUEUE_WARNING_LIMIT:
                    log.warning(f"Websocket message queue is has {self.message_queue.qsize()} pending messages")
                await self.message_queue.put(message)

    async def process_queue(self):
        """TODO"""
        while True:
            message = await self.message_queue.get()
            self._handle_message(message)
            if not self.message_queue.empty():
                # If we've already got another update, then update our
                # orderbook before we call the callback.
                continue

            # Call the callback, our orderbook is now up to date.
            self.order_book_update_callback()

    def _handle_message(self, message):
        """TODO"""
        response = json.loads(message)
        response_type = response['type']

        if response_type == 'snapshot':
            self._handle_snapshot(response)

        elif response_type == 'subscriptions':
            pass
            # TODO
            # self._handle_subscriptions(response)

        elif response_type == 'heartbeat':
            pass
            # print("Got heartbeat")
            # print(response)

        elif response_type == 'l2update':
            print(".", end="", flush=True)
            self._handle_l2_update(response)
        else:
            error_message = f"Got unexpected response: {type}"
            raise Exception(error_message)

    def _handle_subscriptions(self, subscriptions):
        """Check that the subscription acknowledgement message matches our subscribe request"""
        log.debug("Received subscription acknowledgement message")

        # Check that the channels we subscribed to match what we're going to get
        expected_channels = list(self.channels)
        channels = [c['name'] for c in subscriptions['channels']]
        for channel in channels:
            if channel not in expected_channels:
                err_msg = f"Received an unexpected channel: {channel}"
                log.error(err_msg)
                log.error(f"Channels expected: {self.channels}, channels received: {channels}")
                raise Exception(err_msg)
            expected_channels.remove(channel)
        if len(expected_channels) > 0:
            err_msg = f"Did not receive all channels that we expected"
            log.error(err_msg)
            log.error(f"Channels expected: {self.channels}, channels received: {channels}")
            raise Exception(err_msg)

        # Check that the symbols we subscribed to match what we're going to get
        expected_symbols = list(self.symbols)
        symbols = [c['product_ids'] for c in subscriptions['channels']]
        for symbol in symbols:
            if symbol not in expected_symbols:
                err_msg = f"Received an unexpected symbol: {symbol}"
                log.error(err_msg)
                log.error(f"Symbols expected: {self.symbol}, channels received: {symbols}")
                raise Exception(err_msg)
            expected_symbols.remove(symbol)
        if len(expected_symbols) > 0:
            err_msg = f"Did not receive all symbols that we expected"
            log.error(err_msg)
            log.error(f"Channels expected: {self.symbol}, channels received: {symbol}")
            raise Exception(err_msg)

    def _handle_snapshot(self, order_book_snapshot):
        """TODO"""
        log.debug("Received subscription acknowledgement message")

        bids = order_book_snapshot['bids']
        asks = order_book_snapshot['asks']

        # We use a dictionary because when we get an update message it contains only the price levels
        # where the quantity has changed. To afford an efficient update we want to be able to lookup
        # a specific price in O(1) time without having to iterate over the entire orderbook.
        # We need it to be sorted so that we can use bids[0] to get the highest bid etc.
        # bid_quotes are ordered in reverse because the first element should have the highest price.
        bid_quotes = sortedcontainers.SortedDict(operator.neg)
        ask_quotes = SortedDict()
        for bid in bids:
            # gdax uses [price, quantity]
            bid_price = Decimal(bid[0])
            bid_quantity = Decimal(bid[1])
            bid_quotes[bid_price] = bid_quantity

        for ask in asks:
            ask_price = Decimal(ask[0])
            ask_quantity = Decimal(ask[1])
            ask_quotes[ask_price] = ask_quantity

        self.order_book = OrderBook(datetime.datetime.utcnow(), bid_quotes, ask_quotes)

    def _handle_l2_update(self, order_book_update):
        """TODO"""
        changes = order_book_update['changes']
        for change in changes:
            side = change[0]  # Either 'buy' or 'sell'
            price = Decimal(change[1])
            quantity = Decimal(change[2])

            if side == 'buy':
                if quantity == 0:
                    # Zero quantity means that the price level can be removed
                    del self.order_book.bids[price]
                    continue
                self.order_book.bids[price] = quantity
            elif side == 'sell':
                if quantity == 0:
                    del self.order_book.asks[price]
                    continue
                self.order_book.asks[price] = quantity
            else:
                raise Exception("Unexpected update side: " + side)




class GeminiExchange(Exchange):
    """TODO"""
    def __init__(self, order_book_update_callback):
        """Creates a new Exchange"""
        super().__init__()
        self.channels = ['heartbeat', 'level2']
        self.symbols = ['BTC-USD'] # Only on symbol is currently supported
        self.message_queue = asyncio.Queue()
        self.order_book_update_callback = order_book_update_callback

        self.order_book = OrderBook(
            datetime.datetime.utcnow(),
            bids=sortedcontainers.SortedDict(operator.neg),
            asks=SortedDict())

    async def initialize(self):
        """TODO"""
        async with websockets.connect('wss://api.gemini.com/v1/marketdata/BTCUSD?heartbeat=true') as websocket:

            # This blocks waiting for a new websocket message
            async for message in websocket:
                if self.message_queue.qsize() >= 100:
                    log.warning(f"Websocket message queue is has {self.message_queue.qsize()} pending messages")
                await self.message_queue.put(message)

    async def process_queue(self):
        """TODO"""
        while True:
            message = await self.message_queue.get()
            self._handle_message(message)
            if not self.message_queue.empty():
                # If we've already got another update, then update our
                # orderbook before we call the callback.
                continue

            # Call the callback, our orderbook is now up to date.
            self.order_book_update_callback()

    def _handle_message(self, message):
        """TODO"""
        response = json.loads(message)
        response_type = response['type']

        if response_type == 'heartbeat':
            pass
            #print("Got heartbeat")

        elif response_type == 'update':
            print("*", end="", flush=True)
            self._handle_update(response)

        else:
            err_msg = f"Got unexpected response: {type}"
            raise Exception(err_msg)

    def _handle_update(self, update):
        """TODO"""
        events = update['events']
        # Only iterate over change events
        for event in (e for e in events if e['type'] == 'change'):
            side = event['side']
            price = Decimal(event['price'])
            quantity = Decimal(event['remaining'])

            if side == 'bid':
                if quantity == 0:
                    # Zero quantity means that the price level can be removed
                    del self.order_book.bids[price]
                    continue
                self.order_book.bids[price] = quantity
            elif side == 'ask':
                if quantity == 0:
                    del self.order_book.asks[price]
                    continue
                self.order_book.asks[price] = quantity
            else:
                raise Exception("Unexpected update side: " + side)



### EXAMPLE ###


last_printed_percent = Decimal(10)
def on_data():
    # Update slice
    # Call algorithm

    global last_printed_percent

    if len(gdax.order_book.bids) == 0 or len(gemini.order_book.asks) == 0:
        # Wait for both order books to be initialised
        return

    gdax_best_bid = gdax.order_book.bids.iloc[0]
    gemini_best_ask = gemini.order_book.asks.iloc[0]

    # Can we buy on gemini & sell on gdax for a profit?
    diff = gdax_best_bid - gemini_best_ask
    percent = diff / gemini_best_ask * 100

    if abs(percent - last_printed_percent) > 0.01:
        last_printed_percent = percent
        print("")
        if percent > 0:
            print(f"Profit! Difference is: {percent:.2f}")
        else:
            print(f":( Difference is: {percent:.2f}")

# Main event loop
loop = asyncio.get_event_loop()

# Create our exchanges, these take an on_data callback every time the order book is updated
print("Connecting to gemini orderbook websocket. Every '*' is a gemini orderbook update.")
gemini = GeminiExchange(on_data)
print("Connecting to gdax orderbook websocket. Every '.' is a gdax orderbook update.")
gdax = GdaxExchange(on_data)

# Create tasks to listen to the websocket
gemini_websocket_listener_task = asyncio.ensure_future(gemini.initialize())
gdax_websocket_listener_task = asyncio.ensure_future(gdax.initialize())

# Create tasks to process new updates
gemini_processor_task = asyncio.ensure_future(gemini.process_queue())
gdax_processor_task = asyncio.ensure_future(gdax.process_queue())

tasks = [
            gemini_websocket_listener_task,
            gemini_processor_task,
            gdax_websocket_listener_task,
            gdax_processor_task
        ]


try:
    # Run our tasks - if everything functions well, this will run forever.
    finished, pending = loop.run_until_complete(asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION))

    # If we've got here, then a task has throw an exception.

    # Finished task(s) have thrown an exception. Let's observe the task to log the exception.
    for task in finished:
        try:
            task.result()
        except Exception as ex:
            log.exception(ex)

    # Pending tasks are still running, attempt to gracefully cancel them all.
    for task in pending:
        task.cancel()

    # Wait for up to 2 seconds for the tasks to gracefully return
    finished_cancelled_tasks, pending_cancelled_tasks = loop.run_until_complete(asyncio.wait(pending, timeout=2))
    try:
        # They most like finished because we told them to cancel, when we observe them we'll catch
        # the asyncio.CancelledError.
        for task in finished_cancelled_tasks:
            task.result()

        # If a task is still pending it hasn't finished cleaning up in the timeout period
        # and you'll see: "Task was destroyed but it is pending" as we forcefully kill it.
    except asyncio.CancelledError:
        pass
        # If a task does not have an outer try..except that catches CancelledError then
        # t.result() will raise a CancelledError. This is fine.
finally:
    loop.stop()
    loop.close()
