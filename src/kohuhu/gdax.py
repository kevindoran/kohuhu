from kohuhu.exchanges import ExchangeClient
import json
import websockets
from decimal import Decimal
import logging
from kohuhu.exchanges import ExchangeState
import time
import base64
import hmac
import hashlib
from enum import Enum

log = logging.getLogger(__name__)


class OrderResponse:
    def __init__(self):
        pass


class GdaxExchange(ExchangeClient):
    def __init__(self, api_credentials=None):
        """Creates a new Gdax Exchange"""
        super().__init__()
        self.exchange_state = ExchangeState("gdax", self)
        self.channels = ['user', 'heartbeat', 'level2']  # user channel will only receive messages if authenticated
        self.symbol = 'BTC-USD'
        self.message_queue = asyncio.Queue()
        self._on_update_callback = None

        # Authentication attributes
        self.api_credentials = api_credentials
        self.authenticate = self.api_credentials is not None

        # Websocket sequence check attributes
        self.received_message_count = 0
        self.last_sequence_number = None

    def set_on_change_callback(self, callback):
        """Sets the callback that is invoked when the state of the exchange
        changes. For example, the order book is updated, or an order is filled."""
        self._on_update_callback = callback

    def initialize(self):
        """TODO"""
        market_data_receive_task = self._open_websocket_feed
        process_market_data_task = self._process_websocket_messages

        return market_data_receive_task, process_market_data_task

    async def _open_websocket_feed(self):
        """
        Open the websocket feed to Gdax` and listen for all market data updates
        for orders and trades. Gdax uses a single websocket to feed all information.
        """
        try:
            async with websockets.connect('wss://ws-feed.gdax.com') as websocket:
                subscribe_message = json.dumps(self._build_subscribe_parameters())

                # We must send a subscribe message within 5 seconds of opening the websocket
                await websocket.send(subscribe_message)

                # This blocks waiting for a new websocket message
                async for message in websocket:
                    if self.message_queue.qsize() >= 100:
                        log.warning(f"Websocket message queue is has {self.message_queue.qsize()} pending messages")
                    await self.message_queue.put(message)
        except websockets.exceptions.InvalidStatusCode as ex:
            if str(ex.status_code).startswith("5"):
                log.error("Exchange offline")

    def _build_subscribe_parameters(self):
        """
        Builds the subscribe parameters dictionary, including the authenticate parameters
        if authentication is enabled.
        :return: A dict of subscribe parameters.
        """
        subscribe_params = {
            'type': 'subscribe',
            'product_ids': [self.symbol],
            'channels': self.channels
        }

        if self.authenticate:
            subscribe_params.update(self._build_authenticate_parameters())

        return subscribe_params

    def _build_authenticate_parameters(self):
        """
        Builds the authenticate parameters that when included with a subscribe message
        will authenticate the client against the gdax websocket.
        :return: A dict of auth parameters.
        """
        timestamp = str(time.time())
        message = timestamp + 'GET' + '/users/self/verify'
        message = message.encode('ascii')
        hmac_key = base64.b64decode(self.api_credentials.api_secret)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode('utf-8')

        auth_params = {
            'signature': signature_b64,
            'key': self.api_credentials.api_key,
            'passphrase': self.api_credentials.passphrase,
            'timestamp': timestamp
        }

        return auth_params

    async def _process_websocket_messages(self):
        """TODO"""
        while True:
            message = await self.message_queue.get()
            self.received_message_count += 1
            self._handle_message(message)
            if not self.message_queue.empty():
                # If we've already got another update, then update our
                # orderbook before we call the callback.
                continue

            # Call the callback, our orderbook is now up to date.
            self._on_update_callback()

    def _handle_message(self, message):
        """TODO"""
        response = json.loads(message)
        response_type = response['type']

        if response_type == 'snapshot':
            self._handle_snapshot(response)

        elif response_type == 'subscriptions':
            self._handle_subscriptions(response)

        elif response_type == 'heartbeat':
            pass
            #TODO: for some reason the sequence number does not match our counts.
            #self._handle_heartbeat(response)

        elif response_type == 'l2update':
            print(".", end="", flush=True)
            self._handle_l2_update(response)

        # Valid orders sent to the matching engine are confirmed immediately and are in the received state.
        # If an order executes against another order immediately, the order is considered done.
        # An order can execute in part or whole. Any part of the order not filled immediately,
        # will be considered open. Orders will stay in the open state until canceled or subsequently
        # filled by new orders. Orders that are no longer eligible for matching (filled or canceled)
        # are in the done state.
        elif response_type == 'received':
            self._handle_order(response)
        elif response_type == 'open':
            self._handle_order(response)
        elif response_type == 'done':
            self._handle_order(response)
        elif response_type == 'match':
            self._handle_order(response)
        elif response_type == 'change':
            self._handle_order(response)
        else:
            error_message = f"Got unexpected response: {response_type}"
            raise Exception(error_message)

    def _handle_heartbeat(self, heartbeat):
        """
        Checks that the sequence number in the heartbeat message matches the number of
        messages that we have received over the websocket channel.
        If there is a mismatch, an exception will be raised.
        """
        current_sequence_number = heartbeat['sequence']

        # If this is the first heartbeat, start counting websocket messages from now.
        if self.last_sequence_number is None:
            self.last_sequence_number = current_sequence_number
            self.received_message_count = 0
            return

        # Otherwise check that the difference in the sequence numbers matches our count.
        expected_messages_received = current_sequence_number - self.last_sequence_number
        if expected_messages_received != self.received_message_count:
            error_message = f"Expected {expected_messages_received} but only received " \
                            f"{self.received_message_count} since last heartbeat"
            log.error(error_message)
            raise Exception(error_message)

        # Reset the counts for the next heartbeat
        self.last_sequence_number = current_sequence_number
        self.received_message_count = 0

    def _handle_order(self, order):
        """TODO"""
        print(order)

    def _handle_subscriptions(self, subscriptions):
        """Check that the subscription acknowledgement message matches our subscribe request"""
        log.debug("Received subscription acknowledgement message")

        channels = subscriptions['channels']
        if len(channels) != len(self.channels):
            err_msg = f"Received unexpected channels: {channels}"
            raise Exception(err_msg)

        for channel in channels:
            channel_name = channel['name']
            if channel_name not in self.channels:
                err_msg = f"Received an unexpected channel: {channel}"
                log.error(err_msg)
                raise Exception(err_msg)

            # Check symbols
            channel_symbols = channel['product_ids']
            if len(channel_symbols) != 1:
                err_msg = f"Received unexpected symbols: {channel_symbols} for channel {channel_name}"
                raise Exception(err_msg)
            if channel_symbols[0] != self.symbol:
                err_msg = f"Received unexpected symbol: {channel_symbols[0]} for channel {channel_name}"
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
        for bid in bids:
            # gdax uses [price, quantity]
            bid_price = Decimal(bid[0])
            bid_quantity = Decimal(bid[1])
            self.exchange_state.order_book().set_bids_remaining(bid_price, bid_quantity)

        for ask in asks:
            ask_price = Decimal(ask[0])
            ask_quantity = Decimal(ask[1])
            self.exchange_state.order_book().set_asks_remaining(ask_price, ask_quantity)

    def _handle_l2_update(self, order_book_update):
        """TODO"""
        changes = order_book_update['changes']
        for change in changes:
            side = change[0]  # Either 'buy' or 'sell'
            price = Decimal(change[1])
            quantity = Decimal(change[2])

            if side == 'buy':
                self.exchange_state.order_book().set_bids_remaining(price, quantity)
            elif side == 'sell':
                self.exchange_state.order_book().set_asks_remaining(price, quantity)
            else:
                raise Exception("Unexpected update side: " + side)



last_printed_percent = Decimal(10)
def on_data():
    # Update slice
    # Call algorithm

    global last_printed_percent


    #gdax_best_bid = gdax.exchange_stateorder_book.bids.iloc[0]

    # Can we buy on gemini & sell on gdax for a profit?
    diff = Decimal(16000) - 15000
    percent = diff / 15000 * 100

    if abs(percent - last_printed_percent) > 0.01:
        last_printed_percent = percent
        print("")
        if percent > 0:
            print(f"Profit! Difference is: {percent:.2f}")
        else:
            print(f":( Difference is: {percent:.2f}")


def exchange_offline(message):
    # Do something!
    print(message)

# Main event loop
import asyncio
loop = asyncio.get_event_loop()


from kohuhu import credentials
credentials.load_credentials("../../api_credentials.json")
creds = credentials.credentials_for('gdax')


# Create our exchanges, these take an on_data callback every time the order book is updated
# print("Connecting to gemini orderbook websocket. Every '*' is a gemini orderbook update.")
# gemini = GeminiExchange(on_data, exchange_offline)
print("Connecting to gdax orderbook websocket. Every '.' is a gdax orderbook update.")
gdax = GdaxExchange()
gdax.set_on_change_callback(on_data)

# Create tasks to listen to the websocket
# gemini_websocket_listener_task = asyncio.ensure_future(gemini.initialize())
web_task, process_task = gdax.initialize()
gdax_websocket_listener_task = asyncio.ensure_future(web_task())

# Create tasks to process new updates
# gemini_processor_task = asyncio.ensure_future(gemini.process_queue())
gdax_processor_task = asyncio.ensure_future(process_task())

tasks = [
            # gemini_websocket_listener_task,
            # gemini_processor_task,
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