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
from kohuhu.custom_exceptions import InvalidOperationError

log = logging.getLogger(__name__)


class GdaxExchange(ExchangeClient):
    default_websocket_url = 'wss://ws-feed.gdax.com'

    def __init__(self,
                 api_credentials=None,
                 websocket_url=default_websocket_url):
        """Creates a new Gdax Exchange"""
        super().__init__()
        # Public attributes
        self._exchange_state = ExchangeState('gdax', self)

        """Indicates that this exchange is both connected and has fully populated the orderbook"""
        self.order_book_ready = asyncio.Event()

        # Private attributes
        self._websocket_url = websocket_url
        self._channels = ['user', 'heartbeat', 'level2']  # user channel will only receive messages if authenticated
        self._symbol = 'BTC-USD'
        self._websocket = None
        self._message_queue = asyncio.Queue()
        self._on_update_callback = None

        self._api_credentials = api_credentials
        self._authenticate = self._api_credentials is not None

        self._received_message_count = 0
        self._last_sequence_number = None

    def set_on_change_callback(self, callback):
        """Sets the callback that is invoked when the state of the exchange
        changes. For example, the order book is updated, or an order is filled."""
        self._on_update_callback = callback

    async def run(self):
        """
        Run this Gdax exchange, listening for and processing websocket messages.

        Usage:
            loop.run_until_complete(gdax.run())
        """
        try:
            # Open our websocket
            await self._connect_websocket()

            # Group our background coroutines into a single task and wait on this
            await asyncio.gather(
                self._listen_websocket_feed(),
                self._process_websocket_messages())
        finally:
            await self._close_websocket()

    def exchange_state(self):
        return self._exchange_state

    async def _connect_websocket(self):
        """
        Open the websocket feed to Gdax for all market data updates, orders
        and trades. Gdax uses a single websocket to feed all information.
        """
        self._websocket = await websockets.connect(self._websocket_url)

        # We must send a subscribe message within 5 seconds of opening the websocket
        subscribe_message = json.dumps(self._build_subscribe_parameters())
        await self._websocket.send(subscribe_message)

    async def _close_websocket(self):
        """Closes the websocket connection"""
        if self._websocket is not None:
            await self._websocket.close()

    async def _listen_websocket_feed(self):
        """
        Listen for all market data updates, orders, and trades.
        Gdax uses a single websocket to feed all information.
        """
        if self._websocket is None:
            raise InvalidOperationError("Websocket is not connected. You must call "
                                        "connect_websocket() before listening on the "
                                        "websocket channel.")
        try:
            # This blocks waiting for a new websocket message
            async for message in self._websocket:
                if self._message_queue.qsize() >= 100:
                    log.warning(f"Websocket message queue is has {self._message_queue.qsize()} pending messages")
                await self._message_queue.put(message)
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
            'product_ids': [self._symbol],
            'channels': self._channels
        }

        if self._authenticate:
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
        hmac_key = base64.b64decode(self._api_credentials.api_secret)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode('utf-8')

        auth_params = {
            'signature': signature_b64,
            'key': self._api_credentials.api_key,
            'passphrase': self._api_credentials.passphrase,
            'timestamp': timestamp
        }

        return auth_params

    async def _process_websocket_messages(self):
        """Processes messages added to the message queue by the websocket task. This calls
        the _handle_message() method for each message then calls the _on_update_callback().
        If multiple messages are received at once (or in a short interval), _on_update_callback
        will only be called once"""
        while True:
            message = await self._message_queue.get()
            self._received_message_count += 1
            self._handle_message(message)
            if not self._message_queue.empty():
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
        log.debug("Received heartbeat message")
        current_sequence_number = heartbeat['sequence']

        # If this is the first heartbeat, start counting websocket messages from now.
        if self._last_sequence_number is None:
            self._last_sequence_number = current_sequence_number
            self._received_message_count = 0
            return

        # Otherwise check that the difference in the sequence numbers matches our count.
        expected_messages_received = current_sequence_number - self._last_sequence_number
        if expected_messages_received != self._received_message_count:
            error_message = f"Expected {expected_messages_received} but only received " \
                            f"{self._received_message_count} since last heartbeat"
            log.error(error_message)
            raise Exception(error_message)

        # Reset the counts for the next heartbeat
        self._last_sequence_number = current_sequence_number
        self._received_message_count = 0

    def _handle_order(self, order):
        """TODO"""
        print(order)

    def _handle_subscriptions(self, subscriptions):
        """Check that the subscription acknowledgement message matches our subscribe request"""
        log.debug("Received subscription acknowledgement message")

        channels = subscriptions['channels']
        if len(channels) != len(self._channels):
            err_msg = f"Received unexpected channels: {channels}"
            raise Exception(err_msg)

        for channel in channels:
            channel_name = channel['name']
            if channel_name not in self._channels:
                err_msg = f"Received an unexpected channel: {channel}"
                log.error(err_msg)
                raise Exception(err_msg)

            # Check symbols
            channel_symbols = channel['product_ids']
            if len(channel_symbols) != 1:
                err_msg = f"Received unexpected symbols: {channel_symbols} for channel {channel_name}"
                raise Exception(err_msg)
            if channel_symbols[0] != self._symbol:
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
            self._exchange_state.order_book().set_bids_remaining(bid_price, bid_quantity)

        for ask in asks:
            ask_price = Decimal(ask[0])
            ask_quantity = Decimal(ask[1])
            self._exchange_state.order_book().set_asks_remaining(ask_price, ask_quantity)

        # After having received a snapshot response, we consider the exchange orderbook
        # to be ready.
        self.order_book_ready.set()

    def _handle_l2_update(self, order_book_update):
        """TODO"""
        changes = order_book_update['changes']
        for change in changes:
            side = change[0]  # Either 'buy' or 'sell'
            price = Decimal(change[1])
            quantity = Decimal(change[2])

            if side == 'buy':
                self._exchange_state.order_book().set_bids_remaining(price, quantity)
            elif side == 'sell':
                self._exchange_state.order_book().set_asks_remaining(price, quantity)
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

async def send_orders_when_ready():
    print("Order book not yet ready")
    await gdax.order_book_ready.wait()
    print("Order book is now ready")

try:
    asyncio.ensure_future(send_orders_when_ready())
    loop.run_until_complete(gdax.run())
finally:
    loop.stop()
    loop.close()
