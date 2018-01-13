from kohuhu.exchanges import ExchangeClient, Quote
import json
import websockets
from decimal import Decimal
import logging
import kohuhu.exchanges as exchanges
from kohuhu.exchanges import ExchangeState
import time
import base64
import hmac
import hashlib
from kohuhu.custom_exceptions import InvalidOperationError
import asyncio
import datetime
import requests
from requests.auth import AuthBase
import uuid

log = logging.getLogger(__name__)


class CoinbaseExchangeAuth(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (
                request.body or '')
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message.encode("ascii"), hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode('utf-8')
        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request


class GdaxExchange(ExchangeClient):
    """Gdax exchange client.

       Attributes:
           exchange_state (ExchangeState):   The exchange state
           order_book_ready (bool):  Indicates that this exchange is both
               connected and has fully populated the orderbook.
    """
    standard_exchange_name = 'gdax'
    sandbox_exchange_name = 'gdax_sandbox'

    standard_websocket_url = 'wss://ws-feed.gdax.com'
    standard_rest_api_url = 'https://api.gdax.com'

    sandbox_websocket_url = 'wss://ws-feed-public.sandbox.gdax.com'
    sandbox_rest_api_url = 'https://api-public.sandbox.gdax.com'

    def __init__(self, api_credentials=None, sandbox=False):
        """Creates a new Gdax Exchange."""
        if sandbox:
            self.exchange_id = self.sandbox_exchange_name
            self._websocket_url = self.sandbox_websocket_url
            self._rest_url = self.sandbox_rest_api_url
        else:
            self.exchange_id = self.standard_exchange_name
            self._websocket_url = self.standard_websocket_url
            self._rest_url = self.standard_rest_api_url
        super().__init__(self.exchange_id)
        self.exchange_state = ExchangeState(self.exchange_id, self)
        self.order_book_ready = asyncio.Event()
        self._channels = ['user', 'heartbeat',
                          'level2']  # user channel will only receive messages if authenticated
        self._symbol = 'BTC-USD'
        self._websocket = None
        self._message_queue = asyncio.Queue()
        self._background_task = None
        self._on_update_callback = lambda: None
        self._create_actions = []
        self._uuid_to_action = {}
        self._cancel_actions = {}
        self._api_credentials = api_credentials
        self._authenticate = self._api_credentials is not None
        self._coinbase_authenticator = None
        if self._authenticate:
            self._coinbase_authenticator = CoinbaseExchangeAuth(
                api_credentials.api_key, api_credentials.api_secret,
                api_credentials.passphrase)

        self._exchange_latency_limit = 10  # Seconds we are willing to be behind the exchange (+ the interval below)
        self._exchange_latency_check_interval = 5  # How often we check if we've gone beyond this limit
        self._last_heartbeat_time = None

        self._running = False

    def set_on_change_callback(self, callback):
        """Sets the callback that is invoked when the state of the exchange
        changes. For example, the order book is updated, or an order is filled."""
        self._on_update_callback = callback

    async def run(self):
        """Run this Gdax exchange, listening for and processing websocket messages.

        Usage:
            loop.run_until_complete(gdax.run())
        """
        try:
            # Group our background coroutines into a single task and wait on this
            self._background_task = self.run_task()
            self._running = True
            try:
                await self._background_task
            except asyncio.CancelledError:
                # A cancelled error is expected if we've called stop(). Confirm
                # this is the case by checking if we are still running.
                if self._running:
                    raise
                else:
                    pass
        finally:
            # Clean up if we have an exception
            await self.stop()

    def run_task(self):
        """A lower level version of run() that returns the sub-coroutines future used to run
        the Gdax exchange. You must manually cancel this future if you wish to stop the exchange."""

        # Our coroutine that processes messages that the listener coroutine has
        # added to the queue. This will run forever.
        process_messages_coro = self._process_websocket_messages()

        # Checks that we're still receiving messages on the websocket.
        watchdog_coro = self._watchdog()

        async def listen_websocket():
            # Open our websocket first
            await self._connect_websocket()

            # Then listen for messages, this will run forever.
            await self._listen_websocket_feed()

        return asyncio.gather(listen_websocket(), process_messages_coro,
                              watchdog_coro)

    async def stop(self):
        """Stop all background tasks and close the websocket"""
        self._running = False
        if self._background_task is not None:
            self._background_task.cancel()
        await self._close_websocket()

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
            raise InvalidOperationError(
                "Websocket is not connected. You must call "
                "connect_websocket() before listening on the "
                "websocket channel.")
        try:
            # This blocks waiting for a new websocket message
            async for message in self._websocket:
                await self._message_queue.put(message)
        except websockets.exceptions.InvalidStatusCode as ex:
            if str(ex.status_code).startswith("5"):
                log.error("Exchange offline")

    def _build_subscribe_parameters(self):
        """
        Builds the subscribe parameters dictionary, including the authenticate parameters
        if authentication is enabled.
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
            self._handle_message(message)
            if not self._message_queue.empty():
                # If we've already got another update, then update our
                # orderbook before we call the callback.
                continue

            # Call the callback, our orderbook is now up to date.
            self._on_update_callback()

    def _handle_message(self, msg):
        """The main handler for all websocket messages. This method will call
        the appropriate sub-handler based on the message type."""
        message = json.loads(msg)
        response_type = message['type']

        if response_type == 'snapshot':
            self._handle_snapshot(message)

        elif response_type == 'subscriptions':
            self._handle_subscriptions(message)

        elif response_type == 'heartbeat':
            self._handle_heartbeat(message)

        elif response_type == 'l2update':
            self._handle_l2_update(message)

        # Valid orders sent to the matching engine are confirmed immediately and are in the received state.
        # If an order executes against another order immediately, the order is considered done.
        # An order can execute in part or whole. Any part of the order not filled immediately,
        # will be considered open. Orders will stay in the open state until canceled or subsequently
        # filled by new orders. Orders that are no longer eligible for matching (filled or canceled)
        # are in the done state.
        elif response_type == 'received':
            self._handle_order(message)
        elif response_type == 'open':
            self._handle_order(message)
        elif response_type == 'done':
            self._handle_order(message)
        elif response_type == 'match':
            self._handle_order(message)
        elif response_type == 'change':
            self._handle_order(message)
        else:
            error_message = f"Got unexpected response: {response_type}"
            raise Exception(error_message)

    def _handle_heartbeat(self, heartbeat):
        """Handles the heartbeat message, validating the websocket stream is functioning correctly.

        This method checks that the 'time' value of successive heartbeats is no greater than 1.5 seconds
        or less than 0.5 seconds. This ensures that no heartbeats have been dropped, as they are sent
        every 1 second.
        Note: heartbeats also come with a sequence number. This is only useful when consuming the 'full'
        subscription because it only counts the number of messages sent on that subscription regardless of
        what you have subscribed to.

        """
        heartbeat_time = datetime.datetime.strptime(heartbeat['time'],
                                                    "%Y-%m-%dT%H:%M:%S.%fZ")
        log.debug(f"Received heartbeat with time: {heartbeat_time}")

        # This is the first heartbeat, just set our last time value and return.
        if self._last_heartbeat_time is None:
            self._last_heartbeat_time = heartbeat_time
            return

        # Check that this heartbeat is within 0.5 - 1.5s of the last.
        delta = heartbeat_time - self._last_heartbeat_time
        if delta < datetime.timedelta(
                seconds=0.5) or delta > datetime.timedelta(seconds=1.5):
            error_message = f"Heartbeat time value <0.5s or > 1.5s from last heartbeat time value. " \
                            f"Last value: {self._last_heartbeat_time}, current value: {heartbeat_time}"
            log.error(error_message)
            raise Exception(error_message)

        # Set the new last heartbeat time.
        self._last_heartbeat_time = heartbeat_time

    def _handle_order(self, order):
        """TODO"""
        raise NotImplementedError("Order handling has not been implemented")

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
        """Handles the Gdax snapshot message. This is sent shortly after opening the websocket and before
        any l2_update messages are sent. The snapshot message contains the full snapshot of the orderbook
        at the time it was sent. Any subsequent l2_update messages are delta messages only."""
        log.debug("Received subscription acknowledgement message")

        bids = order_book_snapshot['bids']
        asks = order_book_snapshot['asks']

        for bid in bids:
            # gdax uses [price, quantity]
            bid_price = Decimal(bid[0])
            bid_quantity = Decimal(bid[1])
            bid_quote = Quote(price=bid_price, quantity=bid_quantity)
            self.exchange_state.order_book().bids().set_quote(bid_quote)

        for ask in asks:
            ask_price = Decimal(ask[0])
            ask_quantity = Decimal(ask[1])
            ask_quote = Quote(price=ask_price, quantity=ask_quantity)
            self.exchange_state.order_book().asks().set_quote(ask_quote)

        # After having received a snapshot response, we consider the exchange orderbook
        # to be ready.
        self.order_book_ready.set()

    def _handle_l2_update(self, order_book_update):
        """L2_Update messages contain a change in the orderbook. Specifically, for any quote price that
        has changed, they include this price along with the new quantity at that price level. If a price
        level is removed, the quantity will be zero."""
        changes = order_book_update['changes']
        for change in changes:
            side = change[0]  # Either 'buy' or 'sell'
            price = Decimal(change[1])
            quantity = Decimal(change[2])
            quote = Quote(price=price, quantity=quantity)

            if side == 'buy':
                self.exchange_state.order_book().bids().set_quote(quote)
            elif side == 'sell':
                self.exchange_state.order_book().asks().set_quote(quote)
            else:
                raise Exception("Unexpected update side: " + side)

    async def _watchdog(self):
        """A continuously running method that periodically checks that a heartbeat message has been received
        recently.

        This check ensures that the latency between our processing of the exchange, and the real state of
        the exchange, is not too large. A large latency could indicate network issues, too much processing,
        or an issue with Gdax.
        """
        while True:
            await asyncio.sleep(self._exchange_latency_check_interval)
            if self._last_heartbeat_time is None:
                # We haven't started yet.
                continue
            utc_now = datetime.datetime.utcnow()
            time_since_last_heartbeat = utc_now - self._last_heartbeat_time
            if time_since_last_heartbeat > datetime.timedelta(
                    seconds=self._exchange_latency_limit):
                error_message = f"No heartbeat message processed in the last {time_since_last_heartbeat} " \
                                f"seconds. Time now:{utc_now}, last heartbeat: {self._last_heartbeat_time}"
                log.error(error_message)
                raise Exception(error_message)

    def _send_http_request(self, path, json_body=None, method='post'):
        url = self._rest_url + path
        function_to_call = getattr(requests, method)
        data = json.dumps(json_body) if json_body else None
        response = function_to_call(url, data=data,
                                    auth=self._coinbase_authenticator)
        if response.status_code != requests.codes.ok:
            raise Exception(f"Request for {url} failed. Response code received:"
                            f" {response.status_code}. Content: {response.content}")
        return response

    def update_balance(self):
        if not self._authenticate:
            raise InvalidOperationError("Exchange is not authenticated. You must authenticate this "
                                        "exchange by supplying apiCredentials to the constructor "
                                        "before calling this method.")

        accounts_path = "/accounts"
        response = self._send_http_request(accounts_path, method='get')
        self._update_balance_from_response(response.json())

    def _update_balance_from_response(self, json_data):
        # Note: this method is split out from update_balance for easy testing.
        for account in json_data:
            currency = account['currency']
            # Balance: total funds in the account.
            balance = Decimal(account['balance'])
            # Holds: funds on hold (not available for use).
            # Note: the Gdax example uses "hold" as the key, but the description
            # states "holds" as the key.
            holds = Decimal(account['hold'])
            available = Decimal(account['available'])
            # Unused
            # id = account['id']
            # margin_enabled = account['margin_enabled']
            # funded_amount = account['funded_amount']
            # default_amount = account['default_amount']
            self.exchange_state.balance().set_free(currency, available)
            self.exchange_state.balance().set_on_hold(currency, holds)

    def execute_action(self, action):
        if not self._authenticate:
            raise InvalidOperationError("Exchange is not authenticated. You must authenticate this "
                                        "exchange by supplying apiCredentials to the constructor "
                                        "before calling this method.")

        if action.exchange != self.exchange_id:
            raise InvalidOperationError(f"An action for exchange "
                                        f"'{action.exchange}' was given to "
                                        f"GDAX.")
        if type(action) == exchanges.CreateOrder:
            self._create_actions.append(action)
            orders_path = "/orders"
            params = self._new_order_parameters(action)
            self._send_http_request(orders_path, params)
        elif type(action) == exchanges.CancelOrder:
            self._cancel_actions[action.order_id] = action
            cancel_order_path = "/orders/" + action.order_id
            self._send_http_request(cancel_order_path, method='delete')

    def _new_order_parameters(self, create_order_action):
        """Generates the API parameters to execute the given create action."""
        parameters = {}
        # Inspired from SO, Gdax has unspecified strict requirements on the form
        # of the client order id.
        # https://stackoverflow.com/questions/47763321/gdax-api-request-returning-error-400-badrequest
        client_id = str(uuid.uuid4())
        self._uuid_to_action[client_id] = create_order_action
        parameters['client_oid'] = client_id
        parameters['side'] = 'buy' if create_order_action.side == \
                                      exchanges.Side.BID else 'sell'
        parameters['product_id'] = "BTC-USD"
        # Self-trade prevention flag. In the event that our trade would be
        # matched with another trade of ours:
        #     dc: decrease and cancel (default).
        #     co: cancel oldest.
        #     cn: cancel newest.
        #     cb: cancel both.
        # Unused:
        # parameters['stp'] = None  # The default seems fine. We shouldn't
        # ever encounter this.
        if create_order_action.type == exchanges.Order.Type.LIMIT:
            parameters['type'] = 'limit'
            parameters['price'] = str(create_order_action.price)
            parameters['size'] = str(create_order_action.amount)
            # Time in force:
            #   GTC: good till cancelled.
            #   GTT: good till time.
            #   IOC: immediate or cancel.
            #   FOK: fill or kill. Same as IOC except the whole order must
            #        me filled instead of just cancelling the remaining, as with
            #        IOC.
            parameters['time_in_force'] = 'GTC'
            # Unused:
            # parameters['cancel_after']
            # parameters['post_only']
        else:
            # TODO: we should probably be using a limit order with a
            # 'immediate or cancel' flag.
            parameters['type'] = 'market'
            parameters['size'] = str(create_order_action.amount)
            # Unused:
            # parameters['funds']
        return parameters
