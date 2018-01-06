import kohuhu.exchanges as exchanges
from kohuhu.exchanges import ExchangeClient
from kohuhu.exchanges import ExchangeState
import kohuhu.encryption as encryption
import asyncio
import base64
import random
from enum import Enum
from enum import auto
import websockets
import logging
import datetime
from decimal import Decimal
import json
import hmac
from hashlib import sha384
import kohuhu.credentials as credentials
import requests

from decimal import Decimal

log = logging.getLogger(__name__)


class OrderResponse:
    """A response from Gemini's order/events endpoint."""

    class Type(Enum):
        INITIAL = auto()
        ACCEPTED = auto()
        REJECTED = auto()
        BOOKED = auto()
        FILL = auto()
        CANCELLED = auto()
        CANCEL_REJECTED = auto()
        CLOSED = auto()

    class Behavior(Enum):
        IMMEDIATE_OR_CANCEL = auto()
        MAKER_OR_CANCEL = auto()

    class Fill:
        """Part of an order response when the event type is 'fill'.

        This class may not be needed at all.
        """
        def __init__(self):
            self.trade_id = None
            self.liquidity = None
            self.price = None
            self.amount = None
            self.fee = None
            self.fee_currency = None

        @classmethod
        def from_json_dict(cls, json_dict):
            fill = cls()
            fill.trade_id = json_dict['trade_id']
            fill.liquidity = json_dict['liquidity']
            fill.price = json_dict['price']
            fill.amount = json_dict['amount']
            fill.fee = json_dict['fee']
            fill.fee_currency = json_dict['fee_currency']

    def __init__(self):
        self.type = None
        self.socket_sequence = None
        self.order_id = None
        self.event_id = None
        self.api_session = None
        self.client_order_id = None
        self.symbol = None
        self.side = None
        self.behavior = None
        self.order_type = None
        self.timestamp = None
        self.timestampms = None
        self.is_live = None
        self.is_cancelled = None
        self.is_hidden = None
        self.avg_execution_price = None
        self.executed_amount = None
        self.remaining_amount = None
        self.original_amount = None
        self.price = None
        self.total_spend = None

    def update_order(self, order):
        """Update the order object with the information from this response."""
        order.order_id = self.order_id
        order.average_price = self.avg_execution_price
        order.symbol = self.symbol
        order.side = self.side
        order.type = self.order_type
        order.amount = self.original_amount
        order.price = self.price
        order.filled = self.executed_amount
        order.remaining = self.remaining_amount
        if self.is_cancelled:
            order.status = exchanges.Order.Status.CANCELLED
        elif self.is_live:
            order.status = exchanges.Order.Status.OPEN
        else:
            order.status = exchanges.Order.Status.CLOSED
        return order

    @classmethod
    def from_json_dict(cls, json_dict):
        """Create an OrderResponse from a dict representation of JSON."""
        response = cls()
        # type
        response.type = cls.Type[json_dict['type'].upper()]
        # order_id
        response.order_id = json_dict['order_id']
        # api_session
        response.api_session = json_dict['api_session']
        # client_order_id
        response.client_order_id = json_dict.get('client_order_id', None)
        # symbol
        response.symbol = json_dict['symbol']
        # side
        if json_dict['side'] == 'sell':
            response.side = exchanges.Order.Side.ASK
        elif json_dict['side'] == 'buy':
            response.side = exchanges.Order.Side.BID
        else:
            raise Exception(f"Unexpected order side: {json_dict['side']}.")
        # behaviour
        behavior = json_dict.get('behavior', None)
        if behavior:
            if behavior == 'immediate-or-cancel':
                response.behavior = cls.Behavior.IMMEDIATE_OR_CANCEL
            elif behavior == 'maker-or-cancel':
                response.behavior = cls.Behavior.MAKER_OR_CANCEL
            else:
                raise Exception(f"Unexpected behaviour type {behavior}.")
        # order_type
        order_type = json_dict['order_type']
        if order_type == 'exchange limit':
            response.order_type = exchanges.Order.Type.LIMIT
        elif json_dict['order_type'] == 'market buy' or 'market sell':
            response.order_type = exchanges.Order.Type.MARKET
        else:
            # This might be a little strict: we might manually make
            # our own auction orders which could cause this to be hit.
            raise Exception(f"Unexpected order type: {order_type}.")
        # timestamp
        response.timestamp = json_dict['timestamp']
        # timestampms
        response.timestamp_ms = json_dict['timestampms']
        # is_live
        response.is_live = json_dict['is_live']
        # is_cancelled
        response.is_cancelled = json_dict['is_cancelled']
        # is_hidden
        response.is_hidden = json_dict['is_hidden']
        # avg_execution_price
        if 'avg_execution_price' in json_dict:
            response.avg_execution_price = \
                Decimal(json_dict['avg_execution_price'])
        # executed_amount
        if 'executed_amount' in json_dict:
            response.executed_amount = Decimal(json_dict['executed_amount'])
        # remaining_amount
        if 'remaining_amount' in json_dict:
            response.remaining_amount = Decimal(json_dict['remaining_amount'])
        # original_amount
        if 'original_amount' in json_dict:
            response.original_amount = Decimal(json_dict['original_amount'])
        # price
        if 'price' in json_dict:
            response.price = Decimal(json_dict['price'])
        # socket_sequence
        response.socket_sequence = json_dict['socket_sequence']
        return response


class GeminiExchange(ExchangeClient):

    class SocketInfo:
        """State associated with a websocket.

        Attributes:
            heartbeat_seq (int): the next expected heartbeat sequence number.
            seq (int): the next expected socket sequence number.
            heartbeat_timestamp_ms (int): the timestamp in milliseconds at which
                the last heartbeat was received.
            ws (WebSocketClientProtocol): the websocket socket.
            queue (asyncio.Queue): a queue used to access the messages received
                on this websocket.
        """
        def __init__(self):
            self.heartbeat_seq = 0
            self.seq = 0
            self.heartbeat_timestamp_ms = None
            self.ws = None
            self.connected_event = asyncio.Event()
            self.queue = asyncio.Queue()

    standard_exchange_name = "gemini"
    sandbox_exchange_name = "gemini_sandbox"
    standard_rest_url_base = "https://api.gemini.com"
    sandbox_rest_url_base = "https://api.sandbox.gemini.com"
    standard_wss_url_base = "wss://api.gemini.com"
    sandbox_wss_url_base = 'wss://api.sandbox.gemini.com'

    def __init__(self, sandbox=False):
        exchange_id = self.sandbox_exchange_name if sandbox \
            else self.standard_exchange_name
        super().__init__(exchange_id)
        self.request_retry_limit = 4
        self._rest_url_base = self.sandbox_rest_url_base if sandbox \
            else self.standard_rest_url_base
        self._wss_url_base = self.sandbox_wss_url_base if sandbox \
            else self.standard_wss_url_base
        self.exchange_state = ExchangeState(self.exchange_id, self)
        self._actions = []
        self._cancel_actions = {}
        self._orders_sock_info = self.SocketInfo()
        self._market_data_sock_info = self.SocketInfo()
        self._orders = {}

    def _nonce(self):
        """"A nonce for the Gemini exchange API.

        Gemini requires that the nonce is increasing.
        """
        # Note: if we use multithreading for a single exchange, this may
        # cause an issue.
        delta = datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)
        return int(delta.total_seconds() * 1000)

    def coroutines(self):
        """Returns all the co-routines to be run in an event loop."""
        orders_receive_coro = self._listen_on_orders()
        market_data_receive_coro = self._listen_on_market_data()
        process_orders_coro = self._process_queue(callback=self._handle_orders,
                                socket_info=self._orders_sock_info)
        process_market_data_coro = self._process_queue(
                                callback=self._handle_market_data,
                                socket_info=self._market_data_sock_info,
                                has_heartbeat_seq=False)
        return (orders_receive_coro, market_data_receive_coro,
                process_orders_coro, process_market_data_coro)

    def _encode_and_sign(self, dict_payload, encoding="ascii"):
        """Encode the payload for sending and calculate it's hash signature."""
        payload_bytes = json.dumps(dict_payload).encode(encoding)
        b64 = base64.b64encode(payload_bytes)
        creds = credentials.credentials_for(self.exchange_id)
        secret_bytes = creds.api_secret.encode(encoding)
        signature = hmac.new(secret_bytes, b64, sha384).hexdigest()
        return b64, signature

    def _create_headers(self, path, parameters=None, encoding="ascii"):
        """Returns the headers to be send with a Gemini API request.

        Gemini sends all it's data in the HTTP headers. Data is send in the
        X-GEMINI-PAYLOAD header. When sending over rest, the data should be
        encoded in ASCII, and when using websockets, the data should be encoded
        in UTF-8.

        Args:
            path (str): path of the endpoint. Gemini requires the endpoint path
                to be specified as a parameter within the JSON payload.
            parameters (dict): the data to be send in the JSON payload. This
                method appends the endpoint path and the nonce when sending
                the parameters.
            encoding (str): the encoding to use for the payload (ASCII for REST
                and UTF-8 for websockets).
        """
        if parameters is None:
            parameters = dict()
        payload = {
            'request': path,
            'nonce': self._nonce()
        }
        payload.update(parameters)
        creds = credentials.credentials_for(self.exchange_id)
        b64, signature = self._encode_and_sign(payload, encoding)
        headers = {
            # I think these two headers are set by default.
            #'Content-Type': 'text/plain',
            #'Content-Length': 0,
            'X-GEMINI-PAYLOAD': b64.decode(encoding),
            'X-GEMINI-APIKEY': creds.api_key,
            'X-GEMINI-SIGNATURE': signature
        }
        return headers

    async def open_orders_websocket(self):
        """Opens the websocket for getting our order details.

        This co-routine should not be a background task, as it can be
        important to open the websocket before doing something else (tests
        require this).
        """
        orders_path = '/v1/order/events'
        headers = self._create_headers(orders_path, encoding="utf-8")
        # Filter order events so that only events from this key are sent.
        creds = credentials.credentials_for(self.exchange_id)
        order_events_url = self._wss_url_base + orders_path + \
                           f'?heartbeat=true&apiSessionFilter={creds.api_key}'

        # Uncommented until we have the orders websocket working correctly.
        self._orders_sock_info.ws = await websockets.client.connect(
            order_events_url, extra_headers=headers)
        self._orders_sock_info.connected_event.set()

    async def open_market_data_websocket(self):
        """Opens the websocket for getting market data.

        This co-routine should not be a background task, as it can be
        important to open the websocket before doing something else (tests
        require this).
        """
        market_data_url = self._wss_url_base + \
                          '/v1/marketdata/BTCUSD?heartbeat=true'
        self._market_data_sock_info.ws = await websockets.client.connect(
            market_data_url)
        self._market_data_sock_info.connected_event.set()

    async def close_orders_websocket(self):
        await self._orders_sock_info.ws.close()

    async def close_market_data_websocket(self):
        await self._market_data_sock_info.ws.close()

    async def _listen_on_orders(self):
        """Listen on the orders websocket for updates to our orders."""
        await self._orders_sock_info.connected_event.wait()
        async for message in self._orders_sock_info.ws:
            if self._orders_sock_info.queue.qsize() >= 100:
                log.warning("Websocket message queue is has "
                            f"{self._orders_sock_info.queue.qsize()} pending "
                            "messages.")
            await self._orders_sock_info.queue.put(message)

    async def _listen_on_market_data(self):
        """Listen on the market websocket for order book updates."""
        await self._market_data_sock_info.connected_event.wait()
        async for message in self._market_data_sock_info.ws:
            if self._market_data_sock_info.queue.qsize() >= 100:
                log.warning("Websocket message queue is has "
                            f"{self._market_data_sock_info.queue.qsize()} pending"
                            " messages.")
            await self._market_data_sock_info.queue.put(message)

    async def _process_queue(self, callback, socket_info,
                             has_heartbeat_seq=True):
        """Wait on a websocket and call callback when a message is received.

        This method parses the message to JSON, deals with heartbeat messages
        and checks message sequence numbers. This processing is required for
        all Gemini websocket endpoints.
        """
        pending_callback = False
        while True:
            unparsed_message = await socket_info.queue.get()
            response = json.loads(unparsed_message)
            # Sometimes the response is a list sometimes not. Convert to list.
            message_list = response if type(response) == list else [response]
            if not message_list:
                log.warning("Received empty message from Gemini. This isn't a "
                            "type of response documented in their API docs.")
                continue
            if message_list[0]['type'] == 'heartbeat':
                if has_heartbeat_seq:
                    self._process_heartbeat(message_list[0], socket_info)
                continue
            # A non heartbeat message.
            for message in message_list:
                self._check_sequence(message, socket_info)
                state_update = callback(message)
                if state_update:
                    pending_callback = True
            if not socket_info.queue.empty():
                continue
            if pending_callback:
                self.exchange_state.update_publisher.notify()
                pending_callback = False

    @staticmethod
    def _check_sequence(response, socket_info):
        """Checks that the socket sequence of a response is valid."""
        # Subscription acknowledgement are received before the socket sequence
        # begins. If this is a subscription ack, we don't need to do anything.
        if response['type'] == 'subscription_ack':
            if socket_info.seq != 0:
                raise Exception("Subscription acknowledgements should be sent "
                                "before the socket sequence is incremented.")
            return
        socket_seq = response['socket_sequence']
        if socket_seq != socket_info.seq:
            raise Exception("We have missed a socket_sequence. The previous"
                            f" sequence was {socket_info.seq} and the "
                            f"latest is {socket_seq}.")
        socket_info.seq += 1

    @staticmethod
    def _process_heartbeat(response, socket_info):
        """Check if the heartbeat is valid. Update our heartbeat records.

        Args:
            response (dict): json response from the Gemini API.
            socket_info (SocketInfo): the SocketInfo of the connection. This
                will be updated with the latest heartbeat info.

        Returns:
            (bool): True if the response was a heartbeat, False otherwise.
        """
        if response['type'] != 'heartbeat':
            raise Exception("_process_heartbeat() called for a non-heartbeat"
                            f" request (type: {response['type']}).")
        timestamp_ms = response['timestampms']
        heartbeat_seq = response['sequence']
        # TODO: is the field socket_sequence always present?
        socket_seq = response['socket_sequence']
        # Unused response data:
        # trace_id is used fo logging. No use for it yet.
        #trace_id = response['trade_id']
        if socket_seq == 0:
            raise Exception("The heartbeat should never be the first message to"
                            "start the socket sequence.")
        if heartbeat_seq != socket_info.heartbeat_seq:
            raise Exception("We have missed a heartbeat sequence. The previous "
                            f"sequence was {socket_info.heartbeat_seq} and the "
                            f"latest is{heartbeat_seq}.")
        socket_info.heartbeat_seq += 1
        socket_info.heartbeat_timestamp_ms = timestamp_ms

    def _handle_market_data(self, response):
        """Updates the order book when a market data update is received.

        Args:
            response (dict): the socket message as a JSON dict.

        Returns:
            bool: True if the underlying exchange state has been changed.
        """
        if response['type'] != 'update':
            err_msg = f"Got unexpected response: {response['type']}"
            logging.info(err_msg)
            return
        events = response['events']
        # Only iterate over change events.
        for event in (e for e in events if e['type'] == 'change'):
            side = event['side']
            price = Decimal(event['price'])
            quantity = Decimal(event['remaining'])
            if side == 'bid':
                self.exchange_state.order_book().set_bids_remaining(price,
                                                                    quantity)
            elif side == 'ask':
                self.exchange_state.order_book().set_asks_remaining(price,
                                                                    quantity)
            else:
                raise Exception("Unexpected update side: " + side)
        return True

    def _handle_orders(self, response):
        """Update the order records when a message is received on /order/events.

        Args:
            response (dict): the socket message as a JSON dict.

        Returns:
            bool: True if the underlying exchange state has been changed.
        """
        response_type = response['type']
        state_updated = False
        if response_type == "subscription_ack":
            # Insure the subscription details are expected. Don't do anything.
            account_id = response['accountId']
            # TODO: should we do anything with the subscription id?
            # subscription_id = response['subscriptionId']
            symbol_filter = response['symbolFilter']
            api_session_filter = response['apiSessionFilter']
            event_type_filter = response['eventTypeFilter']
            if len(symbol_filter) or len(event_type_filter):
                raise Exception("No symbol or event type were specified, but "
                                "filters were registered.")
            if len(api_session_filter) != 1:
                raise Exception("1 session filter should have been registered."
                                f"{len(api_session_filter)} were registered.")
            accepted_key = api_session_filter[0]
            if accepted_key != credentials.credentials_for(self.exchange_id)\
                    .api_key:
                raise Exception("The whitelisted api session key does not "
                                "match our session key.")
        elif response_type == "initial":
            # Create a new order record for the initial response.
            order_response = OrderResponse.from_json_dict(response)
            new_order = exchanges.Order()
            order_response.update_order(new_order)
            existing_order = self.exchange_state.order(new_order.order_id)
            if existing_order:
                raise Exception("An initial response was received for an "
                                "existing order (id: {new_order.order_id}).")
            self.exchange_state.set_order(new_order.order_id, new_order)
            state_updated = True
        elif response_type == "accepted":
            # Create a new order. Mark the corresponding action as successful.
            order_response = OrderResponse.from_json_dict(response)
            new_order = exchanges.Order()
            order_response.update_order(new_order)
            self.exchange_state.set_order(new_order.order_id, new_order)
            found_action = False
            for a in self._actions:
                if id(a) == order_response.client_order_id:
                    if a.order is not None:
                        raise Exception("An order accept message was received, "
                                        "but its corresponding action already "
                                        "has an order (id:{a.order.order_id}).")
                    a.order = new_order
                    # I don't know if we need this status.
                    a.status = exchanges.Action.Status.SUCCESS
                    found_action = True
                    break
            if not found_action:
                raise Exception("Received an order accept message, but no "
                                "matching order action was found.")
            state_updated = True
        elif response_type == "rejected":
            order_response = OrderResponse.from_json_dict(response)
            log.warning(f"An order was rejected. Reason: " + response['reason'])
            new_order = exchanges.Order()
            order_response.update_order(new_order)
            self.exchange_state.set_order(new_order.order_id, new_order)
            found_action = False
            for a in self._actions:
                if id(a) == order_response.client_order_id:
                    if a.order is not None:
                        raise Exception("An order reject message was received, "
                                        "but its corresponding action already "
                                        "has an order (id:{a.order.order_id}).")
                    a.order = new_order
                    a.status = exchanges.Action.Status.FAILED
                    found_action = True
                    break
            if not found_action:
                raise Exception("Received an order reject message, but no "
                                "matching order action was found.")
            state_updated = True
        elif response_type == "booked":
            # I don't think we need to act on this.
            log.info("Order booked. Order id:{response['order_id']}.")
        elif response_type == "fill":
            order_response = OrderResponse.from_json_dict(response)
            order = self.exchange_state.order(order_response.order_id)
            if not order:
                raise Exception("Received a fill response for an unknown order "
                                f"(id:{order_response.order_id}).")
            log.info("Order fill response received for order id: "
                     f"{order_response.order_id}.")
            order_response.update_order(order)
            state_updated = True
            # TODO: we could add some checks here to see if our fee calculation
            # is correct.
        elif response_type == "cancelled":
            order_response = OrderResponse.from_json_dict(response)
            order = self.exchange_state.order(order_response.order_id)
            reason = response.get('reason', 'No reason provided.')
            # Unused:
            # cancel_command_id = response.get('cancel_command_id', None)
            if not order:
                raise Exception("Received a cancelled response for an unknown "
                                f"order (id:{order_response.order_id}). Reason:"
                                f"{reason}")
            log.info("Order fill response received for order id: "
                     f"{order_response.order_id}. Reason: {reason}")
            cancel_action = self._cancel_actions.get(order_response.order_id,
                                                     None)
            if not cancel_action:
                raise Exception("Received a cancel response but can't find a "
                                "matching cancel action.")
            cancel_action.status = exchanges.Action.Status.SUCCESS
            state_updated = True
        elif response_type == "cancel_rejected":
            order_response = OrderResponse.from_json_dict(response)
            reason = response.get('reason', 'No reason provided.')
            log.warning("Failed to cancel order (id: "
                        f"{order_response.order_id}). Reason: {reason}")
            cancel_action = self._cancel_actions.get(order_response.order_id,
                                                     None)
            if not cancel_action:
                raise Exception("Received a cancel rejected response but can't "
                                "find a matching cancel action.")
            cancel_action.status = exchanges.Action.Status.FAILED
            state_updated = True
        elif response_type == "closed":
            order_response = OrderResponse.from_json_dict(response)
            order = self.exchange_state.order(order_response.order_id)
            if not order:
                raise Exception("Received a close response for an unknown order"
                                f" (id:{order_response.order_id}).")
            log.info("Order close response received for order id: "
                     f"{order_response.order_id}.")
            order_response.update_order(order)
            state_updated = True
        else:
            raise Exception(f"Unexpected response type: {response_type}.")
        return state_updated

    def execute_action(self, action):
        """Ren the given action on this exchange."""
        if action.exchange != self.exchange_id:
            raise Exception(f"An action for exchange '{action.exchange}' was "
                            "given to GeminiExchange.")
        if type(action) == exchanges.CreateOrder:
            new_order_path = "/v1/order/new"
            params = self._new_order_parameters(action)
            self._post_http_request(new_order_path, params)
        elif type(action) == exchanges.CancelOrder:
            cancel_order_path = "v1/order/cancel"
            params = self._cancel_order_parameters(action)
            self._post_http_request(cancel_order_path, params)

    def _cancel_order_parameters(self, cancel_order_action):
        """Generates the API parameters to execute the given cancel action."""
        parameters = {
            'order_id': cancel_order_action.order_id
        }
        return parameters

    def _new_order_parameters(self, create_order_action):
        """Generates the API parameters to execute the given create action."""
        parameters = {}
        parameters['client_order_id'] = str(id(create_order_action))
        parameters['amount'] = str(create_order_action.amount)
        parameters['symbol'] = "btcusd"
        parameters['side'] = 'buy' if create_order_action.side == \
            exchanges.Order.Side.BID else 'sell'
        # The only supported type is a limit order.
        parameters['type'] = 'exchange limit'
        # A market order needs to be carried out as a limit order.
        if create_order_action.type == exchanges.Order.Type.MARKET:
            parameters['options'] = ["immediate-or-cancel"]
            # TODO: there is an opportunity to provide extra safety.
            temp_max_price = "1000000" # $1 million
            temp_min_price = "0"
            if create_order_action.side == exchanges.Order.Side.BID:
                parameters['price'] = temp_max_price
            else:
                parameters['price'] = temp_min_price
        else:
            parameters['price'] = str(create_order_action.price)
        return parameters

    def _post_http_request(self, path, parameters=None):
        """Sends a POST to the Gemini API. Retries on failure up to 4 times.

        Attributes:
            path (str): the API path to post to (e.g. /v1/orders/new).
            parameters (dict): a dictionary of parameters to be encoded into
                the payload header.
        Returns:
            (response): the response (from the requests package).
        """
        if not parameters:
            parameters = None
        url = self._rest_url_base + path
        success = False
        response = None
        for i in range(0, self.request_retry_limit):
            # Create the headers each time, as we need an updated nonce.
            headers = self._create_headers(path, parameters)
            response = requests.post(url, headers=headers)
            if response.status_code == requests.codes.ok:
                success = True
                break
        if not success:
            raise Exception("Failed to POST a request after "
                            f"{self.request_retry_limit} attempts. \n"
                            f"URL: {url}. \n"
                            f"Parameters: {str(parameters)}")
        return response

    def update_order_book(self):
        # The order book is kept up to date automatically.
        pass

    def update_balance(self):
        check_balance_path = "/v1/balances"
        r = self._post_http_request(check_balance_path)
        self._update_balance_from_response(r.json())

    def _update_balance_from_response(self, json_response):
        for balance in json_response:
            currency = balance['currency']
            amount = Decimal(balance['amount'])
            available = Decimal(balance['available'])
            # Unused:
            # availableForWithdrawl = Decimal(balance['availableForWithdrawl'])

            free = available
            on_hold = amount - available
            self.exchange_state.balance().set_free(currency, free)
            self.exchange_state.balance().set_on_hold(currency, on_hold)

    def update_orders(self):
        # The orders are updated automatically.
        pass
