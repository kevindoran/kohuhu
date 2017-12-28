import kohuhu.exchanges as exchanges
from kohuhu.exchanges import ExchangeClient
from kohuhu.exchanges import ExchangeState
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

from decimal import Decimal

log = logging.getLogger(__name__)


class GeminiExchange(ExchangeClient):

    class SocketInfo:
        def __init__(self):
            self.heartbeat_seq = 0
            self.seq = 0
            self.heartbeat_timestamp_ms = None

    class OrderResponse:
        """A response from Gemini's order/events endpoint."""
        class Type(Enum):
            INITIAL = auto()
            ACCEPTED = auto()
            BOOKED = auto()
            FILL = auto()
            CANCELLED = auto()
            CANCELLED_REJECTED = auto()
            CLOSED = auto()

        class Behavior(Enum):
            IMMEDIATE_OR_CANCEL = auto()
            MAKER_OR_CANCEL = auto()

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
            order.order_id = self.order_id
            order.average_price = self.avg_execution_price
            order.symbol = self.symbol
            order.side = self.side
            order.type = self.order_type
            order.amount = self.original_amount
            order.price = self.price
            order.remaining = self.remaining_amount
            if self.is_cancelled:
                order.status = exchanges.Order.Status.CANCELLED
            elif self.is_live:
                order.status = exchanges.Order.Status.OPEN
            else:
                order.status = exchanges.Order.Status.CLOSED
            return order

        @staticmethod
        def from_json_dict(cls, json_dict):
            response = GeminiExchange.OrderResponse()
            response.type = cls.Type[json_dict['type'].toupper()]
            response.order_id = json_dict['order_id']
            response.api_session = json_dict['api_session']
            response.client_order_id = json_dict.get('client_order_id', None)
            response.symbol = json_dict['symbol']
            if json_dict['side'] == 'sell':
                response.side = exchanges.Order.Side.BID
            elif json_dict['side'] == 'buy':
                response.side = exchanges.Order.Side.ASK
            else:
                raise Exception(f"Unexpected order type: {json_dict['side']}.")
            behavior = json_dict.get('behavior', None)
            if behavior == 'immediate-or-cancel':
                response.behavior = cls.Behavior.IMMEDIATE_OR_CANCEL
            elif behavior == 'maker-or-cancel':
                response.behavior = cls.Behavior.MAKER_OR_CANCEL
            else:
                raise Exception(f"Unexpected behaviour type {behavior}.")

            order_type = json_dict['order_type']
            if order_type == 'exchange limit':
                response.order_type = exchanges.Order.Type.LIMIT
            elif json_dict['order_type'] == 'market buy' or 'market sell':
                response.order_type = exchanges.Order.Type.MARKET
            else:
                # This might be a little strict: we might manually make
                # our own auction orders which could cause this to be hit.
                raise Exception(f"Unexpected order type: {order_type}.")
            response.order_type = json_dict['order_type']
            response.timestamp = json_dict['timestamp']
            response.timestamp_ms = json_dict['timestampms']
            response.is_live = json_dict['is_live']
            response.is_cancelled = json_dict['is_cancelled']
            response.is_hidden = json_dict['is_hidden']
            if 'avg_execution_price' in json_dict:
                response.avg_execution_price = \
                    Decimal(json_dict['avg_execution_price'])
            response.executed_amount = Decimal(json_dict['executed_amount'])
            response.remaining_amount = Decimal(json_dict['remaining_amount'])
            response.original_amount = Decimal(json_dict['original_amount'])
            response.price = Decimal(json_dict['price'])
            response.socket_sequence = json_dict['socket_sequence']
            return response

    def __init__(self):
        self.exchange_state = ExchangeState("gemini", self)
        self._market_data_path = '/v1/marketdata/BTCUSD'
        self._orders_path = '/v1/orders/events'
        self._market_data_url = \
            'wss://api.gemini.com/v1/marketdata/BTCUSD?heartbeat=true'
        self._order_events_url = \
            'wss://api.gemini.com/v1/orders/events?heartbeat=true'
        self._actions = []
        self._orders_sock_info = self.SocketInfo()
        self._market_data_sock_info = self.SocketInfo()
        self._orders = {}

        # The market data queue contains contains websocket responses from the
        # public market data websocket feed.
        self._market_data_queue = asyncio.Queue()

        # The orders queue contains websocket responses from the private orders
        # websocket feed.
        self._orders_queue = asyncio.Queue()
        self._on_update_callback = None

    def set_on_change_callback(self, callback):
        self._on_update_callback = callback

    def initialize(self):
        orders_receive_task = self._open_orders_websocket()
        market_data_receive_task = self._open_market_data_websocket()
        process_orders_task = asyncio.ensure_future(
            self._process_queue(self._orders_queue,
                                callback=self._handle_orders))
        process_market_data_task = asyncio.ensure_future(
            self._process_queue(self._market_data_queue,
                                callback=self._handle_market_data))
        return (orders_receive_task, market_data_receive_task,
                process_orders_task, process_market_data_task)

    @classmethod
    def generate_nonce(cls, length=8):
        """Generate pseudorandom number.

        From SO: https://stackoverflow.com/questions/5590170/what-is-the-stand
        ard-method-for-generating-a-nonce-in-python

        Note: this can be moved to encryption.py if it seems useful.
        """
        return ''.join([str(random.randint(0, 9)) for i in range(length)])

    def _create_headers(self, url):
        payload = base64.b64encode(json.dumps({
            'request': self._orders_path,
            'nonce': self.generate_nonce()
        }, separators=(',', ':')))

        creds = credentials.credentials_for("gemini")

        # TODO: double check this is the correct way of generating the signature.
        signature = hmac.new(creds.api_secret, payload, sha384).hexdigest()

        headers = {
            f'X-GEMINI-PAYLOAD: {payload}',
            f'X-GEMINI-APIKEY:{creds.api_key}',
            f'X-GEMINI-SIGNATURE:{signature}'
        }
        return headers

    async def _open_orders_websocket(self):
        try:
            async with websockets.connect(self._order_events_url,
                    extra_headers=self._create_headers(self._orders_path)) \
                    as websocket:
                # Block waiting for a new websocket message.
                async for message in websocket:
                    if self._orders_queue.qsize() >= 100:
                        log.warning("Websocket message queue is has "
                                    f"{self._orders_queue.qsize()} pending "
                                    "messages.")
                    await self._orders_queue.put(message)
        except websockets.exceptions.InvalidStatusCode as ex:
            if str(ex.status_code).startswith("5"):
                self._exchange_offline_callback(message=ex)

    async def _open_market_data_websocket(self):
        try:
            async with websockets.connect(self._market_data_url) as websocket:
                # Block waiting for a new websocket message.
                async for message in websocket:
                    if self._market_data_queue.qsize() >= 100:
                        log.warning("Websocket message queue is has "
                                    f"{self._market_data_queue.qsize()} pending"
                                    " messages.")
                    await self._market_data_queue.put(message)
        except websockets.exceptions.InvalidStatusCode as ex:
            if str(ex.status_code).startswith("5"):
                self._exchange_offline_callback(message=ex)

    async def _process_queue(self, queue, callback):
        while True:
            message = await queue.get()
            response = json.loads(message)
            if self._process_heartbeat(response):
                continue
            callback(message)
            if not queue.empty():
                continue
            self._on_update_callback()

    def _process_heartbeat(self, response, socket_info):
        """Checks if the response is a heartbeat response and processes it.

        Args:
            response (dict): json response from the Gemini API.
            socket_info (SocketInfo): the SocketInfo of the connection.
        """
        if response['type'] == 'heartbeat':
            timestamp_ms = response['timestampms']
            heartbeat_seq = response['sequence']
            # TODO: is the field socket_sequence always present?
            socket_seq = response['socket_sequence']
            # Unused:
            # trace_id is used fo logging. No use for it yet.
            #trace_id = response['trade_id']
            if socket_seq == 0:
                raise Exception("The heartbeat should never be the first "
                                "message to start the socket sequence.")
            if socket_seq != socket_info.seq:
                raise Exception("We have missed a socket_sequence. The previous"
                                f" sequence was {socket_info.seq} and the "
                                f"latest is {socket_seq}.")
            socket_info.seq += 1
            if heartbeat_seq != socket_info.heartbeat_seq:
                raise Exception("We have missed a heartbeat sequence. The "
                                "previous sequence was "
                                f"{socket_info.heartbeat_seq} and the latest is"
                                f"{heartbeat_seq}.")
            socket_info.heartbeat_seq += 1
            socket_info.heartbeat_timestamp_ms = timestamp_ms
            return True
        return False

    def _handle_market_data(self, response):
        if response['type'] != 'update':
            raise Exception()
        if response != 'update':
            err_msg = f"Got unexpected response: {type}"
            raise Exception(err_msg)

        print("*", end="", flush=True)
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
            self._on_update_callback()

    def _handle_orders(self, response):
        response_type = response['type']
        if response_type == "subscription_ack":
            account_id = response['accountId']
            # TODO: what to do with the subscription id?
            subscription_id = response['subscriptionId']
            symbol_filter = response['symbolFilter']
            api_session_filter = response['apiSessionFilter']
            event_type_filter = response['eventTypeFilter']
            if len(symbol_filter) or len(api_session_filter) or \
                    len(event_type_filter):
                raise Exception("No filters were specified, but filters were "
                                "registered.")
            return

        if response_type == "initial":
            order_response = self.OrderResponse.from_json_dict(response)
            new_order = exchanges.Order()
            order_response.update_order(new_order)
            self._orders[new_order.order_id] = new_order
            return
        elif response_type == "accepted":
            order_response = self.OrderResponse.from_json_dict(response)
            new_order = exchanges.Order()
            order_response.update_order(new_order)
            self._orders[new_order.order_id] = new_order
            for a in self._actions:
                if id(a) == order_response.client_order_id:
                    if a.order is not None:
                        raise Exception("An order accept message was received, "
                                        "but its corresponding action already "
                                        "has an order (id:{a.order.order_id}).")
                    a.order = new_order
                    # I don't know if we need this status.
                    a.status = exchanges.Action.Status.SUCCESS
                    return
            raise Exception("Received an order accept message, but no matching"
                            " order action was found.")
        elif response_type == "rejected":
            order_response = self.OrderResponse.from_json_dict(response)
            log.warning(f"An order was rejected. Reason: " + response['reason'])
            new_order = exchanges.Order()
            order_response.update_order(new_order)
            self._orders[new_order.order_id] = new_order
            for a in self._orders:
                if id(a) == order_response.client_order_id:
                    if a.order is not None:
                        raise Exception("An order reject message was received, "
                                        "but its corresponding action already "
                                        "has an order (id:{a.order.order_id}).")
                    a.order = new_order
                    a.status = exchanges.Action.Status.FAILED
                    return
            raise Exception("Received an order reject message, but no matching "
                            "order action was found.")
            pass
        elif response_type == "booked":
            # I don't think we need to act on this.
            log.info("Order booked. Order id:{response['order_id']}.")
            return
        elif response_type == "fill":
            pass
        elif response_type == "cancelled":
            pass
        elif response_type == "cancel_rejected":
            pass
        elif response_type == "closed":
            pass
        else:
            raise Exception(f"Unexpected response type: {respones_type}.")

    def _exchange_offline_callback(self, message):
        print(message)





    def _place_order(self):
        raise NotImplementedError()

    def exchange_state(self):
        raise NotImplementedError()

    def update_order_book(self):
        raise NotImplementedError()

    def update_balance(self):
        raise NotImplementedError()

    def update_orders(self):
        raise NotImplementedError()

    def execute_action(self, action):
        raise NotImplementedError()
