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
        def __init__(self):
            self.heartbeat_seq = 0
            self.seq = 0
            self.heartbeat_timestamp_ms = None

    def __init__(self):
        self._exchange_state = ExchangeState("gemini", self)
        self._market_data_path = '/v1/marketdata/BTCUSD'
        self._orders_path = '/v1/orders/events'
        self._market_data_url = \
            'wss://api.gemini.com/v1/marketdata/BTCUSD?heartbeat=true'
        self._order_events_url = \
            'wss://api.gemini.com/v1/orders/events?heartbeat=true'
        self._actions = []
        self._cancel_actions = {}
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
                                callback=self._handle_orders,
                                socket_info=self._orders_sock_info))
        process_market_data_task = asyncio.ensure_future(
            self._process_queue(self._market_data_queue,
                                callback=self._handle_market_data,
                                socket_info=self._market_data_sock_info))
        return (orders_receive_task, market_data_receive_task,
                process_orders_task, process_market_data_task)

    async def _open_orders_websocket(self):
        try:
            # Prepare headers.
            payload = base64.b64encode(json.dumps({
                'request': self._orders_path,
                'nonce': encryption.generate_nonce()
            }, separators=(',', ':')))

            creds = credentials.credentials_for("gemini")

            # TODO: double check this is the correct way of generating the signature.
            signature = hmac.new(creds.api_secret, payload, sha384).hexdigest()

            headers = {
                f'X-GEMINI-PAYLOAD: {payload}',
                f'X-GEMINI-APIKEY:{creds.api_key}',
                f'X-GEMINI-SIGNATURE:{signature}'
            }

            # Filter order events so that only events from this key are sent.
            url = self._order_events_url
            url += f"apiSessionFilter={creds.api_key}"
            async with websockets.connect(url, extra_headers=headers) \
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
                # TODO
                #self._exchange_offline_callback(message=ex)
                pass

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
                # TODO
                #self._exchange_offline_callback(message=ex)
                pass

    async def _process_queue(self, queue, callback, socket_info):
        while True:
            message = await queue.get()
            response = json.loads(message)
            if response['type'] == 'heartbeat':
                self._process_heartbeat(response, socket_info)
                continue
            self._check_sequence(response, socket_info)
            callback(message)
            if not queue.empty():
                continue
            self._on_update_callback()

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
        """

        Args:
            response (dict): json response from the Gemini API.
            socket_info (SocketInfo): the SocketInfo of the connection.

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
        # Unused:
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
                self._exchange_state.order_book().set_bids_remaining(price,
                                                                    quantity)
            elif side == 'ask':
                self._exchange_state.order_book().set_asks_remaining(price,
                                                                    quantity)
            else:
                raise Exception("Unexpected update side: " + side)
            self._on_update_callback()

    def _handle_orders(self, response):
        response_type = response['type']
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
            if accepted_key != credentials.credentials_for("gemini").api_key:
                raise Exception("The whitelisted api session key does not "
                                "match our session key.")
        elif response_type == "initial":
            # Create a new order record for the initial response.
            order_response = OrderResponse.from_json_dict(response)
            new_order = exchanges.Order()
            order_response.update_order(new_order)
            existing_order = self._exchange_state.order(new_order.order_id)
            if existing_order:
                raise Exception("An initial response was received for an "
                                "existing order (id: {new_order.order_id}).")
            self._exchange_state.set_order(new_order.order_id, new_order)
        elif response_type == "accepted":
            # Create a new order. Mark the corresponding action as successful.
            order_response = OrderResponse.from_json_dict(response)
            new_order = exchanges.Order()
            order_response.update_order(new_order)
            self._exchange_state.set_order(new_order.order_id, new_order)
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
        elif response_type == "rejected":
            order_response = OrderResponse.from_json_dict(response)
            log.warning(f"An order was rejected. Reason: " + response['reason'])
            new_order = exchanges.Order()
            order_response.update_order(new_order)
            self._exchange_state.set_order(new_order.order_id, new_order)
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
        elif response_type == "booked":
            # I don't think we need to act on this.
            log.info("Order booked. Order id:{response['order_id']}.")
        elif response_type == "fill":
            order_response = OrderResponse.from_json_dict(response)
            order = self._exchange_state.order(order_response.order_id)
            if not order:
                raise Exception("Received a fill response for an unknown order "
                                f"(id:{order_response.order_id}).")
            log.info("Order fill response received for order id: "
                     f"{order_response.order_id}.")
            order_response.update_order(order)
            # TODO: we could add some checks here to see if our fee calculation
            # is correct.
        elif response_type == "cancelled":
            order_response = OrderResponse.from_json_dict(response)
            order = self._exchange_state.order(order_response.order_id)
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
        elif response_type == "closed":
            order_response = OrderResponse.from_json_dict(response)
            order = self._exchange_state.order(order_response.order_id)
            if not order:
                raise Exception("Received a close response for an unknown order"
                                f" (id:{order_response.order_id}).")
            log.info("Order close response received for order id: "
                     f"{order_response.order_id}.")
            order_response.update_order(order)
        else:
            raise Exception(f"Unexpected response type: {response_type}.")

    def _place_order(self):
        raise NotImplementedError()

    def exchange_state(self):
        return self._exchange_state

    def update_order_book(self):
        raise NotImplementedError()

    def update_balance(self):
        raise NotImplementedError()

    def update_orders(self):
        raise NotImplementedError()

    def execute_action(self, action):
        raise NotImplementedError()
