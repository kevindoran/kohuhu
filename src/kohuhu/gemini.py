from kohuhu.exchanges import ExchangeClient
from kohuhu.exchanges import ExchangeState
import asyncio
import websockets
import logging
import json
from decimal import Decimal

log = logging.getLogger(__name__)


class GeminiExchange(ExchangeClient):

    def __init__(self):
        self.exchange_state = ExchangeState("gdax", self)
        self._market_data_queue = asyncio.Queue()
        self._orders_queue = asyncio.Queue()
        self._market_data_url = \
            'wss://api.gemini.com/v1/marketdata/BTCUSD?heartbeat=true'
        self._events_url = \
            'wss://api.gemini.com/v1/orders/events?heartbeat=true'

    def initialize(self):
        orders_receive_task = self.open_market_data_websocket()
        market_data_receive_task = self.open_orders_websocket()
        process_orders_task = asyncio.ensure_future(
            self._process_queue(self._orders_queue,
                                callback=self._handle_orders))
        process_market_data_task = asyncio.ensure_future(
            self._process_queue(self._market_data_queue,
                                callback=self._handle_market_data))
        return (orders_receive_task, market_data_receive_task,
                process_orders_task, process_market_data_task)

    async def open_orders_websocket(self):
        try:
            async with websockets.connect(self._events_url) as websocket:
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

    async def open_market_data_websocket(self):
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

    # Note: we can't have a more general method like below, as the notification
    # after update require different signatures for market and order updates.
    async def _process_queue(self, queue, callback):
        to_notify = set()
        while True:
            message = await queue.get()
            response = json.loads(message)
            if self._process_heartbeat(response):
                continue
            publishers = callback(message)
            to_notify.update(publishers)
            if not queue.empty():
                continue
            for publisher in to_notify:
                publisher.notify_subscribers()

    def _process_heartbeat(self, response):
        if response['type'] == 'heartbeat':
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
                to_update = (self.exchange_state.order_book().any_publisher,
                             self.exchange_state.order_book().bids_publisher)
                return to_update
            elif side == 'ask':
                self.exchange_state.order_book().set_asks_remaining(price,
                                                                    quantity)
                to_update = (self.exchange_state.order_book().any_publisher,
                             self.exchange_state.order_book().asks_publisher)
            else:
                raise Exception("Unexpected update side: " + side)

    def _handle_orders(self, message):
        raise NotImplementedError()

    def _exchange_offline_callback(self, message):
        print(message)

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
