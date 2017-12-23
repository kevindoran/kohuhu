
import enum
from enum import auto
import asyncio
import json
import datetime
import websockets
import sortedcontainers
from sortedcontainers import SortedDict
import operator
from decimal import Decimal
import logging
import threading
import traceback

class Quote:
    def __init__(self, price, quantity):
        self.price = price
        self.quantity = quantity



# bids is a dict of {price:quantity}
class OrderBook:
    def __init__(self, timestamp, bids, asks):
        self.timestamp = timestamp
        self.bids = bids
        self.asks = asks


class Exchange:
    def __init__(self):
        self.order_book = None


class GdaxExchange(Exchange):

    def __init__(self, new_data_callback):
        super().__init__()
        self.count = 0
        self.best_bid = 0
        self.best_ask = 1000000
        self.message_queue = asyncio.Queue()
        self.new_data_callback = new_data_callback


    async def initialize(self):
        async with websockets.connect('wss://ws-feed.gdax.com') as websocket:
            subscribe_message = {
                "type": "subscribe",
                "product_ids": [
                    "BTC-USD"
                ],
                "channels": [
                    "level2"
                ]
            }

            await websocket.send(json.dumps(subscribe_message))
            # order_book_snapshot_json = await websocket.recv()
            # order_book_snapshot = json.loads(order_book_snapshot_json)
            # if order_book_snapshot['type'] != 'snapshot':
            #     raise Exception("Expected snapshot, got: " + order_book_snapshot['type'])



            # print(self.order_book.bids)
            # print(next(iter(self.order_book.bids.items())))

            async for message in websocket:
                if self.message_queue.qsize() > 10:
                    print("Queue > 10")
                await self.message_queue.put(message)
                print(".", end="", flush=True)

    async def process_queue(self):
        try:
            while True:
                message = await self.message_queue.get()
                self.handle_message(message)
                self.new_data_callback()
        except asyncio.CancelledError:
            print("Received cancellation error: exiting process_queue")
            return

    def handle_message(self, message):
        # await asyncio.sleep(0)

        response = json.loads(message)
        response_type = response['type']

        if response_type == 'snapshot':
            print("Got snapshot response")
            self.handle_snapshot(response)

        elif response_type == 'subscriptions':
            print("Got subscriptions response")

        elif response_type == 'l2update':
            # print("Got l2udpate response")
            print(".", end="", flush=True)
            self.handle_l2_update(response)
        else:
            error_message = f"Got unexpected response: {type}"
            raise Exception(error_message)

        # bid_quotes = sortedcontainers.SortedDict(operator.neg)
        # bid_quotes[1] = '7834'
        # bid_quotes[3] = '234'
        # bid_quotes[2] = '777'

        # self.order_book = OrderBook(datetime.datetime.utcnow(), bid_quotes, None)

        # best_bid = self.order_book.bids.iloc[0]
        # second_bid = self.order_book.bids.iloc[1]
        #
        # print(f"{best_bid} - {second_bid}")
        # print(self.order_book.bids)
        # best_bid = self.order_book.bids.iloc[0]
        # if best_bid != self.best_bid:
        #     print("")
        #     print(f"New best bid: {best_bid}")
        #     self.best_bid = best_bid



    def handle_snapshot(self, order_book_snapshot):
        bids = order_book_snapshot['bids']
        asks = order_book_snapshot['asks']

        bid_quotes = sortedcontainers.SortedDict(operator.neg)
        ask_quotes = SortedDict()
        for bid in bids:
            bid_price = Decimal(bid[0])
            bid_quantity = Decimal(bid[1])
            bid_quotes[bid_price] = bid_quantity

        for ask in asks:
            ask_price = Decimal(ask[0])
            ask_quantity = Decimal(ask[1])
            ask_quotes[ask_price] = ask_quantity

        self.order_book = OrderBook(datetime.datetime.utcnow(), bid_quotes, ask_quotes)

    def handle_l2_update(self, order_book_update):

        changes = order_book_update['changes']
        for change in changes:
            side = change[0]
            price = Decimal(change[1])
            quantity = Decimal(change[2])

            if side == 'buy':
                if quantity == 0:
                    # Zero quantity means that the price level can be removed
                    self.order_book.bids.pop(price, None)  # Use pop in case the item does not exist
                    continue
                self.order_book.bids[price] = quantity
                continue
            elif side == 'sell':
                if quantity == 0:
                    # Zero quantity means that the price level can be removed
                    self.order_book.asks.pop(price, None)
                    continue
                self.order_book.asks[price] = quantity
                continue
            else:
                raise Exception("Unexpected update side: " + side)







# q = Queue()
# t = threading.Thread(target=gdax.initialize)
# print("here")

loop = asyncio.get_event_loop()

best_bid = 0

def on_data():
    global best_bid
    new_best_bid = gdax.order_book.bids.iloc[0]
    if new_best_bid != best_bid:
        print("")
        print(f"New best bid: {new_best_bid}")
        best_bid = new_best_bid

gdax = GdaxExchange(on_data)


gdax_websocket_listener_task = asyncio.ensure_future(gdax.initialize())
processor_task = asyncio.ensure_future(gdax.process_queue())
tasks = [gdax_websocket_listener_task, processor_task]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Run our tasks - if everything functions well, this will run forever.
    finished, pending = loop.run_until_complete(asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION))

    # Finished task(s) have thrown an exception, log now.
    for task in finished:
        #logger.exception(task.exception())
        try:
            task.result()
        except Exception as ex:
            logger.exception(ex)
            #traceback.print_exc()
        #print(f"Type: {type(task)}")
        #print(f"In {task} received exception: {task.exception()}")

    # If we've got here, then a task has throw an exception.
    # Attempt to gracefully cancel all pending tasks.
    for task in pending:
        task.cancel()

    # Wait for up to 2 seconds for the tasks to gracefully return
    finished_cancelled_tasks, pending_cancelled_tasks = loop.run_until_complete(asyncio.wait(pending, timeout=2))
    try:
        for task in finished_cancelled_tasks:
            task.result()

        # If a task is still pending it hasn't finished cleaning up in the timeout period,
        # just throw an exception ("Task was destroyed but it is pending")
    except asyncio.CancelledError:
        # If a task does not have an outer try..except that catches CancelledError then
        # t.result() will raise a CancelledError
        print("Task did not handle a cancel request explicitly")
finally:
    loop.stop()
    loop.close()
