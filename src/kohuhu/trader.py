from enum import Enum, auto
import time
from datetime import datetime
import asyncio
from decimal import Decimal
import logging

log = logging.getLogger(__name__)


class Publisher:
    """Calls registered callbacks."""

    def __init__(self):
        self._update_callbacks = set()
        self.last_updated_at = datetime.min

    def updated(self, data, timestamp=None):
        self.last_updated_at = timestamp if timestamp else datetime.now()
        for c in self._update_callbacks:
            c(data)

    def add_callback(self, callback):
        self._update_callbacks.add(callback)

    def remove_callback(self, callback):
        self._update_callbacks.remove(callback)


class OrderBook:
    """Represents an order book on an exchange.

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
        self._bids = bids
        self._asks = asks
        self.bids_publisher = Publisher()
        self.asks_publisher = Publisher()
        self.any_publisher = Publisher()

    def set_bids_remaining(self, at_price, remaining):
        self._bids[at_price] = remaining
        self.bids_publisher.updated(self)
        self.any_publisher.updated(self)

    def bids_remaining(self, at_price):
        return self._bids[at_price]


class Order:
    class Side(Enum):
        ASK = auto()
        BID = auto()

    class Type(Enum):
        MARKET = auto()
        LIMIT = auto()

    class Status(Enum):
        OPEN = auto()
        CLOSED = auto()
        CANCELLED = auto()

    def __init__(self):
        self.price = None
        self.symbol = None
        self.side = None
        self.type = None
        self.amount = None
        self.filled = None
        self.remaining = None
        self.status = None


class Balance:

    def __init__(self):
        self._free = {}
        self._on_hold = {}

    def free(self, symbol):
        pass

    def on_hold(self, symbol):
        pass


class ExchangeState:
    """Latest information for an exchange.

    Note: I'm not sure what data is needed and how granular it should be given.
    I just guessed at some simple methods to begin with.
    """
    def __init__(self, exchange_id ,fetcher):
        # Lets assume that the order book, personal orders and balance use the
        # ccxt structure.
        self.exchange = exchange_id
        self._order_book = None
        self._orders = {}
        self._balance = None
        self.fetcher = fetcher

    def populate(self):
        self._order_book = None
        self._orders = {}
        self._balance = None
        if self.always_fetch_order_book:
            self._order_book = self.fetcher.get_order_book(self.exchange)
        if self.always_fetch_balance:
            self._order_book = self.fetcher.get_balance(self.exchange)

    def order_book(self, force_update=False):
        """
        The order book for the exchange.

        Example order book:
        {
            'bids': [
                [ price, amount ],
                [ price, amount ],
                ...
            ],
            'asks': [
                [ price, amount ],
                [ price, amount ],
                ...
            ],
            'timestamp': 1499280391811, // Unix Timestamp in milliseconds (seconds * 1000)
            'datetime': '2017-07-05T18:47:14.692Z', // ISO8601 datetime string with milliseconds
        }
        """
        if force_update:
            self._order_book = self.fetcher.get_order_book()
        return self._order_book

    def set_order_book(self, in_order_book):
        self._order_book = in_order_book

    def order(self, order_id, force_update):
        """
        A specific order on the exchange.

        Assuming we go with the ccxt data structure. The return type is a dict,
        and it looks like:
        {
           'id':        '12345-67890:09876/54321', // string
           'datetime':  '2017-08-17 12:42:48.000', // ISO8601 datetime with ms
           'timestamp':  1502962946216, // Unix timestamp in milliseconds
           'status':    'open',         // 'open', 'closed', 'canceled'
           'symbol':    'ETH/BTC',      // symbol
           'type':      'limit',        // 'market', 'limit'
           'side':      'buy',          // 'buy', 'sell'
           'price':      0.06917684,    // float price in quote currency
           'amount':     1.5,           // ordered amount of base currency
           'filled':     1.0,           // filled amount of base currency
           'remaining':  0.5,           // remaining amount to fill
           'trades':   [ ... ],         // a list of order trades/executions
           'fee':      {                // fee info, if available
               'currency': 'BTC',       // which currency the fee is (usually quote)
               'cost': 0.0009,          // the fee amount in that currency
           },
           'info':     { ... },         // original unparsed order structure
        }
        """
        if force_update:
            self._orders[order_id] = self.fetcher.get_order(self.exchange, order_id)
        return self._orders.get(order_id, None)

    def set_order(self, order_id, order_info):
        self._orders[order_id] = order_info

    def balance(self, force_update):
        """Returns the balance on the exchange.

        Follows the ccxt structure:

        {
            'info':  { ... },    // the original untouched non-parsed reply with details
            //-------------------------------------------------------------------------
            // indexed by availability of funds first, then by currency
            'free':  {           // money, available for trading, by currency
                'BTC': 321.00,   // floats...
                'USD': 123.00,
                ...
            },
            'used':  { ... },    // money on hold, locked, frozen, or pending, by currency
            'total': { ... },    // total (free + used), by currency
            //-------------------------------------------------------------------------
            // indexed by currency first, then by availability of funds
            'BTC':   {           // string, three-letter currency code, uppercase
                'free': 321.00   // float, money available for trading
                'used': 234.00,  // float, money on hold, locked, frozen or pending
                'total': 555.00, // float, total balance (free + used)
            },
            'USD':   {           // ...
                'free': 123.00   // ...
                'used': 456.00,
                'total': 579.00,
            },
            ...
        }
        """
        if force_update:
            self._balance = self.fetcher.get_balance()
        return self._balance

    def set_balance(self, balance):
        self._balance = balance


class State:
    """Latest state of all exchanges.

    Attributes:
        timestamp (datetime.Datetime): the time the slice was created. This is
            used by Algorithms instead of datetime.datetime.now().
    """

    def __init__(self):
        self._exchange_state = {}
        self.timestamp = datetime.now()

    def for_exchange(self, exchange_id):
        return self._exchange_state.get(exchange_id, None)

    def add_exchange(self, exchange_state):
        self._exchange_state[exchange_state.exchange_id] = exchange_state

    def exchanges(self):
        return self._exchange_state.values()


class Algorithm:
    """Subclass Algorithm and pass it to Trader to make trades.

    In order to have flexibility to test any Algorithm subclass, it is
    important that this class doesn't make any direct request for data or
    to carry out any actions. This class should take data from the data slices
    and return actions. This allows testing code to run the algorithm on fake
    data and to check the behaviour of the actions without having them run.
    """
    def __init__(self):
        pass

    def initialize(self, state, timer, action_queue):
        raise NotImplementedError("Subclasses should implement this method.")
        # timer.do_every(timedelta(seconds=1), self.tick)

    def on_tick(self, time_now):
        raise NotImplementedError("Subclasses should implement this method.")


class Action:
    """An action to run on an exchange."""

    def __init__(self, exchange_id):
        self.exchange = exchange_id

    @property
    def name(self):
        """The name of the action."""
        raise NotImplementedError("Subclasses should implement this method.")

    def __repr__(self):
        raise NotImplementedError("Subclasses should implement this method.")


class CreateOrder(Action):
    """Represents the action of creating an order.

    Attributes:
        amount (Decimal): amount of BTC to buy/sell.
        price  (Decimal): price in exchange currency per BTC. Non for market
            orders. Note: we could have separate class for market and limit
            orders so that this field doesn't have to be none for market orders.
        side (CreateOrder.Side): whether the order is a buy or sell order.
        type (CreateOrder.Type): whether the order is a market or limit order.
        order_id (int): the order_id of the created order. This gets filled
            when the order is executed by an executor. An algorithm should hold
            onto any order actions they return if they wish to access the
            order_id that is created.
    """

    class Status(Enum):
        PENDING = auto()
        SUCCESS = auto()
        FAILED = auto()

    def __init__(self, exchange_id, side, type, amount, price=None):
        super().__init__(exchange_id)
        self.amount = amount
        self.price = price
        self.side = side
        self.type = type
        self.order = None
        # Note: this property might move to the Action class.
        self.status = self.Status.PENDING

    @Action.name.getter
    def name(self):
        side_name = self.side.name.lower()
        side_name = side_name[:1].upper() + side_name[1:]
        type_name = self.type.name.lower()
        type_name = type_name[:1].upper() + type_name[1:]
        return "{} {} order".format(type_name, side_name)

    def __repr__(self):
        return "{} for {} BTC at {} $/BTC on {}".format(self.name, self.amount,
            self.price if self.price else "market rate", self.exchange)


class CancelOrder(Action):
    """Represents the action of cancelling an order."""

    def __init__(self, order_id, exchange_id):
        super().__init__(exchange_id)
        self.order_id = order_id

    @Action.name.getter
    def name(self):
        return "Cancel order"

    def __repr__(self):
        return "{} for order ID: {}".format(self.name, self.order_id)


class Executor:
    """Executes actions for one or multiple exchange."""

    def __init__(self, supported_exchanges):
        self._supported_exchanges = supported_exchanges

    def execute(self, action):
        """Executes an action on an exchange.

         The default implementation simply prints the action details.

         Args:
             action (Action): the action to run.
             on_exchange (str): the id of the exchange to run the action on.
         """
        if action.exchange not in self.supported_exchanges:
            raise Exception("This executor doesn't support the exchange {}."
                            .format(action.exchange))
        print("Action requested on exchange {}: {}".format(action.exchange,
                                                           action))


class Fetcher:
    """A Fetcher knows how to fill an ExchangeSlice for one or multiple
    exchanges.

    Here we can abstract away the fact that different exchanges need different
    hacks to get working.
    """
    def __init__(self, supported_exchanges):
        """
        Args:
            supported_exchanges (set): set of exchange id's that this fetcher
                supports.
        """
        self.supported_exchanges = supported_exchanges

    def get_balance(self, exchange):
        self._check_support(exchange)
        raise Exception("Not implemented yet.")

    # I'm not sure this is needed:
    #def get_personal_orders(self, exchange):
    #    self._check_support(exchange)
    #    raise Exception("Not implemented yet.")

    def get_order(self, exchange, id):
        self._check_support(exchange)
        raise Exception("Not implemented yet.")

    def get_order_book(self, exchange):
        """Retrieves an order book.

        Args:
            exchange (ccxt.Exchange): the exchange to fetch the data for.
        """
        self._check_support(exchange)
        raise NotImplementedError("TODO")

    def _check_support(self, exchange_id):
        if exchange_id not in self.supported_exchanges:
            raise Exception("This fetcher doesn't support the exchange {}."
                            .format(exchange_id))


class Timer:
    """A callback scheduler.

    Inspired from SO: https://stackoverflow.com/a/28034554/754300
    """
    def __init__(self):
        self.tasks = []

    def do_every(self, period, f):
        timer_task = asyncio.ensure_future(self._do_every(period, f))
        self.tasks.append(timer_task)

    async def _do_every(self, period, f):
        def tick():
            #t = time.time()
            init_time = datetime.now()
            count = 0
            while True:
                count += 1
                target_time = init_time + count*period
                diff = target_time - datetime.now()
                yield max(diff, 0)
        ticker = tick()
        for delta in tick():
            await asyncio.sleep(delta.seconds)
            # await f() ?
            f(datetime.now())


class Trader:
    """Runs algorithms.

    Test code might look like:
    algo = MyAlgo()
    gdax = GDaxExchange()
    gemini = GeminiExchange()
    exchanges = [gdax, gemini]
    trader = Trader(algo, exchanges)
    trader.initialize()
    # Edit the state here...
    algorithm.on_tick()
    actions = trader.timer.action_queue.deque()
    # check if actions were correct.

    Attributes:
        actions: a list of action tuples. Each list entry contains the actions
            that were created by the algorithm at a certain step.
    """
    def __init__(self, algorithm, exchanges):
        self._algorithm = algorithm
        self.timer = Timer()
        self._fetchers = {}
        self._state = State()
        self.exchanges = exchanges
        self.action_queue = []
        for e in exchanges:
            self._state.add_exchange(e.state)

    def initialize(self):
        """Calls initialize on the algorithm."""
        self._algorithm.initialize(self._state, self.action_queue)

    def start(self):
        """Starts the trader.

        When testing, this method doesn't need to be called, and the data can be
        set directly.
        """
        tasks = []
        for e in self.exchanges:
            tasks.append(asyncio.ensure_future(e.initialize()))
            tasks.append(asyncio.ensure_future(e.process_queue()))
        tasks.extend(self.timer.tasks)
        loop = asyncio.get_event_loop()
        try:
            # Run the tasks. If everything works well, this will run forever.
            finished, pending = loop.run_until_complete(
            asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION))

            # If we've got here, then a task has throw an exception.

            # Finished task(s) have thrown an exception. Let's observe the task
            # to log the exception.
            for task in finished:
                try:
                    task.result()
                except Exception as ex:
                    log.exception(ex)

            # Pending tasks are still running. Gracefully cancel them all.
            for task in pending:
                task.cancel()

            # Wait for up to 2 seconds for the tasks to gracefully return.
            finished_cancelled_tasks, pending_cancelled_tasks = \
                loop.run_until_complete(asyncio.wait(pending, timeout=2))
            try:
                # They most likely finished because we told them to cancel, when
                # we observe them we'll catch the asyncio.CancelledError.
                for task in finished_cancelled_tasks:
                    task.result()

                # If a task is still pending it hasn't finished cleaning up in
                # the timeout period and you'll see:
                #   "Task was destroyed but it is pending."
                # as we forcefully kill it.
            except asyncio.CancelledError:
                pass
                # If a task does not have an outer try..except that catches
                # CancelledError then t.result() will raise a CancelledError.
                # This is fine.
        finally:
            loop.stop()
            loop.close()

    def add_action(self, action):
        self.action_queue.append(action)

# Draft area:

def on_order_success(self, order_action):
    pass

def on_order_accepted(self, order_action):
    pass

def on_order_rejected(self, order_action):
    pass

def on_order_fill(self, order_action):
    pass

def on_onder_closed(self, order_action):
    pass

def on_order_cancelled(self, order_action):
    pass