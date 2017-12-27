from decimal import Decimal
import logging
import operator
from enum import Enum
from enum import auto
from sortedcontainers import SortedDict
from datetime import datetime


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
    def __init__(self):#, timestamp), bids, asks):
        #self.timestamp = timestamp
        self._bids = SortedDict(operator.neg)
        self._asks = SortedDict()
        self.bids_publisher = Publisher(self)
        self.asks_publisher = Publisher(self)
        self.any_publisher = Publisher(self)

    # Note: do we want to use the ordered dict directly, or through methods?
    def set_bids_remaining(self, at_price, remaining):
        if remaining == 0:
            del self._bids[at_price]
        else:
            self._bids[at_price] = remaining
        # Don't update subscribers here automatically as:
        #   a) we may want to bunch multiple changes per update (eg REST)
        #   b) it is easy to disable the callbacks when testing.
        # Instead, subscribers are notified by an ExchangeClient.

    def set_asks_remaining(self, at_price, remaining):
        if remaining == 0:
            del self._asks[at_price]
        else:
            self._asks[at_price] = remaining

    def bids_remaining(self, at_price):
        return self._bids[at_price]

    def asks_remaining(self, at_price):
        return self._asks[at_price]

    def bid_prices(self):
        return self._bids.keys()

    def bid_by_index(self, index):
        price = self._bids.iloc[index]
        amount = self._bids[price]
        return BidPricePair(price, amount)

    def asks(self):
        return self._asks.keys()


class BidPricePair:
    """A price, amount tuple. So we never mix up the order."""
    def __init__(self, price, amount):
        self.price = price
        self.amount = amount


class Order:
    """Represents an order placed on an exchange.

    Note: we may wish to separate this class into Order, LimitOrder
    and MarketOrder.
    """
    class Type(Enum):
        LIMIT = auto()
        MARKET = auto()

    class Side(Enum):
        ASK = auto()
        BID = auto()

    class Status(Enum):
        OPEN = auto()
        CLOSED = auto()
        CANCELLED = auto()

    def __init__(self):
        self.order_id = None
        self.average_price = None
        self.symbol = None
        self.side = None
        self.type = None
        self.amount = None
        self.filled = None
        self.price = None
        self.remaining = None
        self.status = None


class Balance:

    def __init__(self):
        self._free = {}
        self._on_hold = {}

    def free(self, symbol):
        return self._free.get(symbol, Decimal(0))

    def on_hold(self, symbol):
        return self._on_hold.get(symbol, Decimal(0))

    def set_free(self, symbol, amount):
        self._free[symbol] = amount

    def set_on_hold(self, symbol, amount):
        self._on_hold[symbol] = amount


class ExchangeState:
    """Latest information for an exchange.

    Note: I'm not sure what data is needed and how granular it should be given.
    I just guessed at some simple methods to begin with.
    """
    def __init__(self, exchange_id, exchange_client):
        # Lets assume that the order book, personal orders and balance use the
        # ccxt structure.
        self.exchange_id = exchange_id
        self._order_book = OrderBook()
        self._orders = {}
        self._balance = Balance()
        self.exchange_client = exchange_client

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
            self.exchange_client.update_order_book()
        return self._order_book

    def order(self, order_id, force_update=False):
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
            self.exchange_client.update_order(self.exchange_id, order_id)
        return self._orders.get(order_id, None)

    def set_order(self, order_id, order_info):
        self._orders[order_id] = order_info

    def balance(self, force_update=False):
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
            self.exchange_client.update_balance()
        return self._balance


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


class ExchangeClient:

    def exchange_state(self):
        pass

    def update_order_book(self):
        pass

    def update_balance(self):
        pass

    def update_orders(self):
        pass

    def execute_action(self, action):
        pass


class Publisher:
    """Calls registered callbacks."""

    def __init__(self, data=None):
        self._data = data
        self._update_callbacks = set()

    def notify(self):
        for c in self._update_callbacks:
            if self._data:
                c(id)
            else:
                c()

    def add_callback(self, callback):
        self._update_callbacks.add(callback)

    def remove_callback(self, callback):
        self._update_callbacks.remove(callback)
