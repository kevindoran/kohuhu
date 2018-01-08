from decimal import Decimal
import logging
import operator
from enum import Enum
from enum import auto
from sortedcontainers import SortedDict
from datetime import datetime


class Side(Enum):
    """The side of the orderbook a quote or order is on."""
    BID = auto()
    ASK = auto()


class SortedQuotes:
    def __init__(self, side):
        """A data structure of bid or ask quotes that provides fast lookup of the best nth
        bid / best nth ask using the index operator and fast insertion with set_quote().

        Usage:
            bids = SortedQuotes(Side.BID)
            quote_a = Quote(price=Decimal(500), quantity=Decimal(2))
            bids.set_quote(quote_a)
            quote_b = Quote(price=Decimal(600), quantity=Decimal(1))
            bids.set_quote(quote_b)

            >> bids[0]
                [price=600, quantity=1]
        """
        # We use a dictionary because when we get an update message it contains only the price levels
        # where the quantity has changed. To afford an efficient update we want to be able to lookup
        # a specific price in O(1) time without having to iterate over the entire orderbook.
        # We need it to be sorted so that we can use bids[0] to get the highest bid etc.
        # bid_quotes are ordered in reverse because the first element should have the highest price.
        if side == Side.BID:
            self._price_quantity_dict = SortedDict(operator.neg)
        elif side == Side.ASK:
            self._price_quantity_dict = SortedDict()
        else:
            raise ValueError("Quote side must be supplied")

    def __getitem__(self, index):
        price = self._price_quantity_dict.iloc[index]
        quantity = self._price_quantity_dict[price]
        return Quote(price, quantity)

    def set_quote(self, quote):
        price = quote.price
        quantity = quote.quantity
        if quantity == 0:
            del self._price_quantity_dict[price]
        else:
            self._price_quantity_dict[price] = quantity

    def __len__(self):
        return self._price_quantity_dict.__len__()

    def __str__(self):
        return self._price_quantity_dict.__str__()


class OrderBook:
    """Represents an order book on an exchange.

    Attributes:
        bids Sorted quotes of bids on this exchange.
        asks Sorted quotes of bids on this exchange.
    """
    def __init__(self):
        self._bids = SortedQuotes(Side.BID)
        self._asks = SortedQuotes(Side.ASK)

    def bids(self):
        return self._bids

    def asks(self):
        return self._asks


class Quote:
    """A single bid or ask quote on the exchange"""
    def __init__(self, price, quantity):
        self.price = price
        self.quantity = quantity

    def __eq__(self, other):
        return isinstance(other, Quote) and self.__dict__ == other.__dict__

    def __str__(self):
        return f"[price={self.price}, quantity={self.quantity}]"

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

    def _check_symbol(self, symbol):
        if symbol.upper() != symbol:
            logging.warning("The currency symbols should be upper-case. "
                            f"Invalid symbol: '{symbol}' given. Switching it "
                            "to upper-case.")
        return symbol.upper()


    def free(self, symbol):
        symbol = self._check_symbol(symbol)
        return self._free.get(symbol, Decimal(0))

    def on_hold(self, symbol):
        symbol = self._check_symbol(symbol)
        return self._on_hold.get(symbol, Decimal(0))

    def set_free(self, symbol, amount):
        symbol = self._check_symbol(symbol)
        self._free[symbol] = amount

    def set_on_hold(self, symbol, amount):
        symbol = self._check_symbol(symbol)
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
        self.update_publisher = Publisher()

    def order_book(self, force_update=False):
        """The order book for the exchange."""
        if force_update:
            self.exchange_client.update_order_book()
        return self._order_book

    def order(self, order_id, force_update=False):
        """A specific order on the exchange. """
        if force_update:
            self.exchange_client.update_order(self.exchange_id, order_id)
        return self._orders.get(order_id, None)

    def set_order(self, order_id, order_info):
        self._orders[order_id] = order_info

    def balance(self, force_update=False):
        """Returns the balance on the exchange."""
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

    class Status(Enum):
        PENDING = auto()
        SUCCESS = auto()
        FAILED = auto()

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

    def __init__(self, exchange_id, order_id):
        super().__init__(exchange_id)
        self.order_id = order_id
        # TODO: we may wish to move the status attribute into the Action class.
        self.status = self.Status.PENDING

    @Action.name.getter
    def name(self):
        return "Cancel order"

    def __repr__(self):
        return "{} for order ID: {}".format(self.name, self.order_id)


class ExchangeClient:
    """Keeps the ExchangeState of an exchange up to date. Also executes actions.
    """
    def __init__(self, exchange_id):
        self.exchange_id = exchange_id
        self.exchange_state = None

    def run_task(self):
        """Returns a future for starting and running the exchange client."""
        return NotImplementedError("Subclasses must implement this function.")

    def update_order_book(self):
        """Retrieve the latest order book information."""
        return NotImplementedError("Subclasses must implement this function.")

    def update_balance(self):
        """Retrieves the latest balance information."""
        return NotImplementedError("Subclasses must implement this function.")

    def update_orders(self):
        """Retrieves the latest information on all our orders."""
        return NotImplementedError("Subclasses must implement this function.")

    def execute_action(self, action):
        """Executes the given action on this exchange."""
        return NotImplementedError("Subclasses must implement this function.")


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
