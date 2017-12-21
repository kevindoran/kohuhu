from enum import Enum, auto


class ExchangeSlice:
    """Data from a single at a point in time.

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
        self.always_fetch_order_book = True
        self.always_fetch_balance = False

    def populate(self):
        self._order_book = None
        self._orders = {}
        self._balance = None
        if self.always_fetch_order_book:
            self._order_book = self.fetcher.get_order_book(self.exchange)
        if self.always_fetch_balance:
            self._order_book = self.fetcher.get_balance(self.exchange)

    @property
    def order_book(self):
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
        if not self._order_book:
            if self.always_fetch_order_book:
                raise Exception("The order book for {} should have been "
                                "pre-fetched.".format(self.exchange))
            self._order_book = self.fetcher.get_order_book()
        return self._order_book

    @order_book.setter
    def set_order_book(self, in_order_book):
        self._order_book = in_order_book

    def order(self, order_id):
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
        if not order_id in self._orders:
            self._orders[order_id] = self.fetcher.get_order(self.exchange, order_id)
        return self._orders[order_id]

    @property
    def balance(self):
        if not self._balance:
            if self.always_fetch_balance:
                raise Exception("Balance should have been pre-fetched.")
            self._balance = self.fetcher.get_balance()
        return self._balance

    @balance.setter
    def set_balance(self, balance):
        self._balance = balance

class Algorithm:
    """Subclass Algorithm and pass it to Trader to make trades.

    In order to have flexibility to test any Algorithm subclass, it is
    important that this class doesn't make any direct request for data or
    to carry out any actions. This class should take data from the data slices
    and return actions. This allows testing code to run the algorithm on fake
    data and to check the behaviour of the actions without having them run.
    """
    def __init__(self):
        self.exchanges = []
        pass

    def initialize(self, exchanges_to_use):
        self.exchanges = exchanges_to_use

    def on_data(self, slice):
        raise NotImplementedError("Subclasses should implement this method.")


class Action:
    """An action to run on an exchange."""

    def __init__(self):
        pass

    @property
    def name(self):
        """The name of the action."""
        raise NotImplementedError("Subclasses should implement this method.")

    def __repr__(self):
        raise NotImplementedError("Subclasses should implement this method.")


class OrderAction(Action):
    """Represents the action of creating an order.

    Attributes:
        amount (Decimal): amount of BTC to buy/sell.
        price  (Decimal): price in exchange currency per BTC. Non for market
            orders. Note: we could have separate class for market and limit
            orders so that this field doesn't have to be none for market orders.
        side (OrderAction.Side): whether the order is a buy or sell order.
        type (OrderAction.Type): whether the order is a market or limit order.
        order_id (int): the order_id of the created order. This gets filled
            when the order is executed by an executor. An algorithm should hold
            onto any order actions they return if they wish to access the
            order_id that is created.
    """

    class Side(Enum):
        ASK = auto()
        BID = auto()

    class Type(Enum):
        Market = auto()
        Limit = auto()

    def __init__(self, exchange_id, side, type, amount, price=None):
        super().__init__()
        self.exchange = exchange_id
        self.amount = amount
        self.price = price
        self.side = side
        self.type = type
        self.order_id = None

    @Action.name.getter
    def name(self):
        side_name = self.side.name.lower
        side_name = side_name[:1].upper() + side_name[1:]
        type_name = self.type.name.lower
        type_name = type_name[:1].upper() + type_name[1:]
        return "{} {} order".format(type_name, side_name)

    def __repr__(self):
        return "{} for {} BTC at {} $/BTC on {}".format(self.name, self.amount,
            self.price if self.price else "market rate", self.exchange)

class CancelOrder(Action):
    """Represents the action of cancelling an order."""

    def __init__(self, order_id):
        super().__init__()
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

    def execute(self, action, on_exchange):
        """Executes an action on an exchange.

         The default implementation simply prints the action details.

         Args:
             action (Action): the action to run.
             on_exchange (str): the id of the exchange to run the action on.
         """
        if on_exchange not in self.supported_exchanges:
            raise Exception("This executor doesn't support the exchange {}."
                            .format(on_exchange))
        print("Action requested on exchange {}: {}".format(on_exchange, action))


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


class Slice:
    """Combines exchange slices."""
    def __init__(self):
        self._exchange_slices = {}

    def for_exchange(self, exchange_id):
        return self._exchange_slices[exchange_id]

    def set_slice(self, exchange_id, exchange_slice):
        self._exchange_slices[exchange_id] = exchange_slice


class Trader:
    """Runs algorithms.

    Test code might look like:
    algo = MyAlgo()
    trader = Trader(algo)
    trader.initialize()
    # create some fake data.
    trader.add_slice(some_fake_data)
    trader.step()
    # check if actions were correct.


    Attributes:
        actions: a list of action tuples. Each list entry contains the actions
            that were created by the algorithm at a certain step.
    """
    def __init__(self, algorithm, exchanges_to_use):
        self._algorithm = algorithm
        self._fetchers = {}
        self.exchanges = exchanges_to_use
        self.last_actions = []
        self.next_slice = None

    def set_fetcher(self, fetcher, supported_exchanges):
        for id in supported_exchanges:
            self._fetchers[id] = fetcher

    def initialize(self):
        """Calls initialize on the algorithm."""
        self._algorithm.initialize(self.exchanges)

    def fetch_next_slice(self):
        """Fetches data from the exchanges.

        When testing, this method doesn't need to be called, and the data
        can be set directly.
        """
        self.next_slice = Slice()
        for id in self.exchanges:
            fetcher = self._fetchers[id]
            exchange_slice = ExchangeSlice(id, fetcher)
            self.next_slice.set_slice(id, exchange_slice)

    def step(self):
        self.last_actions = self._algorithm.on_data(self.next_slice)
        return self.last_actions
