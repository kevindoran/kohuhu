from enum import Enum, auto

class ExchangeSlice:
    """Data from a single at a point in time.

    Note: I'm not sure what data is needed and how granular it should be given.
    I just guessed at some simple methods to begin with.
    """
    def __init__(self, fetcher):
        # Lets assume that the order book, personal orders and balance use the
        # ccxt structure.
        self._order_book = None
        self._personal_orders = None
        self._balance = None
        self.fetcher = fetcher
        self.always_fetch_order_book = True
        self.always_fetch_personal_orders = True
        self.always_fetch_balance = False

    def populate(self):
        self._order_book = None
        self._personal_orders = None
        self._balance = None
        if self.always_fetch_order_book:
            self._order_book = self.fetcher.get_order_book()
        if self.always_fetch_personal_orders:
            self._order_book = self.fetcher.get_personal_orders()
        if self.always_fetch_balance:
            self._order_book = self.fetcher.get_balance()

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
                raise Exception("The order book should have been pre-fetched.")
            self._order_book = self.fetcher.get_order_book()
        return self._order_book

    @property
    def personal_orders(self):
        if not self.personal_orders:
            if self.always_fetch_personal_orders:
                raise Exception("Personal orders should have been pre-fetched.")
            self._personal_orders = self.fetcher.get_personal_orders()
        return self._personal_orders

    @property
    def balance(self):
        if not self._balance:
            if self.always_fetch_balance:
                raise Exception("Balance should have been pre-fetched.")
            self._balance = self.fetcher.get_balance()
        return self._balance

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
    """Represents the action of creating an order."""

    class Side(Enum):
        ASK = auto()
        BID = auto()

    class Type(Enum):
        Market = auto()
        Limit = auto()

    def __init__(self, side, type, amount, price):
        super().__init__()
        self.amount = amount
        self.price = price
        self.side = side
        self.type = type

    @Action.name.getter
    def name(self):
        side_name = self.side.name.lower
        side_name = side_name[:1].upper() + side_name[1:]
        type_name = self.type.name.lower
        type_name = type_name[:1].upper() + type_name[1:]
        return "{} {} order".format(type_name, side_name)

    def __repr__(self):
        return "{} for {} BTC at {} $/BTC".format(self.name, self.amount,
                                                  self.type)

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

    def get_personal_orders(self, exchange):
        self._check_support(exchange)
        raise Exception("Not implemented yet.")

    def get_order_book(self, exchange):
        """Retrieves an order book.

        Args:
            exchange (ccxt.Exchange): the exchange to fetch the data for.
        """
        self._check_support(exchange)

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
        self._slices = []
        self.actions = []
        self._fetchers = {}
        self.exchanges = exchanges_to_use

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
        next_slice = Slice()
        for id in self.exchanges:
            fetcher = self._fetchers[id]
            exchange_slice = ExchangeSlice(fetcher)
            next_slice.set_slice(id, exchange_slice)
        self._slices.append(next_slice)

    def add_slice(self, slice):
        self._slices.append(slice)

    def step(self):
        self._algorithm.on_data(self._slices[-1])

    def step_algorithm(self):
        actions = self._algorithm.on_data()
        self.actions.append(actions)
