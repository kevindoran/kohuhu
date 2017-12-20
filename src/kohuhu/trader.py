
class ExchangeSlice:
    """Data from a single at a point in time.

    Note: I'm not sure what data is needed and how granular it should be given.
    I just guessed at some simple methods to begin with.
    """
    def __init__(self):
        pass

    def order_book(self):
        raise NotImplementedError("TODO")

    def personal_orders(self):
        raise NotImplementedError("TODO")


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

    def name(self):
        raise NotImplementedError("Subclasses should implement this method.")

    def __repr__(self):
        raise NotImplementedError("Subclasses should implement this method.")


class LimitOrder(Action):

    def __init__(self):
        super().__init__()

    def name(self):
        pass

    def __repr__(self):
        pass


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

    def fetch(self, exchange):
        """Retrieves and parsers the data for the given exchange into an
        ExchangeSlice.

        Args:
            exchange (ccxt.Exchange): the exchange to fetch the data for.

        Returns:
            (ExchangeSlice): the ExchangeSlice for the given exchange.
        """
        if exchange not in self.supported_exchanges:
            raise Exception("This fetcher doesn't support the exchange {}."
                            .format(exchange))
        raise NotImplementedError("Subclasses should implement this method.")


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
        """"""
        next_slice = Slice()
        for id in self.exchanges:
            if id in self._fetchers:
                exchange_slice = self._fetchers[id]
                next_slice.exchange_slices[id] = exchange_slice
        self._slices.append(next_slice)

    def add_slice(self, slice):
        self._slices.append(slice)

    def step(self):
        self._algorithm.on_data(self._slices[-1])

    def step_algorithm(self):
        actions = self._algorithm.on_data()
        self.actions.append(actions)
