import kohuhu.trader as trader

class OneWayPairArbitrage(trader.Algorithm):


    def __init__(self):
        super().__init__()
        self.exchange_buy_on = None
        self.exchange_sell_on = None
        self.live_limit_orders = []

    def initialize(self, exchanges_to_use):
        if len(exchanges_to_use) != 2:
            raise Exception("OneWayPairArbitrage uses 2 exchanges, {} given."
                            .format(len(exchanges_to_use)))
        self.exchange_buy_on = exchanges_to_use[0]
        self.exchange_sell_on = exchanges_to_use[0]

    def on_data(self, slice):
        if not len(self.live_limit_orders):

        my_orders = slice.for_exchange(self.exchange_buy_on)
        for o in my_orders:



