import kohuhu.trader as trader
from kohuhu.trader import OrderAction
import kohuhu.exchanges as exchanges
from decimal import Decimal

class OneWayPairArbitrage(trader.Algorithm):


    def __init__(self):
        super().__init__()
        self.exchange_buy_on = None
        self.exchange_sell_on = None
        self.live_limit_orders = True
        self.max_bid_amount_in_btc = Decimal(0.5)
        self.bid_amount_in_btc = Decimal(0.5)
        self.profit_target = 0.05 # percent.

    def initialize(self, exchanges_to_use):
        if len(exchanges_to_use) != 2:
            raise Exception("OneWayPairArbitrage uses 2 exchanges, {} given."
                            .format(len(exchanges_to_use)))
        self.exchange_buy_on = exchanges_to_use[0]
        self.exchange_sell_on = exchanges_to_use[0]

    # Note: should the slice contain balance info? Or maybe we could include
    # the fetchers as part of the input. We need some sort of on demand data,
    # I think.
    def on_data(self, slice):
        if not self.live_limit_orders:
            # Caculate the limit order price.
            order_book = slice.for_exchange(self.exchange_sell_on).order_book
            available_bid_amount = Decimal(0)
            bid_index = Decimal(0)
            effective_market_price = Decimal(0)
            filled = Decimal(0)
            remaining = self.bid_amount_in_btc
            while available_bid_amount < self.bid_amount_in_btc:
                next_highest_bid = order_book['bids'][bid_index]
                price = Decimal(next_highest_bid[0])
                amount = max(Decimal(next_highest_bid[1]), remaining)
                fraction_of_trade = amount / self.bid_amount_in_btc
                effective_market_price += fraction_of_trade * price

            bid_price = self.calculate_bid_limit_price(self.exchange_buy_on,
                                                       self.exchange_sell_on,
                                                       effective_market_price,
                                                       self.profit_target)
            bid_action = OrderAction(OrderAction.Side.BID,
                                     OrderAction.Type.Limit,
                                     amount=self.bid_amount_in_btc,
                                     price=bid_price)
            self.live_limit_orders = True
            return bid_action
        else:
            # TODO check to see if the order has been met.


    @staticmethod
    def calculate_bid_limit_price(cls, exchange_to_buy_on, exchange_to_sell_on,
                                  market_price_to_sell, profit_target):
        buy_maker_fee = exchanges.fees(exchange_to_buy_on)[0]
        sell_taker_fee = exchanges.fees(exchange_to_sell_on)[1]

        # Calculate bid limit price.
        # TODO: work through an example on each line.
        # Create bid limit on e1, sell at market on e2
        # Note: fee_factor is always less than 1. E.g., fee listed as 0.8
        # becomes a fee factor: 1/(1+0.08) = 0.926
        # fee_factor = (1/(1+e2.taker_fee) * 1/(1+e1.maker_fee)))
        # profit_factor = (e2.highest_bid/bid_limit) * fee_factor
        # profit_factor / fee_factor = e2.highest_bid/bid_limit
        # fee_factor / profit_factor = bid_limit / e2.highest_bid
        # bid_limit = fee_factor * e2.highest_bid / profit_factor
        fee_factor = exchanges.fee_as_factor(buy_maker_fee) * \
                     exchanges.fee_as_factor(sell_taker_fee)
        bid_limit = fee_factor * market_price_to_sell / profit_target
        return bid_limit
