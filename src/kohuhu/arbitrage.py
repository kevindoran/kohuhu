import kohuhu.trader as trader
from kohuhu.trader import OrderAction
import kohuhu.exchanges as exchanges
from decimal import Decimal


class OneWayPairArbitrage(trader.Algorithm):
    """Carries out buy->sell arbitrage in one direction between two exchanges.
    """

    def __init__(self):
        super().__init__()
        self.exchange_buy_on = None
        self.exchange_sell_on = None
        self.live_limit_order = None
        self.market_order = None
        self.max_bid_amount_in_btc = Decimal(0.5)
        self.bid_amount_in_btc = Decimal(0.5)
        self.profit_target = 0.05 # percent.

    def initialize(self, exchanges_to_use):
        if len(exchanges_to_use) != 2:
            raise Exception("OneWayPairArbitrage uses 2 exchanges, but {} were"
                            " given.".format(len(exchanges_to_use)))
        self.exchange_buy_on = exchanges_to_use[0]
        self.exchange_sell_on = exchanges_to_use[1]

    def on_data(self, slice):
        if not self.live_limit_order:
            # Calculate the BTC market price on the exchange to sell on.
            order_book = slice.for_exchange(self.exchange_sell_on).order_book
            sell_price = self.calculate_effective_sell_price(
                self.bid_amount_in_btc, order_book)
            # Calculate the bid price to make a certain profit.
            bid_price = self.calculate_bid_limit_price(self.exchange_buy_on,
                                                       self.exchange_sell_on,
                                                       sell_price,
                                                       self.profit_target)
            # Create and return the action.
            bid_action = OrderAction(self.exchange_buy_on,
                                     OrderAction.Side.BID,
                                     OrderAction.Type.Limit,
                                     amount=self.bid_amount_in_btc,
                                     price=bid_price)
            self.live_limit_order = bid_action
            return bid_action
        else:
            # TODO check to see if the order has been met.
            if self.live_limit_order.order_id is not None:
                # The order has been placed.
                order = slice.order(self.live_limit_order.order_id)
                if order['amount'] == 0:
                    raise Exception("Ops, we made an order for 0 BTC. Something"
                                    " isn't right.")
                if order['remaining'] == 0:
                    # Our order has completed! So lets execute a market sell.
                    # The market may have moved since we placed our limit order.
                    # TODO: should be buy anyway?
                    marketBidAction = OrderAction(self.exchange_sell_on,
                                                  OrderAction.Side.BID,
                                                  OrderAction.Type.Market,
                                                  amount=self.bid_amount_in_btc)
                    # I'm not sure if we need this again. Maybe good to check
                    # that the order succeeded.
                    self.market_order = marketBidAction
                    self.live_limit_order = None
                    return marketBidAction
            else:
                # The order hasn't been placed yet. Nothing to do.
                return None


    @staticmethod
    def calculate_effective_sell_price(sell_amount, order_book):
        """Calculates the effective price on a market for the given amount."""
        capacity_counted = Decimal(0)
        bid_index = Decimal(0)
        effective_market_price = Decimal(0)
        filled = Decimal(0)
        remaining = sell_amount
        while capacity_counted < sell_amount:
            next_highest_bid = order_book['bids'][bid_index]
            price = Decimal(next_highest_bid[0])
            bid_amount = Decimal(next_highest_bid[1])
            amount_used = max(bid_amount, remaining)
            fraction_of_trade = amount_used / sell_amount
            effective_market_price += fraction_of_trade * price
            capacity_counted += bid_amount
        return effective_market_price

    @staticmethod
    def calculate_bid_limit_price(cls, exchange_to_buy_on, exchange_to_sell_on,
                                  market_price_to_sell, profit_target):
        """Calculates the bid price needed to make the given profit."""
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
