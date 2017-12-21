import kohuhu.trader as trader
from kohuhu.trader import CreateOrder
import kohuhu.exchanges as exchanges
from decimal import Decimal
import logging


class OneWayPairArbitrage(trader.Algorithm):
    """Carries out buy->sell arbitrage in one direction between two exchanges.
    """

    def __init__(self):
        super().__init__()
        self.exchange_buy_on = None
        self.exchange_sell_on = None
        self.live_limit_order = None
        self.market_orders = []
        self.max_bid_amount_in_btc = Decimal(0.5)
        self.bid_amount_in_btc = Decimal(0.5)
        self.previous_fill_amount = Decimal(0)
        self.profit_target = Decimal(0.05)  # percent.

    def initialize(self, exchanges_to_use):
        if len(exchanges_to_use) != 2:
            raise Exception("OneWayPairArbitrage uses 2 exchanges, but {} were"
                            " given.".format(len(exchanges_to_use)))
        self.exchange_buy_on = exchanges_to_use[0]
        self.exchange_sell_on = exchanges_to_use[1]

    def on_data(self, slice):
        if not self.live_limit_order:
            # No buy limit action has been created yet. Make one.
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
            bid_action = CreateOrder(self.exchange_buy_on,
                                     CreateOrder.Side.BID,
                                     CreateOrder.Type.LIMIT,
                                     amount=self.bid_amount_in_btc,
                                     price=bid_price)
            self.live_limit_order = bid_action
            return [bid_action, ]
        else:
            # We have a buy limit action.
            if self.live_limit_order.order_id is not None:
                # The action has been executed and the order has been placed.
                # Every time the order gets more filled, make a market sell
                # order on the other exchange by the fill amount.
                order = slice.for_exchange(self.exchange_buy_on) \
                    .order(self.live_limit_order.order_id)
                if order['amount'] == 0:
                    raise Exception("Ops, we made an order for 0 BTC on {}. "
                                    "Something isn't right."
                                    .format(self.exchange_buy_on))
                fill_amount = Decimal(order['filled'])
                if fill_amount > self.bid_amount_in_btc:
                    raise Exception(
                        "Something isn't right. Our order {} got filled ({}) "
                        "more than the amount we placed ({}) on {}."
                            .format(self.live_limit_order.order_id,
                                    order['filled'],
                                    self.bid_amount_in_btc,
                                    self.exchange_buy_on))

                if fill_amount > self.previous_fill_amount:
                    # Our buy order has been filled more, lets create a sell
                    # order on the other exchange.
                    fill_diff = fill_amount - self.previous_fill_amount
                    logging.info(
                        "The limit buy order ({}) has been filled more (prev "
                        "fill: {}, current: {}). About to place a market sell "
                        "order for {} on {}.".format(
                            self.live_limit_order.order_id,
                            self.previous_fill_amount, fill_amount, fill_diff,
                            self.exchange_sell_on))
                    marketBidAction = CreateOrder(self.exchange_sell_on,
                                                  CreateOrder.Side.ASK,
                                                  CreateOrder.Type.MARKET,
                                                  amount=fill_diff)

                    # Store the order action, although I'm not sure if we
                    # will need them again. Maybe for logging.
                    self.market_orders.append(marketBidAction)

                    if fill_amount == self.bid_amount_in_btc:
                        logging.info("Our buy limit order ({}) on {} has been "
                                     "fully filled."
                                     .format(self.live_limit_order.order_id,
                                     self.exchange_buy_on))
                        self.live_limit_order = None
                    return [marketBidAction]
                else:
                    logging.info("Order action has been placed, but it's not "
                                 "completed yet.")
                    return []

            else:
                # The order hasn't been placed yet. Nothing to do.
                logging.info("Waiting for order action to be placed.")
                return []

    @staticmethod
    def calculate_effective_sell_price(sell_amount, order_book):
        """Calculates the effective price on a market for the given amount."""
        capacity_counted = Decimal(0)
        bid_index = 0
        effective_market_price = Decimal(0)
        remaining = sell_amount
        while capacity_counted < sell_amount:
            if bid_index >= len(order_book['bids']):
                raise Exception("Something is not right: there is only {} BTC "
                                "buy orders on an exchange. We wont be able to "
                                "fill our market order. Something is probably "
                                "broken.".format(capacity_counted))
            next_highest_bid = order_book['bids'][bid_index]
            # TODO: can we get a clean way to separate the amount and price
            # information better?
            price = Decimal(next_highest_bid[0])
            bid_amount = Decimal(next_highest_bid[1])
            amount_used = min(bid_amount, remaining)
            fraction_of_trade = amount_used / sell_amount
            effective_market_price += fraction_of_trade * price
            capacity_counted += bid_amount
            bid_index += 1
        return effective_market_price

    @staticmethod
    def calculate_bid_limit_price(exchange_to_buy_on, exchange_to_sell_on,
                                  market_price_to_sell, profit_target):
        """Calculates the bid price needed to make the given profit."""
        buy_maker_fee = exchanges.fees(exchange_to_buy_on).maker
        sell_taker_fee = exchanges.fees(exchange_to_sell_on).taker

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
