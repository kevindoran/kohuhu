import kohuhu.trader as trader
from kohuhu.trader import CreateOrder
from kohuhu.trader import CancelOrder
import kohuhu.exchanges as exchanges
from decimal import Decimal
import logging
import datetime


class OneWayPairArbitrage(trader.Algorithm):
    """Carries out buy->sell arbitrage in one direction between two exchanges.

    Attributes:
        poll_period (timedelta): the minimum time between two runs of the
            algorithm routine.
        limit_order_update_period (timedelta): the minimum time between checking
            and possibly updating the bid order limit (due to having to change
            the price).
        exchange_to_buy_on (str): the exchange to place bid limit orders on.
        exchange_to_sell_on (str): the exchange to place market ask orders
            on.
        bid_amount_in_btc (Decimal): the amount to order when placing the
            bid limit order.
        order_update_threshold (Decimal): when the difference between the
            original bid limit order price and the current best limit order
            price as a fraction of the current best limit order price exceeds
            this threshold, the original bid limit order will be cancelled. A
            new order will be made on the next invocation.

                measure = (original_order_price - best_price) / best_price

                if measure > threshold:
                    cancel the original order.
        profit_target (Decimal): the percentage profit to achieve after
            accounting for maker/taker fees.
    """

    def __init__(self, exchange_to_buy_on, exchange_to_sell_on):
        """Create the algorithm.

        Args:
            exchange_to_buy_on (str): the exchange to place limit buy orders on.
            exchange_to_sell_on (str): the exchange to place market sell orders
                on.
        """
        super().__init__()
        self.poll_period = datetime.timedelta(milliseconds=900)
        self.limit_order_update_period = datetime.timedelta(seconds=10)
        self.exchange_buy_on = exchange_to_buy_on
        self.exchange_sell_on = exchange_to_sell_on
        self.bid_amount_in_btc = Decimal(0.5)
        self.order_update_threshold = Decimal(0.1) # percent
        self.profit_target = Decimal(0.05)  # percent.
        self._last_run_at = datetime.datetime.min
        self._last_limit_order_update_at = None
        self._live_limit_action = None
        self._previous_fill_amount = Decimal(0)
        self._market_orders_made = []

    def initialize(self, exchanges_to_use):
        if self.exchange_buy_on not in exchanges_to_use or \
           self.exchange_sell_on not in exchanges_to_use:
            raise Exception(
                "This algorithm was set to buy on {} and sell on {}, but these "
                "exchanges were not present in the available exchanges: {}"
                .format(self.exchange_buy_on, self.exchange_sell_on,
                        ",".join((e for e in exchanges_to_use))))

    def on_data(self, data_slice):
        # Any actions will be collected here. They are returned at the end.
        actions = []
        time_now = data_slice.timestamp
        # If poll_period hasn't elapsed since the last run, return.
        should_poll = (time_now - self._last_run_at) >= self.poll_period
        if not should_poll:
            return actions
        self._last_run_at = data_slice.timestamp

        # Allow limit order update if  limit_order_update_period has elapsed
        # since last update or creation.
        should_update_limit_order = \
            self._last_limit_order_update_at is not None and \
            (time_now - self._last_limit_order_update_at) >= \
                self.limit_order_update_period

        order_book = data_slice.for_exchange(self.exchange_sell_on).order_book

        # Create a bid limit action if there is none.
        if not self._live_limit_action:
            # Calculate the BTC market price on the exchange to sell on.
            self._live_limit_action = self._create_bid_limit_order(order_book)
            self._last_limit_order_update_at = time_now
            actions.append(self._live_limit_action)
            return actions

        # There is a bid limit action.
        # If the order hasn't been placed yet, do nothing.
        if self._live_limit_action.order_id is None:
            # The order hasn't been placed yet. Nothing to do.
            logging.info("Waiting for order action to be placed.")
            if should_update_limit_order:
                logging.warning("The limit order hasn't been placed yet, "
                                "but the order update period has been "
                                "reached. There may be a bug, or the limit "
                                "order update period might be too short.")
            return actions

        # The action has been executed and the order has been placed. Every time
        # the order gets more filled, make a market sell order on the other
        # exchange by the fill amount.
        order = data_slice.for_exchange(self.exchange_buy_on).order(
            self._live_limit_action.order_id)
        self._sanity_check_order(order)
        fill_amount = Decimal(order['filled'])
        # If our bid order has been filled more, create an ask order on the
        # other exchange.
        if fill_amount > self._previous_fill_amount:
                ask_action = self._create_market_ask_order(fill_amount)
                actions.append(ask_action)
                return actions
        else:
            logging.info("The limit buy order has not been filled any further.")

        if should_update_limit_order:
            # We have created a limit order previously, but it is time to check
            # if it needs updating.
            if not self._live_limit_action:
                raise Exception("Logic error: the algorithm should not have "
                                "reached this point.")
            new_best_price = self._calculate_effective_sell_price(
                self.bid_amount_in_btc, order_book)
            original_price = Decimal(order['price'])
            if self._should_cancel_order(original_price, new_best_price,
                                         self.order_update_threshold):
                actions.append(CancelOrder(self.exchange_buy_on,
                                           self._live_limit_action.order_id))
                # Note: do we want the order_update_period to represent time
                # between actual updates, or just checks (more frequent)?
                self._last_limit_order_update_at = time_now
                self._live_limit_action = None
        return actions

    def _create_market_ask_order(self, latest_fill_amount):
        """Create a market ask order.

        After the bid limit order has been filled more, call this method to
        make a ask market order on the other exchange.

        Args:
            latest_fill_amount (Decimal): the amount of the bid limit order
                that has been filled.

        Returns:
            (CreateOrder): a ask market order.
        """
        fill_diff = latest_fill_amount - self._previous_fill_amount
        # TODO: What is the minimum amount of bitcoin we should be buying,
        # or does it not matter?
        logging.info("The limit buy order ({}) has been filled more (prev "
                     "fill: {}, current: {}). About to place a market sell "
                     "order for {} on {}.".format(
            self._live_limit_action.order_id, self._previous_fill_amount,
            latest_fill_amount, fill_diff, self.exchange_sell_on))
        market_ask_action = CreateOrder(self.exchange_sell_on,
                                        CreateOrder.Side.ASK,
                                        CreateOrder.Type.MARKET,
                                        amount=fill_diff)
        # Store the order action, although I'm not sure if we will need them
        # again. Maybe for logging.
        self._market_orders_made.append(market_ask_action)
        self._previous_fill_amount = latest_fill_amount
        # TODO: Do we need rounding here?
        if latest_fill_amount == self.bid_amount_in_btc:
            logging.info("Our buy limit order ({}) on {} has been fully "
                         "filled.".format(
                self._live_limit_action.order_id, self.exchange_buy_on))
            # TODO: might want to double check that the order is finished.
            self._live_limit_action = None
            # We can return here, as we know we don't need to update the
            # limit order, as it is finished.
        return market_ask_action

    def _create_bid_limit_order(self, order_book_of_sell_exchange):
        """Create the appropriate bid limit order action.

        Args:
            order_book_of_sell_exchange (ccxt order book): the order book of
                the exchange that BTC will be sold on. This is needed to
                calculate the correct price for the bid limit order.

        Returns:
            (CreateOrder): the bid limit order that was created.
        """
        sell_price = self._calculate_effective_sell_price(
            self.bid_amount_in_btc, order_book_of_sell_exchange)
        # Calculate the bid price to make the required profit.
        bid_price = self._calculate_bid_limit_price(self.exchange_buy_on,
                                                    self.exchange_sell_on,
                                                    sell_price,
                                                    self.profit_target)
        # Create and return the action.
        bid_action = CreateOrder(self.exchange_buy_on, CreateOrder.Side.BID,
                                 CreateOrder.Type.LIMIT,
                                 amount=self.bid_amount_in_btc, price=bid_price)
        return bid_action

    def _sanity_check_order(self, order):
        if order['amount'] == 0:
            raise Exception("Ops, we made an order for 0 BTC on {}. Something "
                            "isn't right." .format(self.exchange_buy_on))
        fill_amount = Decimal(order['filled'])
        if fill_amount > self.bid_amount_in_btc:
            raise Exception(
                "Something isn't right. Our order {} got filled ({}) more than "
                "the amount we placed ({}) on {}.".format(
                    self._live_limit_action.order_id, order['filled'],
                    self.bid_amount_in_btc, self.exchange_buy_on))

    @staticmethod
    def _should_cancel_order(original_price, new_best_price, threshold):
        """Determines if a bid limit order should be cancelled.

        Determines if a bid limit order at original_price should be cancelled
        given that the most up-to-date recommendation for a bid limit order
        is new_best_price.

        This is so simple it probably doesn't need separating, but the main
        routine needs simplification, so it was separated.

        Args:
            original_price (Decimal): the price of the original bid limit order.
            new_best_price (Decimal): the latest price recommendation for
                creating a bid limit order.
            threshold (Decimal): the % difference between the current and best
                price over which the order should be cancelled.
        """
        should_cancel = \
            (new_best_price - original_price) / new_best_price >  threshold
        return  should_cancel

    @staticmethod
    def _calculate_effective_sell_price(sell_amount, order_book):
        """Calculates the effective price on a market for the given amount.

        If you make a market ask order for more BTC than the top market bid, the
        effective price that will be fetched for your BTC will be an average of
        the price of all market bids that get consumed.

        Args:
            sell_amount (Decimal): the amount of BTC that will be sold.
            order_book  (ccxt order book): the order book of the exchange on
                which you intend to sell BTC.

        Returns:
            (Decimal): the mean price in $ / BTC that the given about of BTC
                will be sold for.
        """
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
    def _calculate_bid_limit_price(exchange_to_buy_on, exchange_to_sell_on,
                                   market_price_to_sell, profit_target):
        """Calculates the bid price needed to make the given profit.

        Args:
            exchange_to_buy_on (str): the id of the exchange that a bid limit
                order will be placed.
            exchange_to_sell_on (str): the id of the exchange that an ask market
                order will be placed.
            market_price_to_sell (str): the current market price of BTC on the
                exchange_to_sell_on.
            profit_target (Decimal): the percentage profit that should be
                achieved by buying on exchange_to_buy_on and selling on
                exchange_to_sell_on. Maker/taker fees are taken into account.

        Returns:
            (Decimal): the price to create a bid limit order on the
                exchange_to_buy_on to achieve the profit target.
        """
        buy_maker_fee = exchanges.fees(exchange_to_buy_on).maker
        sell_taker_fee = exchanges.fees(exchange_to_sell_on).taker

        # Calculate bid limit price.
        # Create bid limit on e1, sell at market on e2
        # Note: fee_factor is always less than 1. E.g., fee listed as 0.8
        # becomes a fee factor: 1/(1+0.08) = 0.926
        #
        # fee_factor = (1/(1+e2.taker_fee) * 1/(1+e1.maker_fee)))
        # profit_factor = (e2.highest_bid/bid_limit) * fee_factor
        # profit_factor / fee_factor = e2.highest_bid/bid_limit
        # fee_factor / profit_factor = bid_limit / e2.highest_bid
        # bid_limit = fee_factor * e2.highest_bid / profit_factor
        #
        # Example:
        #   buy_maker_fee = 0.004  # 0.4 %
        #   sell_taker_fee = 0.002 # 0.2 %
        #   profit_factor = 1.05   # 5.0 %
        #   market_price_to_sell = 16,000   # USD / BTC
        #   fee_factor = (1/(1+0.004)) * (1/(1+0.002)) = 0.994 (3dp)
        #   bid_limit = 0.994 * 16,000 / 1.05
        #             = 15147 $USD / BTC (5 sf)
        fee_factor = exchanges.fee_as_factor(buy_maker_fee) * \
                     exchanges.fee_as_factor(sell_taker_fee)
        bid_limit = fee_factor * market_price_to_sell / profit_target
        return bid_limit
