import kohuhu.trader as trader
from kohuhu.exchanges import CreateOrder
from kohuhu.exchanges import CancelOrder
from kohuhu.exchanges import Action
from kohuhu.exchanges import Order
from kohuhu.exchanges import Side
import kohuhu.exchanges_old as exchanges
from decimal import Decimal
import logging
import datetime
import decimal
import kohuhu.currency as currency


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

    default_bid_amount_btc = Decimal(0.5)

    def __init__(self, exchange_to_buy_on, exchange_to_sell_on):
        """Create the algorithm.

        Args:
            exchange_to_buy_on (str): the exchange to place limit buy orders on.
            exchange_to_sell_on (str): the exchange to place market sell orders
                on.
        """
        super().__init__()
        self.poll_period = datetime.timedelta(seconds=3)
        self.exchange_buy_on = exchange_to_buy_on
        self.exchange_sell_on = exchange_to_sell_on
        # TODO:
        # It is probably easier to have the bid amount in USD, as it is easier
        # for us to understand how much we are spending. It also more directly
        # relates to how much we will have to deposit into our account.
        # self.bid_amount_in_usd = Decimal(500)
        self.bid_amount_in_btc = Decimal("0.01")
        self.order_update_threshold = Decimal("0.1")  # percent
        self.profit_target = Decimal("0.05")  # percent.
        self._live_limit_action = None
        self._live_cancel_action = None
        self._previous_fill_amount = Decimal(0)
        self._market_orders_made = []
        self._add_action_callback = None
        self._state = None

    def status_str(self):
        status = ""
        if self._state:
            exch_1_state = self._state.for_exchange(self.exchange_buy_on)
            exch_2_state = self._state.for_exchange(self.exchange_sell_on)
            if exch_1_state.order_book().asks():
                status += "Min ask on exchange 1: " \
                          f"{exch_1_state.order_book().asks()[0]}\n"
            if exch_2_state.order_book().bids():
                status += "Max bid on exchange 2: " \
                          f"{exch_2_state.order_book().bids()[0]}\n"
            order_id = None
            if self._live_limit_action and self._live_limit_action.order:
                order_id = self._live_limit_action.order.order_id
                status += f"Live order ID on exchange 1: {order_id}"
        return status

    def initialize(self, state, timer, add_action_callback):
        self._state = state
        self._add_action_callback = add_action_callback
        if not state.for_exchange(self.exchange_buy_on) or \
                not state.for_exchange(self.exchange_sell_on):
            raise Exception(
                "This algorithm was set to buy on {} and sell on {}, but these "
                "exchanges were not present in the available exchanges: {}"
                    .format(self.exchange_buy_on, self.exchange_sell_on,
                            ",".join((e for e in state.exchanges()))))
        state.for_exchange(self.exchange_buy_on).update_publisher.add_callback(
            self.on_update)
        state.for_exchange(self.exchange_buy_on).update_publisher.add_callback(
            self.on_update)
        timer.do_every(self.poll_period, self.on_timer)

    def on_timer(self, time):
        self.on_data()

    def on_update(self, publisher_id, update_description):
        self.on_data()

    def on_data(self):
        # Create a bid limit action if there is none.
        if not self._live_limit_action:
            # Calculate the BTC market price on the exchange to sell on.
            self._live_limit_action = self._create_bid_limit_order(self._state)
            print("Putting one on")
            self._add_action_callback(self._sanity_check_action(
                self._live_limit_action))

        # There is a bid limit action.
        if self._live_limit_action.status == CreateOrder.Status.FAILED:
            logging.warning("Resetting the algorithm as the limit bid order "
                            "failed to be placed.")
            print("Resetting the algorithm as the limit bid order "
                  "failed to be placed.")
            self._live_limit_action = None
            return
        elif self._live_limit_action.status == CreateOrder.Status.PENDING:
            # The order hasn't been placed yet. Nothing to do.
            logging.info("Waiting for order action to be placed.")
            print("Waiting for order action to be placed.")
            return
        else:
            # The order action must have been successfully executed.
            if self._live_limit_action.status != CreateOrder.Status.SUCCESS:
                raise Exception("The action status should be either FAILED, "
                                "PENDING or SUCCESS, but was: "
                                f"{self._live_limit_action.status}.")

        # The action has been executed and the order has been placed. Every time
        # the order gets more filled, make a market sell order on the other
        # exchange by the fill amount.
        self._process_fill(self._state)

        # Process any planned cancellations.
        # TODO: we may wish to allow cancellations while the order is still
        # pending. For now, it is not supported, as execution only reaches here
        # after a limit order has been successfully placed.
        skip_updating_limit_order = False
        if self._live_cancel_action:
            if self._live_cancel_action.status == Action.Status.PENDING:
                pass
            elif self._live_cancel_action.status == Action.Status.SUCCESS:
                if self._live_limit_action.status == Order.Status.OPEN:
                    raise Exception("A cancel action was marked as successful "
                                    "but the order is still marked as open.")
                # The cancel action was successful. We can clear both actions
                # and return.
                self._live_limit_action = None
                self._live_cancel_action = None
            elif self._live_cancel_action.status == Action.Status.FAILED:
                raise NotImplementedError("A cancel action failed. This "
                                          "situation is currently not handled.")
            # Don't update_bid_limit_order()
            # Note: using returns may make it difficult to understand and debug
            # the code. We may need to switch to using boolean flags to mark
            # what should be skipped futher down in the loop.
            return

        # Check the status of the order. Only proceed if the order is open.
        if self._live_limit_action.order.status == Order.Status.CANCELLED:
            raise NotImplementedError("Our order has been cancelled without our"
                                      " intention. This situation is currently "
                                      "not handled.")
        elif self._live_limit_action.order.status == Order.Status.CLOSED:
            # Reset our order records if the order is completed.
            self._live_limit_action = None
            self._live_cancel_action = None
            self._previous_fill_amount = Decimal(0)
            return
        else:
            if self._live_limit_action.order.status != Order.Status.OPEN:
                raise Exception("Unexpected order status: "
                                f"{self._live_limit_action.status}.")

        self.update_bid_limit_order()

    def update_bid_limit_order(self):
        # If we haven't created a bid limit action, there is nothing to update.
        if not self._live_limit_action:
            return

        # If the bid limit action hasn't been placed yet, also return. This is
        # done as we can't cancel the order if it hasn't been placed yet.
        if not self._live_limit_action.order:
            return

        exch_2_order_book = self._state.for_exchange(self.exchange_sell_on) \
            .order_book()
        new_best_price = self._calculate_effective_sell_price(
            self.bid_amount_in_btc, exch_2_order_book)
        original_price = self._live_limit_action.order.price
        if self._should_cancel_order(original_price, new_best_price):
            self._live_cancel_action = CancelOrder(
                self.exchange_buy_on, self._live_limit_action.order.order_id)
            self._add_action_callback(self._live_cancel_action)


    def _sanity_check_action(self, action):
        # Don't make any market bid orders.
        if action.type == Order.Type.MARKET and \
                action.side == Side.BID:
            raise Exception("This algorithm shouldn't be making market bid "
                            "orders.")
        # Don't make any limit ask orders.
        if action.type == Order.Type.LIMIT and \
                action.side == Side.ASK:
            raise Exception("This algorithm shouldn't be making limit ask "
                            "orders.")
        # Don't make asks on the exchange to bid on.
        if action.exchange == self.exchange_buy_on:
            if action.side == Side.ASK:
                raise Exception("This algorithm shouldn't be making ask "
                                "orders on the {} exchange."
                                .format(self.exchange_buy_on))
        # Don't make bids on the exchange to ask on.
        if action.exchange == self.exchange_sell_on:
            if action.side == Side.BID:
                raise Exception("This algorithm shouldn't be making bid "
                                "orders on the {} exchange."
                                .format(self.exchange_sell_on))
        return action

    def _process_fill(self, state):
        """After a bid order has been filled more, place new orders if needed.

        After the bid limit order has been filled more, call this method to
        make an ask market order on the other exchange.

        Args:
            state (Slice): the lastest data slice.
        """
        order = self._live_limit_action.order
        self._sanity_check_order(order)
        latest_fill_amount = order.filled
        # TODO: need to insure we don't have any rounding issues here.
        fill_diff = latest_fill_amount - self._previous_fill_amount
        if fill_diff == Decimal(0):
            logging.info("The limit buy order has not been filled any further.")
        else:
            logging.info("The limit buy order ({}) has been filled more (prev "
                         "fill: {}, current: {}). About to place a market sell "
                         "order for {} on {}.".format(
                self._live_limit_action.order.order_id,
                self._previous_fill_amount, latest_fill_amount, fill_diff,
                self.exchange_sell_on))
            market_ask_action = CreateOrder(self.exchange_sell_on,
                                            Side.ASK, Order.Type.MARKET,
                                            amount=fill_diff)
            self._add_action_callback(
                self._sanity_check_action(market_ask_action))
            # Store the order action, although I'm not sure if we will need them
            # again. Maybe for logging.
            self._market_orders_made.append(market_ask_action)
            self._previous_fill_amount = latest_fill_amount
        if latest_fill_amount == self.bid_amount_in_btc:
            logging.info("Our buy limit order ({}) on {} has been fully filled."
                         .format(self._live_limit_action.order.order_id,
                                 self.exchange_buy_on))
            # If the order is filled, it should be marked as closed.
            if not order.status == Order.Status.CLOSED:
                raise Exception("An order has been fully filled but not marked "
                                "as closed. Order ID: {order.order_id}.")

    def _create_bid_limit_order(self, data_slice):
        """Create the appropriate bid limit order action.

        The bid should be for the self.bid_amount_usd worth of Bitcoin, or
        if our balance is not enough, the maximum we can trade.

        Args:
            order_book_of_ask_exchange (ccxt order book): the order book of
                the exchange that BTC will be sold on. This is needed to
                calculate the correct price for the bid limit order.
            balance_on_bid_exchange (ccxt balance): the balance on the exchange
                a bid will be placed on.

        Returns:
            (CreateOrder): the bid limit order that was created.
        """
        balance = data_slice.for_exchange(self.exchange_buy_on).balance(
            force_update=True)
        order_book = data_slice.for_exchange(self.exchange_sell_on).order_book()
        sell_price = self._calculate_effective_sell_price(
            self.bid_amount_in_btc, order_book)
        # Calculate the bid price to make the required profit.
        bid_price = self._calculate_bid_limit_price(self.exchange_buy_on,
                                                    self.exchange_sell_on,
                                                    sell_price,
                                                    self.profit_target)
        btc_amount = self.bid_amount_in_btc
        usd_balance = balance.free('USD')

        # TODO: is it okay to assume that the fees are not on top, and thus they
        # will not cause us to run our balance negative with this calculation?

        # Let's round to 3 dp so that the numbers are easy for us to watch.
        # Round down, as we need end up with an amount we can afford.
        three_dp = Decimal("0.001")
        max_can_afford = (usd_balance / btc_amount).quantize(three_dp,
                                                             decimal.ROUND_DOWN)
        btc_amount = min(btc_amount, max_can_afford)
        if btc_amount == Decimal(0):
            raise Exception("Not enough funds to buy on "
                            f"{self.exchange_buy_on}.")
        # Create and return the action.
        rounded_price = currency.round_to_cents(bid_price)
        bid_action = CreateOrder(self.exchange_buy_on, Side.BID,
                                 Order.Type.LIMIT, amount=btc_amount,
                                 price=rounded_price)
        self._last_limit_order_update_at = data_slice.timestamp
        return bid_action

    def _sanity_check_order(self, order):
        if order is None:
            raise Exception("Error: order is None.")
        if order.amount == Decimal(0):
            raise Exception("Ops, we made an order for 0 BTC on {}. Something "
                            "isn't right.".format(self.exchange_buy_on))
        fill_amount = order.filled
        if fill_amount > self.bid_amount_in_btc:
            raise Exception(
                "Something isn't right. Our order {} got filled ({}) more than "
                "the amount we placed ({}) on {}.".format(
                    self._live_limit_action.order_id, order.filled,
                    self.bid_amount_in_btc, self.exchange_buy_on))

    def _should_cancel_order(self, bid_price, new_best_sell_price):
        """Determines if a bid limit order should be cancelled.

        Determines if a bid limit order at original_price should be cancelled
        given that the most up-to-date recommendation for a bid limit order
        is new_best_price.

        This is so simple it probably doesn't need separating, but the main
        routine needs simplification, so it was separated.

        Args:
            bid_price (Decimal): the price of the current bid limit order.
            new_best_sell_price (Decimal): the latest price recommendation for
                creating a bid limit order.
        """
        expected_profit_factor = self._calculate_profit_factor(
            self.exchange_buy_on, self.exchange_sell_on, bid_price,
            new_best_sell_price)
        target_profit_factor = Decimal(1) + self.profit_target
        profit_diff = abs(expected_profit_factor - target_profit_factor)
        should_cancel = profit_diff > self.order_update_threshold
        if should_cancel:
            logging.info("Profit has changed by {:.2%}. Expected profit factor"
                         ": {:.2}. Target profit factor: {:.2}. Order should be"
                         " cancelled.".format(profit_diff,
                                              expected_profit_factor,
                                              target_profit_factor))
        return should_cancel

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
            if bid_index >= len(order_book.bids()):
                raise Exception("Something is not right: there is only {} BTC "
                                "buy orders on an exchange. We wont be able to "
                                "fill our market order. Something is probably "
                                "broken.".format(capacity_counted))
            # TODO: can we get a clean way to separate the amount and price
            # information better?
            highest_bid = order_book.bids()[bid_index]
            amount_used = min(highest_bid.quantity, remaining)
            fraction_of_trade = amount_used / sell_amount
            effective_market_price += fraction_of_trade * highest_bid.price
            capacity_counted += highest_bid.quantity
            bid_index += 1
        return effective_market_price

    @staticmethod
    def _calculate_profit_factor(exchange_to_buy_on, exchange_to_sell_on,
                                 buy_price, sell_price):
        # From the explanation of _calculate_bid_limit_price, we ended up with
        # the equation:
        #       bid_limit = fee_factor * e2.highest_bid / profit_factor
        # Rearranging for profit, we get:
        #       profit_factor = (fee_factor * highest_bid) / bid_limit
        fee_factor = OneWayPairArbitrage._fee_factor(exchange_to_buy_on,
                                                     exchange_to_sell_on)
        profit_factor = (fee_factor * sell_price) / buy_price
        return profit_factor

    @staticmethod
    def _fee_factor(exchange_to_buy_on, exchange_to_sell_on):
        # Note: fee_factor is always less than 1. E.g., fee listed as 0.8
        # becomes a fee factor: 1/(1+0.08) = 0.926
        #
        # fee_factor = (1/(1+e2.taker_fee) * 1/(1+e1.maker_fee)))
        buy_maker_fee = exchanges.fees(exchange_to_buy_on).maker
        sell_taker_fee = exchanges.fees(exchange_to_sell_on).taker
        fee_factor = exchanges.fee_as_factor(buy_maker_fee) * \
                     exchanges.fee_as_factor(sell_taker_fee)
        return fee_factor

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
        # Relationship:
        #  fees go up  -> bid goes down
        #  profit target goes up -> bid goes down
        #
        # Example:
        #   buy_maker_fee = 0.004  # 0.4 %
        #   sell_taker_fee = 0.002 # 0.2 %
        #   profit_factor = 1.05   # 5.0 %
        #   market_price_to_sell = 16,000   # USD / BTC
        #   fee_factor = (1/(1+0.004)) * (1/(1+0.002)) = 0.994 (3dp)
        #   bid_limit = 0.994 * 16,000 / 1.05
        #             = 15147 $USD / BTC (5 sf)
        profit_factor = Decimal(1) + profit_target
        fee_factor = OneWayPairArbitrage._fee_factor(exchange_to_buy_on,
                                                     exchange_to_sell_on)
        bid_limit = fee_factor * market_price_to_sell / profit_factor
        return bid_limit
