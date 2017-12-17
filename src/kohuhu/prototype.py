import time
from datetime import datetime
from enum import Enum
from decimal import Decimal
import kohuhu.exchanges as exchanges


class LimitAction:

    class Type(Enum):
        ASK = 1
        BID = 2

    def __init__(self, type, on_exchange, counter_on_exchange, price):
        self.type = type
        self.on_exchange = on_exchange
        # Profit is a function of price difference and fees. Thus,
        # to calculate the limit price to ask, you must guess what exchange
        # you will be selling on, and what profit you are likely to get.
        # Probably doesn't need to be stored here, as it will be recalculated
        # when selling anyway. Including so I don't forget that it's important.
        self.counter_on_exchange = counter_on_exchange
        self.price = price


def submit_order(order):
    pass

def get_cross_market_spread(exchanges):
    """
    Return the spread of the union of exchanges.
    """
    return None

def get_actions(exchanges, profit_target):
    """
    Get the list of limit bids and asks that should be submitted.

    :param exchanges: the list of exchanges to use
    :param profit_target: the minimum profit to achieve after accounting for
                           maker/taker fees.
    """
    askActions = []
    bidActions = []
    for e1 in exchanges:
        # The best ask action is the one that achieves the profit target
        # with the lowest price (as it will be more likely to be executed).
        # The opposite for bit actions.
        best_ask_action = None
        best_bid_action = None
        for e2 in exchanges:
            maker_fee_e1, taker_fee_e1 = exchanges.fees(e1)
            maker_fee_e2, taker_fee_e2 = exchanges.fees(e2)
            market_spread_e1 =  exchanges.market_spread(e1)
            market_spread_e2 = exchanges.market_spread(e2)

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
            fee_factor = exchanges.fee_as_factor(maker_fee_e1) * \
                         exchanges.fee_as_factor(taker_fee_e2)
            bid_limit = fee_factor * market_spread_e2.highest_bid / profit_target
            if best_bid_action is None or best_bid_action.price < bid_limit:
                best_bid_action = LimitAction(LimitAction.Type.BID, e1, e2,
                                            bid_limit)

            # Calculate ask limit price.
            # Create ask limit on e1, buy at market on e2.
            # profit_factor = (ask_limit/e2.lowest_ask) * fee_factor
            # ask_limit = e2.lowest_ask * (profit_factor / fee_factor)
            # Sanity check:
            # ask_limit goes up with greater profit and fee requirements, and
            # is proportional to the other market ask price.
            ask_limit =  market_spread_e2.lowest_ask * (profit_target /
                                                        fee_factor)
            if best_ask_action is None or best_ask_action.price > ask_limit:
                best_ask_action = LimitAction(LimitAction.Type.BID, e1, e2,
                                              ask_limit)
        askActions.append(best_ask_action)
        bidActions.append(best_bid_action)
        return askActions, bidActions

def get_accepted_limit_orders():
    return [LimitAction(None, None, None)]

def profit_for_shorted_ask(on_exchange, exchange_to_mirror_ask):
    # Move some of the logic from get actions above to here.
    return None

def profit_for_shorted_bid(on_exchange, exchange_to_mirror_bid):
    # Move some of the logic from get actions above to here.
    return None

def sumbit_market_order():
    pass

exchanges = []
# 4% profit after all fees (buyer/taker fees, transaction fee and amortized
# currency fees).
profit_target = 1.04
transaction_fee_estimated = 14 # USD
# Make trades in batches of USD or USD equivalent value:
trade_amount = 1000
currency_fee_amortized = 0.005 # percentage
profit_before_amortized_costs = profit_target + currency_fee_amortized

# Assuming infinite liquidity on all markets (lol).
poll_period_secs = 0.5
bid_ask_update_period_secs = 10
last_bid_ask_update_time = datetime.min
askLimits = []
bidLimits = []
while True:
    if (datetime.now() - last_bid_ask_update_time).seconds > \
            bid_ask_update_period_secs:
        last_bid_ask_update_time = datetime.now()
        askLimits, bidLimits = get_actions(exchanges, profit_before_amortized_costs)
        # Need some way of checking if orders are within some threshold of
        # existing ones; maybe we can have more responsive orders if we do them
        # only when we need to.
        for a in askLimits:
            submit_order(a)
        for b in bidLimits:
            submit_order(b)

    for order in get_accepted_limit_orders():
        if order.type == LimitAction.Type.ASK:
            best_market_ask = None
            for e in exchanges:
                # Find the most profitible exchange to short the ask across.
                # It may have changed since the original LimitAction was
                # created.
                pass
            sumbit_market_order(best_market_ask)
        if order.type == LimitAction.Type.BID:
            best_market_bid = None
            for e in exchanges:
                # Same as above.
                pass
            sumbit_market_order(best_market_bid)

    time.sleep(poll_period_secs)