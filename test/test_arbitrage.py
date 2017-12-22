import pytest
import kohuhu.arbitrage as arbitrage
import kohuhu.trader


def test_one_way_pair_arbitrage():
    # It doesn't actually used these exchanges. The exchanges just have to be
    # something for which we have fee data.
    buy_on_exchange = 'gdax_sandbox'
    sell_on_exchange = 'gemini_sandbox'
    algorithm = arbitrage.OneWayPairArbitrage(buy_on_exchange, sell_on_exchange)
    trader = kohuhu.trader.Trader(algorithm, [buy_on_exchange, sell_on_exchange])
    trader.initialize()

    fake_data_for_exchange_1 = kohuhu.trader.ExchangeSlice(buy_on_exchange,
                                                           fetcher=None)
    fake_data_for_exchange_2 = kohuhu.trader.ExchangeSlice(sell_on_exchange,
                                                           fetcher=None)

    fake_order_book_2 = {
        'bids': [[20000,  0.2],
                 [1600,   5.0]],
        'asks': [[21000,  2.3],
                 [21300,  0.7]],
        'timestamp': 0
    }

    fake_data_for_exchange_1.order_book = {}
    fake_data_for_exchange_2.order_book = fake_order_book_2
    fake_slice = kohuhu.trader.Slice()
    fake_slice.set_slice(buy_on_exchange, fake_data_for_exchange_1)
    fake_slice.set_slice(sell_on_exchange, fake_data_for_exchange_2)
    trader.next_slice = fake_slice

    actions = trader.step()
    assert len(actions) == 1
    buy_order_action = actions[0]
    assert buy_order_action.type == kohuhu.trader.CreateOrder.Type.LIMIT
    assert buy_order_action.side == kohuhu.trader.CreateOrder.Side.BID
    assert buy_order_action.exchange == buy_on_exchange

    fake_slice.timestamp = fake_slice.timestamp + algorithm.poll_period
    actions = trader.step()
    assert len(actions) == 0

    # Add the order id to the action and fill in the order information. The
    # the order isn't completed yet.
    order_id = '5'
    buy_order_action.order_id = order_id
    order_info = {
        'id': order_id,
        'amount': algorithm.bid_amount_in_btc,
        'filled': 0.0,
        'remaining': algorithm.bid_amount_in_btc
    }
    fake_data_for_exchange_1.set_order(order_id, order_info)
    fake_slice.timestamp = fake_slice.timestamp + algorithm.poll_period
    actions = trader.step()
    assert len(actions) == 0

    # Change the order info so that the order is completed.
    # The algorithm should create a market buy order on the second exchange.
    order_info['filled'] = algorithm.bid_amount_in_btc
    order_info['remaining'] = 0
    fake_data_for_exchange_1.set_order(order_id, order_info)
    fake_slice.timestamp = fake_slice.timestamp + algorithm.poll_period
    actions = trader.step()
    assert len(actions) == 1
    assert actions[0].type == kohuhu.trader.CreateOrder.Type.MARKET
    assert actions[0].side == kohuhu.trader.CreateOrder.Side.ASK
    assert actions[0].exchange == sell_on_exchange
    assert actions[0].amount == algorithm.bid_amount_in_btc


















