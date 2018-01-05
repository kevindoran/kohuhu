from kohuhu.gemini import GeminiExchange
import kohuhu.exchanges as exchanges
import pytest
import kohuhu.credentials as credentials
from decimal import Decimal

credentials.load_credentials("api_credentials.json.example")
gemini_example_api_key = "abcdefghi"


@pytest.fixture
def real_credentials():
    """A bit of a hacky fixture that switches to real credentials then back."""
    credentials.load_credentials("api_credentials.json")
    yield None
    credentials.load_credentials("api_credentials.json.example")


def test_encode_and_sign(real_credentials):
    """Tests that _encode_and_sign() produces the correct payload and signature.

    The expected signature and payload were obtained by intercepting a
    successful REST request made by the test_get_balance() test in
    test_gemini_integration.py. To recreate the expected signature and payload,
    run test_get_balance() with proxy mode enabled and intercept the REST
    request with a tool such as BurpSuite.
    """
    # Setup
    payload = {
        'request': '/v1/balances',
        'nonce': 1515123745863
    }
    # For REST, the signature should be UTF-8 and the payload should be ASCII.
    # The b infront of the expected_payload effectively makes it ASCII encoded.
    # Without b prefix, Python 3 strings default to UTF-8 encoding.
    expected_signature = "6713960635c1996274fc35642084bdedc74a6a483bf35b0c3c2f8c331f1ee427711ae30b61cfc2be856c65a0c849ec52"
    expected_payload = b"eyJyZXF1ZXN0IjogIi92MS9iYWxhbmNlcyIsICJub25jZSI6IDE1MTUxMjM3NDU4NjN9"
    gemini = GeminiExchange(sandbox=True)

    # Action
    b64, signature = gemini._encode_and_sign(payload)

    # Check.
    assert expected_signature == signature
    assert b64 == expected_payload


@pytest.fixture
def heartbeat_response():
    test_heartbeat_response = {
        "type": "heartbeat",
        "timestampms": 123456789,
        "sequence": 0,
        "socket_sequence": 10,
        "trace_id": "some_sort_of_id"
    }
    return test_heartbeat_response


def test_process_heartbeat(heartbeat_response):
    """Test that GeminiExchange correctly handles heartbeat responses."""
    socket_info = GeminiExchange.SocketInfo()

    # Test that an expected heartbeat is correctly processed.
    socket_info.seq = heartbeat_response['socket_sequence']
    GeminiExchange._process_heartbeat(heartbeat_response, socket_info)
    assert socket_info.heartbeat_seq == heartbeat_response['sequence'] + 1
    assert socket_info.heartbeat_timestamp_ms == \
           heartbeat_response['timestampms']

    # Test that an unexpected heartbeat causes an exception to be raised.
    # Expect the wrong heartbeat sequence:
    socket_info.heartbeat_seq = heartbeat_response['sequence'] + 1
    with pytest.raises(Exception):
        GeminiExchange._process_heartbeat(heartbeat_response, socket_info)


def test_check_sequence(heartbeat_response):
    socket_info = GeminiExchange.SocketInfo()

    # Test that an expected socket sequence is correctly handled.
    socket_info.seq = 13
    heartbeat_response['socket_sequence'] = 13
    GeminiExchange._check_sequence(heartbeat_response, socket_info)
    assert socket_info.seq == 14

    # Tests that an unexpected socket sequence causes an exception.
    socket_info.seq = 13
    heartbeat_response['socket_sequence'] = 14
    with pytest.raises(Exception):
        GeminiExchange._check_sequence(heartbeat_response, socket_info)

@pytest.fixture
def subscription_ack_response():
    test_response = {
        "type": "subscription_ack",
        "accountId": 266,
        "subscriptionId": "ws-order-events-266-as1hpfh4bmin4limcj0",
        "symbolFilter": [],
        "apiSessionFilter": [gemini_example_api_key],
        "eventTypeFilter": []
    }
    return test_response


def test_process_subscription_ack(subscription_ack_response):
    exchange = GeminiExchange()

    # No exceptions should be thrown.
    exchange._handle_orders(subscription_ack_response)

    # Clear the api session filter. An exception should be thrown.
    subscription_ack_response['apiSessionFilter'] = []
    with pytest.raises(Exception):
        exchange._handle_orders(subscription_ack_response)


@pytest.fixture
def initial_response():
    test_response = {
        "type": "initial",
        "order_id": "652150",
        "api_session": "UI",
        "symbol": "btcusd",
        "side": "buy",
        "order_type": "exchange limit",
        "timestamp": "1478789840",
        "timestampms": 1478789840842,
        "is_live": True,
        "is_cancelled": False,
        "is_hidden": False,
        "avg_execution_price": "713.95",
        "executed_amount": "2",
        "remaining_amount": "3",
        "original_amount": "5",
        "price": "713.95",
        "socket_sequence": 0
    }
    return test_response


def test_process_initial(initial_response):
    """Test that GeminiExchange creates an order for an 'initial' response."""
    exchange = GeminiExchange()
    # Test that the initial order is added to the exchange state.
    exchange._handle_orders(initial_response)
    order = exchange.exchange_state().order(initial_response['order_id'])
    assert order
    assert order.order_id == initial_response['order_id']
    assert order.status == exchanges.Order.Status.OPEN
    assert order.side == exchanges.Order.Side.BID
    assert order.type == exchanges.Order.Type.LIMIT
    assert order.price == Decimal(initial_response['price'])
    assert order.amount == Decimal(initial_response['original_amount'])
    assert order.filled == Decimal(initial_response['executed_amount'])
    assert order.remaining == Decimal(initial_response['remaining_amount'])
    assert order.average_price == \
           Decimal(initial_response['avg_execution_price'])

    # Test than an exception is raised if an initial response is received for
    # an existing order.
    with pytest.raises(Exception):
        exchange._handle_orders(initial_response)


@pytest.fixture
def accepted_limit_bid_response():
    amount = Decimal(10)
    action = exchanges.CreateOrder("gemini", amount, exchanges.Order.Side.BID,
                                   exchanges.Order.Type.LIMIT)
    test_response = {
        "type": "accepted",
        "order_id": "6310",
        "event_id": "6311",
        "api_session": "UI",
        "symbol": "btcusd",
        "side": "buy",
        "order_type": "exchange limit",
        "timestamp": "1478203017",
        "timestampms": 1478203017455,
        "is_live": True,
        "is_cancelled": False,
        "is_hidden": False,
        "avg_execution_price": "0",
        "original_amount": "1",
        "price": "721.24",
        "socket_sequence": 302,
        "client_order_id": id(action)
    }
    return test_response, action


def test_process_accepted_limit_bid(accepted_limit_bid_response):
    """Test that a limit buy accepted response is processed correctly."""
    response = accepted_limit_bid_response[0]
    action = accepted_limit_bid_response[1]
    exchange = GeminiExchange()

    # Test that the initial order is added to the exchange state.
    # Setup
    # First we need to hackily add a fake action to the action list.
    exchange._actions = [action]

    # Action
    exchange._handle_orders(response)
    order = exchange.exchange_state().order(response['order_id'])
    assert order.order_id == response['order_id']
    assert order.status == exchanges.Order.Status.OPEN
    assert order.side == exchanges.Order.Side.BID
    assert order.type == exchanges.Order.Type.LIMIT
    assert order.amount == Decimal(response['original_amount'])
    assert order.price == Decimal(response['price'])
    assert order.average_price == Decimal(response['avg_execution_price'])
    assert action.status == exchanges.Action.Status.SUCCESS
    assert action.order == order

    # Test that an exception is thrown if there is no matching action.
    exchange._actions = []
    with pytest.raises(Exception):
        exchange._handle_orders(response)


@pytest.fixture
def accepted_market_sell_response():
    amount = Decimal(10)
    action = exchanges.CreateOrder("gemini", amount, exchanges.Order.Side.ASK,
                                   exchanges.Order.Type.MARKET)
    test_response = {
        "type": "accepted",
        "order_id": "6320",
        "event_id": "6321",
        "api_session": "UI",
        "symbol": "btcusd",
        "side": "sell",
        "order_type": "market sell",
        "timestamp": "1478204198",
        "timestampms": 1478204198989,
        # I think this means that the order has been matched immediately and
        # there wont be a following booked event.
        "is_live": True,
        "is_cancelled": False,
        "is_hidden": False,
        "avg_execution_price": "0",
        "original_amount": "500",
        "socket_sequence": 32307,
        "client_order_id": id(action)
    }
    return test_response, action


def test_process_accepted_market_sell(accepted_market_sell_response):
    response = accepted_market_sell_response[0]
    action = accepted_market_sell_response[1]
    exchange = GeminiExchange()

    # Test that the initial order is added to the exchange state.
    # Setup
    # First we need to hackily add a fake action to the action list.
    exchange._actions = [action]

    # Action
    exchange._handle_orders(response)
    order = exchange.exchange_state().order(response['order_id'])
    assert order.order_id == response['order_id']
    assert order.status == exchanges.Order.Status.OPEN
    assert order.side == exchanges.Order.Side.ASK
    assert order.type == exchanges.Order.Type.MARKET
    assert order.amount == Decimal(response['original_amount'])
    assert order.average_price == Decimal(response['avg_execution_price'])
    assert action.status == exchanges.Action.Status.SUCCESS
    assert action.order == order

    # Test that an exception is thrown if there is no matching action.
    exchange._actions = []
    with pytest.raises(Exception):
        exchange._handle_orders(response)


@pytest.fixture
def rejected_response():
    amount = Decimal(10)
    action = exchanges.CreateOrder("gemini", amount, exchanges.Order.Side.BID,
                                   exchanges.Order.Type.LIMIT)
    test_response = {
        "type": "rejected",
        "order_id": "104246",
        "event_id": "104247",
        "reason": "InvalidPrice",
        "api_session": "UI",
        "symbol": "btcusd",
        "side": "buy",
        "order_type": "exchange limit",
        "timestamp": "1478205545",
        "timestampms": 1478205545047,
        "is_live": False,
        # Note: the example on the API docs don't include is_cancelled for
        # is_hidden in the response. However, they are marked as required in
        # the 'Response' section. is_cancelled is also specifically mentioned
        # as being present in the section on rejected responses.
        "is_cancelled": False,
        "is_hidden": False,
        "original_amount": "5",
        "price": "703.14444444",
        "socket_sequence": 310311,
        "client_order_id": id(action)
    }
    return test_response, action


def test_process_rejected(rejected_response):
    response = rejected_response[0]
    action = rejected_response[1]
    exchange = GeminiExchange()

    # Test that the initial order is added to the exchange state.
    # Setup
    # First we need to hackily add a fake action to the action list.
    exchange._actions = [action]

    # Action
    exchange._handle_orders(response)
    order = exchange.exchange_state().order(response['order_id'])
    assert order.order_id == response['order_id']
    assert order.status == exchanges.Order.Status.CLOSED
    assert order.side == exchanges.Order.Side.BID
    assert order.type == exchanges.Order.Type.LIMIT
    assert order.amount == Decimal(response['original_amount'])
    assert order.price == Decimal(response['price'])
    assert action.status == exchanges.Action.Status.FAILED
    assert action.order == order

    # Test that an exception is thrown if there is no matching action.
    exchange._actions = []
    with pytest.raises(Exception):
        exchange._handle_orders(response)


@pytest.fixture
def complete_fill_response():
    amount = "1"
    price = "721.24"
    order_id = 6310

    order = exchanges.Order()
    order.order_id = order_id
    order.side = exchanges.Order.Side.BID
    order.type = exchanges.Order.Type.LIMIT
    order.price = Decimal(price)
    order.amount = Decimal(amount)
    order.remaining = Decimal(amount)
    order.filled = Decimal(0)
    order.status = exchanges.Order.Status.OPEN

    test_response = {
        "type": "fill",
        "order_id": order_id,
        "api_session": "UI",
        "symbol": "btcusd",
        "side": "buy",
        "order_type": "exchange limit",
        "timestamp": "1478203017",
        "timestampms": 1478203017455,
        "is_live": False,
        "is_cancelled": False,
        "is_hidden": False,
        "avg_execution_price": price,
        "executed_amount": amount,
        "remaining_amount": "0",
        "original_amount": amount,
        "price": price,
        "fill": {
            "trade_id": "6312",
            "liquidity": "Taker",
            "price": price,
            "amount": amount,
            "fee": "1.8031",
            "fee_currency": "USD"
        },
        "socket_sequence": 201961
    }
    return test_response, order


def test_process_complete_fill(complete_fill_response):
    response = complete_fill_response[0]
    order = complete_fill_response[1]

    # Setup
    exchange = GeminiExchange()
    exchange.exchange_state().set_order(order.order_id, order)

    # Action
    exchange._handle_orders(response)

    # Test that the order is updated and closed.
    order = exchange.exchange_state().order(order.order_id)
    assert order
    assert order.remaining == Decimal(0)
    assert order.filled == Decimal(response['executed_amount'])
    assert order.status == exchanges.Order.Status.CLOSED

    # Test that an exception is thrown if no matching order is found.
    response['order_id'] = 10
    with pytest.raises(Exception):
        exchange._handle_orders(response)


@pytest.fixture
def partial_fill_response():
    amount = "785.020886"
    price = "0.01514"
    order_id = 6310

    order = exchanges.Order()
    order.order_id = order_id
    order.side = exchanges.Order.Side.BID
    order.type = exchanges.Order.Type.LIMIT
    order.price = Decimal(price)
    order.amount = Decimal(amount)
    order.remaining = Decimal(amount)
    order.filled = Decimal(0)
    order.status = exchanges.Order.Status.OPEN

    test_response = {
        "type": "fill",
        "order_id": order_id,
        "api_session": "UI",
        # Note: symbol was ethbtc in the API docs.
        "symbol": "btcusd",
        "side": "sell",
        "order_type": "exchange limit",
        "timestamp": "1478729284",
        # The following line was in the API example, however, I think it's a
        # typo or it's outdated:
        #"timestampMs": 1478729284169,
        # Corrected:
        "timestampms": 1478729284169,
        "is_live": True,
        "is_cancelled": False,
        "is_hidden": False,
        "avg_execution_price": price,
        # The following line was in the API example, however, I think it's a
        # typo or it's outdated:
        #"total_executed_amount": "481.95988631",
        # Corrected:
        "executed_amount": "481.95988631",
        "remaining_amount": "303.06099969",
        "original_amount": amount,
        "original_price": price,
        "fill": {
            "trade_id": "557315",
            "liquidity": "Maker",
            "price": "0.01514",
            "amount": "481.95988631",
            "fee": "0.0182421816968335",
            "fee_currency": "BTC"
        },
        "socket_sequence": 471177
    }

    return test_response, order


def test_process_partial_fill(partial_fill_response):
    response = partial_fill_response[0]
    order = partial_fill_response[1]

    # Setup
    exchange = GeminiExchange()
    exchange.exchange_state().set_order(order.order_id, order)

    # Action
    exchange._handle_orders(response)

    # Test that the order is updated (but still open).
    order = exchange.exchange_state().order(order.order_id)
    assert order
    assert order.remaining == Decimal(response['remaining_amount'])
    assert order.filled == Decimal(response['executed_amount'])
    assert order.status == exchanges.Order.Status.OPEN

    # Test that an exception is thrown if no matching order is found.
    response['order_id'] = 10
    with pytest.raises(Exception):
        exchange._handle_orders(response)


@pytest.fixture
def cancelled_response():
    amount = "10"
    price = "800.00"
    order_id = 6361

    order = exchanges.Order()
    order.order_id = order_id
    order.side = exchanges.Order.Side.BID
    order.type = exchanges.Order.Type.LIMIT
    order.price = Decimal(price)
    order.amount = Decimal(amount)
    order.remaining = Decimal(amount)
    order.filled = Decimal(0)
    order.status = exchanges.Order.Status.CANCELLED

    cancelled_action = exchanges.CancelOrder("gemini", order_id)

    test_response = {
      "type": "cancelled",
      "order_id": order_id,
      "event_id": "6363",
      "api_session": "UI",
      "symbol": "btcusd",
      "side": "buy",
      "order_type": "exchange limit",
      "timestamp": "1478204410",
      "timestampms": 1478204410558,
      "is_live": False,
      "is_cancelled": True,
      "is_hidden": False,
      "avg_execution_price": "0.00",
      "executed_amount": "0",
      "remaining_amount": amount,
      "original_amount": "10",
      "price": "800.00",
      "socket_sequence": 47473,
    }

    return test_response, order, cancelled_action


def test_process_cancelled(cancelled_response):
    response = cancelled_response[0]
    order = cancelled_response[1]
    cancel_action = cancelled_response[2]

    # Setup
    exchange = GeminiExchange()
    exchange.exchange_state().set_order(order.order_id, order)
    exchange._cancel_actions = {order.order_id: cancel_action}

    # Action
    exchange._handle_orders(response)

    # Test that the order is updated and marked as cancelled.
    order = exchange.exchange_state().order(order.order_id)
    assert order
    assert order.remaining == Decimal(response['remaining_amount'])
    assert order.filled == Decimal(response['executed_amount'])
    assert order.price == Decimal(response['price'])
    assert order.status == exchanges.Order.Status.CANCELLED
    # The action should be marked as successful.
    assert cancel_action.status == exchanges.Action.Status.SUCCESS

    # Test that an exception is thrown if there is no matching cancel action.
    exchange._cancel_actions = {}
    with pytest.raises(Exception):
        exchange._handle_orders(response)

    # Test that an exception is thrown if no matching order is found.
    exchange._cancel_actions = {order.order_id: cancel_action}
    response['order_id'] = 10
    with pytest.raises(Exception):
        exchange._handle_orders(response)


@pytest.fixture
def cancel_rejected_response():
    amount = "5"
    price = "721.24"
    order_id = 6425

    order = exchanges.Order()
    order.order_id = order_id
    order.side = exchanges.Order.Side.BID
    order.type = exchanges.Order.Type.LIMIT
    order.price = Decimal(price)
    order.amount = Decimal(amount)
    order.remaining = Decimal(amount)
    order.filled = Decimal(0)
    order.status = exchanges.Order.Status.OPEN

    cancelled_action = exchanges.CancelOrder("gemini", order_id)

    test_response = {
      "type": "cancel_rejected",
      "order_id": order_id,
      "event_id": "6434",
      "cancel_command_id": "6433",
      "reason": "IneligibleTiming",
      "api_session": "UI",
      "symbol": "btcusd",
      "side": "buy",
      "order_type": "auction-only limit",
      "timestamp": "1478204773",
      "timestampms": 1478204773113,
      "is_live": True,
      "is_cancelled": False,
      "is_hidden": True,
      "avg_execution_price": "0.00",
      "executed_amount": "0",
      "remaining_amount": amount,
      "original_amount": amount,
      "price": price,
      "socket_sequence": 312300
    }
    return test_response, order, cancelled_action


def test_process_cancel_rejected(cancel_rejected_response):
    response = cancel_rejected_response[0]
    order = cancel_rejected_response[1]
    cancel_action = cancel_rejected_response[2]

    # Setup
    exchange = GeminiExchange()
    exchange.exchange_state().set_order(order.order_id, order)
    exchange._cancel_actions = {order.order_id: cancel_action}

    # Action
    exchange._handle_orders(response)

    # Test that the order values are correct. It should still be open.
    order = exchange.exchange_state().order(order.order_id)
    assert order
    assert order.remaining == Decimal(response['remaining_amount'])
    assert order.filled == Decimal(response['executed_amount'])
    assert order.price == Decimal(response['price'])
    assert order.status == exchanges.Order.Status.OPEN
    # The cancelled action should be marked as failed.
    assert cancel_action.status == exchanges.Action.Status.FAILED

    # Test that an exception is thrown if no matching order is found.
    exchange._cancel_actions = {order.order_id: cancel_action}
    response['order_id'] = 10
    with pytest.raises(Exception):
        exchange._handle_orders(response)


@pytest.fixture
def closed_response():

    amount = "785.020886"
    remaining = "0"
    price = "0.01514"
    order_id = 556309

    order = exchanges.Order()
    order.order_id = order_id
    order.side = exchanges.Order.Side.ASK
    order.type = exchanges.Order.Type.LIMIT
    order.price = Decimal(price)
    order.amount = Decimal(amount)
    order.remaining = Decimal(remaining)
    order.filled = Decimal(0)
    order.status = exchanges.Order.Status.OPEN

    test_response = {
        "type": "closed",
        "order_id": order_id,
        "event_id": "557345",
        "api_session": "UI",
        # The example originally used ethbtc. Changed to btcusd:
        #"symbol": "ethbtc",
        "symbol": "btcusd",
        "side": "sell",
        "order_type": "exchange limit",
        "timestamp": "1478729298",
        # Another possible typo/outdated example:
        #"timestampMs": 1478729298197,
        # Corrected:
        "timestampms": 1478729298197,
        "is_live": False,
        "is_cancelled": False,
        "is_hidden": False,
        "avg_execution_price": "0.01514",
        "executed_amount": amount,
        "remaining_amount": remaining,
        "original_amount": amount,
        "price": price,
        "socket_sequence": 126296
    }

    return test_response, order


def test_process_closed(closed_response):
    response = closed_response[0]
    order = closed_response[1]

    # Setup
    exchange = GeminiExchange()
    exchange.exchange_state().set_order(order.order_id, order)

    # Action
    exchange._handle_orders(response)

    # Test
    # Test that the order has been marked as closed.
    order = exchange.exchange_state().order(order.order_id)
    assert order
    assert order.status == exchanges.Order.Status.CLOSED

    # Test that an exception is thrown if no matching order is found.
    response['order_id'] = 10
    with pytest.raises(Exception):
        exchange._handle_orders(response)


def test_unexpected_response_type(closed_response):
    # Setup.
    exchange = GeminiExchange()
    # Change the response type of the (valid) closed response.
    response = closed_response[0]
    response["type"] = "stolen"

    # Action
    with pytest.raises(Exception) as excinfo:
        exchange._handle_orders(response)
    assert 'Unexpected' in str(excinfo.value)

