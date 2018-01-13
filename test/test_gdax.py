import pytest
import asyncio
import json
from kohuhu.gdax import GdaxExchange
from test.common import wait_until
from decimal import Decimal
import decimal
import kohuhu.credentials as credentials

credentials.load_credentials()


@pytest.fixture()
def heartbeat_response():
    return {
        "type": "heartbeat",
        "sequence": 90,
        "last_trade_id": 20,
        "product_id": "BTC-USD",
        "time": "2014-11-07T08:19:28.464459Z"
    }


@pytest.fixture()
def empty_l2_update_response():
    return{
        "type": "l2update",
        "product_id": "BTC-USD",
        "changes": []
    }

@pytest.fixture
async def gdax_exchange():
    gdax = GdaxExchange(credentials.credentials_for("gdax_sandbox"))
    listen_websocket_task = asyncio.ensure_future(gdax._process_websocket_messages())
    yield gdax

    # Clean up
    listen_websocket_task.cancel()

@pytest.mark.asyncio
async def test_handle_heartbeat(gdax_exchange, heartbeat_response, empty_l2_update_response):
    # -- Setup --
    gdax = gdax_exchange

    # -- Action --
    gdax._handle_heartbeat(heartbeat_response)

    # -- Check --
    assert gdax._last_sequence_number == heartbeat_response["sequence"]

    # -- Action --
    new_message_count = 15
    for i in range(new_message_count):
        await gdax._message_queue.put(json.dumps(empty_l2_update_response))

    await wait_until(lambda: gdax._message_queue.qsize() == 0)

    # -- Check --
    expected_err_msg = f"Heartbeat sequence number increase of {0} did not match " \
                       f"internal count of websocket messages of {new_message_count}"
    with pytest.raises(Exception) as ex:
        gdax._handle_heartbeat(heartbeat_response)
    assert str(ex.value) == expected_err_msg


@pytest.fixture
def balance_response():
    response = [
        {
            "id": "71452118-efc7-4cc4-8780-a5e22d4baa53",
            "currency": "BTC",
            "balance": "1.0700000000000000",
            "available": "0.5001000000000000",
            "hold": "0.05069900000000000",
            "profile_id": "75da88c5-05bf-4f54-bc85-5c775bd68254"
        },
        {
            "id": "e316cb9a-0808-4fd7-8914-97829c1925de",
            "currency": "USD",
            "balance": "80.2301373066930000",
            "available": "79.2266348066930000",
            "hold": "1.0035025000000000",
            "profile_id": "75da88c5-05bf-4f54-bc85-5c775bd68254"
        }
    ]
    return response


def test_handle_balance(balance_response):
    credentials.load_credentials("api_credentials.json.example")
    gdax_client = GdaxExchange(
        api_credentials=credentials.credentials_for("gdax_sandbox"))
    assert gdax_client.exchange_state.balance().free("BTC") == Decimal(0)
    assert gdax_client.exchange_state.balance().on_hold("BTC") == Decimal(0)
    assert gdax_client.exchange_state.balance().free("USD") == Decimal(0)
    assert gdax_client.exchange_state.balance().on_hold("USD") == Decimal(0)
    gdax_client._update_balance_from_response(balance_response)
    assert gdax_client.exchange_state.balance().free("BTC") == \
           Decimal("0.5001")
    assert gdax_client.exchange_state.balance().on_hold("BTC") == \
           Decimal("0.050699")
    assert gdax_client.exchange_state.balance().free("USD") == \
           Decimal("79.226634806693")
    assert gdax_client.exchange_state.balance().on_hold("USD") == \
           Decimal("1.0035025")



