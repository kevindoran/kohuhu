import pytest
import asyncio
import json
import datetime
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
    gdax = GdaxExchange(credentials.credentials_for("gdax_sandbox"),
                        sandbox=True)
    listen_websocket_task = asyncio.ensure_future(gdax._process_websocket_messages())
    yield gdax

    # Clean up
    listen_websocket_task.cancel()

@pytest.mark.asyncio
async def test_handle_heartbeat(gdax_exchange, heartbeat_response, empty_l2_update_response):
    # -- Setup --
    gdax = gdax_exchange
    heartbeat_time = datetime.datetime.strptime(heartbeat_response['time'],
                                                    "%Y-%m-%dT%H:%M:%S.%fZ")

    # -- Action --
    gdax._handle_heartbeat(heartbeat_response)

    # -- Check --
    assert gdax._last_heartbeat_time == heartbeat_time

    # -- Setup --
    # Add 1.1 second to heartbeat and check for no errors
    heartbeat_time = heartbeat_time + datetime.timedelta(seconds=1.1)
    heartbeat_response['time'] = heartbeat_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # -- Action --
    gdax._handle_heartbeat(heartbeat_response)

    # -- Check --
    assert gdax._last_heartbeat_time == heartbeat_time

    # --Setup --
    # Add 2.1 second to heartbeat and check for error
    heartbeat_time = heartbeat_time + datetime.timedelta(seconds=2.1)
    heartbeat_response['time'] = heartbeat_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # -- Action --
    with pytest.raises(Exception) as ex:
        gdax._handle_heartbeat(heartbeat_response)


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
        api_credentials=credentials.credentials_for("gdax_sandbox"),
        sandbox=True)
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



