import pytest
import asyncio
import json
from kohuhu.gdax import GdaxExchange
from test.common import wait_until


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
    gdax = GdaxExchange()
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

