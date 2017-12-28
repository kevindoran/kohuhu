from kohuhu.gemini import GeminiExchange
import pytest


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
    """Test that GeminiExchange handles an expected heartbeat response."""
    exchange = GeminiExchange()

    socket_info = GeminiExchange.SocketInfo()
    socket_info.seq = heartbeat_response['socket_sequence']

    exchange._process_heartbeat(heartbeat_response, socket_info)
    assert socket_info.seq == heartbeat_response['socket_sequence'] + 1
    assert socket_info.heartbeat_seq == heartbeat_response['sequence'] + 1
    assert socket_info.heartbeat_timestamp_ms == \
           heartbeat_response['timestampms']


def test_raise_on_bad_heartbeat(heartbeat_response):
    """Test that GeminiExchange handles an unexpected heartbeat response."""
    exchange = GeminiExchange()
    socket_info = GeminiExchange.SocketInfo()
    socket_info.seq = heartbeat_response['socket_sequence']
    # Expect the wrong heartbeat sequence:
    socket_info.heartbeat_seq = heartbeat_response['sequence'] + 1
    with pytest.raises(Exception):
        exchange._process_heartbeat(heartbeat_response, socket_info)


