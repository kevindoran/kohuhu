import datetime
import asyncio


async def wait_until(test, max_wait=datetime.timedelta(seconds=3)):
    """Wait until the specified test is True, for up to max_wait time."""
    start_time = datetime.datetime.now()
    while not test():
        await asyncio.sleep(1)
        if (datetime.datetime.now() - start_time) > max_wait:
            return False
    return True
