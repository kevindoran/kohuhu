import datetime
import asyncio
import os

async def wait_until(test, max_wait=datetime.timedelta(seconds=3)):
    """Wait until the specified test is True, for up to max_wait time."""
    start_time = datetime.datetime.now()
    while not test():
        await asyncio.sleep(1)
        if (datetime.datetime.now() - start_time) > max_wait:
            return False
    return True


def enableProxy():
    os.environ["HTTP_PROXY"] = "http://127.0.0.1:8080"
    os.environ["HTTPS_PROXY"] = "https://127.0.0.1:8080"
    # For use with BurpSuite.
    # Taken from here: https://www.th3r3p0.com/random/python-requests-and-burp-suite.html
    os.environ["REQUESTS_CA_BUNDLE"] = "/home/k/.ssh/burpsuite_cert.pem"