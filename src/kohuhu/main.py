import asyncio
import time
import datetime


class AsyncCounter:
    count = 0
    ready = asyncio.Event()

    async def count_to_ten(self):
        for i in range(10):
            await asyncio.sleep(1)
            self.count += 1

            # We consider ourselves ready at 3
            if self.count == 3:
                self.ready.set()


# -- Main 'thread' enters here.
async_counter = AsyncCounter()

# -- Main 'thread' schedules this future to be run when, and if, we run the default event loop.
asyncio.ensure_future(async_counter.count_to_ten())
print(f"Time after scheduling count: {datetime.datetime.now()}")

time.sleep(5)
print(f"Time after main sleep: {datetime.datetime.now()}")

# -- run_until_complete() triggers stuff in the event loop and 'blocks' the main 'thread' until
#    the passed parameter returns.
asyncio.get_event_loop().run_until_complete(async_counter.ready.wait())

print(f"Time after event_loop block: {datetime.datetime.now()}")
print(f"Count: {async_counter.count}")


