from enum import Enum, auto
import time
from datetime import datetime
import asyncio
import logging
from kohuhu.exchanges import State

log = logging.getLogger(__name__)

class Algorithm:
    """Subclass Algorithm and pass it to Trader to make trades.

    In order to have flexibility to test any Algorithm subclass, it is
    important that this class doesn't make any direct request for data or
    to carry out any actions. This class should take data from the data slices
    and return actions. This allows testing code to run the algorithm on fake
    data and to check the behaviour of the actions without having them run.
    """
    def __init__(self):
        pass

    def initialize(self, state, timer, action_queue):
        raise NotImplementedError("Subclasses should implement this method.")
        # timer.do_every(timedelta(seconds=1), self.tick)


class Timer:
    """A callback scheduler.

    Inspired from SO: https://stackoverflow.com/a/28034554/754300
    """
    def __init__(self):
        self.tasks = []

    def do_every(self, period, f):
        timer_task = asyncio.ensure_future(self._do_every(period, f))
        self.tasks.append(timer_task)

    async def _do_every(self, period, f):
        def tick():
            init_time = datetime.now()
            count = 0
            while True:
                count += 1
                target_time = init_time + count*period
                diff = target_time - datetime.now()
                yield max(diff, 0)
        ticker = tick()
        for delta in tick():
            await asyncio.sleep(delta.seconds)
            # await f() ?
            f(datetime.now())


class Trader:
    """Runs algorithms.

    A real run might look like:
    algo = MyAlgo()
    gdax = GDaxExchange()
    gemini = GeminiExchange()
    trader = Trader(algo, exchanges)
    trader.initialize()
    trader.start()

    Test code might look like:
    algo = MyAlgo()
    gdax = GDaxExchange()
    gemini = GeminiExchange()
    exchanges = [gdax, gemini]
    trader = Trader(algo, exchanges)
    # Edit the state here...
    algorithm.on_tick()
    actions = trader.timer.action_queue.deque()
    # check if actions were correct.


    Attributes:
        actions: a list of action tuples. Each list entry contains the actions
            that were created by the algorithm at a certain step.
    """
    def __init__(self, algorithm, exchanges):
        self._algorithm = algorithm
        self.timer = Timer()
        self._fetchers = {}
        self.state = State()
        self.exchanges = exchanges
        self.action_queue = []
        for e in exchanges:
            self.state.add_exchange(e.state)

    def initialize(self):
        """Calls initialize on the algorithm."""
        self._algorithm.initialize(self.state, self.timer, self.action_queue)

    def start(self):
        """Starts the trader.

        When testing, this method doesn't need to be called, and the data can be
        set directly.
        """
        tasks = []
        for e in self.exchanges:
            self.state.add_exchange(e.exchange_state())
            tasks_for_exchange = e.initialize()
            tasks.append(tasks_for_exchange)

        tasks.extend(self.timer.tasks)
        loop = asyncio.get_event_loop()
        try:
            # Run the tasks. If everything works well, this will run forever.
            finished, pending = loop.run_until_complete(
            asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION))

            # If we've got here, then a task has throw an exception.

            # Finished task(s) have thrown an exception. Let's observe the task
            # to log the exception.
            for task in finished:
                try:
                    task.result()
                except Exception as ex:
                    log.exception(ex)

            # Pending tasks are still running. Gracefully cancel them all.
            for task in pending:
                task.cancel()

            # Wait for up to 2 seconds for the tasks to gracefully return.
            finished_cancelled_tasks, pending_cancelled_tasks = \
                loop.run_until_complete(asyncio.wait(pending, timeout=2))
            try:
                # They most likely finished because we told them to cancel, when
                # we observe them we'll catch the asyncio.CancelledError.
                for task in finished_cancelled_tasks:
                    task.result()

                # If a task is still pending it hasn't finished cleaning up in
                # the timeout period and you'll see:
                #   "Task was destroyed but it is pending."
                # as we forcefully kill it.
            except asyncio.CancelledError:
                pass
                # If a task does not have an outer try..except that catches
                # CancelledError then t.result() will raise a CancelledError.
                # This is fine.
        finally:
            loop.stop()
            loop.close()

    def add_action(self, action):
        self.action_queue.append(action)
