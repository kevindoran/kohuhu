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
    to carry out any actions. This class is given data and should return
    actions. This allows testing code to run the algorithm on fake data and to
    check the behaviour of the actions without having them run.
    """
    def __init__(self):
        pass

    def initialize(self, state, timer, add_action_callback):
        raise NotImplementedError("Subclasses should implement this method.")


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
    actions = Queue()
    algo.initialize(exchanges, DummyTimer(), actions.put)
    # Then edit the state here.
    # Then check if actions were correct. They should be in the actions queue.
    """
    def __init__(self, algorithm=None, exchanges=None):
        if not exchanges:
            exchanges = []
        self.state = State()
        self.exchanges = exchanges
        self.action_queue = asyncio.Queue()
        self._algorithm = algorithm
        self._timer = Timer()
        self._loop = None
        self._tasks = []

    def initialize(self):
        """Calls initialize on the algorithm."""
        if self._algorithm:
            self._algorithm.initialize(self.state, self._timer,
                                       self._add_action)

    def _add_action(self, action):
        """Adds an action to the action queue."""
        self.action_queue.put_nowait(action)

    def log_updates(self):
        print("Update received.")
        # TODO: How to print with logging?
        #log.info("Update received.")

    async def _process_actions(self):
        """Call exchange_client.execute() for each action created by the algo.
        """
        while True:
            action = await self.action_queue.get()
            executed = False
            for exchange_client in self.exchanges:
                if action.exchange == exchange_client.exchange_id():
                    exchange_client.execute(action)
                    executed = True
                    break
            if not executed:
                raise Exception("An action was created for an exchange that "
                                "does not have a matching exchange client. "
                                f"Action: {action}")

    def start(self):
        """Starts the trader.

        When testing, this method doesn't need to be called, and the data can be
        set directly.
        """
        for e in self.exchanges:
            self.state.add_exchange(e.exchange_state)
            e.exchange_state.update_publisher.add_callback(self.log_updates)
            run_task = e.run_task()
            self._tasks.append(run_task)

        self._tasks.extend(self._timer.tasks)
        self._tasks.append(asyncio.ensure_future(self._process_actions()))
        self._loop = asyncio.get_event_loop()
        try:
            # Run the tasks. If everything works well, this will run forever.
            finished, pending = self._loop.run_until_complete(
            asyncio.wait(self._tasks, return_when=asyncio.FIRST_EXCEPTION))

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
                self._loop.run_until_complete(asyncio.wait(pending, timeout=2))
            try:
                # They most likely finished because we told them to cancel, when
                # we observe them we'll catch the asyncio.CancelledError.
                for task in finished_cancelled_tasks:
                    task.result()

                # If a task is still pending it hasn't finished cleaning up in
                # the timeout period and you'll see:
                #   "Task was destroyed but it is pending."
                # as we forcefully kill it.
            except asyncio.CancelledError as ex:
                log.exception(ex)
                # If a task does not have an outer try..except that catches
                # CancelledError then t.result() will raise a CancelledError.
                # This is fine.
        finally:
            self._loop.stop()
            self._loop.close()


if __name__ == "__main__":
    from kohuhu.gemini import GeminiExchange
    import kohuhu.credentials as credentials
    credentials.load_credentials()
    # Start the trader without any algorithm set.
    trader = Trader(algorithm=None, exchanges=[GeminiExchange(sandbox=True)])
    trader.start()