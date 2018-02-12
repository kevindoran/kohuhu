from enum import Enum, auto
import time
from datetime import datetime
from datetime import timedelta
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
                yield max(diff, timedelta(seconds=0))
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
    def __init__(self, algorithm, exchanges):
        self.state = State()
        self.exchanges = exchanges
        self.action_queue = asyncio.Queue()
        self._algorithm = algorithm
        self._timer = Timer()
        self._loop = None
        self._tasks = []

    def _add_action(self, action):
        """Adds an action to the action queue."""
        log.info(f"Adding action to the queue: {action}.")
        self.action_queue.put_nowait(action)

    def log_updates(self, id, description):
        log.info(f"Update received from {id}. Description: {description}.")
        # TODO: How to print with logging?
        #log.info("Update received.")

    def log_status(self, time):
        if self._algorithm:
            log.info("Trader status update:")
            log.info(self._algorithm.status_str() + "\n")
        else:
            log.info("No algorithm loaded.")

    async def _process_actions(self):
        """Call exchange_client.execute() for each action created by the algo.
        """
        while True:
            action = await self.action_queue.get()
            executed = False
            for exchange_client in self.exchanges:
                if action.exchange == exchange_client.exchange_id:
                    exchange_client.execute_action(action)
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

        self._algorithm.initialize(self.state, self._timer, self._add_action)
        self._timer.do_every(timedelta(seconds=10), self.log_status)
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


def init_loggers():
    console_output = logging.StreamHandler()
    console_output.setLevel(logging.INFO)
    console_output_formatter = logging.Formatter('%(message)s')
    console_output.setFormatter(console_output_formatter)

    file_output = logging.FileHandler('./logs/kohuhu.log')
    file_output.setLevel(logging.DEBUG)
    # Add timestamp & level to logs
    file_output_formatter = logging.Formatter('%(asctime)s:%(levelname)s: %(message)s')
    file_output.setFormatter(file_output_formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(console_output)
    root_logger.addHandler(file_output)
    root_logger.setLevel(logging.DEBUG)  # We need this so that all logs are propogated to the handlers


if __name__ == "__main__":
    init_loggers()
    log.info("Starting trader")
    from kohuhu.gemini import GeminiExchange
    from kohuhu.gdax import GdaxExchange
    from kohuhu.arbitrage import OneWayPairArbitrage
    import kohuhu.credentials as credentials
    credentials.load_credentials()
    gemini_creds = credentials.credentials_for('gemini_sandbox', owner='kevin')
    gdax_creds = credentials.credentials_for('gdax_sandbox', owner='kevin')
    # Start the trader without any algorithm set.
    algo = OneWayPairArbitrage(exchange_to_buy_on="gemini_sandbox",
                               exchange_to_sell_on="gdax_sandbox")
    trader = Trader(algorithm=algo,
        exchanges=[GeminiExchange(api_credentials=gemini_creds, sandbox=True),
                   GdaxExchange(api_credentials=gdax_creds, sandbox=True)])
    trader.start()
