import datetime
import time
import asyncio
from decimal import Decimal
from kohuhu.gdax import GdaxExchange
from kohuhu.gemini import GeminiExchange
from kohuhu.exchanges_old import fee_as_factor


class GdaxGeminiArbitrageLogger:
    """Writes the current Gdax & Gemini best bids, asks, and post-fee inter-exchange spread
    to a csv file.

    By supplying the on_update method of this instance to the Gemini & Gdax exchange publish
    callbacks, you will get a printout of the best bids and asks from Gdax & Gemini every time
    the best bid or ask of either exchange changes in price and results in a profitable arbitrage
    opportunity after fees.

    Parameters:
            gdax (GdaxExchange): The Gdax Exchange.
            gemini (GeminiExchange): The Gemini Exchange.
            csv_dir: The directory to write the csv file into.
            log_every_spread_update (bool): Whether to log every change, even if not profitable.
            log_every_minute (bool): Whether to log every minute, even if not profitable.
    """
    def __init__(self, gdax, gemini, csv_dir, log_every_spread_update=False, log_every_minute=True):
        self._gdax = gdax
        self._gemini = gemini
        csv_filename = f'gdax_gemini_bids_asks_{int(time.time())}.csv'
        self._csv_path = f'{csv_dir}/{csv_filename}'
        self._log_every_spread_update = log_every_spread_update
        self._log_every_minute = log_every_minute

        self._gdax_best_bid_price = None
        self._gdax_best_ask_price = None
        self._gemini_best_bid_price = None
        self._gemini_best_ask_price = None

        self._last_log_time = None

        with open(self._csv_path, 'w') as file:
            file.write(
                "timestamp,"
                "gdax_best_bid_price,gdax_best_bid_quantity,"
                "gdax_best_ask_price,gdax_best_ask_quantity,"
                "gemini_best_bid_price,gemini_best_bid_quantity,"
                "gemini_best_ask_price,gemini_best_ask_quantity,"
                "sell_gdax_buy_gemini_spread_after_fees, sell_gemini_buy_gdax_spread_after_fees\n")

    def on_update(self):
        gdax_bids = self._gdax.exchange_state.order_book().bids()
        gdax_asks = self._gdax.exchange_state.order_book().asks()
        gdax_best_bid = gdax_bids[0]
        gdax_best_ask = gdax_asks[0]

        gemini_bids = self._gemini.exchange_state.order_book().bids()
        gemini_asks = self._gemini.exchange_state.order_book().asks()
        gemini_best_bid = gemini_bids[0]
        gemini_best_ask = gemini_asks[0]

        # If no change to the best prices, simply return.
        if gdax_best_bid.price == self._gdax_best_bid_price and \
                gdax_best_ask.price == self._gdax_best_ask_price and \
                gemini_best_bid.price == self._gemini_best_bid_price and \
                gemini_best_ask.price == self._gemini_best_ask_price:
            # No update to our best bid or asks.
            return

        # Determine the effective spread
        fee = Decimal("0.0025")  # Taker fee for both exchanges
        fee_factor = fee_as_factor(fee)
        effective_gdax_best_bid_price = gdax_best_bid.price * fee_factor
        effective_gdax_best_ask_price = gdax_best_ask.price / fee_factor
        effective_gemini_best_bid_price = gemini_best_bid.price * fee_factor
        effective_gemini_best_ask_price = gemini_best_ask.price / fee_factor

        effective_sell_gdax_buy_gemini_spread = effective_gdax_best_bid_price - effective_gemini_best_ask_price
        effective_sell_gemini_buy_gdax_spread = effective_gemini_best_bid_price - effective_gdax_best_ask_price
        effective_sell_gdax_buy_gemini_spread = effective_sell_gdax_buy_gemini_spread.quantize(Decimal('.01'))
        effective_sell_gemini_buy_gdax_spread = effective_sell_gemini_buy_gdax_spread.quantize(Decimal('.01'))

        # Check for the forced minute update
        current_time = datetime.datetime.utcnow()
        should_do_minute_log = self._log_every_minute and (
                self._last_log_time is None or
                current_time - self._last_log_time > datetime.timedelta(minutes=1))

        if self._log_every_spread_update or \
           should_do_minute_log or \
           effective_sell_gdax_buy_gemini_spread > 0 or \
           effective_sell_gemini_buy_gdax_spread > 0:
            # Write the results
            with open(self._csv_path, 'a') as file:
                file.write(
                    f"{datetime.datetime.utcnow()},"
                    f"{gdax_best_bid.price},{gdax_best_bid.quantity},"
                    f"{gdax_best_ask.price},{gdax_best_ask.quantity},"
                    f"{gemini_best_bid.price},{gemini_best_ask.quantity},"
                    f"{gemini_best_ask.price},{gemini_best_ask.quantity},"
                    f"{effective_sell_gdax_buy_gemini_spread},{effective_sell_gemini_buy_gdax_spread}\n")
            self._last_log_time = current_time

        # Update our last best bids/asks
        self._gdax_best_bid_price = gdax_best_bid.price
        self._gdax_best_ask_price = gdax_best_ask.price
        self._gemini_best_bid_price = gemini_best_bid.price
        self._gemini_best_ask_price = gemini_best_ask.price


def main():
    # Create exchanges
    gdax = GdaxExchange()
    gemini = GeminiExchange()

    # Start our exchanges
    loop = asyncio.get_event_loop()
    tasks = asyncio.gather(gdax.run_task(), gemini.run_task())

    # Wait until they are both ready
    loop.run_until_complete(gdax.order_book_ready.wait())
    loop.run_until_complete(gemini.setup_event())

    # Register the callbacks
    bid_ask_spread_logger = GdaxGeminiArbitrageLogger(gdax=gdax, gemini=gemini,
                                                      csv_dir='.',
                                                      log_every_spread_update=False,
                                                      log_every_minute=True)
    gdax.set_on_change_callback(bid_ask_spread_logger.on_update)
    gemini.exchange_state.update_publisher.add_callback(bid_ask_spread_logger.on_update)

    # Run forever
    loop.run_until_complete(tasks)


if __name__ == "__main__":
    main()
