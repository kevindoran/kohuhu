from decimal import Decimal
import ccxt
import kohuhu.credentials

# Random comments:
# Note: it looks like XBT is the newest symbol for Bitcoin markets.
# Hmmm...maybe we don't need the market call.
# btc_market = exchange(exchange_id).market('BTC/USD')

# Utils

def fee_as_factor(fee_percent):
    """Convert a fee to it's effect as a factor.

    For example: 0.008 (0.8%) gets converted to 0.9259 (4sf)
    Note: 8% should be inputted as 0.08.
    """
    return Decimal(1) / (Decimal(1) + fee_percent)


def fee_as_percentage(fee_factor):
    """Convert a fee represented as a factor back to a percentage. """
    return (Decimal(1) / fee_factor) - Decimal(1)


# Exchange methods.

_exchanges = {}
kohuhu.credentials.load_credentials()


def load_exchange(id, with_authorization=False):
    # Inspired from: https://github.com/ccxt/ccxt/issues/369
    base_id = id
    if id.endswith("_sandbox"):
        is_sandbox = True
        base_id = id[0:-len("_sandbox")]
    else:
        is_sandbox = False
    exchange = getattr(ccxt, base_id)()
    cred = kohuhu.credentials.credentials_for(id)
    if with_authorization and not cred:
        raise Exception("Failed to authorize access to exchange {}.".format(id))
    # Note: the following if could be: if credentials, if we want to authorize
    # by default, if available.
    if with_authorization:
        cred.authorize(exchange)
    exchange.load_markets()
    _exchanges[id] = exchange


def exchange(id):
    if id not in _exchanges:
        load_exchange(id)
    return _exchanges[id]


def btc_market_spread(exchange_id):
    """Returns the BTC/USD market spread of the exchange.
    :return: a MarketSpread object.
    """
    # We will likely need to some currency conversion here for when we deal with
    # NZ markets.
    # Reference: https://github.com/ccxt/ccxt/wiki/Manual#market-price
    order_book_top = exchange(exchange_id).fetch_order_book('BTC/USD',
                                                            {'depth': 1})
    highest_bid = order_book_top['bids'][0]
    lowest_ask = order_book_top['asks'][0]
    highest_bid_price, highest_bid_amount = highest_bid if highest_bid else \
        (None, None)
    lowest_ask_price, lowest_ask_amount = lowest_ask if lowest_ask else \
        (None, None)
    spread =  MarketSpread(
                    exchange_id,
                    highest_bid=Order(highest_bid_price, highest_bid_amount),
                    lowest_ask=Order(lowest_ask_price, lowest_ask_amount))
    return spread


def fees(exchange_id):
    # Getting the maker taker fees is exchange dependent.
    if exchange_id == 'gdax':
        gdax = exchange(exchange_id)
        fees = gdax.fees['trading']
        maker_fee = fees['maker']
        taker_fee = fees['taker']
    elif exchange_id == 'gemini':
        # Some hard-coding, as Gemini doesn't have their fees exposed in their
        # API (thus, isn't in ccxt). Their fees are dynamic. But unless we start
        # trading very large amounts, the fees are (as of 2017-12-17):
        maker_fee = 0.0025
        taker_fee = 0.0025
    else:
        raise Exception("The fees for the exchange: {} are unknown.".format(
            exchange_id))
    return Fees(maker_fee, taker_fee)


class Fees:
    def __init__(self, maker, taker):
        self.maker = maker
        self.taker = taker


class Order:
    def __init__(self, price, amount):
        self.price = price
        self.amount = amount


class MarketSpread:
    def __init__(self, exchange_id, highest_bid, lowest_ask):
            self.exchange_id = exchange_id
            self.highest_bid = highest_bid
            self.lowest_ask = lowest_ask
