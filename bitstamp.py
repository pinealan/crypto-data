import time
from contextlib import contextmanager

import pysher


def setup_file_handler(name):
    fh = logging.FileHandler(name + '.log', mode='a')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    return fh


def setup_logger(name, fh):
    lg = logging.getLogger(name)
    lg.addHandler(fh)
    lg.setLevel(logging.DEBUG)
    return lg


@contextmanager
def connect():
    try:
        exchange = BitstampFeed()
        yield exchange
    finally:
        exchange.close()


class BitstampFeed:

    def __init__(self):
        api_key = 'de504dc5763aeef9ff52'
        self.pusher = pysher.Pusher(api_key, auto_sub=True)
        self.pusher.connection.needs_reconnect = True
        time.sleep(2)


    def connect(self):
        self.pusher.connect()


    def close(self):
        self.pusher.disconnect()


    def onTrade(self, pair, callback):
        if pair == 'btcusd':
            self._bindSocket('live_trades', 'trade', callback)
        else:
            self._bindSocket('live_trades_' + pair, 'trade', callback)


    def onCreate(self, pair, callback):
        if pair == 'btcusd':
            self._bindSocket('live_orders', 'order_created', callback)
        else:
            self._bindSocket('live_orders_' + pair, 'order_created', callback)


    def onDelete(self, pair, callback):
        if pair == 'btcusd':
            self._bindSocket('live_orders', 'order_deleted', callback)
        else:
            self._bindSocket('live_orders_' + pair, 'order_deleted', callback)


    def onChange(self, pair, callback):
        if pair == 'btcusd':
            self._bindSocket('live_orders', 'order_changed', callback)
        else:
            self._bindSocket('live_orders_' + pair, 'order_changed', callback)


    def onOrderbook(self, pair, callback):
        if pair == 'btcusd':
            self._bindSocket('order_book', 'data', callback)
        else:
            self._bindSocket('order_book_' + pair, 'data', callback)


    def _bindSocket(self,  channel_name, event, callback):

        if channel_name not in self.pusher.channels:
            self.pusher.subscribe(channel_name)

        channel = self.pusher.channels[channel_name]
        channel.bind(event, callback)
