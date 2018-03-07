import time
import os
import logging
from contextlib import contextmanager

import pysher


formatter = logging.Formatter('%(message)s')


def setup_file(name):
    directory = os.path.dirname(name)
    if not os.path.exists(directory) and directory != '':
        os.makedirs(directory, exist_ok=True)

    fh = logging.FileHandler(name, mode='a')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    return fh


def setup_logger(name, fh):
    lg = logging.getLogger(name)
    lg.addHandler(fh)
    lg.setLevel(logging.DEBUG)
    return lg


def setup_log_and_file(name):
    fh = setup_file(name)
    lg = setup_logger(name, fh)
    return fh, lg


@contextmanager
def connect():
    try:
        exchange = BitstampFeed()
        exchange.connect()
        yield exchange
    finally:
        exchange.close()


class BitstampFeed:

    def __init__(self):
        api_key = 'de504dc5763aeef9ff52'
        self.pusher = pysher.Pusher(api_key, auto_sub=True)
        self.pusher.connection.needs_reconnect = True


    def connect(self):
        self.pusher.connect()

        # @Incomplete properly detect connection establishment
        time.sleep(2)


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