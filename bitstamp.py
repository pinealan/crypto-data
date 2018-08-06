import os
import time
import logging
import contextlib

import pysher


@contextlib.contextmanager
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
        self.pusher = pysher.Pusher(api_key, auto_sub=True, log_level=logging.DEBUG)
        self.pusher.connection.needs_reconnect = True

    def is_connected(self):
        return self.pusher.connection.socket.sock is not None

    def connect(self):
        self.pusher.connect()

        # @Incomplete properly detect connection establishment
        time.sleep(2)

    def close(self):
        self.pusher.disconnect()

    @staticmethod
    def _remove_btc(pair):
        return '_' + pair if pair != 'btcusd' else ''

    def on(self, event, pair, cb):
        pair = self._remove_btc(pair)

        if event == 'order_create':
            self._bindSocket('live_orders{}'.format(pair), 'order_created', cb)
        elif event == 'order_delete':
            self._bindSocket('live_orders{}'.format(pair), 'order_deleted', cb)
        elif event == 'order_take':
            self._bindSocket('live_orders{}'.format(pair), 'order_changed', cb)
        elif event == 'trade':
            self._bindSocket('live_trades{}'.format(pair), 'trade', cb)

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
