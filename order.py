import logging
import time
import json
import itertools

import pysher

formatter = logging.Formatter('%(message)s')

create = logging.getLogger('create')
delete = logging.getLogger('delete')
change = logging.getLogger('change')

create.setLevel(logging.INFO)
delete.setLevel(logging.INFO)
change.setLevel(logging.INFO)

coins  = ['bch', 'btc', 'eth', 'ltc', 'xrp']
events = ['create', 'delete', 'change']

fhs = {}
lgs = {}
for coin in coins:
    for event in events:
        name = coin + event

        fh = logging.FileHandler(name + '.log', mode='a')
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        fhs[name] = fh

        lg = logging.getLogger(name)
        lg.addHandler(fh)
        lg.setLevel(logging.DEBUG)
        lgs[name] = lg


class BitstampFeed:

    def __init__(self):
        api_key = 'de504dc5763aeef9ff52'
        self.pusher = pysher.Pusher(api_key, auto_sub=True)
        self.pusher.connection.needs_reconnect = True
        self.pusher.connect()
        time.sleep(2)


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


    def _bindSocket(self,  channel_name, event, callback):

        if channel_name not in self.pusher.channels:
            self.pusher.subscribe(channel_name)

        channel = self.pusher.channels[channel_name]
        channel.bind(event, callback)


def loginfo(logger):
    def wrapper(x):
        logger.info(x)
    return wrapper


def main():
    bs = BitstampFeed()

    for coin in coins:
        bs.onCreate(coin + 'usd', loginfo(lgs[coin + 'create']))
        bs.onDelete(coin + 'usd', loginfo(lgs[coin + 'delete']))
        bs.onChange(coin + 'usd', loginfo(lgs[coin + 'change']))

    connection = bs.pusher.connection
    while connection.is_alive():
        time.sleep(10)

    log.error('Thread disconnected')


if __name__ == '__main__':
    main()
