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

fhs = {}
lgs = {}
for coin in coins:
        name = coin + 'order'

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


def loginfo(logger):
    def wrapper(x):
        logger.info(str(time.time()) + ': ' + x)
    return wrapper


def main():
    bs = BitstampFeed()

    for coin in coins:
        bs.onOrderbook(coin + 'usd', loginfo(lgs[coin + 'order']))

    connection = bs.pusher.connection
    time.sleep(2)


if __name__ == '__main__':
    main()
