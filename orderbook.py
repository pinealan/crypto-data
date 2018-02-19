import logging
import time
import json
import itertools

import pysher
from bitstamp import BitstampFeed

# Logging formatter
formatter = logging.Formatter('%(message)s')

# Coins that we are going to track
coins  = ['bch', 'btc', 'eth', 'ltc', 'xrp']

fhs = {}
lgs = {}
for coin in coins:
        name = coin + 'order'

        fh = logging.FileHandler('order/' + name + '.log', mode='a')
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        fhs[name] = fh

        lg = logging.getLogger(name)
        lg.addHandler(fh)
        lg.setLevel(logging.DEBUG)
        lgs[name] = lg


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
