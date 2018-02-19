import logging
import time
import json
import itertools

import pysher
from bitstamp import BitstampFeed

formatter = logging.Formatter('%(message)s')

coins  = ['bch', 'btc', 'eth', 'ltc', 'xrp']
events = ['create', 'delete', 'change']

fhs = {}
lgs = {}
for coin in coins:
    for event in events:
        name = coin + event

        fh = logging.FileHandler('order-diff/' + name + '.log', mode='a')
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        fhs[name] = fh

        lg = logging.getLogger(name)
        lg.addHandler(fh)
        lg.setLevel(logging.DEBUG)
        lgs[name] = lg


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
