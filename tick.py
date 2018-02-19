import logging
import time
import json

import pysher
from bitstamp import BitstampFeed

formatter = logging.Formatter('%(message)s')

coins  = ['bch', 'btc', 'eth', 'ltc', 'xrp']

fhs = {}
lgs = {}
for coin in coins:
        fh = logging.FileHandler('tick/' + coin + '.log', mode='a')
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        fhs[coin] = fh

        lg = logging.getLogger(coin)
        lg.addHandler(fh)
        lg.setLevel(logging.DEBUG)
        lgs[coin] = lg

fh = logging.FileHandler('bitstamp.log', mode='a')
fh.setLevel(logging.WARNING)
fh.setFormatter(logging.Formatter('%(asctime)s %(message)s'))

log = logging.getLogger('pysher')
log.addHandler(fh)


def main():
    bs = BitstampFeed()

    for coin in coins:
        bs.onTrade(coin + 'usd', lambda x: lgs[coin].info(x))

    connection = bs.pusher.connection

    while connection.is_alive():
        time.sleep(10)

    log.error('Thread disconnected')

if __name__ == '__main__':
    main()
