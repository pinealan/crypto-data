import logging
import time
import json

import pysher
from bitstamp import connect, setup_log_and_file


coins  = ['bch', 'btc', 'eth', 'ltc', 'xrp']

fhs = {}
lgs = {}
for coin in coins:
    fh, lg = setup_log_and_file('tick/' + coin + '.log')
    fhs[coin] = fh
    lgs[coin] = lg

f = logging.FileHandler('bitstamp.log', mode='w')
f.setLevel(logging.WARNING)
f.setFormatter(logging.Formatter('%(asctime)s %(message)s'))

log = logging.getLogger('pysher')
log.addHandler(f)


# Named function is required for loop, otherwise lambdas will be discarded
def log_info(logger):
    def log(x):
        logger.info(x)
    return log


def main():
    with connect() as conn:
        for coin in coins:
            conn.onTrade(coin + 'usd', log_info(lgs[coin]))
        while True:
            time.sleep(5)

    # Log disconnection
    log.error('Thread disconnected')


if __name__ == '__main__':
    main()
