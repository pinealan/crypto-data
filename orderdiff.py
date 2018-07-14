import logging
import time
import json
import itertools

import pysher
from bitstamp import connect, setup_log_and_file

formatter = logging.Formatter('%(message)s')

coins  = ['bch', 'btc', 'eth', 'ltc', 'xrp']

fhs = {}
lgs = {}
for coin in coins:
    fh, lg = setup_log_and_file('/mnt/orderdiff/' + coin + '.log')
    fhs[coin] = fh
    lgs[coin] = lg

f = logging.FileHandler('bitstamp.log', mode='w')
f.setLevel(logging.WARNING)
f.setFormatter(logging.Formatter('%(asctime)s %(message)s'))

log = logging.getLogger('pysher')
log.addHandler(f)


# Named function is required for loop, otherwise lambdas will be discarded
def log_info(logger, diff_type):
    def log(x):
        logger.info(x[:-1] + ', "type": "' + diff_type + '"}')
    return log


def main():
    with connect() as conn:
        for coin in coins:
            conn.onCreate(coin + 'usd', log_info(lgs[coin], 'create'))
            conn.onDelete(coin + 'usd', log_info(lgs[coin], 'delete'))
            conn.onChange(coin + 'usd', log_info(lgs[coin], 'change'))
        while True:
            time.sleep(5)

    # Log disconnection
    log.error('Thread disconnected')


if __name__ == '__main__':
    main()
