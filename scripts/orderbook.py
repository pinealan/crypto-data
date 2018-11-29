import os
import sys
import time
import json
import logging
from functools import partial

import requests as req

from feed import bitstamp
from datasink import Datasink
import datasink


CONFIG_FILE = 'book.conf'


def write_orderbook_to_sink(book, sink):
    sink.write(book)


def req_orderbook(pair):
    res = req.get('https://bitstamp.net/api/v2/order_book/{}'.format(pair))
    if res.status_code != 200:
        raise ConnectionError(res.status_code)
    return res.text


def main(
        *,
        root='cryptle-exchange/bitstamp-book',
        backend='os',
        resolution='day',
        pairs=('btcusd', 'bchusd', 'ethusd', 'xrpusd')
    ):
    ext    = 'json'
    sinks = {}
    for pair in pairs:
        sinks[pair] = Datasink(
            root='-'.join([root, pair]),
            ext=ext,
            namemode=1,
            resolution=resolution,
            backend=backend
        )

    consec_fail_count = 0

    while True:
        try:
            for pair in pairs:
                data = req_orderbook(pair)
                write_orderbook_to_sink(data, sinks[pair])
                consec_fail_count = 0  # reset counter
            time.sleep(600)
        except ConnectionError:
            consec_fail_count += 1
            if consec_fail_count < 10:
                # HTTP errors are not important so we can tolerate a number of them
                # Sleep for a bit before trying again
                time.sleep(5)
            else:
                # Connection may have degraded, wait a bit longer
                time.sleep(300)
        except KeyboardInterrupt:
            print('\rTerminating...')
            return 0
        except Exception:
            logging.error('Uncaught exception %s', e)
            return 1


if __name__ == '__main__':
    config = {}
    if os.path.isfile(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            for line in f:
                name, var = line.partition('=')[::2]
                config[name.strip()] = var.strip()
    datasink.stdout_logger()
    sys.exit(main(**config))
