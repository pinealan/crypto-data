import os
import time
import json
import logging
from functools import partial

import requests as req

from feed import bitstamp
from datasink import Datasink, log_to_stdout


def write_orderbook_to_sink(book, sink):
    sink.write(book)


def req_orderbook(pair):
    res = req.get('https://bitstamp.net/api/v2/order_book/{}'.format(pair))
    if res.status_code != 200:
        raise ConnectionError(res.status_code)
    return res.text


def special_name():
    return str(int(time.time()))


def main(*, root, pairs, resolution=Datasink.DAY, backend=None):
    ext    = 'json'

    log_to_stdout()

    sinks = {}
    for pair in pairs:
        sinks[pair] = Datasink(
            root='/'.join([root, pair]),
            ext=ext,
            resolution=resolution,
            backend=backend,
            filename=special_name)

    while True:
        for pair in pairs:
            data = req_orderbook(pair)
            write_orderbook_to_sink(data, sinks[pair])
        time.sleep(600)


if __name__ == '__main__':
    config = {
        'root': 'cryptle-exchange/bitstamp-book',
        'backend': 'os',
        'resolution': 'day',
        'pairs': ['btcusd']
    }

    if os.path.isfile('book.conf'):
        with open('book.conf') as f:
            for line in f:
                name, var = line.partition('=')[::2]
                config[name.strip()] = var.strip()
    main(**config)
