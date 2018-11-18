import os
import sys
import time
import json
import logging
from functools import partial

from feed import bitstamp
from datasink import Datasink, log_to_stdout


def write_tick_to_sink(record, sink):
    rec = json.loads(record)
    fields = ['id', 'price', 'amount', 'timestamp']
    msg = ','.join([str(rec[field]) for field in fields])
    sink.write(msg)


def main(
        *,
        root='cryptle-exchange/bitstamp-tick',
        pairs=('btcusd', 'bchusd', 'ethusd', 'xrpusd'),
        resolution=Datasink.MINUTE,
        backend='os'
    ):
    header = ['id', 'price', 'amount', 'time']
    header = ','.join(header)
    ext    = 'csv'

    # Prepare sinks
    sinks = {}
    for pair in pairs:
        sinks[pair] = Datasink(
            root='/'.join([root, pair]),
            ext=ext,
            header=header,
            resolution=resolution,
            backend=backend,
        )

    conn = bitstamp.BitstampFeed()
    conn.connect()

    for pair in pairs:
        conn.onTrade(pair, partial(write_tick_to_sink, sink=sinks[pair]))

    while True:
        try:
            while conn.is_connected():
                time.sleep(0.2)
        except ConnectionError:
            # reconnect
            conn.connect()
        except KeyboardInterrupt:
            print('Terminating...')
            conn.close()
            return 0
        except Exception:
            logging.error('Uncaught exception %s', e)
            break


if __name__ == '__main__':
    config = {}
    if os.path.isfile('tick.conf'):
        with open('tick.conf') as f:
            for line in f:
                name, var = line.partition('=')[::2]
                config[name.strip()] = var.strip()
    logging.basicConfig(level=logging.INFO)
    sys.exit(main(**config))
