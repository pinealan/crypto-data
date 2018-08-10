import time
import json
from functools import partial

import bitstamp
from datasink import Datasink


def write_tick_to_sink(record, sink):
    rec = json.loads(record)
    fields = ['id', 'price', 'amount', 'timestamp']
    msg = ','.join([str(rec[field]) for field in fields])
    sink.write(msg)


def main(*, root, pairs, resolution=Datasink.DAY, backend=None):
    header = ['id', 'price', 'amount', 'time']
    header = ','.join(header)
    ext    = '.csv'

    # Prepare sinks
    sinks = {}
    for pair in pairs:
        sinks[pair] = Datasink(
            root='/'.join([root, pair]),
            ext=ext,
            header=header,
            resolution=resolution,
            backend=backend)

    with bitstamp.connect() as conn:
        for pair in pairs:
            conn.onTrade(pair, partial(write_tick_to_sink, sink=sinks[pair]))
        while conn.is_connected():
            time.sleep(5)


if __name__ == '__main__':
    config = {
        'pairs': ['bchusd', 'btcusd', 'ethusd', 'ltcusd', 'xrpusd']
    }
    with open('tick.conf') as f:
        for line in f:
            name, var = line.partition('=')[::2]
            config[name.strip()] = var.strip()
    main(**config)
