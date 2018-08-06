import time
import json
from functools import partial

import bitstamp
from datasink import DataSink


def record_diff(record, diff_type, sink):
    rec = json.loads(record)
    rec['diff_type'] = diff_type

    # Bitstamp API changes without notice with no updates to docs
    # Be aware
    fields = ['id', 'price', 'amount', 'order_type', 'diff_type', 'microtimestamp']
    msg = ','.join([str(rec[field]) for field in fields])
    sink.write(msg)
    return


def main(*, path, pairs, resolution=DataSink.DAY, backend=None):
    # Use csv header
    header = ['time', 'id', 'price', 'volume', 'order_type', 'diff_type', 'src_time']
    header = ','.join(header)
    ext    = '.csv'

    # Prepare sinks
    sinks = {}
    for pair in pairs:
        sinks[pair] = DataSink(
            path='/'.join([path, pair]),
            ext=ext,
            header=header,
            resolution=resolution,
            backend=backend)

    with bitstamp.connect() as conn:
        for pair in pairs:
            conn.onCreate(pair, partial(record_diff, diff_type='create', sink=sinks[pair]))
            conn.onDelete(pair, partial(record_diff, diff_type='delete', sink=sinks[pair]))
            conn.onChange(pair, partial(record_diff, diff_type='take', sink=sinks[pair]))
        while conn.is_connected():
            time.sleep(2)


if __name__ == '__main__':
    # default config
    config = {
        'pairs': ['bchusd', 'btcusd', 'ethusd', 'ltcusd', 'xrpusd'],
    }
    with open('diff.conf') as f:
        for line in f:
            name, var = line.partition('=')[::2]
            config[name.strip()] = var.strip()
    main(**config)
