import os
import time
import json
import logging
from functools import partial

from feed import bitstamp
from datasink import Datasink, log_to_stdout


def record_diff(record, diff_type, sink):
    rec = json.loads(record)
    rec['diff_type'] = diff_type

    # Beware of unannounced Bitstamp API changes and with no updates to docs
    fields = ['id', 'price', 'amount', 'order_type', 'diff_type', 'microtimestamp']
    msg = ','.join([str(rec[field]) for field in fields])
    sink.write(msg)
    return


def main(
        *,
        root='cryptle-exchange/bitstamp-diff',
        pairs=('btcusd', 'bchusd', 'ethusd', 'xrpusd'),
        resolution=Datasink.MINUTE,
        backend='os'
    ):
    # Use csv header
    header = ['time', 'id', 'price', 'volume', 'order_type', 'diff_type', 'src_time']
    header = ','.join(header)
    ext    = 'csv'

    log_to_stdout(level=logging.DEBUG)

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

    conn = bitstamp.Bitstamp()
    conn.connect()

    for pair in pairs:
        conn.onCreate(pair, partial(record_diff, diff_type='create', sink=sinks[pair]))
        conn.onDelete(pair, partial(record_diff, diff_type='delete', sink=sinks[pair]))
        conn.onChange(pair, partial(record_diff, diff_type='take', sink=sinks[pair]))

    while True:
        try:
            while conn.connected():
                time.sleep(0.2)
        except ConnectionError:
            # reconnect
            conn.connect()
        except KeyboardInterrupt:
            print('Terminating...')
            conn.disconnect()
            return 0


if __name__ == '__main__':
    config = {}
    if os.path.isfile('diff.conf'):
        with open('diff.conf') as f:
            for line in f:
                name, var = line.partition('=')[::2]
                config[name.strip()] = var.strip()
    sys.exit(main(**config))
