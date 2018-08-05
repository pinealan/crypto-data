import time
import json
from functools import partial

import bitstamp
from datasink import DataSink


# Absolute paths are prefered. Relative paths will be relative to the script.
DATA_DIR = 'orderdiff'


def record_diff(record, diff_type, sink):
    rec = json.loads(record)
    rec['diff_type'] = diff_type

    # Bitstamp API changes without notice with no updates to docs
    # Be aware
    fields = ['id', 'price', 'amount', 'order_type', 'diff_type', 'microtimestamp']
    msg = ','.join([str(rec[field]) for field in fields])
    sink.write(msg)
    return


def main():
    # Configurations
    pairs  = ['bchusd', 'btcusd', 'ethusd', 'ltcusd', 'xrpusd']
    header = ['time', 'id', 'price', 'volume', 'order_type', 'diff_type', 'src_time']
    ext = '.csv'

    # Use csv header
    header = ','.join(header)

    # Prepare sinks
    sinks = {pair: DataSink(path='/'.join([DATA_DIR, pair]), ext=ext, header=header) for pair in pairs}

    with bitstamp.connect() as conn:
        for pair in pairs:
            conn.onCreate(pair, partial(record_diff, diff_type='create', sink=sinks[pair]))
            conn.onDelete(pair, partial(record_diff, diff_type='delete', sink=sinks[pair]))
            conn.onChange(pair, partial(record_diff, diff_type='take', sink=sinks[pair]))
        while conn.is_connected():
            time.sleep(2)


if __name__ == '__main__':
    main()
