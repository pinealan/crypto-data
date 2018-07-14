from functools import partial
from datetime import datetime, date
from pathlib import Path
import json
import time

import pysher
import bitstamp


# Absolute paths are prefered. Relative paths will be relative to the script.
DATA_DIR = 'orderdiff'


class DataSink:
    """Abstraction over file management for data curation.

    A directory is created to place all collected data. A date based directory
    hierachy will be used to organise the files. By default the file resolution
    is with day, i.e.

        dataset/
        |-- 2017/
        |   |-- 01/
        |   |   |-- 01.ext
        |   |   |-- 02.ext
        |   |
        |   |-- 02/
        |
        |-- 2018/

    Supported resolutions are [day, month, year].

    Args:
        path: Path to data directory. Absolute paths are prefered. Relative paths will be taken
            relative to script directory.
        ext: (optional) file extension for each file
        header: (optional) string to be put at the top of each file
        footer: (optional) string to be put at the bottom of each file
        timestamp: (optional) Toggle to prepend timestamp to the data records. Defaults to false.
        resolution: (optional) int values defined by the class attributes

    @Future Creates a new data file according to one of the supported strategies.
    """
    DAY   = 10
    MONTH = 20
    YEAR  = 30

    def __init__(
            self,
            path,
            ext='',
            header=None,
            footer=None,
            add_time=True,
            resolution=DAY):
        self._path = Path(path)
        self._ext = ext
        self._res = resolution
        self._header = header
        self._footer = footer
        self._add_time = add_time

        self._time = date.today()
        self._renewfile()
        self._prep()

    def write(self, msg):
        if self._add_time:
            msg = '{},{}'.format(datetime.now().timestamp(), msg)
        msg += '\n'
        self.getfile().write(msg)

    def getfile(self):
        # @Todo: Support other resolutions, only DAY supported right now
        if date.today() != self._time:
            self._time = date.today()
            self._cleanup()
            self._renewfile()
            self._prep()
            return self._file
        else:
            return self._file

    def _renewfile(self):
        # @Todo: Support other resolutions, only DAY supported right now
        p = self._path / self._time.strftime('%Y/%m')
        p.mkdir(mode=0o775, parents=True, exist_ok=True)
        p /= str(self._time.day) + self._ext

        # line buffering, assuming each write will be a line
        self._file = p.open(mode='w', buffering=1)

    def _prep(self):
        if self._header:
            self._file.write(self._header + '\n')

    def _cleanup(self):
        if self._footer:
            self._file._write(self._footer + '\n')
        self._file.close()


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
