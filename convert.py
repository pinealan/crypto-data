#!/usr/bin/python3
import sys
import logging

import numpy as np
import pandas as pd


def print_help():
    usage = [
        'Usage:',
        '   convert --from tick.csv --to candle.csv --in btctick.csv --out btccandle.csv',
        '',
        'Arguments:',
        '   --from <input_data.file_format>',
        '   --to <output_data.file_format>',
        '   --in <input_file>',
        '   --out <output_file>',
        '',
        'Options --to=candle.*',
        '   [--period <seconds>]',
    ]
    print('\n'.join(usage))


def round_down_nearest(n, precision: int):
    return (n // precision) * precision


def tick_from_csv(fname):
    return pd.read_csv(fname)


def tick_from_json(fname):
    return pd.read_json(open(fname), convert_dates=False, lines=True)


def tick_to_candle(tick: pd.DataFrame, period: int) -> pd.DataFrame:
    """Convert tick data to candle data."""
    tick = tick.set_index('timestamp') \
               .drop('buy_order_id', axis=1) \
               .drop('sell_order_id', axis=1) \
               .drop('id', axis=1) \
               .drop('price_str', axis=1) \
               .drop('amount_str', axis=1) \
               .drop('type', axis=1) \
               .sort_index()

    start = round_down_nearest(tick.index.min(), period)
    end   = round_down_nearest(tick.index.max(), period) + period
    bars  = []

    for itr in range(start, end, period):
        index     = (tick.index >= itr) & (tick.index < itr + period)
        bar_ticks = tick[index]

        if bar_ticks.empty:
            op = cl = hi = lo = bars[-1][1]
            vol = 0
        else:
            op  = bar_ticks.min().price
            cl  = bar_ticks.max().price
            hi  = bar_ticks.price.max()
            lo  = bar_ticks.price.min()
            vol = bar_ticks.amount.sum()
        ts  = itr

        bars.append((op, cl, hi, lo, vol, ts))

    return pd.DataFrame(bars, columns=['open', 'close', 'hi', 'low', 'volume', 'timestamp'])


def _tick_to_candle(tick, kwargs):
    period = kwargs['period']
    return tick_to_candle(tick, period)


def candle_to_csv(data, fname):
    data.to_csv(fname, index=False)


_read_table = {
        ('tick', 'csv'): tick_from_csv,
        ('tick', 'json'): tick_from_json,
}


_convert_table = {
        ('tick', 'candle'): _tick_to_candle,
}


_write_table = {
        ('candle', 'csv'): candle_to_csv,
}


def read_data(file, data_fmt, file_fmt):
    return _read_table[(data_fmt, file_fmt)](file)


def convert(data, in_data_fmt, out_data_fmt, kwargs):
    return _convert_table[(in_data_fmt, out_data_fmt)](data, kwargs)


def write_data(data, file, data_fmt, file_fmt):
    _write_table[(data_fmt, file_fmt)](data, file)


def parse_cmdline_args(args):
    kwargs = {}
    for kw, arg in zip(args[::2], args[1::2]):
        if kw.startswith('--'):
            if arg.isdigit():
                kwargs[kw[2:]] = int(arg)
            else:
                kwargs[kw[2:]] = arg
        else:
            logging.error("{} {}".format(kw, arg))
            raise ValueError('Arguments must be in "--key value" format')
    return kwargs


def parse_format(fmt):
    return fmt.split('.')


def main():
    kwargs = parse_cmdline_args(sys.argv[1:])
    logging.debug(kwargs)

    try:
        in_data, in_fmt = parse_format(kwargs['from'])
        out_data, out_fmt = parse_format(kwargs['to'])
        file = kwargs['in']
        out  = kwargs['out']
        data = read_data(file, in_data, in_fmt)
        data = convert(data, in_data, out_data, kwargs)
        write_data(data, out, out_data, out_fmt)
    except Exception as e:
        print(e.__repr__())
        print_help()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
