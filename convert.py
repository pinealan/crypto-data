#!/usr/bin/python3
import sys
import logging
import traceback

import click
import numpy as np
import pandas as pd


def round_down_nearest(n, precision: int):
    return (n // precision) * precision


def tick_from_csv(fname, **kwargs):
    return pd.read_csv(fname, **kwargs)


def _tick_from_csv(fname, **kwargs):
    header = None
    if 'header' in kwargs:
        header = kwargs['header'].split(',')
    return tick_from_csv(fname, names=header)


def tick_from_json(fname):
    return pd.read_json(open(fname), convert_dates=False, lines=True)


def _tick_from_json(fname, **kwargs):
    return tick_from_json(fname)


def tick_to_candle(tick: pd.DataFrame, period: int) -> pd.DataFrame:
    """Convert tick data to candle data."""
    tick = tick.set_index('timestamp') \
               .sort_index()

    start = round_down_nearest(tick.index.min(), period)
    end   = round_down_nearest(tick.index.max(), period) + period
    bars  = []

    for idx, itr in enumerate(range(start, end, period)):
        if not (idx % 100):
            logging.info('Collected {} candles'.format(idx))

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


def _tick_to_candle(tick, **kwargs):
    period = kwargs['period']
    return tick_to_candle(tick, period)


def candle_to_csv(data, fname):
    data.to_csv(fname, index=False)


_read_table = {
        ('tick', 'csv'): _tick_from_csv,
        ('tick', 'json'): _tick_from_json,
}


_convert_table = {
        ('tick', 'candle'): _tick_to_candle,
}


_write_table = {
        ('candle', 'csv'): candle_to_csv,
}

# -----------------------------------------------------
# | Middleware for finding correct converion routines |
# -----------------------------------------------------

def read_data(filename, data_fmt, file_fmt, **kwargs):
    """ Read data.

    Args
    ----
    filename : str
        File name of file to be written to
    data : Any
        Data object
    data_fmt : str
        Finanical data type of output
    file_fmt : str
        File format

    """
    return _read_table[(data_fmt, file_fmt)](filename, **kwargs)


def convert(data, in_format, out_format, **kwargs):
    """ Convert data type.

    Args
    ----
    data : Any
        Data object
    data_fmt : str
        Finanical data type of output
    file_fmt : str
        File format

    """
    return _convert_table[(in_format, out_format)](data, **kwargs)


def write_data(filename, data, data_fmt, file_fmt, **kwargs):
    """ Write data to file.

    Args
    ----
    filename : str
        File name of file to be written to
    data : Any
        Data object
    data_fmt : str
        Finanical data type of output
    file_fmt : str
        File format

    """
    return _write_table[(data_fmt, file_fmt)](data, filename, **kwargs)


@click.command()
@click.option(
    '--infile', default='csv', show_default=True,
    help='Input data file format'
)
@click.option(
    '--outfile', default='csv', show_default=True,
    help='Output data file format'
)
@click.argument('input-format')
@click.argument('output-format')
@click.argument('src')
@click.argument('dest')
@click.pass_context
def main(ctx, infile, outfile, input_format, output_format, src, dest):
    if input_format == output_format and infile == outfile:
        ctx.fail('No conversion needed between identical data types.')

    logging.basicConfig(level=logging.INFO)
    data = read_data(src, input_format, infile)

    if input_format != output_format:
        data = convert(data, input_format, output_format)

    return write_data(dest, data, output_format, outfile)


if __name__ == "__main__":
    main()
