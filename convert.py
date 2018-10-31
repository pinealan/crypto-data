#!/usr/bin/python3
"""Conversion routines between various types of financial market data."""

import sys
import logging
import traceback

import click
import numpy as np
import pandas as pd


def _round_down_nearest(n, precision: int):
    return (n // precision) * precision


def tick_from_csv(fname, **kwargs):
    header = None
    if 'header' in kwargs:
        header = kwargs['header'].split(',')
    return pd.read_csv(fname, names=header)


def tick_from_json(fname, **kwargs):
    return pd.read_json(open(fname), convert_dates=False, lines=True)


def tick_to_candle(tick: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Convert tick data to candle data."""

    # Options
    period = kwargs['period']

    tick = tick.set_index('timestamp') \
               .sort_index()

    start = _round_down_nearest(tick.index.min(), period)
    end   = _round_down_nearest(tick.index.max(), period) + period
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


def tick_to_candle(tick, **kwargs):
    return tick_to_candle(tick, period)


def candle_to_csv(data, fname):
    data.to_csv(fname, index=False)


_read_table = {
        ('tick', 'csv'): tick_from_csv,
        ('tick', 'json'): tick_from_json,
}


_convert_table = {
        ('tick', 'candle'): tick_to_candle,
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
@click.argument('kwargs', nargs=-1)
@click.pass_context
def main(ctx, infile, outfile, input_format, output_format, src, dest, kwargs):
    """Entry point of the financial data conversion tool."""
    kws = {}
    for arg in kwargs:
        pair = arg.split('=')
        if len(pair) == 1:
            ctx.fail("Variadic arguments must come in key-value pairs")
        kws[pair[0]] = pair[1]

    if input_format == output_format and infile == outfile:
        ctx.fail('No conversion needed between identical data types.')

    logging.basicConfig(level=logging.INFO)
    data = read_data(src, input_format, infile, **kws)

    if input_format != output_format:
        data = convert(data, input_format, output_format, **kws)

    return write_data(dest, data, output_format, outfile, **kws)


if __name__ == "__main__":
    main()
