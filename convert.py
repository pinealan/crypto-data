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


# ------------------
# | Input routines |
# ------------------

def tick_from_csv(file, **kwargs):
    header = None
    if 'header' in kwargs:
        header = kwargs['header'].split(',')
    return pd.read_csv(file, names=header)


def tick_from_json(file, **kwargs):
    return pd.read_json(file, convert_dates=False, lines=True)


# -----------------------
# | Conversion routines |
# -----------------------

def tick_to_candle(tick: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Convert tick data to candle data."""

    # Options
    period = int(kwargs['period'])

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


# -------------------
# | Output routines |
# -------------------

def tick_to_tuple(data: pd.DataFrame, file):
    for row in data.iterrows():
        file.write('({})'.format(','.join((
            row.price,
            row.timestamp,
            row.amount,
            row.type,
        ))))


def tick_to_csv(data: pd.DataFrame, file):
    data.to_csv(file, columns=('price', 'timestamp', 'amount', 'type'), index=False)


def candle_to_tuple(data: pd.DataFrame, file, **kwargs):
    pass


def candle_to_csv(data, file, **kwargs):
    data.to_csv(file, index=False)


# -----------------------------------------------------
# | Middleware for finding correct converion routines |
# -----------------------------------------------------

_read_table = {
        ('tick', 'csv'): tick_from_csv,
        ('tick', 'json'): tick_from_json,
}


_convert_table = {
        ('tick', 'candle'): tick_to_candle,
}


_write_table = {
        ('tick', 'tuple'): tick_to_tuple,
        ('tick', 'csv'): tick_to_csv,
        ('candle', 'tuple'): candle_to_tuple,
        ('candle', 'csv'): candle_to_csv,
}


def read_data(file, data_fmt, file_fmt, **kwargs):
    """ Read data.

    Args
    ----
    file : buffer
        An object with a read() method
    data : Any
        Data object
    data_fmt : str
        Finanical data type of output
    file_fmt : str
        File format

    """
    return _read_table[(data_fmt, file_fmt)](file, **kwargs)


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


def write_data(file, data, data_fmt, file_fmt, **kwargs):
    """ Write data to file.

    Args
    ----
    file : buffer
        An object with a read() method
    data : Any
        Data object
    data_fmt : str
        Finanical data type of output
    file_fmt : str
        File format

    """
    return _write_table[(data_fmt, file_fmt)](data, file, **kwargs)


@click.command()
@click.option(
    '-i', '--infile', default='csv', show_default=True,
    help='Input data file format'
)
@click.option(
    '-o', '--outfile', default='csv', show_default=True,
    help='Output data file format'
)
@click.option('-s', '--src', default=sys.stdin,
    help='Name of file to read, read from stdin if not provided'
)
@click.option('-d', '--dest', default=sys.stdout,
    help='Name of file to write, write to stdout if not provided'
)
@click.option('--output-type', default=None, help='Data type of output')
@click.argument('input-type')
@click.argument('kwargs', nargs=-1)
@click.pass_context
def main(ctx, infile, outfile, input_type, output_type, src, dest, kwargs):
    """Entry point of the financial data conversion tool."""
    logging.basicConfig(level=logging.INFO)

    # Parse CLI kwargs into a dict
    kws = {}
    for arg in kwargs:
        pair = arg.split('=')
        if len(pair) == 1:
            ctx.fail("Variadic arguments must come in key-value pairs")
        kws[pair[0]] = pair[1]

    if input_type == output_type and infile == outfile:
        ctx.fail('No conversion needed between identical data types.')

    if output_type is None:
         output_type = input_type

    # Read
    data = read_data(src, input_type, infile, **kws)

    # Convert financial data types
    if input_type != output_type:
        data = convert(data, input_type, output_type, **kws)

    # Write data
    return write_data(dest, data, output_type, outfile, **kws)


if __name__ == "__main__":
    main()
