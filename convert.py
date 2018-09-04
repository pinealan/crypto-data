#!/usr/bin/python3
import sys
import logging

import numpy as np
import pandas as pd


def print_help():
    usage = [
        'Usage: convert <command>',
        ''
    ]
    print('\n'.join(usage))


def round_down_nearest(n, precision: int):
    return (n // precision) * precision


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


def candle_to_csv(data, fname):
    data.to_csv(fname, index=False)


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


def handle_tick_to_candle(kwargs):
    tick   = kwargs['tick']
    period = kwargs['period']
    fname  = kwargs['fname']

    tick   = tick_from_json(tick)
    candle = tick_to_candle(tick, period)
    candle_to_csv(candle, fname)


def main():
    try:
        command = sys.argv[1]
    except IndexError:
        print_help()
        return

    logging.debug(sys.argv)

    kwargs = parse_cmdline_args(sys.argv[1:])
    if command == 'help':
        print_help()
        exit(1)
    elif command == 'tick_to_candle':
        handle_tick_to_candle(kwargs)
    else:
        handle_tick_to_candle(kwargs)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
