import sys

import numpy as np
import pandas as pd


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

        op  = bar_ticks.min()
        cl  = bar_ticks.max()
        hi  = bar_ticks.price.max()
        lo  = bar_ticks.price.min()
        vol = bar_ticks.amount.sum()
        ts  = itr

        bars.append((op, cl, hi, lo, vol, ts))

    return pd.DataFrame(bars, columns=['open', 'close', 'hi', 'low', 'volume', 'timestamp'])


def candle_to_csv(data, fname):
    data.to_csv(fname)


def main():
    pass


if __name__ == "__main__":
    main()
