import time
import logging

import pytest

from feed import bitfinex as bfx
from feed.exception import *


def test_bifinex_wss():
    feed = bfx.BitfinexFeed()
    feed.connect()
    assert feed.connected
    assert feed._recv_thread.isAlive()
    feed.close()


def test_bitfinex_decode_trades():
    channel, kwargs = bfx.decode_evt('trades:tBTCUSD')
    assert channel == 'trades'
    assert kwargs == {'symbol': 'tBTCUSD'}


def test_bitfinex_decode_candles():
    evt = 'candles:tBTCUSD:1m'
    assert bfx.decode_evt(evt) == ('candles', {'key': 'trade:tBTCUSD:1m'})


def test_bitfinex_encode_candles():
    msg = {'channel': 'candles',  'chanId': 1, 'key': 'trade:tBTCUSD:1m'}
    assert bfx.encode_evt(msg) == (1, 'candles:tBTCUSD:1m')


def test_check_connection():
    feed = bfx.BitfinexFeed()
    with pytest.raises(ConnectionClosed):
        feed.on('trades:tBTCUSD', print)


def test_bifinex_on_trade():
    feed = bfx.BitfinexFeed()
    feed.connect()
    feed.on('trades:tBTCUSD', print)
    time.sleep(5)
    feed.close()
