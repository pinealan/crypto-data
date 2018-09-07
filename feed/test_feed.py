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


def test_bitfinex_parse_evt():
    channel, kwargs = bfx.parse_evt('trades:tBTCUSD')
    assert channel == 'trades'
    assert kwargs == {'symbol': 'tBTCUSD'}


def test_check_connection():
    feed = bfx.BitfinexFeed()
    with pytest.raises(ConnectionClosed):
        feed.on('trades:tBTCUSD', print)


def test_bifinex_on_trade():
    feed = bfx.BitfinexFeed()
    feed.connect()
    feed.on('trades:tBTCUSD', print)
    feed.close()
