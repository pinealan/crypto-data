import time
import logging

from feed import bitfinex


def test_bifinex_wss():
    feed = bitfinex.BitfinexFeed()
    feed.connect()
    assert feed.connected
    feed.close()
