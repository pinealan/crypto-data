import time
import logging

from feed import bitfinex


logging.getLogger('websocket').setLevel(logging.DEBUG)


def test_bifinex_wss():
    feed = bitfinex.BitfinexFeed()
    feed.on('trades:tBTCUSD', print)

    assert feed._ws.connected
