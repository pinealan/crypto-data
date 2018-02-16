import logging
import time
import json

import pysher

formatter = logging.Formatter('%(message)s')

btc = logging.getLogger('BTC')
eth = logging.getLogger('ETH')
xrp = logging.getLogger('XRP')
bch = logging.getLogger('BCH')
ltc = logging.getLogger('LTC')

btc.setLevel(logging.INFO)
eth.setLevel(logging.INFO)
xrp.setLevel(logging.INFO)
bch.setLevel(logging.INFO)
ltc.setLevel(logging.INFO)

f1 = logging.FileHandler('btc.log', mode='a')
f2 = logging.FileHandler('eth.log', mode='a')
f3 = logging.FileHandler('xrp.log', mode='a')
f4 = logging.FileHandler('bch.log', mode='a')
f5 = logging.FileHandler('ltc.log', mode='a')

f1.setLevel(logging.INFO)
f2.setLevel(logging.INFO)
f3.setLevel(logging.INFO)
f4.setLevel(logging.INFO)
f5.setLevel(logging.INFO)

f1.setFormatter(formatter)
f2.setFormatter(formatter)
f3.setFormatter(formatter)
f4.setFormatter(formatter)
f5.setFormatter(formatter)

btc.addHandler(f1)
eth.addHandler(f2)
xrp.addHandler(f3)
bch.addHandler(f4)
ltc.addHandler(f5)

fmt = logging.Formatter('%(asctime)s %(message)s')

fh = logging.FileHandler('bitstamp.log', mode='a')
fh.setLevel(logging.WARNING)
fh.setFormatter(fmt)

log = logging.getLogger('pysher')
log.addHandler(fh)

class BitstampFeed:

    def __init__(self):
        api_key = 'de504dc5763aeef9ff52'
        self.pusher = pysher.Pusher(api_key, auto_sub=True)
        self.pusher.connection.needs_reconnect = True
        self.pusher.connect()
        time.sleep(2)


    def onTrade(self, pair, callback):
        if pair == 'btcusd':
            self._bindSocket('live_trades', 'trade', callback)
        else:
            self._bindSocket('live_trades_' + pair, 'trade', callback)


    def _bindSocket(self,  channel_name, event, callback):

        if channel_name not in self.pusher.channels:
            self.pusher.subscribe(channel_name)

        channel = self.pusher.channels[channel_name]
        channel.bind(event, callback)


def main():
    bs = BitstampFeed()

    bs.onTrade('btcusd', lambda x: btc.info(x))
    bs.onTrade('ethusd', lambda x: eth.info(x))
    bs.onTrade('xrpusd', lambda x: xrp.info(x))
    bs.onTrade('bchusd', lambda x: bch.info(x))
    bs.onTrade('ltcusd', lambda x: ltc.info(x))

    connection = bs.pusher.connection

    while connection.is_alive():
        time.sleep(10)

    log.error('Thread disconnected')

if __name__ == '__main__':
    main()
