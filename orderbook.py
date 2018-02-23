import logging
import json

import requests as req
from bitstamp import setup_log_and_file

# Logging formatter
formatter = logging.Formatter('%(message)s')

# Coins that we are going to track
coins  = ['bch', 'btc', 'eth', 'ltc', 'xrp']

fhs = {}
lgs = {}
for coin in coins:
        fh, lg = setup_log_and_file('orderbook/' + coin + '.log')
        fhs[coin] = fh
        lgs[coin] = lg


def reqOrderbook(coin):
    try:
        res = req.get('https://bitstamp.net/api/v2/order_book/' + coin)
        if res.status_code != 200:
            raise ConnectionError(res.status_code)
        return json.loads(res.text)
    except ConnectionError:
        logging.error('Request failed')
        raise


def main():
    for coin in coins:
        endpoint = coin + 'usd'
        lgs[coin].info(reqOrderbook(endpoint))


if __name__ == '__main__':
    main()
