import os
import json
import time
import threading
import logging
from enum import Enum

import websocket


WSSURL = 'wss://api.bitfinex.com/ws/2'


def parse_evt(evt):
    """Parse string representation of market event."""
    channel, *params = evt.split(':')
    kwargs = {}

    # @Todo: Implement all events
    if channel == 'trades':
        kwargs['symbol'] = params[0]
    elif channel == 'ticker':
        pass
    elif channel == 'book':
        pass
    elif channel == 'rawbook':
        pass
    elif channel == 'candle':
        pass
    else:
        raise BadEvent(evt)

    return channel, kwargs


class ErrorCode(Enum):
    ERR_UNK          = 10000
    ERR_GENERIC      = 10001
    ERR_CONCURRENCY  = 10008
    ERR_PARAMS       = 10020
    ERR_CONF_FAIL    = 10050
    ERR_AUTH_FAIL    = 10100
    ERR_AUTH_PAYLOAD = 10111
    ERR_AUTH_SIG     = 10112
    ERR_AUTH_HMAC    = 10113
    ERR_AUTH_NONCE   = 10114
    ERR_UNAUTH_FAIL  = 10200
    ERR_SUB_FAIL     = 10300
    ERR_SUB_MULTI    = 10301
    ERR_UNSUB_FAIL   = 10400
    ERR_READY        = 11000
    EVT_STOP         = 20051
    EVT_RESYNC_START = 20060
    EVT_RESYNC_STOP  = 20061
    EVT_INFO         = 5000


class BitfinexFeed:
    def __init__(self):
        self._subscribed_channels = {}
        self._callbacks = {}
        self._ws = websocket.create_connection(WSSURL)

    def on(self, evt, callback):
        """Bind a callback to an event.

        Args:
            evt: The event to listen for.
            callback: The callback function to be binded.
        """
        if not self._ws.connected:
            raise ConnectionClosed()

        channel, kwargs = parse_evt(evt)

        msg = {'event': 'subscribe', 'channel': channel}.update(kwargs)
        self._send(msg)

    def _send(self, msg):
        raw_msg = json.dumps(msg)
        self._ws.send(raw_msg)

    def _handleMessage(self, msg):
        event = msg['event']
        if event == 'subscribed':
            channel_name = msg['channel']
            channel_id   = msg['chanId']
            self._subscribed_channels[channel_id] = channel_name
        elif event == 'error':
            self._error(msg['code'], msg['msg'])
        else:
            raise BadResponse()

    def _handleUpdate(self, msg):
        channel_id = msg.pop(0)
        cb = self._callbacks[channel_id]
        cb(*msg)

    def _error(self, code, msg):
        if code not in ErrorCode:
            logging.error('Error: {} Unknown error'.format(code))
        else:
            logging.error('Error: {}'.format(code))

    def _onmessage(self, raw_res):
        res = json.loads(raw_res)
        if isinstance(res, dict):
            self._handleMessage(res)
        elif isinstance(res, list):
            self._handleUpdate(res)
        else:
            raise BadResponse()
