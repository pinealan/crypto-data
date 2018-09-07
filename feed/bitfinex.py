import os
import sys
import json
import time
import logging
import traceback
from enum import Enum
from threading import Thread

import websocket

from .exception import *


_log = logging.getLogger(__name__)
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


def parse_raw_msg(msg):
    return json.loads(msg)


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
    """Websocket datafeed for Bitfinex.

    Attributes:
        connected: Connection status.
    """
    def __init__(self, *args, **options):
        self._subscribed_channels = {}
        self._callbacks = {}
        self._recv_thread = None
        self._ws = websocket.WebSocket(*args, **options)

    # ----------
    # Public interface
    # ----------
    def connect(self, **options):
        if self.connected:
            return

        self._ws.connect(WSSURL, **options)
        self._recv_thread = Thread(target=self._recvForever)
        self._recv_thread.setDaemon(True)
        self.running = True
        self._recv_thread.start()

    def close(self):
        if self.connected:
            if self._recv_thread.is_alive():
                self.running = False
                self._recv_thread.join()
            self._ws.close()

    def on(self, evt, callback):
        """Bind a callback to an event.

        Args:
            evt: The event to listen for.
            callback: The callback function to be binded.
        """
        if not self.connected:
            raise ConnectionClosed()
        channel, kwargs = parse_evt(evt)
        msg = {'event': 'subscribe', 'channel': channel}.update(kwargs)
        self._send(msg)

    @property
    def connected(self):
        return self._ws.connected

    # ----------
    # Send outgoing messages
    # ----------
    def _send(self, msg):
        raw_msg = json.dumps(msg)
        self._ws.send(raw_msg)
        _log.debug('Sent: {}'.format(raw_msg))

    # ----------
    # Process incoming messages
    # ----------
    def _recvForever(self):
        """Main loop to receive incoming socket messages.

        Messages are received using the blocking WebSocket.recv() method. The
        raw string messages are then parsed into python objects and passed onto
        the appropriate methods for the corresponding types of message.
        """
        logging.debug('Incoming thread started')
        while self.connected and self.running:
            try:
                try:
                    raw_msg = self._ws.recv()
                except websocket.WebSocketConnectionClosedException:
                    # restart?
                    _log.error('Websocket closed')
                    break
                except websocket.WebSocketTimeoutException:
                    _log.error('Websocket timeout')
                    continue
                else:
                    wsmsg = parse_raw_msg(raw_msg)
                    if isinstance(wsmsg, dict):
                        self._handleMessage(wsmsg)
                    elif isinstance(wsmsg, list):
                        self._handleUpdate(wsmsg)

            # Print traceback for all errors in incoming thread
            except Exception as e:
                _log.error('error from callback {}'.format(e))
                _, _, tb = sys.exc_info()
                traceback.print_tb(tb)
            else:
                _log.debug('Received: {}'.format(raw_msg))

    def _handleMessage(self, msg):
        event = msg['event']
        if event == 'info':
            return
        elif event == 'subscribed':
            channel_name = msg['channel']
            channel_id   = msg['chanId']
            self._subscribed_channels[channel_id] = channel_name
        elif event == 'error':
            code = msg['code']
            msg = msg['msg']
            if code not in ErrorCode:
                _log.error('{} Unknown error'.format(code))
            else:
                _log.error('{}'.format(code))
        else:
            raise BadMessage(msg)

    def _handleUpdate(self, msg):
        channel_id = msg.pop(0)
        cb = self._callbacks[channel_id]
        cb(*msg)
