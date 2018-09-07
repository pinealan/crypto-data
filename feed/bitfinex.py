import os
import sys
import json
import time
import socket
import logging
import traceback
from enum import Enum
from threading import Thread

import websocket as ws

from .exception import *


_log = logging.getLogger(__name__)
WSSURL = 'wss://api.bitfinex.com/ws/2'


def parse_evt(evt):
    """Parse string repr of market event into dict repr with named keys for bitfinex."""
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


def parse_subscribed_msg(msg: dict):
    """Parse subscribed messages from bitfinex into string repr."""
    name = msg['channel']
    cid  = msg['chanId']
    evt  = ''

    # @Todo: Implement all events
    if name == 'trades':
        evt = 'trades:{}'.format(msg['symbol'])
    elif name == 'ticker':
        pass
    elif name == 'book':
        pass
    elif name == 'rawbook':
        pass
    elif name == 'candle':
        pass
    return cid, evt


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
        # { Channel_ID: event, ... }
        self._id_event = {}

        # { event: [cb0, cb1, ...], ... }
        self._callbacks = {}

        # Incoming messages processing thread
        self._recv_thread = None

        self._ws = ws.WebSocket(*args, **options)
        self._ws.settimeout(3)

    # ----------
    # Public interface
    # ----------
    def connect(self, **options):
        """Connect to Bitfinex push data."""
        if self.connected:
            return

        self._ws.connect(WSSURL, **options)
        self._recv_thread = Thread(target=self._recvForever)
        self._recv_thread.setDaemon(True)
        self.running = True
        self._recv_thread.start()

    def close(self):
        """Disconnect from Bitfinex."""
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

        if evt not in self._callbacks:
            _log.info('New event callback:{}'.format(evt))
            self._callbacks[evt] = []
            channel, kwargs = parse_evt(evt)

        msg = {'event': 'subscribe', 'channel': channel}
        msg.update(kwargs)

        self._callbacks[evt].append(callback)
        self._send(msg)
        _log.info('Register callback: {}'.format(evt))

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
        _log.info('Receiver thread started')
        while self.connected and self.running:
            try:
                try:
                    raw_msg = self._ws.recv()
                except ws.WebSocketConnectionClosedException:
                    # @Todo: restart?
                    _log.error('Websocket closed')
                    break
                except ws.WebSocketTimeoutException:
                    _log.debug('Socket timeout')
                    continue
                else:
                    wsmsg = parse_raw_msg(raw_msg)
                    if isinstance(wsmsg, dict):
                        self._handleMessage(wsmsg)
                    elif isinstance(wsmsg, list):
                        self._handleUpdate(wsmsg)

            # @Todo Log traceback for uncaught errors in receiver thread
            except Exception as e:
                _, _, tb = sys.exc_info()
                traceback.print_tb(tb)
                _log.error('(callback error):{}:{}'.format(type(e).__name__, e))
            else:
                _log.debug('Received: {}'.format(raw_msg))

    def _handleMessage(self, msg):
        bfx_event = msg['event']

        if bfx_event == 'info':
            # @Todo logging
            return
        elif bfx_event == 'subscribed':
            cid, evt = parse_subscribed_msg(msg)
            self._id_event[cid] = evt
        elif bfx_event == 'error':
            code = msg['code']
            msg = msg['msg']
            if code not in ErrorCode:
                _log.error('{} Unknown error'.format(code))
            else:
                _log.error('{}'.format(code))
        else:
            raise BadMessage(msg)

    def _handleUpdate(self, msg):
        # Ignore heartbeat
        if msg[1] == 'hb':
            return
        cid = msg.pop(0)
        evt = self._id_event[cid]
        for cb in self._callbacks[evt]:
            cb(*msg)
