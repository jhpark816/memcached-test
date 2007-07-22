#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import sys
import socket
import random
import struct
import exceptions

from memcacheConstants import REQ_MAGIC_BYTE, PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT
import memcacheConstants

class MemcachedError(exceptions.Exception):
    """Error raised when a command fails."""

    def __init__(self, status, msg):
        supermsg='Memcached error #' + `status`
        if msg: supermsg += ":  " + msg
        exceptions.Exception.__init__(self, supermsg)

        self.status=status
        self.msg=msg

    def __repr__(self):
        return "<MemcachedError #%d ``%s''>" % (self.status, self.msg)

class MemcachedClient(object):
    """Simple memcached client."""

    def __init__(self, host='127.0.0.1', port=11211):
        self.s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect_ex((host, port))
        self.r=random.Random()

    def _sendCmd(self, cmd, key, val, opaque, extraHeader=''):
        msg=struct.pack(PKT_FMT, REQ_MAGIC_BYTE,
            cmd, len(key), opaque, len(key) + len(extraHeader) + len(val))
        self.s.send(msg + extraHeader + key + val)

    def _handleSingleResponse(self, myopaque):
        response=self.s.recv(MIN_RECV_PACKET)
        assert len(response) == MIN_RECV_PACKET
        magic, cmd, errcode, opaque, remaining=struct.unpack(PKT_FMT, response)
        rv=self.s.recv(remaining)
        assert magic == REQ_MAGIC_BYTE
        assert myopaque is None or opaque == myopaque
        if errcode != 0:
            raise MemcachedError(errcode,  rv)
        return opaque, rv

    def _doCmd(self, cmd, key, val, extraHeader=''):
        """Send a command and await its response."""
        opaque=self.r.randint(0, 2**32)
        self._sendCmd(cmd, key, val, opaque, extraHeader)
        return self._handleSingleResponse(opaque)[1]

    def _mutate(self, cmd, key, exp, flags, val):
        self._doCmd(cmd, key, val, struct.pack(SET_PKT_FMT, flags, exp))

    def set(self, key, exp, flags, val):
        """Set a value in the memcached server."""
        self._mutate(memcacheConstants.CMD_SET, key, exp, flags, val)

    def add(self, key, exp, flags, val):
        """Add a value in the memcached server iff it doesn't already exist."""
        self._mutate(memcacheConstants.CMD_ADD, key, exp, flags, val)

    def replace(self, key, exp, flags, val):
        """Replace a value in the memcached server iff it already exists."""
        self._mutate(memcacheConstants.CMD_REPLACE, key, exp, flags, val)

    def get(self, key):
        """Get the value for a given key within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GET, key, '')
        return struct.unpack(">I", parts[:4])[0], parts[4:]

    def noop(self):
        """Send a noop command."""
        self._doCmd(memcacheConstants.CMD_NOOP, '', '')

    def delete(self, key):
        """Delete the value for a given key within the memcached server."""
        self._doCmd(memcacheConstants.CMD_DELETE, key, '')

    def flush(self):
        """Flush all storage in a memcached instance."""
        self._doCmd(memcacheConstants.CMD_FLUSH, '', '')
