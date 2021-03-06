#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import sys
import time
import hmac
import socket
import random
import struct
import exceptions

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, DEL_PKT_FMT, INCRDECR_RES_FMT
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

    vbucketId = 0

    def __init__(self, host='127.0.0.1', port=11211):
        self.s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect_ex((host, port))
        self.r=random.Random()

    def close(self):
        self.s.close()

    def __del__(self):
        self.close()

    def _sendCmd(self, cmd, key, val, opaque, extraHeader='', cas=0):
        dtype=0
        msg=struct.pack(REQ_PKT_FMT, REQ_MAGIC_BYTE,
            cmd, len(key), len(extraHeader), dtype, self.vbucketId,
                len(key) + len(extraHeader) + len(val), opaque, cas)
        self.s.send(msg + extraHeader + key + val)

    def _handleKeyedResponse(self, myopaque):
        response = ""
        while len(response) < MIN_RECV_PACKET:
            data = self.s.recv(MIN_RECV_PACKET - len(response))
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?).")
            response += data
        assert len(response) == MIN_RECV_PACKET
        magic, cmd, keylen, extralen, dtype, errcode, remaining, opaque, cas=\
            struct.unpack(RES_PKT_FMT, response)

        rv = ""
        while remaining > 0:
            data = self.s.recv(remaining)
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?).")
            rv += data
            remaining -= len(data)

        assert (magic in (RES_MAGIC_BYTE, REQ_MAGIC_BYTE)), "Got magic: %d" % magic
        assert myopaque is None or opaque == myopaque, \
            "expected opaque %x, got %x" % (myopaque, opaque)
        if errcode != 0:
            raise MemcachedError(errcode,  rv)
        return cmd, opaque, cas, keylen, extralen, rv

    def _handleSingleResponse(self, myopaque):
        cmd, opaque, cas, keylen, extralen, data = self._handleKeyedResponse(myopaque)
        return opaque, cas, data

    def _doCmd(self, cmd, key, val, extraHeader='', cas=0):
        """Send a command and await its response."""
        opaque=self.r.randint(0, 2**32)
        self._sendCmd(cmd, key, val, opaque, extraHeader, cas)
        return self._handleSingleResponse(opaque)

    def _mutate(self, cmd, key, exp, flags, cas, val):
        return self._doCmd(cmd, key, val, struct.pack(SET_PKT_FMT, flags, exp),
            cas)

    def _cat(self, cmd, key, cas, val):
        return self._doCmd(cmd, key, val, '', cas)

    def append(self, key, value, cas=0):
        return self._cat(memcacheConstants.CMD_APPEND, key, cas, value)

    def prepend(self, key, value, cas=0):
        return self._cat(memcacheConstants.CMD_PREPEND, key, cas, value)

    def __incrdecr(self, cmd, key, amt, init, exp):
        something, cas, val=self._doCmd(cmd, key, '',
            struct.pack(memcacheConstants.INCRDECR_PKT_FMT, amt, init, exp))
        return struct.unpack(INCRDECR_RES_FMT, val)[0], cas

    def incr(self, key, amt=1, init=0, exp=0):
        """Increment or create the named counter."""
        return self.__incrdecr(memcacheConstants.CMD_INCR, key, amt, init, exp)

    def decr(self, key, amt=1, init=0, exp=0):
        """Decrement or create the named counter."""
        return self.__incrdecr(memcacheConstants.CMD_DECR, key, amt, init, exp)

    def set(self, key, exp, flags, val):
        """Set a value in the memcached server."""
        return self._mutate(memcacheConstants.CMD_SET, key, exp, flags, 0, val)

    def add(self, key, exp, flags, val):
        """Add a value in the memcached server iff it doesn't already exist."""
        return self._mutate(memcacheConstants.CMD_ADD, key, exp, flags, 0, val)

    def replace(self, key, exp, flags, val):
        """Replace a value in the memcached server iff it already exists."""
        return self._mutate(memcacheConstants.CMD_REPLACE, key, exp, flags, 0,
            val)

    def __parseGet(self, data):
        flags=struct.unpack(memcacheConstants.GET_RES_FMT, data[-1][:4])[0]
        return flags, data[1], data[-1][4:]

    def get(self, key):
        """Get the value for a given key within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GET, key, '')
        return self.__parseGet(parts)

    def cas(self, key, exp, flags, oldVal, val):
        """CAS in a new value for the given key and comparison value."""
        self._mutate(memcacheConstants.CMD_SET, key, exp, flags,
            oldVal, val)

# COLLECTION: ATTR begin
    def getattr(self, key, attrid):
        """get all attributes for the given key """
        data = self._doCmd(memcacheConstants.CMD_GETATTR, key, '')[-1]
        flags, exptime, count, maxcount, maxbkeyrange, type, ovflaction, reserved1, reserved2 = \
               struct.unpack(memcacheConstants.GETATTR_RES_FMT, data)
        if attrid == memcacheConstants.ATTR_FLAGS:
           return flags
        elif attrid == memcacheConstants.ATTR_EXPIRETIME:
           return exptime
        elif attrid == memcacheConstants.ATTR_COUNT:
           return count
        elif attrid == memcacheConstants.ATTR_MAXCOUNT:
           return maxcount
        elif attrid == memcacheConstants.ATTR_MAXBKEYRANGE:
           return maxbkeyrange
        elif attrid == memcacheConstants.ATTR_TYPE:
           return type
        elif attrid == memcacheConstants.ATTR_OVFLACTION:
           return ovflaction
        else:
           return -1

    def setattr(self, key, exptime_f, exptime, maxcount_f, maxcount, maxbkeyrange_f, maxbkeyrange, ovflaction):
        """Set some attributes for the given key """
        return self._doCmd(memcacheConstants.CMD_SETATTR, key, '',
                           struct.pack(memcacheConstants.SETATTR_PKT_FMT,
                                       exptime, maxcount, maxbkeyrange, ovflaction,
                                       exptime_f, maxcount_f, maxbkeyrange_f))
# COLLECTION: ATTR end

# COLLECTION: LOP begin
    def lop_create(self, key, flags=0, exptime=0, maxcount=0, ovflaction=0):
        """Create an empty list """
        return self._doCmd(memcacheConstants.CMD_LOP_CREATE, key, '',
                           struct.pack(memcacheConstants.LOP_CRT_PKT_FMT,
                                       flags, exptime, maxcount, ovflaction, 0, 0, 0))

    def lop_insert(self, key, index, val, create=0, flags=0, exptime=0, maxcount=0):
        """Insert an element into the given list """
        return self._doCmd(memcacheConstants.CMD_LOP_INSERT, key, val,
                           struct.pack(memcacheConstants.LOP_INS_PKT_FMT,
                                       index, flags, exptime, maxcount, create, 0, 0, 0))

    def lop_delete(self, key, from_index, to_index, drop_if_empty=0):
        """Delete some elements from the given list """
        return self._doCmd(memcacheConstants.CMD_LOP_DELETE, key, '',
                           struct.pack(memcacheConstants.LOP_DEL_PKT_FMT,
                                       from_index, to_index, drop_if_empty, 0, 0, 0))

    def __parseLOPGet(self, data):
        """ parse LOP GET result """
        flags = struct.unpack(memcacheConstants.VLENG_RES_FMT, data[:4])[0]
        count = struct.unpack(memcacheConstants.COUNT_RES_FMT, data[4:8])[0]
        offset = 8
        vlen = []
        for n in range(count):
            vlen.append(struct.unpack(memcacheConstants.VLENG_RES_FMT, data[offset:offset+4])[0])
            offset += 4
        vals = []
        for n in range(count):
            vals.append(data[offset:offset+vlen[n]])
            offset += vlen[n]
        return flags, count, vals

    def lop_get(self, key, from_index, to_index, delete=0, drop_if_empty=0):
        """Get(with delete) some elements from the given list """
        data = self._doCmd(memcacheConstants.CMD_LOP_GET, key, '',
                           struct.pack(memcacheConstants.LOP_GET_PKT_FMT,
                                       from_index, to_index, delete, drop_if_empty, 0, 0))[-1]
        return self.__parseLOPGet(data)
# COLLECTION : LOP end

# COLLECTION : SOP begin
    def sop_create(self, key, flags=0, exptime=0, maxcount=0):
        """Create an empty set """
        return self._doCmd(memcacheConstants.CMD_SOP_CREATE, key, '',
                           struct.pack(memcacheConstants.SOP_CRT_PKT_FMT,
                                       flags, exptime, maxcount))

    def sop_insert(self, key, val, create=0, flags=0, exptime=0, maxcount=0):
        """Insert an element into the given set """
        return self._doCmd(memcacheConstants.CMD_SOP_INSERT, key, val,
                           struct.pack(memcacheConstants.SOP_INS_PKT_FMT,
                                       flags, exptime, maxcount, create, 0, 0, 0))

    def sop_delete(self, key, val, drop_if_empty=0):
        """Delete an element from the given set """
        return self._doCmd(memcacheConstants.CMD_SOP_DELETE, key, val,
                           struct.pack(memcacheConstants.SOP_DEL_PKT_FMT,
                                       drop_if_empty, 0, 0, 0))

    def sop_exist(self, key, val):
        """Check if the given value exists in the given set """
        data = self._doCmd(memcacheConstants.CMD_SOP_EXIST, key, val)[-1]
        return struct.unpack(memcacheConstants.EXIST_RES_FMT, data)[0]

    def __parseSOPGet(self, data):
        """ parse SOP GET result """
        flags = struct.unpack(memcacheConstants.VLENG_RES_FMT, data[:4])[0]
        count = struct.unpack(memcacheConstants.COUNT_RES_FMT, data[4:8])[0]
        offset = 8
        vlen = []
        for n in range(count):
            vlen.append(struct.unpack(memcacheConstants.VLENG_RES_FMT, data[offset:offset+4])[0])
            offset += 4
        vals = []
        for n in range(count):
            vals.append(data[offset:offset+vlen[n]])
            offset += vlen[n]
        return flags, count, set(vals)

    def sop_get(self, key, count, delete=0, drop_if_empty=0):
        """Get(with delete) some elements from the given set """
        data = self._doCmd(memcacheConstants.CMD_SOP_GET, key, '',
                           struct.pack(memcacheConstants.SOP_GET_PKT_FMT,
                                       count, delete, drop_if_empty, 0, 0))[-1]
        return self.__parseSOPGet(data)
# COLLECTION : SOP end

# COLLECTION : BOP begin
    def bop_create(self, key, flags=0, exptime=0, maxcount=0, ovflaction=0):
        """Create an empty b+tree """
        return self._doCmd(memcacheConstants.CMD_BOP_CREATE, key, '',
                           struct.pack(memcacheConstants.BOP_CRT_PKT_FMT,
                                       flags, exptime, maxcount, ovflaction, 0, 0, 0))

    def bop_insert(self, key, bkey, val, create=0, flags=0, exptime=0, maxcount=0):
        """Insert an element into the given b+tree """
        return self._doCmd(memcacheConstants.CMD_BOP_INSERT, key, val,
                           struct.pack(memcacheConstants.BOP_INS_PKT_FMT,
                                       bkey, flags, exptime, maxcount, create, 0, 0, 0))

    def bop_delete(self, key, from_bkey, to_bkey, count=0, drop_if_empty=0):
        """Delete some elements from the given b+tree """
        return self._doCmd(memcacheConstants.CMD_BOP_DELETE, key, '',
                           struct.pack(memcacheConstants.BOP_DEL_PKT_FMT,
                                       from_bkey, to_bkey, count, drop_if_empty, 0, 0, 0))

    def __parseBOPGet(self, data):
        """ parse BOP GET result """
        flags = struct.unpack(memcacheConstants.VLENG_RES_FMT, data[:4])[0]
        count = struct.unpack(memcacheConstants.COUNT_RES_FMT, data[4:8])[0]
        offset = 8
        bkey = []
        for n in range(count):
            bkey.append(struct.unpack(memcacheConstants.BKEY_RES_FMT, data[offset:offset+8])[0])
            offset += 8
        vlen = []
        for n in range(count):
            vlen.append(struct.unpack(memcacheConstants.VLENG_RES_FMT, data[offset:offset+4])[0])
            offset += 4
        vals = []
        for n in range(count):
            vals.append(data[offset:offset+vlen[n]])
            offset += vlen[n]
        return flags, count, bkey, vals

    def bop_get(self, key, from_bkey, to_bkey, offset=0, count=0, delete=0, drop_if_empty=0):
        """Get(with delete) some elements from the given b+tree """
        data = self._doCmd(memcacheConstants.CMD_BOP_GET, key, '',
                           struct.pack(memcacheConstants.BOP_GET_PKT_FMT,
                                       from_bkey, to_bkey, offset, count, delete, drop_if_empty, 0, 0))[-1]
        return self.__parseBOPGet(data)

    def bop_count(self, key, from_bkey, to_bkey):
        """Count elements of given bkey range in the given b+tree """
        data = self._doCmd(memcacheConstants.CMD_BOP_COUNT, key, '',
                           struct.pack(memcacheConstants.BOP_CNT_PKT_FMT,
                                       from_bkey, to_bkey))[-1]
        flags = struct.unpack(memcacheConstants.VLENG_RES_FMT, data[:4])[0]
        count = struct.unpack(memcacheConstants.COUNT_RES_FMT, data[4:8])[0]
        return flags, count

# COLLECTION : BOP end

    def version(self):
        """Get the value for a given key within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_VERSION, '', '')

    def sasl_mechanisms(self):
        """Get the supported SASL methods."""
        return set(self._doCmd(memcacheConstants.CMD_SASL_LIST_MECHS,
                               '', '')[2].split(' '))

    def sasl_auth_start(self, mech, data):
        """Start a sasl auth session."""
        return self._doCmd(memcacheConstants.CMD_SASL_AUTH, mech, data)

    def sasl_auth_plain(self, user, password, foruser=''):
        """Perform plain auth."""
        return self.sasl_auth_start('PLAIN', '\0'.join([foruser, user, password]))

    def sasl_auth_cram_md5(self, user, password):
        """Start a plan auth session."""
        try:
            self.sasl_auth_start('CRAM-MD5', '')
        except MemcachedError, e:
            if e.status != memcacheConstants.ERR_AUTH_CONTINUE:
                raise
            challenge = e.msg

        dig = hmac.HMAC(password, challenge).hexdigest()
        return self._doCmd(memcacheConstants.CMD_SASL_STEP, 'CRAM-MD5',
                           user + ' ' + dig)

    def set_vbucket_state(self, vbucket, state):
        return self._doCmd(memcacheConstants.CMD_SET_VBUCKET_STATE,
                           str(vbucket), state)

    def delete_vbucket(self, vbucket):
        return self._doCmd(memcacheConstants.CMD_DELETE_VBUCKET, str(vbucket), '')

    def getMulti(self, keys):
        """Get values for any available keys in the given iterable.

        Returns a dict of matched keys to their values."""
        opaqued=dict(enumerate(keys))
        terminal=len(opaqued)+10
        # Send all of the keys in quiet
        for k,v in opaqued.iteritems():
            self._sendCmd(memcacheConstants.CMD_GETQ, v, '', k)

        self._sendCmd(memcacheConstants.CMD_NOOP, '', '', terminal)

        # Handle the response
        rv={}
        done=False
        while not done:
            opaque, cas, data=self._handleSingleResponse(None)
            if opaque != terminal:
                rv[opaqued[opaque]]=self.__parseGet((opaque, cas, data))
            else:
                done=True

        return rv

    def stats(self, sub=''):
        """Get stats."""
        opaque=self.r.randint(0, 2**32)
        self._sendCmd(memcacheConstants.CMD_STAT, sub, '', opaque)
        done = False
        rv = {}
        while not done:
            cmd, opaque, cas, klen, extralen, data = self._handleKeyedResponse(None)
            if klen:
                rv[data[0:klen]] = data[klen:]
            else:
                done = True
        return rv

    def noop(self):
        """Send a noop command."""
        return self._doCmd(memcacheConstants.CMD_NOOP, '', '')

    def delete(self, key, cas=0):
        """Delete the value for a given key within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_DELETE, key, '', '', cas)

    def flush(self, timebomb=0):
        """Flush all storage in a memcached instance."""
        return self._doCmd(memcacheConstants.CMD_FLUSH, '', '',
            struct.pack(memcacheConstants.FLUSH_PKT_FMT, timebomb))
