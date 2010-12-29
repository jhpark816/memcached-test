#!/usr/bin/env python
"""

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import struct

# Command constants
CMD_GET = 0
CMD_SET = 1
CMD_ADD = 2
CMD_REPLACE = 3
CMD_DELETE = 4
CMD_INCR = 5
CMD_DECR = 6
CMD_QUIT = 7
CMD_FLUSH = 8
CMD_GETQ = 9
CMD_NOOP = 10
CMD_VERSION = 11
CMD_STAT = 0x10
CMD_APPEND = 0x0e
CMD_PREPEND = 0x0f

# SASL stuff
CMD_SASL_LIST_MECHS = 0x20
CMD_SASL_AUTH = 0x21
CMD_SASL_STEP = 0x22

# Replication
CMD_TAP_CONNECT = 0x40
CMD_TAP_MUTATION = 0x41
CMD_TAP_DELETE = 0x42
CMD_TAP_FLUSH = 0x43
CMD_TAP_OPAQUE = 0x44
CMD_TAP_VBUCKET_SET = 0x45

# COLLECTION: ATTR stuff
CMD_GETATTR    = 0x50
CMD_SETATTR    = 0x51

# COLLECTION: LOP stuff
CMD_LOP_INSERT = 0x60
CMD_LOP_DELETE = 0x61
CMD_LOP_GET    = 0x62

# COLLECTION: SOP stuff
CMD_SOP_INSERT = 0x70
CMD_SOP_DELETE = 0x71
CMD_SOP_EXIST  = 0x72
CMD_SOP_GET    = 0x73

# COLLECTION: B+Tree stuff
CMD_BOP_INSERT = 0x80
CMD_BOP_DELETE = 0x81
CMD_BOP_GET    = 0x82

# vbucket stuff
CMD_SET_VBUCKET_STATE = 0x83
CMD_GET_VBUCKET_STATE = 0x84
CMD_DELETE_VBUCKET = 0x85

COMMAND_NAMES = dict(((globals()[k], k) for k in globals() if k.startswith("CMD_")))

# COLLECTION: ATTR ID
ATTR_TYPE       = 0
ATTR_FLAGS      = 1
ATTR_EXPIRETIME = 2
ATTR_COUNT      = 3
ATTR_MAXCOUNT   = 4
ATTR_OVFLACTION = 5

# COLLECTION: ITEM TYPE
ITEM_TYPE_KV    = 1
ITEM_TYPE_LIST  = 2
ITEM_TYPE_SET   = 3
ITEM_TYPE_BTREE = 4

# COLLECTION: OVERFLOW ACTION
OVFL_ERROR         = 1
OVFL_HEAD_TRIM     = 2
OVFL_TAIL_TRIM     = 3
OVFL_SMALLEST_TRIM = 4
OVFL_LARGEST_TRIM  = 5

# COLLECTION: Request and Response Format
COUNT_RES_FMT=">L"
VLENG_RES_FMT=">L"
EXIST_RES_FMT=">L"
BKEY_RES_FMT=">Q"
LOP_INS_PKT_FMT=">lLIIBBBB"
LOP_DEL_PKT_FMT=">ll"
LOP_GET_PKT_FMT=">llBBBB"
SOP_INS_PKT_FMT=">LIIBBBB"
SOP_GET_PKT_FMT=">LBBBB"
BOP_INS_PKT_FMT=">QLIIBBBB"
BOP_DEL_PKT_FMT=">QQL"
BOP_GET_PKT_FMT=">QQLLBBBB"
GETATTR_RES_FMT=">LLllBBBB"
SETATTR_PKT_FMT=">llBBBB"

# TAP flags
TAP_FLAG_BACKFILL          = 0x01
TAP_FLAG_DUMP              = 0x02
TAP_FLAG_LIST_VBUCKETS     = 0x04
TAP_FLAG_TAKEOVER_VBUCKETS = 0x08

TAP_FLAG_TYPES = {TAP_FLAG_BACKFILL: ">Q"}

# Flags, expiration
SET_PKT_FMT=">II"

# flags
GET_RES_FMT=">I"

# How long until the deletion takes effect.
DEL_PKT_FMT=""

## TAP stuff
# eng-specific length, flags, ttl, [res, res, res]; item flags, exp
TAP_MUTATION_PKT_FMT = "HHbxxxII"
TAP_GENERAL_PKT_FMT = "HHbxxx"

# amount, initial value, expiration
INCRDECR_PKT_FMT=">QQI"
# Special incr expiration that means do not store
INCRDECR_SPECIAL=0xffffffff
INCRDECR_RES_FMT=">Q"

# Time bomb
FLUSH_PKT_FMT=">I"

MAGIC_BYTE = 0x80
REQ_MAGIC_BYTE = 0x80
RES_MAGIC_BYTE = 0x81

# magic, opcode, keylen, extralen, datatype, vbucket, bodylen, opaque, cas
REQ_PKT_FMT=">BBHBBHIIQ"
# magic, opcode, keylen, extralen, datatype, status, bodylen, opaque, cas
RES_PKT_FMT=">BBHBBHIIQ"
# min recv packet size
MIN_RECV_PACKET = struct.calcsize(REQ_PKT_FMT)
# The header sizes don't deviate
assert struct.calcsize(REQ_PKT_FMT) == struct.calcsize(RES_PKT_FMT)

EXTRA_HDR_FMTS={
    CMD_SET: SET_PKT_FMT,
    CMD_ADD: SET_PKT_FMT,
    CMD_REPLACE: SET_PKT_FMT,
    CMD_INCR: INCRDECR_PKT_FMT,
    CMD_DECR: INCRDECR_PKT_FMT,
    CMD_DELETE: DEL_PKT_FMT,
    CMD_FLUSH: FLUSH_PKT_FMT,
    CMD_TAP_MUTATION: TAP_MUTATION_PKT_FMT,
    CMD_TAP_DELETE: TAP_GENERAL_PKT_FMT,
    CMD_TAP_FLUSH: TAP_GENERAL_PKT_FMT,
    CMD_TAP_OPAQUE: TAP_GENERAL_PKT_FMT,
    CMD_TAP_VBUCKET_SET: TAP_GENERAL_PKT_FMT
}

EXTRA_HDR_SIZES=dict(
    [(k, struct.calcsize(v)) for (k,v) in EXTRA_HDR_FMTS.items()])

ERR_UNKNOWN_CMD = 0x81
ERR_NOT_FOUND = 0x1
ERR_EXISTS = 0x2
ERR_AUTH = 0x20
ERR_AUTH_CONTINUE = 0x21

# COLLECTION: error code
CREATED_STORED  = 0x30
DELETED_DROPPED = 0x31
ERR_BADTYPE     = 0x32
ERR_OVERFLOW    = 0x33
ERR_BADVALUE    = 0x34
ERR_INDEXOOR    = 0x35
ERR_BKEYOOR     = 0x36
ERR_ELEM_NOENT  = 0x37
ERR_ELEM_EXISTS = 0x38
ERR_BADATTR     = 0x39

