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

import unittest

import memcacheConstants
from mc_bin_client import MemcachedClient, MemcachedError

class ComplianceTest(unittest.TestCase):

    def setUp(self):
        self.mc=MemcachedClient()
        self.mc.flush()

    def tearDown(self):
        self.mc.flush()
        self.mc.close()

    def assertNotExists(self, key):
        try:
            x=self.mc.get(key)
            self.fail("Expected an exception, got " + `x`)
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)

    def assertGet(self, exp, gv):
        self.assertTrue(gv is not None)
        self.assertEquals((gv[0], gv[2]), exp)

# JHPARK: LOP test begin
    def testLOPInsertGet(self):
        """ Test lop insert, get functionality. """
        fixed = 0
        for x in range (0, 2):
            self.assertNotExists("lkey")
            if ((x % 2) == 0):
                fixed = 0
            else:
                fixed = 1
            self.mc.lop_insert("lkey", 0, "datum0", 1, 17, fixed)
            self.mc.lop_insert("lkey", 1, "datum1")
            self.mc.lop_insert("lkey", 2, "datum2")
            self.mc.lop_insert("lkey", 3, "datum3")
            self.mc.lop_insert("lkey", 4, "datum4")
            self.assertEquals(5, self.mc.getattr("lkey", memcacheConstants.ATTR_COUNT))
            self.assertEquals((17, 5, ["datum0", "datum1","datum2","datum3", "datum4"]),
                              self.mc.lop_get("lkey", 0, -1))
            self.assertEquals((17, 3, ["datum0", "datum1", "datum2"]),
                              self.mc.lop_get("lkey", 0, 2))
            self.assertEquals((17, 3, ["datum2", "datum3", "datum4"]),
                              self.mc.lop_get("lkey", 2, 8))
            self.assertEquals((17, 2, ["datum4", "datum3"]), self.mc.lop_get("lkey", -1, -2))
            self.assertEquals((17, 2, ["datum2", "datum3"]), self.mc.lop_get("lkey", -3, 3))
            self.assertEquals((17, 2, ["datum3", "datum2"]), self.mc.lop_get("lkey", -2, 2))
            self.assertEquals((17, 4, ["datum0", "datum1", "datum2", "datum3"]),
                              self.mc.lop_get("lkey", 0, -2))
            self.assertEquals((17, 3, ["datum4", "datum3", "datum2"]),
                              self.mc.lop_get("lkey", 6, -3))
            self.assertEquals((17, 1, ["datum1"]), self.mc.lop_get("lkey", 1, 1))
            try:
                self.mc.lop_get("lkey", 6, 8)
                self.fail("expected index out of range.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_INDEXOOR, e.status)
            try:
                self.mc.lop_get("lkey", -10, -8)
                self.fail("expected index out of range.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_INDEXOOR, e.status)
            self.mc.delete("lkey")
            self.assertNotExists("lkey")

    def testLOPInsertDeletePop(self):
        """ Test lop insert, delete, get with delete functionality. """
        fixed = 0
        for x in range (0, 2):
            self.assertNotExists("lkey")
            if ((x % 2) == 0):
                fixed = 0
            else:
                fixed = 1
            self.mc.lop_insert("lkey", 0, "datum0", 1, 17, fixed)
            self.mc.lop_insert("lkey", -1, "datum9")
            self.mc.lop_insert("lkey", 1, "datum1")
            self.mc.lop_insert("lkey", -2, "datum8")
            self.mc.lop_insert("lkey", 2, "datum2")
            self.mc.lop_insert("lkey", -3, "datum7")
            self.mc.lop_insert("lkey", 3, "datum3")
            self.mc.lop_insert("lkey", -4, "datum6")
            self.mc.lop_insert("lkey", 4, "datum4")
            self.mc.lop_insert("lkey", -5, "datum5")
            self.assertEquals(10, self.mc.getattr("lkey", memcacheConstants.ATTR_COUNT))
            self.assertEquals((17, 10, ["datum0", "datum1","datum2","datum3", "datum4", 
                                        "datum5", "datum6", "datum7", "datum8", "datum9"]),
                              self.mc.lop_get("lkey", 0, -1))
            self.mc.lop_delete("lkey", 8, -3)
            self.mc.lop_delete("lkey", 1, 3)
            try:
                self.mc.lop_delete("lkey", 7, 9)
                self.fail("expected index out of range.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_INDEXOOR, e.status)
            try:
                self.mc.lop_delete("lkey", -9, -8)
                self.fail("expected index out of range.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_INDEXOOR, e.status)
            self.assertEquals(5, self.mc.getattr("lkey", memcacheConstants.ATTR_COUNT))
            self.assertEquals((17, 5, ["datum0", "datum4", "datum5", "datum6", "datum9"]),
                              self.mc.lop_get("lkey", 0, -1))
            self.assertEquals((17, 2, ["datum9","datum6"]), self.mc.lop_get("lkey", 9, -2, 1))
            self.assertEquals((17, 1, ["datum4"]), self.mc.lop_get("lkey", 1, 1, 1))
            try:
                self.mc.lop_get("lkey", 4, 5, 1)
                self.fail("expected index out of range.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_INDEXOOR, e.status)
            try:
                self.mc.lop_get("lkey", -5, -7, 1)
                self.fail("expected index out of range.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_INDEXOOR, e.status)
            self.assertEquals(2, self.mc.getattr("lkey", memcacheConstants.ATTR_COUNT))
            self.assertEquals((17, 2, ["datum0", "datum5"]), self.mc.lop_get("lkey", 0, -1))
            self.mc.lop_delete("lkey", 0, -1)
            self.assertNotExists("lkey")

    def testLOPInsertFailCheck(self):
        """ Test lop insert error check functionality. """
        self.assertNotExists("lkey")
        try:
            self.mc.lop_insert("lkey", 0, "datum0")
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.mc.lop_insert("lkey", 0, "datum0", 1, 17, 1)
        try:
            self.mc.lop_insert("lkey", 2, "datum2")
            self.fail("expected index out of range.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_INDEXOOR, e.status)
        try:
            self.mc.lop_insert("lkey", -3, "datum2")
            self.fail("expected index out of range.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_INDEXOOR, e.status)
        #try:
        #    self.mc.lop_insert("lkey", 1, "datum_new")
        #    self.fail("expected bad value.")
        #except MemcachedError, e:
        #    self.assertEquals(memcacheConstants.ERR_BADVALUE, e.status)
        self.mc.lop_insert("lkey", 1, "datum1")
        self.mc.delete("lkey")
        self.assertNotExists("lkey")

    def testLOPOverflowCheck(self):
        """ Test lop overflow functionality. """
        self.assertNotExists("lkey")
        self.mc.lop_insert("lkey", -1, "datum1", 1, 17)
        self.mc.setattr("lkey", -2, 5, 0); # exptime, maxcount, ovflactoin
        self.assertEquals(1, self.mc.getattr("lkey", memcacheConstants.ATTR_COUNT))
        self.assertEquals(5, self.mc.getattr("lkey", memcacheConstants.ATTR_MAXCOUNT))
        self.assertEquals(memcacheConstants.OVFL_TAIL_TRIM,
                          self.mc.getattr("lkey", memcacheConstants.ATTR_OVFLACTION))
        self.mc.lop_insert("lkey", -1, "datum2")
        self.mc.lop_insert("lkey", -1, "datum3")
        self.mc.lop_insert("lkey", -1, "datum4")
        self.mc.lop_insert("lkey", -1, "datum5")
        self.assertEquals((17, 5, ["datum1","datum2","datum3","datum4","datum5"]),
                          self.mc.lop_get("lkey", 0, -1))
        try:
            self.mc.lop_insert("lkey", 5, "datum6")
            self.fail("expected index out of range.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_INDEXOOR, e.status)
        try:
            self.mc.lop_insert("lkey", -6, "datum6")
            self.fail("expected index out of range.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_INDEXOOR, e.status)
        self.mc.lop_insert("lkey", 2, "datum0")
        self.mc.lop_insert("lkey", 0, "datum6")
        self.mc.lop_insert("lkey", 0, "datum7")
        self.mc.lop_insert("lkey", -1, "datum8")
        self.assertEquals((17, 5, ["datum6","datum1","datum2","datum0","datum8"]),
                          self.mc.lop_get("lkey", 0, -1))
        self.mc.lop_insert("lkey", 4, "datum9")
        self.assertEquals((17, 5, ["datum1","datum2","datum0","datum8","datum9"]),
                          self.mc.lop_get("lkey", 0, -1))
        self.mc.lop_insert("lkey", -5, "datum3")
        self.assertEquals((17, 5, ["datum3","datum1","datum2","datum0","datum8"]),
                          self.mc.lop_get("lkey", 0, -1))
        self.mc.setattr("lkey", -2, 0, memcacheConstants.OVFL_HEAD_TRIM) # exptime, maxcount, ovflaction
        self.assertEquals(memcacheConstants.OVFL_HEAD_TRIM,
                          self.mc.getattr("lkey", memcacheConstants.ATTR_OVFLACTION))
        self.mc.lop_insert("lkey", 2, "datums")
        self.assertEquals((17, 5, ["datum1","datum2","datums","datum0","datum8"]),
                          self.mc.lop_get("lkey", 0, -1))
        self.mc.lop_insert("lkey", 0, "datumt")
        self.assertEquals((17, 5, ["datumt","datum1","datum2","datums","datum0"]),
                          self.mc.lop_get("lkey", 0, -1))
        self.mc.setattr("lkey", -2, 0, memcacheConstants.OVFL_ERROR) # exptime, maxcount, ovflaction
        self.assertEquals(memcacheConstants.OVFL_ERROR,
                          self.mc.getattr("lkey", memcacheConstants.ATTR_OVFLACTION))
        try:
            self.mc.lop_insert("lkey", 2, "datumu")
            self.fail("expected datu structure full.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_OVERFLOW, e.status)
        try:
            self.mc.lop_insert("lkey", 0, "datumu")
            self.fail("expected datu structure full.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_OVERFLOW, e.status)
        try:
            self.mc.lop_insert("lkey", -1, "datumu")
            self.fail("expected datu structure full.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_OVERFLOW, e.status)
        self.assertEquals((17, 5, ["datumt","datum1","datum2","datums","datum0"]),
                          self.mc.lop_get("lkey", 0, -1))
        try:
            self.mc.setattr("lkey", -2, 0, memcacheConstants.OVFL_SMALLEST_TRIM)
            self.fail("expected bad value.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADVALUE, e.status)
        try:
            self.mc.setattr("lkey", -2, 0, memcacheConstants.OVFL_LARGEST_TRIM)
            self.fail("expected bad value.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADVALUE, e.status)
        self.mc.delete("lkey")
        self.assertNotExists("lkey")

    def testLOPNotFoundError(self):
        """ Test lop not found error functionality. """
        self.assertNotExists("lkey")
        try:
            self.mc.lop_get("lkey", 0, 0)
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        try:
            self.mc.lop_delete("lkey", 0, 0)
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)

    def testLOPNotLISTError(self):
        """ Test lop not list item error functionality. """
        self.mc.set("x", 5, 19, "some value")
        self.assertNotExists("skey")
        self.assertNotExists("bkey")
        self.mc.sop_insert("skey", "datum0", 1, 13)
        self.mc.bop_insert("bkey", 1, "datum0", 1, 15)
        try:
            self.mc.lop_insert("x", -1, "datum1")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.lop_insert("skey", -1, "datum1")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.lop_insert("bkey", -1, "datum1")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.lop_get("x", 0, -1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.lop_get("skey", 0, -1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.lop_get("bkey", 0, -1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.lop_delete("x", 0, -1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.lop_delete("skey", 0, -1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.lop_delete("bkey", 0, -1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        self.mc.delete("skey")
        self.mc.delete("bkey")
        self.assertNotExists("skey")
        self.assertNotExists("bkey")

    def testLOPNotKVError(self):
        """ Test lop not kv item error functionality. """
        self.assertNotExists("lkey")
        self.mc.lop_insert("lkey", -1, "datum1", 1, 17, 1)
        self.mc.lop_insert("lkey", -1, "datum2")
        self.mc.lop_insert("lkey", -1, "datum3")
        self.assertEquals((17, 3, ["datum1","datum2","datum3"]),
                          self.mc.lop_get("lkey", 0, -1))
        try:
            self.mc.set("lkey", 5, 19, "some value")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.replace("lkey", 5, 19, "other value")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.prepend("lkey", "thing")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.append("lkey", "thing")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.incr("lkey", 1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.decr("lkey", 1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.get("lkey")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        self.mc.delete("lkey")
        self.assertNotExists("lkey")

    def testLOPExpire(self):
        """ Test lop expire functionality. """
        self.assertNotExists("lkey")
        self.mc.lop_insert("lkey", -1, "datum1", 1, 17)
        self.mc.lop_insert("lkey", -1, "datum2")
        self.mc.lop_insert("lkey", -1, "datum3")
        self.mc.setattr("lkey", 2, 0, 0); # exptime, maxcount, ovflactoin
        self.assertEquals((17, 3, ["datum1","datum2","datum3"]),
                          self.mc.lop_get("lkey", 0, -1))
        time.sleep(2.1)
        try:
            self.mc.lop_get("lkey", 0, -1)
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.assertNotExists("lkey")

    def testLOPFlush(self):
        """ Test lop flush functionality. """
        self.assertNotExists("lkey")
        self.mc.lop_insert("lkey", -1, "datum1", 1, 17)
        self.mc.lop_insert("lkey", -1, "datum2")
        self.mc.lop_insert("lkey", -1, "datum3")
        self.assertEquals((17, 3, ["datum1","datum2","datum3"]),
                          self.mc.lop_get("lkey", 0, -1))
        self.mc.flush()
        try:
            self.mc.lop_get("lkey", 0, -1)
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.assertNotExists("lkey")
# JHPARK: LOP test end

if __name__ == '__main__':
    unittest.main()
