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

# JHPARK: SOP test begin
    def testSOPInsertGet(self):
        """ Test sop insert, get functionality. """
        fixed = 0
        for x in range (0, 2):
            self.assertNotExists("skey")
            if ((x % 2) == 0):
                fixed = 0
            else:
                fixed = 1
            self.mc.sop_insert("skey", "datum0", 1, 13, fixed)
            self.mc.sop_insert("skey", "datum1")
            self.mc.sop_insert("skey", "datum2")
            self.mc.sop_insert("skey", "datum3")
            self.mc.sop_insert("skey", "datum4")
            self.assertEquals(5, self.mc.getattr("skey", memcacheConstants.ATTR_COUNT))
            self.assertEquals((13, 5, set(["datum0","datum1","datum2","datum3","datum4"])),
                              self.mc.sop_get("skey", 10))
            self.assertEquals(1, self.mc.sop_exist("skey", "datum2"))
            self.assertEquals(1, self.mc.sop_exist("skey", "datum0"))
            self.assertEquals(0, self.mc.sop_exist("skey", "datum8"))
            self.mc.delete("skey")
            self.assertNotExists("skey")

    def testSOPInsertDeletePop(self):
        """ Test sop insert, delete, get with delete functionality. """
        fixed = 0
        for x in range (0, 2):
            self.assertNotExists("skey")
            if ((x % 2) == 0):
                fixed = 0
            else:
                fixed = 1
            self.mc.sop_insert("skey", "datum0", 1, 13, fixed)
            self.mc.sop_insert("skey", "datum1")
            self.mc.sop_insert("skey", "datum2")
            self.mc.sop_insert("skey", "datum3")
            self.mc.sop_insert("skey", "datum4")
            self.mc.sop_insert("skey", "datum5")
            self.mc.sop_insert("skey", "datum6")
            self.mc.sop_insert("skey", "datum7")
            self.mc.sop_insert("skey", "datum8")
            self.mc.sop_insert("skey", "datum9")
            try:
                self.mc.sop_insert("skey", "datum3")
                self.fail("expected ELEMENT_EXISTS.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_ELEM_EXISTS, e.status)
            self.assertEquals(10, self.mc.getattr("skey", memcacheConstants.ATTR_COUNT))
            self.assertEquals((13, 10, set(["datum0", "datum1","datum2","datum3", "datum4",
                                            "datum5", "datum6", "datum7", "datum8", "datum9"])),
                              self.mc.sop_get("skey", 10))
            self.mc.sop_delete("skey", "datum1")
            self.mc.sop_delete("skey", "datum3")
            self.mc.sop_delete("skey", "datum5")
            self.mc.sop_delete("skey", "datum7")
            self.mc.sop_delete("skey", "datum9")
            try:
                self.mc.sop_delete("skey", "datum3")
                self.fail("expected NOT_FOUND_ELEMENT.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_ELEM_NOENT, e.status)
            try:
                self.mc.sop_delete("skey", "datum10")
                self.fail("expected NOT_FOUND_ELEMENT.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_ELEM_NOENT, e.status)
            self.assertEquals((13, 5, set(["datum0", "datum2", "datum4", "datum6", "datum8"])),
                              self.mc.sop_get("skey", 10, 1))
            self.assertNotExists("skey")

    def testSOPInsertFailCheck(self):
        """ Test sop insert error check functionality. """
        self.assertNotExists("skey")
        try:
            self.mc.sop_insert("skey", "datum0")
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.mc.sop_insert("skey", "datum0", 1, 13, 1)
        try:
            self.mc.sop_insert("skey", "datum0")
            self.fail("expected ELEMENT_EXISTS.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_ELEM_EXISTS, e.status)
        try:
            self.mc.sop_insert("skey", "datum_new")
            self.fail("expected bad value.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADVALUE, e.status)
        self.mc.sop_insert("skey", "datum1")
        self.mc.delete("skey")
        self.assertNotExists("skey")

    def testSOPOverflowCheck(self):
        """ Test sop overflow functionality. """
        self.assertNotExists("skey")
        self.mc.sop_insert("skey", "datum1", 1, 13)
        self.mc.setattr("skey", -2, 5, 0) # exptime, maxcount, ovflaction
        self.assertEquals(1, self.mc.getattr("skey", memcacheConstants.ATTR_COUNT))
        self.assertEquals(5, self.mc.getattr("skey", memcacheConstants.ATTR_MAXCOUNT))
        self.assertEquals(memcacheConstants.OVFL_ERROR,
                          self.mc.getattr("skey", memcacheConstants.ATTR_OVFLACTION))
        self.mc.sop_insert("skey", "datum2")
        self.mc.sop_insert("skey", "datum3")
        self.mc.sop_insert("skey", "datum4")
        self.mc.sop_insert("skey", "datum5")
        self.assertEquals((13, 5, set(["datum1","datum2","datum3","datum4","datum5"])),
                          self.mc.sop_get("skey", 5))
        try:
            self.mc.sop_insert("skey", "datum6")
            self.fail("expected data structure full.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_OVERFLOW, e.status)
        try:
            self.mc.setattr("skey", -2, 0, memcacheConstants.OVFL_HEAD_TRIM)
            self.fail("expected bad value.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADVALUE, e.status)
        try:
            self.mc.setattr("skey", -2, 0, memcacheConstants.OVFL_TAIL_TRIM)
            self.fail("expected bad value.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADVALUE, e.status)
        try:
            self.mc.setattr("skey", -2, 0, memcacheConstants.OVFL_SMALLEST_TRIM)
            self.fail("expected bad value.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADVALUE, e.status)
        try:
            self.mc.setattr("skey", -2, 0, memcacheConstants.OVFL_LARGEST_TRIM)
            self.fail("expected bad value.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADVALUE, e.status)
        self.mc.delete("skey")
        self.assertNotExists("skey")

    def testSOPNotFoundError(self):
        """ Test sop not found error functionality. """
        self.assertNotExists("skey")
        try:
            self.mc.sop_get("skey", 1, 1)
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        try:
            self.mc.sop_exist("skey", "datum1")
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        try:
            self.mc.sop_delete("skey", "datum1")
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)

    def testSOPNotSETError(self):
        """ Test sop not set item error functionality. """
        self.mc.set("x", 5, 19, "some value")
        self.assertNotExists("lkey")
        self.assertNotExists("bkey")
        self.mc.lop_insert("lkey", 0, "datum0", 1, 17)
        self.mc.bop_insert("bkey", 1, "datum0", 1, 15)
        try:
            self.mc.sop_insert("x", "datum1")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.sop_insert("lkey", "datum1")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.sop_insert("bkey", "datum1")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.sop_delete("x", "datum0")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.sop_delete("lkey", "datum0")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.sop_delete("bkey", "datum0")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.sop_exist("x", "datum0")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.sop_exist("lkey", "datum0")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.sop_exist("bkey", "datum0")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.sop_get("x", 1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.sop_get("lkey", 1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.sop_get("bkey", 1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        self.mc.delete("lkey")
        self.mc.delete("bkey")
        self.assertNotExists("lkey")
        self.assertNotExists("bkey")

    def testSOPNotKVError(self):
        """ Test sop not kv item error functionality. """
        self.assertNotExists("skey")
        self.mc.sop_insert("skey", "datum1", 1, 13)
        self.mc.sop_insert("skey", "datum2")
        self.mc.sop_insert("skey", "datum3")
        self.assertEquals((13, 3, set(["datum1","datum2","datum3"])),
                          self.mc.sop_get("skey", 5))
        try:
            self.mc.set("skey", 5, 19, "some value")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.replace("skey", 5, 19, "other value")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.prepend("skey", "thing")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.append("skey", "thing")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.incr("skey", 1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.decr("skey", 1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.get("skey")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        self.mc.delete("skey")
        self.assertNotExists("skey")

    def testSOPExpire(self):
        """ Test sop expire functionality. """
        self.assertNotExists("skey")
        self.mc.sop_insert("skey", "datum1", 1, 13)
        self.mc.sop_insert("skey", "datum2")
        self.mc.sop_insert("skey", "datum3")
        self.mc.setattr("skey", 2, 0, 0) # exptime, maxcount, ovflaction
        self.assertEquals((13, 3, set(["datum1","datum2","datum3"])),
                          self.mc.sop_get("skey", 5))
        time.sleep(2.1)
        try:
            self.mc.sop_get("skey", 1, 1)
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.assertNotExists("skey")

    def testSOPFlush(self):
        """ Test sop flush functionality. """
        self.assertNotExists("skey")
        self.mc.sop_insert("skey", "datum1", 1, 13)
        self.mc.sop_insert("skey", "datum2")
        self.mc.sop_insert("skey", "datum3")
        self.assertEquals((13, 3, set(["datum1","datum2","datum3"])),
                          self.mc.sop_get("skey", 5))
        self.mc.flush()
        try:
            self.mc.sop_get("skey", 5)
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.assertNotExists("skey")
# JHPARK: SOP test end

if __name__ == '__main__':
    unittest.main()
