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

# JHPARK: BOP test begin
    def bulkBOPInsert(self, key, from_bkey, to_bkey, width, create=0, flags=0, fixed=0):
        if (from_bkey <= to_bkey):
            to_bkey = to_bkey + 1
            increment = width
        else: 
            to_bkey = to_bkey - 1
            increment = -width
        for x in range(from_bkey, to_bkey, increment):
            if (fixed == 0): 
                data = key + "_data_" + str(x)
            else:
                data = key + "_data_"
            self.mc.bop_insert(key, x, data, create, flags, fixed)

    def assertBOPGet(self, key, width, flags, fixed, from_bkey, to_bkey, offset=0, count=0, delete=0):
        valcnt = 0
        res_bkey = []
        res_data = []
        saved_from_bkey = from_bkey
        saved_to_bkey   = to_bkey
        if (from_bkey <= to_bkey):
            if ((from_bkey % width) != 0): 
                from_bkey = from_bkey + (width - (from_bkey % width))
            if (offset > 0):
                from_bkey = from_bkey + (offset * width) 
            if ((to_bkey % width) != 0):
                to_bkey = to_bkey - (to_bkey % width)
            to_bkey = to_bkey + 1  
            increment = width
        else: 
            if ((from_bkey % width) != 0): 
                from_bkey = from_bkey - (from_bkey % width)
            if (offset > 0):
                from_bkey = from_bkey - (offset * width) 
            if ((to_bkey % width) != 0):
                to_bkey = to_bkey + (width - (to_bkey % width))
            to_bkey = to_bkey - 1
            increment = -width
        for x in range (from_bkey, to_bkey, increment):
            if (fixed == 0):
                data = key + "_data_" + str(x)
            else: 
                data = key + "_data_"
            res_bkey.append(x)
            res_data.append(data) 
            valcnt = valcnt + 1
            if (count > 0 and valcnt >= count):
                break
        self.assertEquals((flags, valcnt, res_bkey, res_data),
                          self.mc.bop_get(key, saved_from_bkey, saved_to_bkey, offset, count, delete))
          
    def testBOPInsertGet(self):
        """ Test bop insert, get functionality. """
        min    = 10
        max    = 10000
        width  = 10 
        create = 1
        flags  = 11
        fixed  = 0 
        for x in range (0, 14):
            """print "testBOPInsertGet" + str(x)"""
            self.assertNotExists("bkey")
            if ((x % 2) == 0):
                fixed = 0
            else:
                fixed = 1
            if (x < 2):  
                self.bulkBOPInsert("bkey", min, max, width, create, flags, fixed)
            elif (x >= 2 and x < 4):
                self.bulkBOPInsert("bkey", max, min, width, create, flags, fixed)
            elif (x >= 4 and x < 6): 
                self.bulkBOPInsert("bkey", min, max, width*2, create, flags, fixed)
                self.bulkBOPInsert("bkey", min+width, max, width*2, create, flags, fixed)
            elif (x >= 6 and x < 8): 
                self.bulkBOPInsert("bkey", max, min, width*2, create, flags, fixed)
                self.bulkBOPInsert("bkey", max-width, min, width*2, create, flags, fixed)
            elif (x >= 8 and x < 10): 
                self.bulkBOPInsert("bkey", min+(0*width), max, width*3, create, flags, fixed)
                self.bulkBOPInsert("bkey", min+(1*width), max, width*3, create, flags, fixed)
                self.bulkBOPInsert("bkey", min+(2*width), max, width*3, create, flags, fixed)
            elif (x >= 10 and x < 12): 
                self.bulkBOPInsert("bkey", max-(0*width), min, width*3, create, flags, fixed)
                self.bulkBOPInsert("bkey", max-(1*width), min, width*3, create, flags, fixed)
                self.bulkBOPInsert("bkey", max-(2*width), min, width*3, create, flags, fixed)
            else:
                self.bulkBOPInsert("bkey", min, max, width*4, create, flags, fixed)
                self.bulkBOPInsert("bkey", (min+max)-(1*width), min, width*4, create, flags, fixed)
                self.bulkBOPInsert("bkey", min+(2*width), max, width*4, create, flags, fixed)
                self.bulkBOPInsert("bkey", (min+max)-(3*width), min, width*4, create, flags, fixed)
            self.assertEquals(1000, self.mc.getattr("bkey", memcacheConstants.ATTR_COUNT))
            self.assertBOPGet("bkey", width, flags, fixed, 6500, 6500)
            self.assertBOPGet("bkey", width, flags, fixed, 2300, 2300)
            self.assertBOPGet("bkey", width, flags, fixed, 2000, 2255)
            self.assertBOPGet("bkey", width, flags, fixed, 2000, 2255, 10, 20)
            self.assertBOPGet("bkey", width, flags, fixed, 2000, 2255, 10, 0)
            self.assertBOPGet("bkey", width, flags, fixed, 8700, 150)
            self.assertBOPGet("bkey", width, flags, fixed, 7690, 8870, 0, 50)
            self.assertBOPGet("bkey", width, flags, fixed, 6540, 2300, 0, 80)
            self.assertBOPGet("bkey", width, flags, fixed, 6540, 2300, 40, 40);
            self.assertBOPGet("bkey", width, flags, fixed, 6540, 2300, 40, 0);
            self.assertBOPGet("bkey", width, flags, fixed, 5, 1000, 0, 20)
            self.assertBOPGet("bkey", width, flags, fixed, 10005, 9000, 0, 30)
            try:
                self.mc.bop_get("bkey", 655, 655)
                self.fail("expected NOT_FOUND_ELEMENT.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_ELEM_NOENT, e.status)
            try:
                self.mc.bop_get("bkey", 0, 5)
                self.fail("expected NOT_FOUND_ELEMENT.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_ELEM_NOENT, e.status)
            try:
                self.mc.bop_get("bkey", 20000, 15000)
                self.fail("expected NOT_FOUND_ELEMENT.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_ELEM_NOENT, e.status)
            self.mc.delete("bkey")
            self.assertNotExists("bkey")

    def testBOPInsertDeletePop(self):
        """ Test bop insert, delete, get with delete functionality. """
        min    = 10
        max    = 20000
        width  = 10 
        create = 1
        flags  = 11
        fixed  = 0
        for x in range (0, 14):
            """print "testBOPInsertDeletePop" + str(x)"""
            self.assertNotExists("bkey")
            if ((x % 2) == 0):
                fixed = 0
            else:
                fixed = 1
            if (x < 2):  
                self.bulkBOPInsert("bkey", min, max, width, create, flags, fixed)
            elif (x >= 2 and x < 4):
                self.bulkBOPInsert("bkey", max, min, width, create, flags, fixed)
            elif (x >= 4 and x < 6): 
                self.bulkBOPInsert("bkey", min, max, width*2, create, flags, fixed)
                self.bulkBOPInsert("bkey", min+width, max, width*2, create, flags, fixed)
            elif (x >= 6 and x < 8): 
                self.bulkBOPInsert("bkey", max, min, width*2, create, flags, fixed)
                self.bulkBOPInsert("bkey", max-width, min, width*2, create, flags, fixed)
            elif (x >= 8 and x < 10): 
                self.bulkBOPInsert("bkey", min+(0*width), max, width*3, create, flags, fixed)
                self.bulkBOPInsert("bkey", min+(1*width), max, width*3, create, flags, fixed)
                self.bulkBOPInsert("bkey", min+(2*width), max, width*3, create, flags, fixed)
            elif (x >= 10 and x < 12): 
                self.bulkBOPInsert("bkey", max-(0*width), min, width*3, create, flags, fixed)
                self.bulkBOPInsert("bkey", max-(1*width), min, width*3, create, flags, fixed)
                self.bulkBOPInsert("bkey", max-(2*width), min, width*3, create, flags, fixed)
            else:
                self.bulkBOPInsert("bkey", min, max, width*4, create, flags, fixed)
                self.bulkBOPInsert("bkey", (min+max)-(1*width), min, width*4, create, flags, fixed)
                self.bulkBOPInsert("bkey", min+(2*width), max, width*4, create, flags, fixed)
                self.bulkBOPInsert("bkey", (min+max)-(3*width), min, width*4, create, flags, fixed)
            self.assertEquals(2000, self.mc.getattr("bkey", memcacheConstants.ATTR_COUNT))
            self.mc.bop_delete("bkey", 1000, 1999);
            self.mc.bop_delete("bkey", 10000, 0, 100);
            self.mc.bop_delete("bkey", 15550, 17000, 50);
            self.mc.bop_delete("bkey", 8700, 4350, 50);
            self.mc.bop_delete("bkey", 0, 2000, 100);
            self.mc.bop_delete("bkey", 2020, 2020);
            self.mc.bop_delete("bkey", 22000, 2000, 100);
            self.assertEquals(1499, self.mc.getattr("bkey", memcacheConstants.ATTR_COUNT))
            self.assertBOPGet("bkey", width, flags, fixed, 3000, 5000, 0, 100, 1)
            self.assertBOPGet("bkey", width, flags, fixed, 13000, 11000, 50, 50, 1)
            self.assertBOPGet("bkey", width, flags, fixed, 13000, 11000, 0, 50, 1)
            self.assertBOPGet("bkey", width, flags, fixed, 13400, 15300, 0, 50, 1)
            self.assertBOPGet("bkey", width, flags, fixed, 7200, 5980, 0, 50, 1)
            self.assertEquals(1199, self.mc.getattr("bkey", memcacheConstants.ATTR_COUNT))
            self.assertBOPGet("bkey", width, flags, fixed, 5800, 6200, 0, 40)
            self.assertBOPGet("bkey", width, flags, fixed, 5820, 610, 0, 70)
            self.assertBOPGet("bkey", width, flags, fixed, 2100, 3200, 0, 60)
            self.assertBOPGet("bkey", width, flags, fixed, 15000, 14000, 0, 30)
            self.assertBOPGet("bkey", width, flags, fixed, 14200, 14400, 0, 100)
            self.assertBOPGet("bkey", width, flags, fixed, 14200, 14900, 0, 100)
            self.assertEquals(1199, self.mc.getattr("bkey", memcacheConstants.ATTR_COUNT))
            if (fixed == 0):
                self.assertEquals((flags, 1, [2010], ["bkey_data_2010"]),
                                  self.mc.bop_get("bkey", 0, 2010))
                self.assertEquals((flags, 2, [10010, 9000], ["bkey_data_10010","bkey_data_9000"]),
                                  self.mc.bop_get("bkey", 10010, 9000))
            else:
                self.assertEquals((flags, 1, [2010], ["bkey_data_"]),
                                  self.mc.bop_get("bkey", 0, 2010))
                self.assertEquals((flags, 2, [10010, 9000], ["bkey_data_","bkey_data_"]),
                                  self.mc.bop_get("bkey", 10010, 9000))
            try:
                self.mc.bop_get("bkey", 0, 900)
                self.fail("expected NOT_FOUND_ELEMENT.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_ELEM_NOENT, e.status)
            try:
                self.mc.bop_get("bkey", 20000, 19100)
                self.fail("expected NOT_FOUND_ELEMENT.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_ELEM_NOENT, e.status)
            try:
                self.mc.bop_delete("bkey", 0, 900)
                self.fail("expected NOT_FOUND_ELEMENT.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_ELEM_NOENT, e.status)
            try:
                self.mc.bop_get("bkey", 0, 900, 0, 1)
                self.fail("expected NOT_FOUND_ELEMENT.")
            except MemcachedError, e:
                self.assertEquals(memcacheConstants.ERR_ELEM_NOENT, e.status)
            self.mc.bop_delete("bkey", 0, 100000);
            self.assertNotExists("bkey")

    def testBOPInsertFailCheck(self):
        """ Test bop insert error check functionality. """
        self.assertNotExists("bkey")
        try:
            self.mc.bop_insert("bkey", 10, "datum1")
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.mc.bop_insert("bkey", 10, "datum1", 1, 11, 1)
        try:
            self.mc.bop_insert("bkey", 10, "datum1")
            self.fail("expected ELEMENT_EXISTS.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_ELEM_EXISTS, e.status)
        try:
            self.mc.bop_insert("bkey", 20, "datum_new")
            self.fail("expected bad value.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADVALUE, e.status)
        self.mc.bop_insert("bkey", 20, "datum2")
        self.mc.delete("bkey")
        self.assertNotExists("bkey")

    def testBOPOverflowCheck(self):
        """ Test bop overflow functionality. """
        self.assertNotExists("bkey")
        self.mc.bop_insert("bkey", 10, "datum1", 1, 11, 1)
        self.mc.setattr("bkey", -2, 5, 0); # exptime, maxcount, ovflaction 
        self.assertEquals(1, self.mc.getattr("bkey", memcacheConstants.ATTR_COUNT))
        self.assertEquals(5, self.mc.getattr("bkey", memcacheConstants.ATTR_MAXCOUNT))
        self.assertEquals(memcacheConstants.OVFL_SMALLEST_TRIM,
                          self.mc.getattr("bkey", memcacheConstants.ATTR_OVFLACTION))
        self.mc.bop_insert("bkey", 30, "datum3")
        self.mc.bop_insert("bkey", 50, "datum5")
        self.mc.bop_insert("bkey", 70, "datum7")
        self.mc.bop_insert("bkey", 90, "datum9")
        self.assertEquals(5, self.mc.getattr("bkey", memcacheConstants.ATTR_COUNT))
        self.mc.bop_insert("bkey", 80, "datum8")
        try:
            self.mc.bop_insert("bkey", 10, "datum1")
            self.fail("expected bkey out of range.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BKEYOOR, e.status)
        self.mc.bop_insert("bkey", 60, "datum6")
        self.assertEquals((11, 5, [50, 60, 70, 80, 90], ["datum5","datum6","datum7","datum8","datum9"]),
                          self.mc.bop_get("bkey", 0, 100))
        self.mc.setattr("bkey", -2, 0, memcacheConstants.OVFL_LARGEST_TRIM) # exptime, maxcount, ovflaction 
        self.mc.bop_insert("bkey", 30, "datum3")
        self.mc.bop_insert("bkey", 40, "datum4")
        try:
            self.mc.bop_insert("bkey", 90, "datum9")
            self.fail("expected bkey out of range.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BKEYOOR, e.status)
        self.assertEquals((11, 5, [30, 40, 50, 60, 70], ["datum3","datum4","datum5","datum6","datum7" ]),
                          self.mc.bop_get("bkey", 0, 100))
        self.mc.setattr("bkey", -2, 0, memcacheConstants.OVFL_ERROR) # exptime, maxcount, ovflaction 
        try:
            self.mc.bop_insert("bkey", 20, "datum2")
            self.fail("expected data structure full.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_OVERFLOW, e.status)
        try:
            self.mc.bop_insert("bkey", 80, "datum2")
            self.fail("expected data structure full.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_OVERFLOW, e.status)
        try:
            self.mc.setattr("bkey", -2, 0, memcacheConstants.OVFL_HEAD_TRIM)
            self.fail("expected bad value.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADVALUE, e.status)
        try:
            self.mc.setattr("bkey", -2, 0, memcacheConstants.OVFL_TAIL_TRIM)
            self.fail("expected bad value.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADVALUE, e.status)
        self.mc.delete("bkey")
        self.assertNotExists("bkey")

    def testBOPNotFoundError(self):
        """ Test bop not found error functionality. """
        self.assertNotExists("bkey")
        try:
            self.mc.bop_get("bkey", 0, 100)
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        try:
            self.mc.bop_delete("bkey", 0, 100)
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)

    def testBOPNotBTREError(self):
        """ Test bop not b+tree item error functionality. """
        self.mc.set("x", 5, 19, "some value")
        self.assertNotExists("lkey")
        self.assertNotExists("skey")
        self.mc.lop_insert("lkey", 0, "datum1", 1, 13)
        self.mc.sop_insert("skey", "datum1", 1, 15)
        try:
            self.mc.bop_insert("x", 10, "datum10")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.bop_insert("lkey", 10, "datum10")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.bop_insert("skey", 10, "datum10")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.bop_get("x", 0, 100)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.bop_get("lkey", 0, 100)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.bop_get("skey", 0, 100)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.bop_delete("x", 0, 100)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.bop_delete("lkey", 0, 100)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.bop_delete("skey", 0, 100)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        self.mc.delete("lkey")
        self.mc.delete("skey")
        self.assertNotExists("lkey")
        self.assertNotExists("skey")

    def testBOPNotKVError(self):
        """ Test lop not kv item error functionality. """
        self.assertNotExists("bkey")
        self.mc.bop_insert("bkey", 10, "datum1", 1, 17, 1)
        self.mc.bop_insert("bkey", 20, "datum2")
        self.mc.bop_insert("bkey", 30, "datum3")
        self.assertEquals((17, 3, [10, 20, 30], ["datum1","datum2","datum3"]),
                          self.mc.bop_get("bkey", 0, 100))
        try:
            self.mc.set("bkey", 5, 19, "some value")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.replace("bkey", 5, 19, "other value")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.prepend("bkey", "thing")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.append("bkey", "thing")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.incr("bkey", 1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.decr("bkey", 1)
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        try:
            self.mc.get("bkey")
            self.fail("expected not supported operation, bad type.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_BADTYPE, e.status)
        self.mc.delete("bkey")
        self.assertNotExists("bkey")

    def testBOPExpire(self):
        """ Test bop expire functionality. """
        self.assertNotExists("bkey")
        self.mc.bop_insert("bkey", 10, "datum1", 1, 17, 1)
        self.mc.bop_insert("bkey", 20, "datum2")
        self.mc.bop_insert("bkey", 30, "datum3")
        self.mc.setattr("bkey", 2, 0, 0); # exptime, maxcount, ovflaction
        self.assertEquals((17, 3, [10, 20, 30], ["datum1","datum2","datum3"]),
                          self.mc.bop_get("bkey", 0, 100))
        time.sleep(2.1)
        try:
            self.mc.bop_get("bkey", 0, 100)
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.assertNotExists("bkey")

    def testBOPFlush(self):
        """ Test bop flush functionality. """
        self.assertNotExists("bkey")
        self.mc.bop_insert("bkey", 10, "datum1", 1, 17, 1)
        self.mc.bop_insert("bkey", 20, "datum2")
        self.mc.bop_insert("bkey", 30, "datum3")
        self.assertEquals((17, 3, [10, 20, 30], ["datum1","datum2","datum3"]),
                          self.mc.bop_get("bkey", 0, 100))
        self.mc.flush()
        try:
            self.mc.bop_get("bkey", 0, 100)
            self.fail("expected not found error.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.assertNotExists("bkey")
# JHPARK: BOP test end

if __name__ == '__main__':
    unittest.main()
