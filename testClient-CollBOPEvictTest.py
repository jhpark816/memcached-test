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

    def getData(self, key, index, fixed):
        data = key + "_data_" + str(index)
        if (fixed == 1 and len(data) < 20):
            for s in range(len(data), 20):
                data = data + "A"
        return data

    def testBOPLongRun(self):
        """ Test bop long run. """
        key_cnt_ini = 500
        key_cnt_run = 500
        dat_cnt = 4000
        print "BOP prepare begin"  
        for x in range(0, key_cnt_ini):
            bkey = "bkey" + str(x)
            if x % 2 == 0: fixed = 0
            else:          fixed = 1
            for y in range(0, dat_cnt):
                data = self.getData(bkey, y, fixed)
                if y == 0: create = 1
                else:      create = 0
                self.mc.bop_insert(bkey, y, data, create, 11, fixed)
            if x % 10 == 9:
               print "  " + str(x+1) + " b+trees created"
        print "BOP prepare end"  
        print "BOP run begin"  
        for x in range(0, key_cnt_run):
            """ BOP insert """
            bkey = "bkey" + str(x+key_cnt_ini)
            if x % 2 == 0: fixed = 0
            else:          fixed = 1
            for y in range(0, dat_cnt):
                data = self.getData(bkey, y, fixed)
                if y == 0: create = 1
                else:      create = 0
                self.mc.bop_insert(bkey, y, data, create, 11, fixed)
            """ BOP get """
            """ rcnt: run count """
            """ dcnt: data count """
            """ didx: data index """
            for kidx in range(x+key_cnt_ini, x+key_cnt_ini-10, -1):
                bkey = "bkey" + str(kidx)
                if kidx % 2 == 0: fixed = 0
                else:             fixed = 1
                for rcnt in range(0, 10): 
                    keys = [] 
                    vals = []
                    lkey = random.randrange(0, dat_cnt/2)
                    rkey = random.randrange(0, dat_cnt/2)
                    if lkey <= rkey:
                       dcnt = rkey - lkey + 1
                       for didx in range(lkey, rkey+1, 1):
                           data = self.getData(bkey, didx, fixed)
                           keys.append(didx)
                           vals.append(data)
                    else:
                       dcnt = lkey - rkey + 1;
                       for didx in range(lkey, rkey-1, -1):
                           data = self.getData(bkey, didx, fixed)
                           keys.append(didx)
                           vals.append(data)
                    self.assertEquals((11, dcnt, keys, vals), self.mc.bop_get(bkey, lkey, rkey))
            for kidx in range(x, x+10, 1):
                bkey = "bkey" + str(kidx)
                self.assertNotExists(bkey)
            """ BOP delete """
            kidx = x + key_cnt_ini
            bkey = "bkey" + str(kidx)
            if kidx % 2 == 0: fixed = 0
            else:             fixed = 1
            lkey = random.randrange(dat_cnt/2, dat_cnt)
            rkey = random.randrange(dat_cnt/2, dat_cnt)
            self.mc.bop_delete(bkey, lkey, rkey)
            """ BOP run check """
            if x % 10 == 9:
               print "  " + str(x+1) + " BOP runs"
        print "BOP run end"  

if __name__ == '__main__':
    unittest.main()
