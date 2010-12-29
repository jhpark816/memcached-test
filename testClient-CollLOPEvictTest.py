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

    def getData(self, key, index):
        data = key + "_data_" + str(index)
        return data

    def testLOPLongRun(self):
        """ Test lop long run. """
        key_cnt_ini = 600
        key_cnt_run = 500
        dat_cnt = 4000
        print "LOP prepare begin"  
        for x in range(0, key_cnt_ini):
            lkey = "lkey" + str(x)
            for y in range(0, dat_cnt):
                data = self.getData(lkey, y)
                if y == 0: create = 1
                else:      create = 0
                self.mc.lop_insert(lkey, -1, data, create, 17, 0, 0)
            if x % 10 == 9:
               print "  " + str(x+1) + " lists created"
        print "LOP prepare end"  
        print "LOP run begin"  
        for x in range(0, key_cnt_run):
            """ LOP insert """
            kidx = x + key_cnt_ini
            lkey = "lkey" + str(kidx)
            for y in range(0, dat_cnt):
                data = self.getData(lkey, y)
                if y == 0: create = 1
                else:      create = 0
                self.mc.lop_insert(lkey, -1, data, create, 17, 0, 0)
            """ LOP get """
            for kidx in range(x+key_cnt_ini, x+key_cnt_ini-10, -1):
                lkey = "lkey" + str(kidx)
                """ rcnt: run count """
                """ dcnt: data count """
                """ didx: data index """
                for rcnt in range(0, 10): 
                    vals = [] 
                    lval = random.randrange(0, dat_cnt/2)
                    rval = random.randrange(0, dat_cnt/2)
                    if lval <= rval:
                       dcnt = rval - lval + 1
                       for didx in range(lval, rval+1, 1):
                           data = self.getData(lkey, didx)
                           vals.append(data)
                    else:
                       dcnt = lval - rval + 1;
                       for didx in range(lval, rval-1, -1):
                           data = self.getData(lkey, didx)
                           vals.append(data)
                    self.assertEquals((17, dcnt, vals), self.mc.lop_get(lkey, lval, rval))
            for kidx in range(x, x+10, 1):
                lkey = "lkey" + str(kidx)
                self.assertNotExists(lkey)
            """ LOP delete """
            for kidx in range(x+key_cnt_ini, x+key_cnt_ini-10, -1):
                lkey = "lkey" + str(kidx)
                ccnt = self.mc.getattr(lkey, memcacheConstants.ATTR_COUNT)
                if ccnt > (dat_cnt/2): 
                    lval = random.randrange(dat_cnt/2, ccnt);
                    rval = random.randrange(dat_cnt/2, ccnt);
                    self.mc.lop_delete(lkey, lval, rval);
            """ LOP run check """
            if x % 10 == 9:
               print "  " + str(x+1) + " LOP runs"
        print "LOP run end"  

if __name__ == '__main__':
    unittest.main()
