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

    def testSOPLongRun(self):
        """ Test sop long run. """
        key_cnt_ini = 500
        key_cnt_run = 500
        dat_cnt = 4000
        print "SOP prepare begin"  
        for x in range(0, key_cnt_ini):
            skey = "skey" + str(x)
            if x % 2 == 0: fixed = 0
            else:          fixed = 1
            for y in range(0, dat_cnt):
                data = self.getData(skey, y, fixed)
                if y == 0: create = 1
                else:      create = 0
                self.mc.sop_insert(skey, data, create, 13, fixed)
            if x % 10 == 9:
               print "  " + str(x+1) + " sets created"
        print "SOP prepare end"  
        print "SOP run begin"  
        for x in range(0, key_cnt_run):
            """ SOP insert """
            skey = "skey" + str(x+key_cnt_ini)
            if x % 2 == 0: fixed = 0
            else:          fixed = 1
            vals = []
            for y in range(0, dat_cnt):
                data = self.getData(skey, y, fixed)
                vals.append(data) 
                if y == 0: create = 1
                else:      create = 0
                self.mc.sop_insert(skey, data, create, 13, fixed)
            """ SOP get """
            self.assertEquals((13, dat_cnt, set(vals)), self.mc.sop_get(skey, dat_cnt))
            """ rcnt: run count """
            """ didx: data index """
            for kidx in range(x+key_cnt_ini, x+key_cnt_ini-10, -1):
                skey = "skey" + str(kidx)
                if kidx % 2 == 0: fixed = 0
                else:             fixed = 1
                for rcnt in range(0, 50): 
                    didx = random.randrange(0, dat_cnt/2)
                    data = self.getData(skey, didx, fixed)
                    self.assertEquals(1, self.mc.sop_exist(skey, data))
            for kidx in range(x, x+10, 1):
                skey = "skey" + str(kidx)
                self.assertNotExists(skey)
            """ SOP delete """
            for kidx in range(x+key_cnt_ini, x+key_cnt_ini-10, -1):
                skey = "skey" + str(kidx)
                if kidx % 2 == 0: fixed = 0
                else:             fixed = 1
                for rcnt in range(0, 50): 
                    didx = random.randrange(dat_cnt/2, dat_cnt)
                    data = self.getData(skey, didx, fixed)
                    exist = self.mc.sop_exist(skey, data)
                    if (exist == 1):
                        self.mc.sop_delete(skey, data)
            """ SOP run check """
            if x % 10 == 9:
               print "  " + str(x+1) + " SOP runs"
        print "SOP run end"  

if __name__ == '__main__':
    unittest.main()
