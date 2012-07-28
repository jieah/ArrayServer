import gevent
import gevent.monkey
gevent.monkey.patch_all()
import gevent_zeromq
gevent_zeromq.monkey_patch()
import zmq

import unittest
import simplejson
import numpy as np
import logging
import time
import test_utils
import os
import shelve
import tables

import blaze.server.rpc as rpc
import blaze.server.rpc.client as client
from blaze.server.blazebroker import BlazeBroker
from blaze.server.blazenode import BlazeNode
from blaze.server.blazeconfig import BlazeConfig, generate_config_hdf5, generate_config_numpy
import blaze.server.redisutils as redisutils
import blaze.server.blazeconfig as blazeconfig
from blaze.array_proxy.blaze_array_proxy import BlazeArrayProxy
import blaze.array_proxy.npproxy as npp


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.debug("starting")

frontaddr = test_utils.frontaddr
class RouterTestCase(test_utils.BlazeWithDataTestCase):
    def test_connect(self):
        assert len(self.broker.nodes) == 1
        
    def test_reconnect(self):
        self.rpcserver.reconnect()
        time.sleep(1) #let reconnects occur
        assert len(self.broker.nodes) == 1
        
    def test_get_csv(self):
        # rpcclient = client.BlazeClient(frontaddr)
        # rpcclient.connect()
        # responseobj, data = rpcclient.rpc(
        #     'get', '/blaze/data/AAPL.txt')
        pass
    
    def test_get(self):
        rpcclient = client.BlazeClient(frontaddr)
        rpcclient.connect()
        responseobj, data = rpcclient.rpc(
            'get', '/blaze/data/gold.hdf5/20100217/names')
        data = data[0]
        assert len(data) == 3
        assert 'GDX' in data
        responseobj, data = rpcclient.rpc(
            'get', '/blaze/data/gold.hdf5/20100217/names',
            data_slice=(0, 1, None))
        data = data[0]
        assert len(data) == 1
        assert 'GDX' in data

        responseobj, data = rpcclient.rpc(
            'get', '/blaze/data/gold.hdf5/20100217'
            )


        assert responseobj['type'] == 'group'
        assert 'names' in responseobj['children']
        assert 'prices' in responseobj['children']
        assert 'dates' in responseobj['children']

    def test_eval_with_hdf5_sources(self):
        rpcclient = client.BlazeClient(frontaddr)
        rpcclient.connect()
        x = rpcclient.blaze_source('/blaze/data/gold.hdf5/20100217/prices')
        y = rpcclient.blaze_source('/blaze/data/gold.hdf5/20100218/prices')
        z = npp.sin((x-y)**3)
        responseobj, data = rpcclient.rpc('eval', data=[z])
        assert responseobj['type'] == 'array'

        xx = tables.openFile(self.hdfpath).getNode('/20100217/prices')[:]
        yy = tables.openFile(self.hdfpath).getNode('/20100218/prices')[:]
        zz = np.sin((xx-yy)**3)
        assert (zz == data[0]).all()
        
    def test_store_array_node(self):
        rpcclient = client.BlazeClient(frontaddr)
        rpcclient.connect()
        x = rpcclient.blaze_source('/blaze/data/gold.hdf5/20100217/prices')
        y = rpcclient.blaze_source('/blaze/data/gold.hdf5/20100218/prices')
        z = npp.sin((x-y)**3)
        rpcclient.rpc('store', urls=['/tmp/mytempdata'], data=[z])
        responseobj, data = rpcclient.rpc('get', '/tmp/mytempdata')
            
        xx = tables.openFile(self.hdfpath).getNode('/20100217/prices')[:]
        yy = tables.openFile(self.hdfpath).getNode('/20100218/prices')[:]
        zz = np.sin((xx-yy)**3)
        assert (zz == data[0]).all()
        
    
    def test_eval_with_numpy_sources(self):
        rpcclient = client.BlazeClient(frontaddr)
        rpcclient.connect()
        x = rpcclient.blaze_source('/blaze/data/test.npy')
        y = npp.sin(x**3)
        responseobj, data = rpcclient.rpc('eval', data=[y])
        assert responseobj['shape'] == [100]
        assert responseobj['type'] == 'array'

        xx = np.load(self.numpypath)
        yy = npp.sin(xx**3)
        assert (yy == data[0]).all()
        
    def test_get_tree(self):
        rpcclient = client.BlazeClient(frontaddr)
        rpcclient.connect()
        tree, _ = rpcclient.rpc('get_metadata_tree', '/blaze/data/gold.hdf5')
        assert tree['children'][0]['children'][0]['type'] == 'array'

    def test_summary_stats(self):
        rpcclient = client.BlazeClient(frontaddr)
        rpcclient.connect()
        prices = rpcclient.blaze_source('/blaze/data/gold.hdf5/20100217/prices')
        responseobj, data = rpcclient.rpc('summary', '/blaze/data/gold.hdf5/20100217/prices')
        summary = responseobj['summary']
        columnsummary = responseobj['colsummary']
        assert summary['shape'] == [1561, 3]
        assert summary['colnames'] == [0, 1, 2]
        assert '0' in columnsummary
        assert '2' in columnsummary
        assert columnsummary['1']['mean'] == 64.07833333333333
    
if __name__ == "__main__":
    unittest.main()

