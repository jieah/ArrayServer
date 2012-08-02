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
import pandas

import arrayserver.server.rpc as rpc
import arrayserver.server.rpc.client as client
from arrayserver.server.arrayserverbroker import ArrayServerBroker
from arrayserver.server.arrayservernode import ArrayServerNode
from arrayserver.server.arrayserverconfig import ArrayServerConfig, generate_config_hdf5, generate_config_numpy
import arrayserver.server.redisutils as redisutils
import arrayserver.server.arrayserverconfig as arrayserverconfig
from arrayserver.array_proxy.arrayserver_array_proxy import ArrayServerArrayProxy
import arrayserver.array_proxy.npproxy as npp


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.debug("starting")

frontaddr = test_utils.frontaddr
class RouterTestCase(test_utils.ArrayServerWithDataTestCase):
    def test_connect(self):
        assert len(self.broker.nodes) == 1
        
    def test_reconnect(self):
        self.rpcserver.reconnect()
        time.sleep(1) #let reconnects occur
        assert len(self.broker.nodes) == 1
        
    def test_get_pandas(self):
        rpcclient = client.ArrayServerClient(frontaddr)
        rpcclient.connect()
        responseobj, data = rpcclient.rpc(
            'get', '/arrayserver/data/pandas.hdf5/a')
        assert isinstance(data[0], pandas.DataFrame)
        
    def test_get_csv(self):
        rpcclient = client.ArrayServerClient(frontaddr)
        rpcclient.connect()
        responseobj, data = rpcclient.rpc(
            'get', '/arrayserver/data/AAPL.txt')
        assert data[0]['Open'][0] == 7.04
        
    def test_get(self):
        rpcclient = client.ArrayServerClient(frontaddr)
        rpcclient.connect()
        responseobj, data = rpcclient.rpc(
            'get', '/arrayserver/data/gold.hdf5/20100217/names')
        data = data[0]
        assert len(data) == 3
        assert data[0][0] == 'GDX'
        responseobj, data = rpcclient.rpc(
            'get', '/arrayserver/data/gold.hdf5/20100217/names',
            data_slice=(0, 1, None))
        data = data[0]
        assert len(data) == 1
        assert data[0][0] == 'GDX'        

        responseobj, data = rpcclient.rpc(
            'get', '/arrayserver/data/gold.hdf5/20100217'
            )

        assert responseobj['type'] == 'group'
        assert 'names' in responseobj['children']
        assert 'prices' in responseobj['children']
        assert 'dates' in responseobj['children']

    def test_store_pandas(self):
        rpcclient = client.ArrayServerClient(frontaddr)
        rpcclient.connect()
        x = pandas.DataFrame(np.arange(20))
        rpcclient.rpc('store', urls=['/tmp/mytempdata'], data=[x])
        responseobj, data = rpcclient.rpc('get', '/tmp/mytempdata')
        data = data[0]
        assert len(data) == 20
    
    # def test_eval_with_numpy_sources(self):
    #     rpcclient = client.ArrayServerClient(frontaddr)
    #     rpcclient.connect()
    #     x = rpcclient.arrayserver_source('/arrayserver/data/test.npy')
    #     y = npp.sin(x**3)
    #     responseobj, data = rpcclient.rpc('eval', data=[y])
    #     assert responseobj['shape'] == [100]
    #     assert responseobj['type'] == 'array'

    #     xx = np.load(self.numpypath)
    #     yy = npp.sin(xx**3)
    #     assert (yy == data[0]).all()
        
    def test_get_tree(self):
        rpcclient = client.ArrayServerClient(frontaddr)
        rpcclient.connect()
        tree, _ = rpcclient.rpc('get_metadata_tree', '/arrayserver/data/gold.hdf5')
        assert tree['children'][0]['children'][0]['type'] == 'array'

    def test_summary_stats(self):
        rpcclient = client.ArrayServerClient(frontaddr)
        rpcclient.connect()
        prices = rpcclient.arrayserver_source('/arrayserver/data/gold.hdf5/20100217/prices')
        responseobj, data = rpcclient.rpc('summary', '/arrayserver/data/gold.hdf5/20100217/prices')
        summary = responseobj['summary']
        columnsummary = responseobj['colsummary']
        assert summary['shape'] == [1561, 3]
        assert summary['colnames'] == [0, 1, 2]
        assert '0' in columnsummary
        assert '2' in columnsummary
        assert columnsummary['1']['mean'] == 109.39397501601509
    
if __name__ == "__main__":
    unittest.main()

