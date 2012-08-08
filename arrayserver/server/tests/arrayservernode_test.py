import gevent
import gevent.monkey
gevent.monkey.patch_all()
import gevent_zeromq
gevent_zeromq.monkey_patch()
import zmq

import unittest
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
            'get', '/arrayserver/data/csv/LeadershipAction.csv')
        assert data[0]['tot_rec'][500] == u'$34,000.90'
        
    def test_get(self):
        rpcclient = client.ArrayServerClient(frontaddr)
        rpcclient.connect()
        responseobj, data = rpcclient.rpc('get', '/arrayserver/data/random.hdf5/a')
        data = data[0]
        assert np.all(data == self.hdf5data_a)
        responseobj, data = rpcclient.rpc(
            'get',
            '/arrayserver/data/random.hdf5/a',
            data_slice=(0, 1, None))
        data = data[0]
        assert np.all(data == self.hdf5data_a[:1])
        responseobj, data = rpcclient.rpc(
            'get', '/arrayserver/data/random.hdf5'
            )
        assert responseobj['type'] == 'group'
        assert 'a' in responseobj['children']
        assert 'b' in responseobj['children']

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
        tree, _ = rpcclient.rpc('get_metadata_tree', '/arrayserver/data/random.hdf5')
        assert tree['children'][0]['type'] == 'array'

    def test_summary_stats(self):
        rpcclient = client.ArrayServerClient(frontaddr)
        rpcclient.connect()
        responseobj, data = rpcclient.rpc('summary',
                                          '/arrayserver/data/random.hdf5/a')
        summary = responseobj['summary']
        columnsummary = responseobj['colsummary']
        assert summary['shape'] == list(self.hdf5data_a.shape)
        assert summary['colnames'] == [0, 1]
        assert '0' in columnsummary
        assert columnsummary['1']['mean'] == 0.43365650027370206
    
if __name__ == "__main__":
    unittest.main()

