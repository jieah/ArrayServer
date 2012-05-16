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
from blaze.server.blazeconfig import BlazeConfig, InMemoryMap, generate_config_hdf5, generate_config_numpy
from blaze.array_proxy.blaze_array_proxy import BlazeArrayProxy
import blaze.array_proxy.npproxy as npp


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.debug("starting")

backaddr = "inproc://#1"
frontaddr = "inproc://#2"

class RouterTestCase(unittest.TestCase):
    def setUp(self):
        testroot = os.path.abspath(os.path.dirname(__file__))
        self.hdfpath = os.path.join(testroot, 'gold.hdf5')
        self.numpypath = os.path.join(testroot, 'test.npy')
        servername = 'myserver'
        self.config = BlazeConfig(InMemoryMap(), InMemoryMap())
        generate_config_hdf5(servername, '/data', self.hdfpath, self.config)
        generate_config_numpy(servername, '/data', '/test', self.numpypath, self.config)
        broker = BlazeBroker(frontaddr, backaddr, timeout=100.0)
        broker.start()
        self.broker = broker
        rpcserver = BlazeNode(backaddr, servername, self.config, interval=100.0)
        rpcserver.start()
        self.rpcserver = rpcserver
        test_utils.wait_until(lambda : len(broker.metadata.pathmap) > 1)

    def tearDown(self):
        if hasattr(self, 'rpcserver'):
            self.rpcserver.kill = True
            test_utils.wait_until(lambda : self.rpcserver.socket.closed)
            print 'rpcserver closed!'
        if hasattr(self, 'broker'):
            self.broker.kill = True
            def done():
                return self.broker.frontend.closed and self.broker.backend.closed
            test_utils.wait_until(done)
            print 'broker closed!'
        #we need this to wait for sockets to close, really annoying
        time.sleep(0.2)

    def test_connect(self):
        node = self.broker.metadata.get_node('/data/20100217/names')
        assert node['shape'] ==  (3,)
        assert node['sources'][0]['localpath'] == '/20100217/names'
        assert '/data/20100217/names' in self.broker.metadata.get_dependencies('myserver')

    def test_reconnect(self):
        self.rpcserver.reconnect()
        time.sleep(1) #let reconnects occur
        node = self.broker.metadata.get_node('/data/20100217/names')
        assert node['shape'] ==  (3,)
        assert node['sources'][0]['localpath'] == '/20100217/names'
        assert '/data/20100217/names' in self.broker.metadata.get_dependencies('myserver')

    def test_get(self):
        rpcclient = client.BlazeClient(frontaddr)
        rpcclient.connect()
        responseobj, data = rpcclient.rpc('get', '/data/20100217/names')
        data = data[0]
        assert len(data) == 3
        assert 'GDX' in data
        responseobj, data = rpcclient.rpc('get', '/data/20100217/names',
                                          data_slice=(0, 1, None))
        data = data[0]
        assert len(data) == 1
        assert 'GDX' in data

        responseobj, data = rpcclient.rpc('get', '/data/20100217')

        assert responseobj['type'] == 'group'
        assert 'names' in responseobj['children']
        assert 'prices' in responseobj['children']
        assert 'dates' in responseobj['children']

    def test_eval_with_hdf5_sources(self):
        rpcclient = client.BlazeClient(frontaddr)
        rpcclient.connect()
        x = rpcclient.blaze_source('/data/20100217/prices')
        y = rpcclient.blaze_source('/data/20100218/prices')
        z = npp.sin((x-y)**3)
        responseobj, data = rpcclient.rpc('eval', data=[z])
        assert responseobj['shape'] == [1561, 3]
        assert responseobj['type'] == 'array'

        xx = tables.openFile(self.hdfpath).getNode('/20100217/prices')[:]
        yy = tables.openFile(self.hdfpath).getNode('/20100218/prices')[:]
        zz = np.sin((xx-yy)**3)
        assert (zz == data[0]).all()

    def test_eval_with_numpy_sources(self):
        rpcclient = client.BlazeClient(frontaddr)
        rpcclient.connect()
        x = rpcclient.blaze_source('/data/test')
        y = npp.sin(x**3)
        responseobj, data = rpcclient.rpc('eval', data=[y])
        assert responseobj['shape'] == [100]
        assert responseobj['type'] == 'array'

        xx = np.load(self.numpypath)
        yy = npp.sin(xx**3)
        assert (yy == data[0]).all()

if __name__ == "__main__":
    unittest.main()

