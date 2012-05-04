import gevent
import gevent.monkey
gevent.monkey.patch_all()
import gevent_zeromq
gevent_zeromq.monkey_patch()
import zmq

import unittest
import rpc
import rpc.client
import rpc.server


import simplejson
import numpy as np
import arrayserver_app as arrayserver
import logging
import time
import test_utils

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.debug("starting")

backaddr = "inproc://#1"
frontaddr = "inproc://#2"
addr = "inproc://#3"

class TestRPC(rpc.server.RPC):
    def __init__(self, protocol_helper=None):
        super(TestRPC, self).__init__(protocol_helper=protocol_helper)
        
    def echo(self, body, dummykv=None, data=None):
        return {'body' : body,'dummykv': dummykv}, data
    
class TestRPCServer(rpc.server.ZParanoidPirateRPCServer):
    def __init__(self, zmqaddr, identity, interval=1000.0, 
                 protocol_helper=None, ctx=None, *args, **kwargs):
    
        super(TestRPCServer, self).__init__(
            zmqaddr, identity, TestRPC(), interval=interval,
            protocol_helper=protocol_helper, ctx=ctx, *args, **kwargs)
        
class SerializationTestCase(unittest.TestCase):
    def test_np_serialization(self):
        a = np.random.random((10,10))
        b = np.array(['abc', 'abcdef'])
        datastrs = rpc.protocol.default_serialize_data([a,b])
        output = rpc.protocol.default_deserialize_data(datastrs)
        assert (output[0] == a).all()
        assert (output[1] == b).all()
        
class RPCTest(unittest.TestCase):
    def tearDown(self):
        if hasattr(self, 'rpcserver'):
            self.rpcserver.kill = True
        if hasattr(self, 'broker'):
            self.broker.kill = True
        if hasattr(self, 'rpcserver'):
            test_utils.wait_until(lambda : self.rpcserver.socket.closed)
            print 'rpcserver closed!'
        if hasattr(self, 'broker'):
            def done():
                return self.broker.frontend.closed and self.broker.backend.closed
            test_utils.wait_until(done)
            print 'broker closed!'            
        #we need this to wait for sockets to close, really annoying
        time.sleep(1.0)
        

    def test_ppirate_rpc(self):
        broker = arrayserver.Broker(frontaddr, backaddr)
        broker.start()
        self.broker = broker
        rpcserver = TestRPCServer(backaddr, 'TEST')
        rpcserver.start()
        test_utils.wait_until(lambda : len(broker.nodes) > 0)
        self.rpcserver = rpcserver
        rpcclient = rpc.client.ZDealerRPCClient(frontaddr)
        rpcclient.connect()
        data = [np.arange(2)]
        response, newdata  = rpcclient.rpc('echo', 'hello',
                                        dummykv='dummy', data=data)
        assert response['body'] == 'hello'
        assert response['dummykv'] == 'dummy'
        assert len(newdata) == 1
        assert (data[0] == newdata[0]).all()
        
    def test_ppirate_rpc_arbitrary_data(self):
        broker = arrayserver.Broker(frontaddr, backaddr)
        broker.start()
        self.broker = broker
        rpcserver = TestRPCServer(backaddr, 'TEST')
        rpcserver.start()
        test_utils.wait_until(lambda : len(broker.nodes) > 0)
        self.rpcserver = rpcserver
        rpcclient = rpc.client.ZDealerRPCClient(frontaddr)
        rpcclient.connect()
        data = [{'hello':'youarehere'}]
        response, newdata  = rpcclient.rpc('echo', 'hello',
                                        dummykv='dummy', data=data)
        assert response['body'] == 'hello'
        assert response['dummykv'] == 'dummy'
        assert len(newdata) == 1
        assert newdata[0] == data[0]


if __name__ == "__main__":
    unittest.main()
    
