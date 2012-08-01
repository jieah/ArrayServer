import time
import zmq
import unittest
import os
import redis

import arrayserver.server.redisutils as redisutils
import arrayserver.server.arrayserverconfig as arrayserverconfig
from arrayserver.server.arrayserverconfig import ArrayServerConfig, generate_config_hdf5, generate_config_numpy
from arrayserver.server.arrayserverbroker import ArrayServerBroker
from arrayserver.server.arrayservernode import ArrayServerNode

backaddr = "inproc://#1"
frontaddr = "inproc://#2"

def wait_until(func, timeout=1.0, interval=0.01):
    st = time.time()
    while True:
        if func():
            return True
        if (time.time() - st) > timeout:
            return False
        time.sleep(interval)

def recv_timeout(socket, timeout):
	poll = zmq.Poller()
	poll.register(socket, zmq.POLLIN)
	socks = dict(poll.poll(timeout=timeout))
	if socks.get(socket, None) == zmq.POLLIN:
		return socket.recv_multipart()
	else:
		return None
	
class ArrayServerWithDataTestCase(unittest.TestCase):
    def setUp(self):
        testroot = os.path.abspath(os.path.dirname(__file__))
        self.hdfpath = os.path.join(testroot, 'data', 'gold.hdf5')
        self.numpypath = os.path.join(testroot, 'data', 'test.npy')
        sourceconfig = {
            'arrayserver' : {
                'type' : 'native',
                'paths' : {
                    'data' : os.path.join(testroot, 'data')
                    }
                }
            }
        servername = 'myserver'
        self.redisproc = redisutils.RedisProcess(9000, '/tmp', save=False)
        wait_for_redis(9000)
        self.config = arrayserverconfig.ArrayServerConfig(servername, port=9000,
                                              sourceconfig=sourceconfig)
        broker = ArrayServerBroker(frontaddr, backaddr, self.config, timeout=100.0)
        broker.start()
        self.broker = broker
        rpcserver = ArrayServerNode(backaddr, 'testnodeident', self.config,
                              interval=100.0)
        rpcserver.start()
        self.rpcserver = rpcserver
        wait_until(lambda : len(broker.nodes) > 1)
        
    def tearDown(self):
        self.redisproc.close()
        if hasattr(self, 'rpcserver'):
            self.rpcserver.kill = True
            wait_until(lambda : self.rpcserver.socket.closed)
            print 'rpcserver closed!'
        if hasattr(self, 'broker'):
            self.broker.kill = True
            def done():
                return self.broker.frontend.closed and self.broker.backend.closed
            wait_until(done)
            print 'broker closed!'
        #we need this to wait for sockets to close, really annoying
        time.sleep(0.2)

def wait_for_redis(redis_port):
    def redis_ping():
        conn = redis.Redis(port=redis_port)
        try:
            status =  conn.ping()
            if status:
                return status
        except redis.ConnectionError as e:
            return False
    return wait_until(redis_ping, timeout=5.0, interval=0.5)
