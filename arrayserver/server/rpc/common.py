import uuid
import threading
import operator
import zmq
import arrayserver.server.constants as constants
import logging
import time
import cPickle as pickle
import numpy as np



class HasZMQSocket(object):
    """
    convenience class for working with zeromq sockets.  You use this if you want
    to use connect/disconnect/reconnect functions with zmq pollers.
    Classes which want to use a socket that gets passed in (one that isn't created
    inside this class, but managed externally) should not use this class
    """
    socket_type = None
    do_bind = True

    def __init__(self, ctx=None, *args, **kwargs):
        super(HasZMQSocket, self).__init__(*args, **kwargs)
        self.ctx = ctx
        if self.ctx is None:
            self.ctx = zmq.Context.instance()

    def reconnect(self):
        self.disconnect()
        self.connect()

    def disconnect(self):
        if hasattr(self, 'poller'):
            self.poller.unregister(self.socket)
        if hasattr(self, 'socket'):
            self.socket.setsockopt(zmq.LINGER,0)
            self.socket.close()

    def connect(self):
        self.socket = self.ctx.socket(self.socket_type)
        if hasattr(self, 'identity'):
            self.socket.setsockopt(zmq.IDENTITY, self.identity)
        if self.do_bind:
            self.socket.bind(self.zmqaddr)
        else:
            self.socket.connect(self.zmqaddr)
        if not hasattr(self, 'poller'):
            self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

