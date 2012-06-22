import uuid
import simplejson
import threading
import operator
import zmq
from blaze.array_proxy.blaze_array_proxy import BlazeArrayProxy
import blaze.server.constants as constants
import logging
import time
import cPickle as pickle
import numpy as np

import blaze.protocol as protocol
import common

log = logging.getLogger(__name__)
class BaseRPCClient(object):
    """Base RPCClient class.  call the rpc method to
    execute a remote procedure.
    To use this class you need to inherit and implement
    reqrep function, which issues
    a request and receives a response

    the request object looks like this
    request object is a python dictionary.
    {'func' : 'execute',
    'args' : [1,2,3,4,5]
    'kwargs' : {'a' : 1, 'b':2}}

    in addition to the request object, we also pass along a
    list of numpy arrays as a data parameter

    we receive a response object {'rpcresponse' : response}, along with
    a list of numpy arrays.  If there is an error, we get back
    {'rpcresponse' : {'error' : errormsg}}
    """
    def __init__(self, ident=None, protocol_helper=None):
        if protocol_helper is None:
            protocol_helper = protcol.ProtocolHelper()
        self.ph = protocol_helper
        self.ident = ident if ident is not None else str(uuid.uuid4())


    def rpc(self, funcname, *args, **kwargs):
        """rpc
        Parameters
        ---------
        funcname : name of function to be executed on remote server
        args : args to be passed in to that function
        kwargs : kwargs to be passed in to that function
            if 'data' is present in kwargs, it will be removed
            and serialized as apart of the data portion of the message.
            the rpc server should inject that back into kwargs before
            exceuting the function
        """
        if 'data' in kwargs:
            dataobj = kwargs.pop('data')
        else:
            dataobj = []
        requestobj = {'func' : funcname,
                      'msgtype' : 'rpcrequest',
                      'args' : args,
                      'kwargs' : kwargs}
        responseobj, dataobj = self.reqrep(requestobj, dataobj)
        return self.ph.unpack_rpc(responseobj), dataobj

class ZDealerRPCClient(common.HasZMQSocket, BaseRPCClient):
    """rpc client using a REQ socket.
    expects to connect to some other REP socket
    """
    socket_type = zmq.DEALER
    do_bind = False
    def __init__(self, zmqaddr, timeout=1000.0,
                 ident=None, protocol_helper=None,
                 ctx=None):
        if protocol_helper is None:
            protocol_helper = protocol.ProtocolHelper()
        super(ZDealerRPCClient, self).__init__(
            ident=ident, protocol_helper=protocol_helper, ctx=ctx)
        self.zmqaddr = zmqaddr
        self.timeout = timeout

    def reqrep(self, requestobj, dataobj):
        self.ph.send_envelope_blaze(self.socket, clientid=self.ident,
                                    reqid=str(uuid.uuid4()), msgobj=requestobj,
                                    dataobjs=dataobj)
        starttime = time.time()
        while True:
            socks = dict(self.poller.poll(timeout=self.timeout))
            if self.socket in socks:
                unpacked = self.ph.recv_envelope_blaze(self.socket,
                                                       deserialize_data=True)
                if unpacked['msgobj'].get('msgtype') == 'rpcresponse':
                    return unpacked['msgobj'], unpacked['dataobjs']
                else:
                    log.debug(unpacked)
            if time.time() - starttime > (self.timeout / 1000.0):
                return [None, None]


class BlazeClient(ZDealerRPCClient):
    """Client class for connecting to Blaze Array Servers
    """
    def __init__(self, zmqaddr, timeout=1000.0,
                 ident=None, protocol_helper=None,
                 ctx=None):
        super(BlazeClient, self).__init__(zmqaddr, timeout=timeout,
            ident=ident, protocol_helper=protocol_helper, ctx=ctx)


    def blaze_source(self, url):
        return BlazeArrayProxy(url, self)




