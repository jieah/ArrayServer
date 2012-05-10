import uuid
import simplejson
import threading
import operator
import zmq
import blaze.server.constants as constants
import logging
import time
import cPickle as pickle
import numpy as np

import protocol
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
        return self.ph.unpack_rpc(responseobj, dataobj)

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
            protocol_helper = protocol.ZMQProtocolHelper()
        super(ZDealerRPCClient, self).__init__(
            ident=ident, protocol_helper=protocol_helper, ctx=ctx)
        self.zmqaddr = zmqaddr
        self.timeout = timeout

    def reqrep(self, requestobj, dataobj):
        multipart_msg = self.ph.pack_blaze(self.ident, str(uuid.uuid4()), requestobj, dataobj)
        multipart_msg = self.ph.pack_envelope([], multipart_msg)
        log.debug('requesting, %s', multipart_msg)
        self.socket.send_multipart(multipart_msg)
        starttime = time.time()
        while True:
            socks = dict(self.poller.poll(timeout=self.timeout))
            if self.socket in socks:
                responsemessages = self.socket.recv_multipart()
                (envelope,
                 responsemessages) = self.ph.unpack_envelope(
                    responsemessages)
                (clientid,
                 msgid,
                 responseobj,
                 responsedatas) = self.ph.unpack_blaze(responsemessages)
                if responseobj.get('msgtype') == 'rpcresponse':
                    return responseobj, responsedatas
                else:
                    log.debug(responseobj)
            if time.time() - starttime > (self.timeout / 1000.0):
                return [None, None]


