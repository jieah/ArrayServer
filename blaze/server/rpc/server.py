import uuid
import simplejson
import threading
import operator
import zmq
import constants
import logging
import time
import cPickle as pickle
import numpy as np

import protocol
import common


class ZMQWorker(threading.Thread):

    def __init__(self, rpc, ctx, ph,
                 clientid, msgid, msgobj, datastrs,
                 *args, **kwargs):
        self.rpc = rpc
        self.ctx = ctx
        self.ph = ph
        self.clientid = clientid
        self.msgid = msgid
        self.msgobj = msgobj
        self.datastrs = datastrs
        super(ZMQWorker, self).__init__(*args, **kwargs)

    def run(self, *args, **kwargs):
        socket = self.ctx.socket(zmq.PUSH)
        socket.connect('inproc://foo')
        dataobjs = self.ph.deserialize_data(self.datastrs)
        responseobj, dataobjs = self.rpc.get_rpc_response(self.msgobj, dataobjs)
        messages = self.ph.pack_blaze(self.clientid, self.msgid,
                                         responseobj, dataobjs)
        socket.send_multipart(messages)
        log.debug("thread SENDING %s", messages)
        socket.close()



log = logging.getLogger(__name__)

class RPC(object):
    """
    acces control:
    we maintain a list of functions that can be remotely executed.  we also
    allow data dependent acces control on a per function basis

    this class also has a class var, authorized_functions, which identifies
    functions on this class that can be remotely executed.  if it is set to None,
    we allow all functions.

    a call to function 'dosoemthing', will also call 'can_dosomething'
    with all the same parameters.  we will not execute the function,
    if 'can_dosomething' exists AND it returns False
    """
    authorized_functions = None
    def __init__(self, protocol_helper=None):
        if protocol_helper is None:
            protocol_helper = protocol.ProtocolHelper()
        self.ph = protocol_helper

    def get_rpc_response(self, msgobj, dataobj):
        """ take the request and call the function specified, if we're allowed to.
        parameters to this are PYTHON objects, we assume they are already
        deserialized at this point.

        Parameters:
        msgobj : request object, see BaseRPCClient
        datobj : list of numpy arrays, see BaseRPCClient

        returns:
        responseobj : python object
        dataobj : list of numpy arrays
        """
        funcname = msgobj['func']
        args = msgobj.get('args', [])
        kwargs = msgobj.get('kwargs', {})

        auth = False
        if self.authorized_functions is not None \
           and funcname not in self.authorized_functions:
            return self.ph.pack_rpc(
                self.ph.error_obj('unauthorized access'), [])

        if hasattr(self, 'can_' + funcname):
            auth = self.can_funcname(*args, **kwargs)
            if not auth:
                return self.ph.pack_rpc(
                    self.ph.error_obj('unauthorized access'), [])
        func = getattr(self, funcname)
        if len(dataobj) > 0:
            kwargs['data'] = dataobj
        responseobj, dataobj = func(*args, **kwargs)
        return self.ph.pack_rpc(responseobj, dataobj)



class ZParanoidPirateRPCServer(common.HasZMQSocket, threading.Thread):
    socket_type = zmq.DEALER
    do_bind = False
    def __init__(self, zmqaddr, identity, rpc, interval=1000.0,
                 protocol_helper=None, ctx=None, *args, **kwargs):
        super(ZParanoidPirateRPCServer, self).__init__(
            ctx=ctx , *args, **kwargs)
        if protocol_helper is None:
            protocol_helper = protocol.ZMQProtocolHelper()
        self.ph = protocol_helper
        self.rpc = rpc
        self.identity = identity
        self.interval = interval
        self.liveness = constants.HEARTBEAT_LIVENESS
        self.last_heartbeat = 0.0
        self.thread_socket = self.ctx.socket(zmq.PULL)
        self.zmqaddr = zmqaddr
        self.workers = {}
        self.envelopes = {}
        self.kill = False

    def run(self):
        self.connect()
        self.thread_socket.bind("inproc://foo")
        self.poller.register(self.thread_socket, zmq.POLLIN)
        while not self.kill:
            self.run_once()
        self.thread_socket.close()
        self.disconnect()

    def connect(self):
        super(ZParanoidPirateRPCServer, self).connect()
        self.socket.send(constants.PPP_READY)

    def handle_heartbeat(self):
        self.socket.send(constants.PPP_HEARTBEAT)

    def handle_message(self, envelope, client, msgid, msgobj, datastrs):
        if msgobj['msgtype'] == 'rpcrequest':
            log.info("handle_message")
            self.handle_rpc(envelope, client, msgid, msgobj, datastrs)

    def handle_rpc(self, envelope, client, msgid, msgobj, datastrs):
        worker = ZMQWorker(self.rpc, self.ctx, self.ph, client,
                           msgid, msgobj, datastrs)
        self.workers[msgid] = worker
        worker.start()
        statusobj = self.ph.working_obj(msgid)
        messages = self.ph.pack_blaze(client, msgid, statusobj, [])
        messages = self.ph.pack_envelope(envelope, messages)
        self.socket.send_multipart(messages)

    def run_once(self):
        #the follow code must be wrapped in an exception handler
        #we don't know what we're getting
        if self.liveness <= 0:
            return self.reconnect()

        socks = dict(self.poller.poll(timeout=self.interval))
        if self.socket in socks:
            self.liveness = constants.HEARTBEAT_LIVENESS
            messages = self.socket.recv_multipart()
            if len(messages) == 1 and messages[0] == constants.PPP_HEARTBEAT:
                log.debug('rpc got heartbeat')
                pass
            else:
                #messages from the outside world come here.
                #this is the part of the loop we must protect
                try:
                    envelope, payload = self.ph.unpack_envelope(messages)
                    (client, msgid, msgobj, datastrs) = self.ph.unpack_blaze(payload, False)
                    self.envelopes[msgid] = envelope
                    self.handle_message(envelope, client, msgid, msgobj, datastrs)
                except Exception as e:
                    log.exception(e)
                    if msgid in self.envelopes[msgid] : self.envelopes.pop(msgid)
        if self.thread_socket in socks:
            messages = self.thread_socket.recv_multipart()
            log.info("node sending from worker %s", messages)
            msgid = messages[1]
            response = self.ph.pack_envelope(self.envelopes[msgid], messages)
            self.socket.send_multipart(response)
            del self.envelopes[msgid]
            del self.workers[msgid]

        if time.time() > self.last_heartbeat + constants.HEARTBEAT_INTERVAL:
            self.handle_heartbeat()
            self.last_heartbeat = time.time()

