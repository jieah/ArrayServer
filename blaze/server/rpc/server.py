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


THREAD_ADDRESS = 'inproc://worker'

class ZMQWorker(threading.Thread):

    def __init__(self, rpc, ctx, ph,
                 clientid, reqid, msgobj, datastrs,
                 *args, **kwargs):
        self.rpc = rpc
        self.ctx = ctx
        self.ph = ph
        self.clientid = clientid
        self.reqid = reqid
        self.msgobj = msgobj
        self.datastrs = datastrs
        super(ZMQWorker, self).__init__(*args, **kwargs)

    def run(self, *args, **kwargs):
        socket = self.ctx.socket(zmq.PUSH)
        socket.connect(THREAD_ADDRESS)
        dataobjs = self.ph.deserialize_data(self.datastrs)
        responseobj, dataobjs = self.rpc.get_rpc_response(self.msgobj, dataobjs)
        messages = self.ph.pack_blaze(self.clientid, self.reqid,
                                         responseobj, dataobjs)
        socket.send_multipart(messages)
        #log.debug("thread SENDING %s", messages)
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
            resp = self.ph.pack_rpc(self.ph.error_obj('unauthorized access'))
            return resp, []

        if hasattr(self, 'can_' + funcname):
            auth = self.can_funcname(*args, **kwargs)
            if not auth:
                resp = self.ph.pack_rpc(self.ph.error_obj('unauthorized access'))
                return resp, []
        func = getattr(self, funcname)
        if len(dataobj) > 0:
            kwargs['data'] = dataobj
        responseobj, dataobj = func(*args, **kwargs)
        return self.ph.pack_rpc(responseobj), dataobj



class ZParanoidPirateRPCServer(common.HasZMQSocket, threading.Thread):
    socket_type = zmq.DEALER
    do_bind = False
    def __init__(self, zmqaddr, identity, rpc, interval=1000,
                 protocol_helper=None, ctx=None, *args, **kwargs):
        super(ZParanoidPirateRPCServer, self).__init__(
            ctx=ctx , *args, **kwargs)
        if protocol_helper is None:
            protocol_helper = protocol.ProtocolHelper()
        self.ph = protocol_helper
        self.rpc = rpc
        self.identity = identity
        self.interval = interval
        self.last_heartbeat = 0.0
        self.last_heartbeat_recvd = time.time()
        self.thread_socket = self.ctx.socket(zmq.PULL)
        self.zmqaddr = zmqaddr
        self.workers = {}
        self.envelopes = {}
        self.kill = False
        self.was_unconnected = True

    def run(self):
        self.connect()
        self.thread_socket.bind(THREAD_ADDRESS)
        self.poller.register(self.thread_socket, zmq.POLLIN)
        while not self.kill:
            try:
                self.run_once()
            except zmq.ZMQError as e:
                log.exception(e)
                break
        self.thread_socket.close()
        self.disconnect()

    def connect(self):
        super(ZParanoidPirateRPCServer, self).connect()
        self.socket.send(constants.PPP_READY)

    def handle_heartbeat(self):
        try:
            self.socket.send(constants.PPP_HEARTBEAT, flags=zmq.NOBLOCK)
            self.last_heartbeat = time.time()
            log.debug('heartbeat sent')
        except zmq.ZMQError as e:
            log.debug('HEARTBEAT FAILED')

    def handle_message(self, unpacked):
        if unpacked['msgobj']['msgtype'] == 'rpcrequest':
            log.info("handle_message")
            self.handle_rpc(unpacked)

    def handle_rpc(self, unpacked):
        worker = ZMQWorker(self.rpc, self.ctx, self.ph,
                           unpacked['clientid'],
                           unpacked['reqid'], unpacked['msgobj'],
                           unpacked['datastrs'])
        self.workers[unpacked['reqid']] = worker
        worker.start()
        statusobj = self.ph.working_obj(unpacked['reqid'])
        self.ph.send_envelope_blaze(self.socket,
                                    envelope=unpacked['envelope'],
                                    clientid=unpacked['clientid'],
                                    reqid=unpacked['reqid'],
                                    msgobj=statusobj)

    def run_once(self):
        socks = dict(self.poller.poll(timeout=self.interval))
        if socks.get(self.socket) == zmq.POLLIN:
            messages = self.socket.recv_multipart()
            if len(messages) == 1 and messages[0] == constants.PPP_HEARTBEAT:
                log.debug('heartbeat received')
                pass
            else:
                #messages from the outside world come here.
                #this is the part of the loop we must protect
                unpacked = {}
                try:
                    unpacked = self.ph.unpack_envelope_blaze(
                        messages,
                        deserialize_data=False)
                    self.envelopes[unpacked['reqid']] = unpacked['envelope']
                    self.handle_message(unpacked)
                except Exception as e:
                    log.exception(e)
                    reqid = unpacked.get('reqid', None)
                    if reqid in self.envelopes:
                        self.envelopes.pop(reqid)
            self.last_heartbeat_recvd = time.time()
        else:
            if time.time() > self.last_heartbeat_recvd + 2*constants.HEARTBEAT_INTERVAL:
                log.info('Heartbeat failure, attempting to reconnect in %0.2f sec...', self.interval/1000)
                time.sleep(self.interval/1000)
                self.reconnect()

        if self.thread_socket in socks:
            messages = self.thread_socket.recv_multipart()
            log.debug("node sending from worker")
            reqid = messages[1]
            response = self.ph.pack_envelope(self.envelopes[reqid], messages)
            self.socket.send_multipart(response)
            del self.envelopes[reqid]
            del self.workers[reqid]

        if time.time() > self.last_heartbeat + constants.HEARTBEAT_INTERVAL:
            self.handle_heartbeat()


