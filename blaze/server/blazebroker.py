import functools
import uuid
from blaze.array_proxy import array_proxy
from blaze.array_proxy import blaze_array_proxy
from blaze.array_proxy import grapheval
from threading import Thread
import constants
import time
import zmq
import logging
log = logging.getLogger(__name__)

HEARTBEAT_LIVENESS = constants.HEARTBEAT_LIVENESS
HEARTBEAT_INTERVAL = constants.HEARTBEAT_INTERVAL

# Heartbeat Protocol constants
PPP_READY = constants.PPP_READY
PPP_HEARTBEAT = constants.PPP_HEARTBEAT

import rpc.protocol as protocol
import rpc.router as router
import blazeconfig

class Node(object):
    def __init__(self, address):
        self.address = address
        self.touch()

    def touch(self):
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

class NodeCollection(dict):
    def __init__(self):
        pass

    def ready(self, node):
        self[node.address] = node

    def purge(self):
        """Look for & kill expired nodes."""
        t = time.time()
        expired = []
        for address,node in self.iteritems():
            if t >= node.expiry: # node is dead
                expired.append(address)
        for address in expired:
            print "W: Idle node expired: %s" % address
            del(self[address])
        return expired


class Broker(Thread):

    def __init__(self, frontaddr, backaddr, config, protocol_helper=None,
                 frontid=None, backid=None, timeout=1000.0):
        self.timeout = timeout
        if frontid is None: frontid = str(uuid.uuid4())
        self.frontid = frontid
        if backid is None: backid = str(uuid.uuid4())
        self.backid = backid
        self.kill = False
        self.context = zmq.Context().instance()

        self.frontend = self.context.socket(zmq.ROUTER)
        self.frontend.setsockopt(zmq.IDENTITY, self.frontid)
        self.frontend.bind(frontaddr)

        self.backend = self.context.socket(zmq.ROUTER)
        self.backend.setsockopt(zmq.IDENTITY, self.backid)
        self.backend.bind(backaddr)

        self.poll_nodes = zmq.Poller()
        self.poll_nodes.register(self.backend, zmq.POLLIN)

        self.poll_both = zmq.Poller()
        self.poll_both.register(self.frontend, zmq.POLLIN)
        self.poll_both.register(self.backend, zmq.POLLIN)
        self.metadata = config
        if protocol_helper is None:
            self.ph = protocol.ProtocolHelper()
        self.callbacks = {}
        super(Broker, self).__init__()

    def purge(self):
        purged = self.nodes.purge()

    def handle_ready(self, address, msg):
        self.handle_heartbeat(address, msg)

    def handle_heartbeat(self, address, msg):
        if not self.nodes.has_key(address):
            self.nodes.ready(Node(address))
        self.nodes[address].touch()

    def backend_rpc(self, funcname, targetident, callback, *args, **kwargs):
        if 'data' in kwargs:
            dataobj = kwargs.pop('data')
        else:
            dataobj = []
        requestobj = {'func' : funcname,
                      'msgtype' : 'rpcrequest',
                      'args' : args,
                      'kwargs' : kwargs}
        reqid = str(uuid.uuid4())
        self.callbacks[reqid] = callback
        multipart_msg = self.ph.pack_envelope_blaze(
            envelope=[targetident], clientid=self.backid,
            reqid=reqid, msgobj=requestobj, dataobjs=dataobj)
        self.backend.send_multipart(multipart_msg)

    def handle_backend(self, frames):
        address, msg = frames[0], frames[1:]
        if len(msg) == 1 and msg[0] == PPP_HEARTBEAT:
            self.handle_heartbeat(address, msg)
        elif len(msg) == 1 and msg[0] == PPP_READY:
            self.handle_ready(address, msg)
        else:
            unpacked = self.ph.unpack_envelope_blaze(msg, deserialize_data=False)
            if unpacked['clientid'] == self.backid and \
                   unpacked['msgobj']['msgtype'] == 'rpcresponse':
                callback = self.callbacks.pop(unpacked['reqid'])
                callback(msgobj, self.ph.deserialize_data(unpacked['datastrs']))
            else:
                # for now just forward backend response directly back to client
                self.frontend.send_multipart(msg)

    def handle_frontend(self, frames):
        # TODO dispatch based on actual location of data
        frames.insert(0, "TEST")
        log.debug('frontend forwarding %s', frames)
        self.backend.send_multipart(frames)
        
    def send_heartbeat(self):
        if time.time() > self.heartbeat_at:
            for node in self.nodes:
                msg = [node, PPP_HEARTBEAT]
                self.backend.send_multipart(msg)
            self.heartbeat_at = time.time() + HEARTBEAT_INTERVAL

    def run(self):
        self.nodes = NodeCollection()
        self.heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        while not self.kill:

            if len(self.nodes) > 0: poller = self.poll_both
            else: poller = self.poll_nodes
            socks = dict(poller.poll(self.timeout))

            # handle worker activity on backend
            if socks.get(self.backend) == zmq.POLLIN:
                frames = self.backend.recv_multipart()
                if not frames: break
                self.handle_backend(frames)

            # handle client activity on the frontend
            if socks.get(self.frontend) == zmq.POLLIN:
                frames = self.frontend.recv_multipart()
                if not frames: break
                self.handle_frontend(frames)

            self.purge()
            self.send_heartbeat()
        self.frontend.close()
        self.backend.close()


class BlazeBroker(Broker, router.RPCRouter):
    def __init__(self, frontaddr, backaddr, config, timeout=1000.0,
                 protocol_helper=None):
        super(BlazeBroker, self).__init__(
            frontaddr, backaddr, config, timeout=timeout,
            protocol_helper=protocol_helper)
        log.info("Starting Blaze Broker")
        
    def handle_frontend(self, frames):
        unpacked = self.ph.unpack_envelope_blaze(frames, deserialize_data=False)
        if unpacked['msgobj']['msgtype'] == 'rpcrequest':
            self.route(unpacked)
        
    def cannot_route(self, unpacked):
        del unpacked['datastrs']
        unpacked['msgobj'] = self.ph.pack_rpc(self.ph.error_obj('cannot route'))

        messages = self.ph.pack_envelope_blaze(**unpacked)
        self.frontend.send_multipart(messages)

    def route_get(self, path, data_slice=None, unpacked=None):
        log.info("route_get")
        node = self.metadata.get_metadata(path)
        if node['type'] != 'group':
            servers = [x['servername'] for x in node['sources']]
            if self.metadata.servername in servers:
                #we have this data, route it to one of our workers
                node = self.nodes.values()[0]   # TODO some sort of fair queuing
                log.info('sending blaze source eval to backend %s' % node)
                self.send_to_address(unpacked, node.address)
            else:
                self.cannot_route(unpacked)
        else:
            unpacked['msgobj'] = self.ph.pack_rpc({'type' : 'group',
                                                   'children' : node['children']})
            messages = self.ph.pack_envelope_blaze(**unpacked)
            self.frontend.send_multipart(messages)

    def route_eval(self, datastrs, unpacked=None):
        log.info("route_eval")
        graph = self.ph.deserialize_data(datastrs)[0]
        array_nodes = grapheval.find_nodes_of_type(
            graph,
            blaze_array_proxy.BlazeArrayProxy)
        if len(array_nodes) == 0:
            node = self.nodes.values[0]   # TODO some sort of fair queuing
            log.info('sending bare eval to backend %s' % node)
            self.send_to_address(unpacked, node.address)            
        else:
            sources = []
            for node in array_nodes:
                sources += self.metadata.get_metadata(node.url)['sources']
            servers = set([source['servername'] for source in sources])
            if self.metadata.servername in servers:
                node = self.nodes.values()[0]
                log.info('sending blaze source eval to backend %s' % node)
                self.send_to_address(unpacked, node.address)
                
    def route_info(self, path, unpacked=None):
        log.info("route_info")
        node = self.metadata.get_metadata(path)
        if node['type'] != 'group':
            servers = [x['servername'] for x in node['sources']]
            if self.metadata.servername in servers:
                #we have this data, route it to one of our workers
                target = self.nodes.values()[0]   # TODO some sort of fair queuing
                self.send_to_address(unpacked, target.address)
            else:
                self.cannot_route(unpacked)

    def route_store(self, url, datastrs, rawmessage=None):
        log.info("route store")
        
    def send_to_address(self, unpacked, ident):
        unpacked['envelope'].insert(0, ident)
        msg = self.ph.pack_envelope_blaze(**unpacked)
        self.backend.send_multipart(msg)                
        

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    import sys
    b = BlazeBroker(sys.argv[1], sys.argv[2])
    b.run()







