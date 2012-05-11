import functools
import uuid
from blaze.array_proxy import array_proxy
from blaze.array_proxy import grapheval
from threading import Thread
import time
import zmq
import logging
log = logging.getLogger(__name__)

HEARTBEAT_LIVENESS = 3 # 3..5 is reasonable
HEARTBEAT_INTERVAL = 1.0 # Seconds

# Heartbeat Protocol constants
PPP_READY = "\x01" # Signals worker is ready
PPP_HEARTBEAT = "\x02" # Signals worker heartbeat
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

    def __init__(self, frontaddr, backaddr, config=None, protocol_helper=None,
                 frontid=None, backid=None):
        if config is None:
            config = blazeconfig.BlazeConfig(blazeconfig.InMemoryMap(),
                                             blazeconfig.InMemoryMap())
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
            self.ph = protocol.ZMQProtocolHelper()
        self.callbacks = {}
        super(Broker, self).__init__()

    def purge(self):
        purged = self.nodes.purge()
        for addr in purged:
            self.metadata.remove(addr)
            
    def handle_contentreport(self, clientid, msgobj, data):
        self.metadata.remove(clientid)
        blazeconfig.merge_configs(self.metadata, data[0])
        log.info("receivied content report from: '%s' containing %d sources" % (clientid, len(data[0].pathmap.keys())))
        
    def handle_ready(self, address, msg):
        self.handle_heartbeat(address, msg)
        self.backend_rpc('get_contentreport', address,
                         functools.partial(self.handle_contentreport, address))
                         
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
        multipart_msg = self.ph.pack_blaze(self.backid, reqid,
                                           requestobj, dataobj)
        self.callbacks[reqid] = callback
        multipart_msg = self.ph.pack_envelope([targetident], multipart_msg)
        self.backend.send_multipart(multipart_msg)
        
    def handle_backend(self, frames):
        address, msg = frames[0], frames[1:]
        if len(msg) == 1 and msg[0] == PPP_HEARTBEAT:
            self.handle_heartbeat(address, msg)
        elif len(msg) == 1 and msg[0] == PPP_READY:
            self.handle_ready(address, msg)
        else:
            envelope, payload = self.ph.unpack_envelope(msg)
            (clientid, msgid, msgobj, datastrs) \
                = self.ph.unpack_blaze(payload, deserialize_data=False)
            if clientid == self.backid and msgobj['msgtype'] == 'rpcresponse':
                callback = self.callbacks.pop(msgid)
                callback(msgobj, self.ph.deserialize_data(datastrs))
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

            socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))

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
    def __init__(self, frontaddr, backaddr, config=None, protocol_helper=None):
        super(BlazeBroker, self).__init__(
            frontaddr, backaddr, config=config, protocol_helper=protocol_helper)

    def handle_frontend(self, frames):
        envelope, payload = self.ph.unpack_envelope(frames)
        (clientid, msgid, msgobj, datastrs) \
            = self.ph.unpack_blaze(payload, deserialize_data=False)
        if msgobj['msgtype'] == 'rpcrequest':
            self.route(msgobj, datastrs, frames)

    def route_get(self, path, data_slice=None, rawmessage=None):
        log.info("route_get")
        node = self.metadata.get_node(path)
        if node['type'] != 'group':
            source = node['sources'][0]
            rawmessage.insert(0, source['servername'])
            self.backend.send_multipart(rawmessage)
        else:
            (envelope, clientid, msgid,
             requestobj, datastrs) = self.ph.unpack_envelope_blaze(
                rawmessage, deserialize_data=False)
            msgobj, dataobjs = self.ph.pack_rpc(
                {'type' : 'group',
                 'children' : node['children']}, [])
            messages = self.ph.pack_envelope_blaze(
                envelope, clientid, msgid, msgobj, dataobjs
                )
            self.frontend.send_multipart(messages)

    def route_eval(self, datastrs, rawmessage=None):
        log.info("route_eval")
        graph = self.ph.deserialize_data(datastrs)[0]
        array_nodes = grapheval.find_nodes_of_type(graph, array_proxy.BlazeArrayProxy)
        if len(array_nodes) == 0:
            # There are no blaze sources, simply farm this out to a random node
            node = self.nodes.values[0]   # TODO some sort of fair queuing
            rawmessage.insert(0, node.address)
            log.info('sending bare eval to backend %s' % node)
            self.backend.send_multipart(rawmessage)
        else:
            sources = []
            for node in array_nodes:
                sources += self.metadata.get_node(node.url)['sources']
            servers = set([source['servername'] for source in sources])
            if len(servers) == 1:
                node = self.nodes[servers.pop()]
                if self.nodes.has_key(node.address):
                    rawmessage.insert(0, node.address)
                    log.info('sending blaze source eval to backend %s' % node)
                    self.backend.send_multipart(rawmessage)





if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    import sys
    b = BlazeBroker(sys.argv[1], sys.argv[2])
    b.run()







