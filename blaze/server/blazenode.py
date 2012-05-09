import tables
import logging
import os
import rpc.server as server
log = logging.getLogger(__name__)
import collections
import blazeconfig
import uuid
from blaze.array_proxy import array_proxy
from blaze.array_proxy import grapheval


class BlazeRPC(server.RPC):
    def __init__(self, config, protocol_helper=None):
        self.metadata = config
        super(BlazeRPC, self).__init__(protocol_helper=protocol_helper)

    def get(self, path, data_slice=None, data=None):
        #kwarg data, bc of rpc
        log.debug("called get")
        node = self.metadata.get_node(path)
        if node['type'] != 'group':
            return self.get_data(node, data_slice=data_slice)

    def get_data(self, node, data_slice=None):
        response = {'type' : node['type']}
        source = node['sources'][0]
        arr = tables.openFile(source['filepath']).getNode(source['localpath'])
        response['shape'] = [int(x) for x in arr.shape]
        if data_slice is None:
            return response, [arr[:]]
        else:
            data_slice = slice(*data_slice)
            return response, [arr[data_slice]]

    def eval(self, data):
        log.info("called eval")
        graph = data[0]
        array_nodes = grapheval.find_nodes_of_type(graph, array_proxy.BlazeArrayProxy)
        for node in array_nodes:
            # TODO we need to handle multiple physical sources
            source = self.metadata.get_node(node.url)['sources'][0]
            source_type = source['type']
            if source_type == 'hdf5':
                arr = tables.openFile(source['filepath']).getNode(source['localpath'])
                node.set_array(arr[:])
            else:
                return self.ph.pack_rpc(
                    self.ph.error_obj('encountered unknown blaze array type %s' % source_type), []
                )

        value = graph.eval()

        response = {'type' : "array"}
        response['shape'] = [int(x) for x in value.shape]
        return response, [value]

class BlazeNode(server.ZParanoidPirateRPCServer):
    def __init__(self, zmq_addr, identity, config, interval=1000.0,
                 protocol_helper=None, ctx=None):
        rpc = BlazeRPC(config)
        self.metadata = config
        super(BlazeNode, self).__init__(
            zmq_addr, identity, rpc, interval=interval,
            protocol_helper=protocol_helper,
            ctx=ctx)

    def connect(self):
        super(BlazeNode, self).connect()
        log.info("blaze node '%s' connecting" % self.identity)
        messages = self.ph.pack_blaze(
            self.identity,
            str(uuid.uuid4()),
            {'msgtype' : 'control:contentreport'},
            [self.metadata.create_inmemory_config()])
        self.socket.send_multipart(messages)
        log.info("blaze node '%s' sent content report" % self.identity)


if __name__ == "__main__":
    import sys
    import shelve
    import logging
    logging.basicConfig(level=logging.DEBUG)
    pathmap = shelve.open(sys.argv[1])
    reversemap = shelve.open(sys.argv[2])
    addr = sys.argv[3]
    servername = sys.argv[4]
    config = blazeconfig.BlazeConfig(pathmap, reversemap)
    node = BlazeNode(addr, servername, config)
    node.run()
