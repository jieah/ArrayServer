import tables
import logging
import os
import rpc.server as server
log = logging.getLogger(__name__)
import collections
import blazeconfig
import uuid
import numpy
from blaze.array_proxy import blaze_array_proxy
from blaze.array_proxy import grapheval
import posixpath as blazepath

class BlazeRPC(server.RPC):
    def __init__(self, config, protocol_helper=None):
        self.metadata = config
        super(BlazeRPC, self).__init__(protocol_helper=protocol_helper)

    def get_metadata_tree(self, path, depth=None):
        return self.metadata.get_tree(path, depth=depth), []
    
    def get(self, path, data_slice=None, data=None):
        #kwarg data, bc of rpc
        log.debug("called get")
        metadata = self.metadata.get_metadata(path)
        if metadata['type'] != 'group':
            return self.get_data(metadata, data_slice=data_slice)

    def get_data(self, metadata, data_slice=None):
        response = {'type' : metadata['type']}
        sources = [x for x in metadata['sources'] \
                  if x['servername'] == self.metadata.servername]
        source = sources[0]
        source_type = source['type']
        if source_type == 'hdf5':
            arr = tables.openFile(source['serverpath']).getNode(source['localpath'])
        elif source_type == 'numpy':
            arr = numpy.load(source['serverpath'])
        response['shape'] = [int(x) for x in arr.shape]            
        if data_slice is None:
            return response, [arr[:]]
        else:
            data_slice = slice(*data_slice)
            return response, [arr[data_slice]]

    def info(self, path):
        metadata = self.metadata.get_metadata(path)
        response = {'type' : metadata['type']}
        arrinfo = {}
        sources = [x for x in metadata['sources'] \
                  if x['servername'] == self.metadata.servername]
        source = sources[0]
        source_type = source['type']
        if source_type == 'hdf5':
            arr = tables.openFile(source['serverpath'])
            arr = arr.getNode(source['localpath'])
        elif source_type == 'numpy':
            arr = numpy.load(source['serverpath'])
        arrinfo['shape'] = arr.shape
        arrinfo['dtype'] = arr.dtype
        return response, [arrinfo]
    
    def eval(self, data):
        log.info("called eval")
        graph = data[0]
        array_nodes = grapheval.find_nodes_of_type(
            graph, blaze_array_proxy.BlazeArrayProxy)
        for node in array_nodes:
            # TODO we need to handle multiple physical sources
            source = self.metadata.get_metadata(node.url)['sources'][0]
            source_type = source['type']
            if source_type == 'hdf5':
                arr = tables.openFile(source['serverpath'])
                arr = arr.getNode(source['localpath'])
                node.set_array(arr[:])
            elif source_type == 'numpy':
                arr = numpy.load(source['serverpath'])
                node.set_array(arr)
            else:
                error = 'encountered unknown blaze array type %s' % source_type
                error = self.ph.pack_rpc(self.ph.error_obj(error))
                return error, []

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

