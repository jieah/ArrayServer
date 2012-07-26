import tables
import logging
import os
import rpc.server as server
log = logging.getLogger(__name__)
import collections
import blazeconfig
import uuid
import numpy as np
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
        
    def _get_data(self, metadata, data_slice=None):
        sources = [x for x in metadata['sources'] \
                  if x['servername'] == self.metadata.servername]
        source = sources[0]
        source_type = source['type']
        arr = None
        if source_type == 'hdf5':
            arr = tables.openFile(source['serverpath']).getNode(source['localpath'])
        elif source_type == 'numpy':
            arr = np.load(source['serverpath'])
        elif source_type == 'disco':
            import disco.ddfs as ddfs
            d = ddfs.DDFS(master=source['conn'])
            arr = list(d.pull(source['tag']))[int(source['index'])]
            arr = np.load(arr)
        return arr
        
    def get_data(self, metadata, data_slice=None):
        arr = self._get_data(metadata)
        response = {'type' : metadata['type']}
        if arr is None:
            error = 'encountered unknown blaze array type %s' % source_type
            error = self.ph.pack_rpc(self.ph.error_obj(error))
            return error, []
        response['shape'] = [int(x) for x in arr.shape]            
        if data_slice is None:
            return response, [arr[:]]
        else:
            data_slice = slice(*data_slice)
            return response, [arr[data_slice]]

    def info(self, path):
        metadata = self.metadata.get_metadata(path)
        arr = self._get_data(metadata)
        response = {'type' : metadata['type']}
        arrinfo = {}
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
            metadata = self.metadata.get_metadata(node.url)
            response, data = self.get_data(metadata)
            if len(data) == 0:
                # some sort of error..
                # length checking to see if there is an error is hacky
                # :hugo
                return response, data
            node.set_array(data[0])
        value = np.ascontiguousarray(graph.eval())
        response = {'type' : "array"}
        response['shape'] = [int(x) for x in value.shape]
        return response, [value]
    
    def summary(self, path):
        metadata = self.metadata.get_metadata(path)
        response, data = self.get_data(metadata)
        if len(data) == 0:
            # some sort of error..
            # length checking to see if there is an error is hacky
            # :hugo
            return response, data
        arr = data[0]
        summary = {}
        summary['shape'] = arr.shape
        if arr.dtype.names:
            colnames = arr.dtype.names
            cols = [arr[x] for x in colnames]            
        else:
            if len(arr.shape) == 1:
                colnames = [0]
                cols = [arr]
            else:
                colnames = range(arr.shape[1])
                cols = [arr[x] for x in colnames]
        summary['colnames'] = colnames
        colsummary = {}
        for cname, col in zip(colnames, cols):
            try:
                colsummary[cname] = continuous_summary(col)
            except Exception as e:
                log.exception(e)
                
        summary = {'summary' : summary,
                   'colsummary' : colsummary}
        return summary, []
    
def continuous_summary(col):
    return dict(
        mean=np.mean(col),
        std=np.std(col),
        max=np.max(col),
        min=np.min(col)
        )
        
    
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

