import tables
import logging
import os
import rpc.server as server
log = logging.getLogger(__name__)
import collections
import arrayserverconfig
import cPickle as pickle
import uuid
import numpy as np
import arrayserver.array_proxy.arrayserver_array_proxy as arrayserver_array_proxy
import arrayserver.array_proxy.array_proxy as array_proxy
from arrayserver.array_proxy import grapheval
import posixpath as arrayserverpath
import pandas


class ArrayServerRPC(server.RPC):
    def __init__(self, config, protocol_helper=None):
        self.metadata = config
        super(ArrayServerRPC, self).__init__(protocol_helper=protocol_helper)

    def get_metadata_tree(self, path, depth=None):
        return self.metadata.get_tree(path, depth=depth), []
    
    def get(self, path, data_slice=None, data=None):
        #kwarg data, bc of rpc
        log.debug("called get")
        metadata = self.metadata.get_metadata(path)
        if metadata['type'] != 'group':
            return self.get_data(metadata, data_slice=data_slice)

    def _get_deferred_data(self, metadata):
        arr = pickle.loads(metadata['deferred'])
        if isinstance(arr, array_proxy.BaseArrayNode):            
            arr = self._eval(arr)
        arr = np.ascontiguousarray(arr)
        return arr

    def _get_data(self, metadata, data_slice=None):
        if metadata['type'] == 'deferredarray':
            return self._get_deferred_data(metadata)
        sources = [x for x in metadata['sources'] \
                  if x['servername'] == self.metadata.servername]
        source = sources[0]
        source_type = source['type']
        arr = None
        if source_type == 'hdf5':
            arr = tables.openFile(source['serverpath'])
            arr = arr.getNode(source['localpath'])
        elif source_type == 'pandashdf5':
            store = pandas.HDFStore(source['serverpath'])
            arr = store[source['hdfstorekey']]
            store.close()
        elif source_type == 'numpy':
            arr = np.load(source['serverpath'])
        elif source_type == 'disco':
            import disco.ddfs as ddfs
            d = ddfs.DDFS(master=source['conn'])
            arr = list(d.pull(source['tag']))[int(source['index'])]
            arr = np.load(arr)
        elif source_type == 'csv':
            import pandas
            arr = pandas.read_csv(source['serverpath']).to_records()
        arr = np.ascontiguousarray(arr)
        return arr
    
    def store(self, urls=[], data=[]):
        for url, arr in zip(urls, data):
            if isinstance(arr, (array_proxy.BaseArrayNode, np.ndarray,
                                pandas.DataFrame)):
                obj = self.metadata.deferredarray_obj(arr)
                self.metadata.create_dataset(url, obj)
        return 'success', []
    
    def get_data(self, metadata, data_slice=None):
        arr = self._get_data(metadata)
        if not isinstance(arr, pandas.DataFrame):
            arr = pandas.DataFrame(arr)
        response = {'type' : metadata['type']}
        if arr is None:
            error = 'encountered unknown arrayserver array type %s' % source_type
            error = self.ph.pack_rpc(self.ph.error_obj(error))
            return error, []
        response['shape'] = [int(x) for x in arr.shape]            
        if data_slice is None:
            return response, [arr[:]]
        else:
            data_slice = slice(*data_slice)
            return response, [arr[data_slice]]

    # def info(self, path):
    #     metadata = self.metadata.get_metadata(path)
    #     arr = self._get_data(metadata)
    #     response = {'type' : metadata['type']}
    #     arrinfo = {}
    #     arrinfo['shape'] = arr.shape
    #     if hasattr(arr, 'dtype'):
    #         arrinfo['dtype'] = arr.dtype
    #     return response, [arrinfo]
    
    def _eval(self, graph):
        array_nodes = grapheval.find_nodes_of_type(
            graph, arrayserver_array_proxy.ArrayServerArrayProxy)
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
        return value
    
    def eval(self, data):
        log.info("called eval")
        value = self._eval(data[0])
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
        mean=np.mean(col).tolist(),
        std=np.std(col).tolist(),
        max=np.max(col).tolist(),
        min=np.min(col).tolist()
        )
        
    
class ArrayServerNode(server.ZParanoidPirateRPCServer):
    def __init__(self, zmq_addr, identity, config, interval=1000.0,
                 protocol_helper=None, ctx=None):
        rpc = ArrayServerRPC(config)
        self.metadata = config
        super(ArrayServerNode, self).__init__(
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
    config = arrayserverconfig.ArrayServerConfig(pathmap, reversemap)
    node = ArrayServerNode(addr, servername, config)
    node.run()

