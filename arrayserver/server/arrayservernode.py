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
import cStringIO

class ArrayServerRPC(server.RPC):
    def __init__(self, config, protocol_helper=None):
        self.metadata = config
        super(ArrayServerRPC, self).__init__(protocol_helper=protocol_helper)
        
    def load_source(self, **source):
        self.metadata.load_source(**source)
        return 'success', []
    
    def get_metadata_tree(self, path, depth=None):
        return self.metadata.get_tree(path, depth=depth), []
    
    def get(self, path, data_slice=None, data=None):
        #kwarg data, bc of rpc
        log.debug("called get")
        metadata = self.metadata.get_metadata(path)
        if metadata['type'] != 'group':
            return self.get_data(metadata, data_slice=data_slice)
        
    def numpy_to_pandas(self, ndarray):
        arr = pandas.DataFrame(ndarray)
        arr.columns = pandas.Index([str(x) for x in arr.columns])
        return arr

    def _get_deferred_data(self, metadata):
        arr = pickle.loads(metadata['deferred'])
        if isinstance(arr, array_proxy.BaseArrayNode):            
            arr = self._eval(arr)
        return arr
    
    def _get_data(self, metadata, data_slice=None):
        #total mess! (hugo's fault)
        if metadata['type'] == 'deferredarray':
            return self._get_deferred_data(metadata)
        sources = [x for x in metadata['sources'] \
                  if x['servername'] == self.metadata.servername]
        source = sources[0]
        source_type = source['type']
        arr = None
        if source_type == 'hdf5':
            with tables.openFile(source['serverpath']) as f:
                arr = f.getNode(source['localpath'])[:]
        elif source_type == 'pandashdf5':
            try:
                store = pandas.HDFStore(source['serverpath'])
                arr = store[source['hdfstorekey']]
            finally:
                store.close()
        elif source_type == 'disco':
            import disco.ddfs as ddfs
            d = ddfs.DDFS(master=source['conn'])
            arr = list(d.pull(source['tag']))[int(source['index'])]
            try:
                arr = np.load(arr)
            except IOError:
                arr.seek(0)
                arr = cStringIO.StringIO(arr.read())
                arr = pandas.read_csv(arr)
        elif source_type == 'csv':
            arr = pandas.read_csv(source['serverpath'])
        elif source_type == 'numpy':
            arr = np.load(source['serverpath'])
        return arr
    
    def store(self, urls=[], data=[]):
        for url, arr in zip(urls, data):
            obj = self.metadata.deferredarray_obj(arr)
            self.metadata.create_dataset(url, obj)
        return 'success', []
    
    def get_data(self, metadata, data_slice=None):
        arr = self._get_data(metadata)
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
        if isinstance(arr, pandas.DataFrame):
            colnames = arr.columns.tolist()
            cols = [arr[x] for x in colnames]
        elif isinstance(arr, np.ndarray):
            if arr.dtype.names:
                colnames = arr.dtype.names
                cols = [arr[x] for x in colnames]
            else:
                if len(arr.shape) == 1:
                    arr = np.expand_dims(arr, -1)
                colnames = range(arr.shape[1])
                cols = [arr[:,x] for x in colnames]
        summary['colnames'] = colnames
        colsummary = {}
        for cname, col in zip(colnames, cols):
            try:
                colsummary[cname] = continuous_summary(col)
            except Exception as e:
                import pdb;pdb.set_trace()
                log.exception(e)
                
        summary = {'summary' : summary,
                   'colsummary' : colsummary}
        log.debug ("returning %s for %s", summary, path)
        return summary, []
    
def continuous_summary(col):
    try:
        mean=np.mean(col).tolist()
    except Exception as e:
        mean = None
    try:
        std=np.std(col).tolist()
    except Exception as e:
        std = None
    try:
        max=np.max(col).tolist()
    except Exception as e:
        max = None
    try:
        min=np.min(col).tolist()
    except Exception as e:
        min = None
    return dict(mean=mean, std=std, max=max, min=min)
        
    
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

