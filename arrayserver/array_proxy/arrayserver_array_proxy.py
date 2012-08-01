from array_proxy import ArrayProxy, ArrayNode
import numpy as np
client = None

class ArrayServerArrayProxy(ArrayProxy):
    """ A Python-level class that wraps a arrayserver data source.  All the methods
    of an ndarray are present here, and most will return an ArrayNode, when called
    on a ArrayServer server node that replicates the data mapping to the ArrayServer URL.

    The ArrayProxy presents a generator array interface that sits on top of an
    actual numpy array.
    """

    def __init__(self, url, client=None):
        """ Creates a new ArrayServerArrayProxy, given a ArrayServer URL as a parent
        value.
        """
        super(ArrayServerArrayProxy,self).__init__(None)
        self.url = url
        self.client = client
        msg, data = client.rpc('info', self.url)
        info = data[0]
        self.cached_shape = info['shape']
        self.cached_dtype = info.get('dtype', None)

    def seval(self):
        if hasattr(self, 'client'):
            return self.client.rpc('eval', data=[self])[1][0]
        else:
            return None

    def _graph_call(self, funcname, args, kw):
        # Return the a graphnode around the unbound method, and supplying
        # self as args[0].  In-place methodds (iadd,etc) are modified to
        # return self.
        node = ArrayNode(funcname, getattr(np.ndarray, funcname), (self,) + args, kw)
        self.add_listener(node)
        return node
    
    def __setstate__(self, dict):
        super(ArrayServerArrayProxy, self).__setstate__(dict)
        self.func = None

    def __getstate__(self):
        result = self.__dict__.copy()
        if 'client' in result:
            del result['client']
        return result
        
