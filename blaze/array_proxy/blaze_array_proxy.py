
from blaze.array_proxy.array_proxy import ArrayProxy

class BlazeArrayProxy(ArrayProxy):
    """ A Python-level class that wraps a blaze data source.  All the methods
    of an ndarray are present here, and most will return an ArrayNode, when called
    on a Blaze server node that replicates the data mapping to the Blaze URL.

    The ArrayProxy presents a generator array interface that sits on top of an
    actual numpy array.
    """

    def __init__(self, url, client=None):
        """ Creates a new BlazeArrayProxy, given a Blaze URL as a parent
        value.
        """
        super(BlazeArrayProxy,self).__init__(None)
        self.url = url
        msg, data = client.rpc('info', self.url)
        info = data[0]
        self.cached_shape = info['shape']
        self.cached_dtype = info['dtype']

    def __setstate__(self, dict):
        super(BlazeArrayProxy, self).__setstate__(dict)
        self.func = None
