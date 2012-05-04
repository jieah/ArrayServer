import tables
import logging
import os
import rpc.server as server
log = logging.getLogger(__name__)
import collections
import blazeconfig
import uuid

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
        messages = self.ph.pack_blaze(
            self.identity,
            str(uuid.uuid4()),
            {'msgtype' : 'control:contentreport'},
            [self.metadata.create_inmemory_config()])
        self.socket.send_multipart(messages)
        
        
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
    
