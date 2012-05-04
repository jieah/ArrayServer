import tables
import logging
import shelve
import os
import rpc.server as server
log = logging.getLogger(__name__)
import collections
import simplejson

DatasetSource = collections.namedtuple('DatasetSource', ['type', 'servername', 'filepath', 'hdf5path', 'shard', 'totalshards', 'dtype', 'shape'])
GroupSource = collections.namedtuple('GroupSource', ['type', 'servername', 'filepath', 'hdf5path'])

#dict with a sync function that does nothing,
#this way we can use the same code for
#the in memory, or on disk maps
def path_history(path):
    paths = []
    currpath = path
    while currpath != "/":
        paths.append(currpath)
        base = os.path.dirname(currpath)
        currpath = base
    paths.append("/")
    paths.reverse()
    return paths

class InMemoryMap(dict):
    def sync(self):
        pass
    
class BlazeConfigError(Exception):
    pass

class BlazeConfig(object):
    def __init__(self, pathmap, reversemap):
        self.pathmap = pathmap
        self.reversemap = reversemap
        #ensure root node exists
        if self.get_node('/') is None:
            self.pathmap['/'] = self.group_obj([])
        
    def create_inmemory_config(self):
        return BlazeConfig(InMemoryMap(self.pathmap),
                           InMemoryMap(self.reversemap))
    def sync(self):
        self.pathmap.sync()
        self.reversemap.sync()

    def create_group(self, path):
        self.safe_insert(os.path.dirname(path),
                         os.path.basename(path),
                         self.group_obj([]))

    def group_obj(self, children):
        return {'type' : 'group',
                'children' : children}
    def source_obj(self, servername, type, filepath, shard, localpath):
        return {'type' : type,
                'servername' : servername,
                'filepath' : filepath,
                'shard' : shard,
                'localpath' : localpath}
                
    def table_obj(self, appendable, dtype, shape, shardinfo, sources):
        return {'type' : 'array',
                'dtype' : dtype,
                'shape' : shape,
                'shardinfo' : shardinfo,
                'sources' : sources}
    
    def array_obj(self, appendable, dtype, shape, shardinfo, sources):
        return {'type' : 'array',
                'dtype' : dtype,
                'shape' : shape,
                'shardinfo' : shardinfo,
                'sources' : sources}
    
    def safe_insert(self, parentpath, name, obj):
        newpath = os.path.join(parentpath, name)
        if self.pathmap.get(newpath) is not None:
            raise BlazeConfigError, 'item already exists'
        
        parent = self.pathmap.get(parentpath)
        if parent is None:
            parent = self.safe_insert(
                os.path.dirname(parentpath),
                os.path.basename(parentpath),
                self.group_obj([]))
            parent = self.pathmap.get(parentpath)                             
        if name not in parent['children']:
            parent['children'].append(name)
            self.pathmap[parentpath] = parent
        self.pathmap[os.path.join(parentpath, name)] = obj
        
    def list_children(self, path):
        group = self.pathmap.get(path)
        if group is None or group['type'] != 'group':
            raise BlazeConfigError
        else:
            return [os.path.join(path, x) for x in group['children']]

    def create_dataset(self, path, obj):
        self.safe_insert(os.path.dirname(path), os.path.basename(path), obj)
        for source in obj['sources']:
            self.add_reverse_map(path, source)

    def add_source(self, path, source):
        node = self.pathmap.get(path)
        if source not in node['sources']:
            node['sources'].append(source)
            self.add_reverse_map(path, source)
        self.pathmap[path] = node
        
    def sourcekey(self, servername, filepath=None, localpath=None):
        data = [servername]
        if filepath is not None:
            data.append(filepath)
        if localpath is not None:
            data.append(localpath)
        return ":".join(data)
    
    def add_reverse_map(self, path, source):
        sourcekey = self.sourcekey(source['servername'],
                                   source['filepath'],
                                   source['localpath'])
        node = self.reversemap.get(sourcekey)
        if node is None:
            self.reversemap[sourcekey] = [path]
        else:
            paths = self.reversemap[sourcekey]
            if paths not in path:
                paths.append(path)
            self.reversemap[sourcekey] = paths
            
    def get_node(self, path):
        return self.pathmap.get(path)
        
    def get_dependencies(self, servername, filepath=None, localpath=None):
        sourcekey = self.sourcekey(servername, filepath, localpath)
        searchkeys = [x for x in self.reversemap.keys() if x.startswith(sourcekey)]
        deps = set()
        for key in searchkeys:
            deps.update(self.reversemap[key])
        return deps
        
def generate_config_hdf5(servername, blazeprefix, datapath, config):
    assert blazeprefix.startswith('/') and not blazeprefix.endswith('/')
    f = tables.openFile(datapath)
    for node in f.walkNodes("/"):
        if isinstance(node, tables.group.Group):
            nodetype = 'group'
        elif isinstance(node, tables.array.Array):
            nodetype = 'array'
        elif isinstance(node, tables.table.Table):
            nodetype = 'table'
        else:
            log.error('unknown type %s', node)
        serverpath = blazeprefix
        if node._v_pathname != "/": serverpath += node._v_pathname
        if nodetype == 'table':
            obj = config.table_obj(False,
                                   node.dtype,
                                   node.shape,
                                   {'shardtype' : 'noshards'},
                                   [config.source_obj(servername,
                                                     'hdf5',
                                                     datapath,
                                                     None)])
            config.create_dataset(serverpath, obj)
        elif nodetype == 'array':
            obj = config.array_obj(
                False, node.dtype, node.shape, {'shardtype' : 'noshards'},
                [config.source_obj(servername, 'hdf5', datapath, None,
                                   node._v_pathname)])
            config.create_dataset(serverpath, obj)

def merge_configs(baseconfig, newconfig):
    for k,v in newconfig.pathmap.iteritems():
        if v['type'] == 'group':
            if baseconfig.get_node(k) is None:
                baseconfig.create_group(k)
        else:
            baseset = baseconfig.get_node(k)
            ##we can't handle replicas, or sharding, but if we could,
            ##this is where that would go.
            assert baseset is None
            baseconfig.create_dataset(k, v)


                
if __name__ == "__main__":
    """
    (pathmapfile, reversemapfile,
     servername, blazeprefix, datapath) = sys.argv[1:]
    """
    import shelve
    import sys
    (pathmapfile, reversemapfile,
     servername, blazeprefix, datapath) = sys.argv[1:]
    config = BlazeConfig(shelve.open(pathmapfile, 'c'),
                         shelve.open(reversemapfile, 'c'))
    generate_config_hdf5(servername, blazeprefix, datapath, config)
    config.sync()
