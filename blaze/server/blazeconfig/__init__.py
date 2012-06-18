import tables
import logging
import shelve
import os
#wow our naming is ridiculous...
import blaze.server.rpc.server as server
import orderedyaml
import yaml
log = logging.getLogger(__name__)
import collections
import simplejson
import numpy
import posixpath as blazepath
import blaze.server.redisutils as redisutils
import redis
import cPickle as pickle
import glob

def serialize(obj):
    return pickle.dumps(obj)
        
def deserialize(strdata):
    if strdata is None:
        return None
    else:
        return pickle.loads(strdata)
    
    
def hget(client, key, field):
    return deserialize(client.hget(key, field))

def hset(client, key, field, val):
    return client.hset(key, field, serialize(val))


#dict with a sync function that does nothing,
#this way we can use the same code for
#the in memory, or on disk maps
def path_split(fpath, pathm=os.path):
    paths = []
    currpath = fpath
    while currpath != pathm.dirname(currpath):
        currpath, head = pathm.split(currpath)
        paths.append(head)
    paths.reverse()
    return paths

def path_history(path):
    paths = []
    currpath = path
    while currpath != "/":
        paths.append(currpath)
        base = blazepath.dirname(currpath)
        currpath = base
    paths.append("/")
    paths.reverse()
    return paths

class BlazeConfigError(Exception):
    pass

class BlazeConfig(object):
    def __init__(self, servername, sourceconfig=None,
                 host='localhost', port=6709):
        """
        Parameters
        ---------
        servername : name of this server
        sourceconfig : usually loaded from yaml,
            tells blaze which data sources it knows about
        """
        self.servername = servername
        self.sourceconfig = sourceconfig
        self.client = redis.Redis(host=host, port=port)
        self.pathmap_key = 'pathmap'
        self.reversemap_key = 'reversemap:' + self.servername
        if not self.client.hexists(self.pathmap_key, '/'):
            with self.client.pipeline() as pipe:
                pipe.watch(self.pathmap_key)
                pipe.multi()
                if not self.client.hexists(self.pathmap_key, '/'):
                    hset(pipe, self.pathmap_key, '/', self.group_obj([]))
                pipe.execute()
        if sourceconfig is not None:
            self.load_sources(sourceconfig)
                
    def load_sources(self, sources):
        for prefix, source in sources.iteritems():
            if source['type'] == 'native':
                for filegroup, path in source['paths'].iteritems():
                    url = blazepath.join('/', prefix, filegroup)
                    if os.path.isdir(path):
                        load_dir(path, url, self.servername, self)
                    else:
                        generate_config_hdf5(
                            self.servername, url, path, self)
                    
    def create_group(self, path):
        self.safe_insert(blazepath.dirname(path),
                         blazepath.basename(path),
                         self.group_obj([]))

    def group_obj(self, children):
        return {'type' : 'group',
                'children' : children}
    
    def source_obj(self, servername, type, **kwargs):
        base = {'type' : type,
                'servername' : servername}
        for k,v in kwargs.iteritems():
            base[k] = v
        return base

    def array_obj(self, sources):
        return {'type' : 'array',
                'sources' : sources}

    def safe_insert(self, parentpath, name, obj):
        with self.client.pipeline() as pipe:
            pipe.watch(self.pathmap_key)
            pipe.multi()
            self._safe_insert(pipe, parentpath, name, obj)
            pipe.execute()
        
    def _safe_insert(self, client, parentpath, name, obj):        
        newpath = blazepath.join(parentpath, name)
        if not self.client.hexists(self.pathmap_key, parentpath):
            parent = self._safe_insert(client, 
                blazepath.dirname(parentpath),
                blazepath.basename(parentpath),
                self.group_obj([]))
        else:
            parent = hget(self.client, self.pathmap_key, parentpath)
        if name not in parent['children']:
            parent['children'].append(name)
            hset(client, self.pathmap_key, parentpath, parent)
        hset(client, self.pathmap_key, newpath, obj)
        return obj
    
    def list_children(self, path):
        group = hget(self.client, self.pathmap_key, path)
        if group is None or group['type'] != 'group':
            raise BlazeConfigError
        else:
            return [blazepath.join(path, x) for x in group['children']]

    def create_dataset(self, path, obj):
        with self.client.pipeline() as pipe:
            pipe.watch(self.pathmap_key)
            pipe.watch(self.reversemap_key)
            pipe.multi()
            self._safe_insert(pipe, blazepath.dirname(path),
                              blazepath.basename(path), obj)
            for source in obj['sources']:
                self._add_reverse_map(pipe, path, source)
            pipe.execute()
            
    def add_source(self, path, source):
        with self.client.pipeline() as pipe:
            pipe.watch(self.pathmap_key)
            pipe.watch(self.reversemap_key)                
            node = hget(pipe, self.pathmap_key, path)
            if source not in node['sources']:
                node['sources'].append(source)
            self._add_reverse_map(pipe, path, source)
            
    def sourcekey(self, sourceobj):
        keys = sourceobj.keys()
        keys.sort()
        vals = []
        for k in keys:
            vals.append(k)
            vals.append(sourceobj[k])
        return ":".join(vals)
    
    def parse_sourcekey(self, sourcekey):
        data = sourcekey.split(":")
        data = [(data[x], data[x+1]) for x in range(0, len(data), 2)]
        return dict(data)
    
    def add_reverse_map(self, path, source):
        with self.client.pipeline() as pipe:
            pipe.watch(self.reversemap_key)
            self._add_reverse_map(pipe, path, source)
            pipe.execute()
                
    def _add_reverse_map(self, client, path, source):
        sourcekey = self.sourcekey(source)
        if not self.client.hexists(self.reversemap_key, sourcekey):
            hset(client, self.reversemap_key, sourcekey, [path])
        else:
            paths = hget(self.client, self.reversemap_key, sourcekey)
            if path not in paths:
                paths.append(path)
            hset(client, self.reversemap_key, sourcekey, paths)
            
    def get_metadata(self, path):
        return hget(self.client, self.pathmap_key, path)
    
    def get_dependencies(self, **kwargs):
        keys = self.client.hkeys(self.reversemap_key)
        deps = set()        
        for key in keys:
            sourceobj = self.parse_sourcekey(key)
            if all([(kwargs.get(k) == sourceobj.get(k)) for k in kwargs.keys()]):
                deps.update(hget(self.client, self.reversemap_key, key))
        return deps
    
    def remove(self, **kwargs):
        with self.client.pipeline() as pipe:
            pipe.watch(self.pathmap_key)
            pipe.watch(self.reversemap_key)
            pipe.multi()
            self._remove(client, **kwargs)
            pipe.execute()
            
    def _remove(self, client, **kwargs):
        affected_paths = self.get_dependencies(**kwargs)
        for path in affected_paths:
            sources = hget(client, self.pathmap_key, path)
            to_remove = []
            for source in sources:
                if all([(kwargs.get(k)==sourceobj.get(k)) for k in kwargs.keys()]):
                    to_remove.append(source)
            for source in to_remove:
                self._remove_source(path, source)
                
    def remove_source(self, path, source):
        with self.client.pipeline() as pipe:
            pipe.watch(self.pathmap_key)
            pipe.watch(self.reversemap_key)
            pipe.multi()
            self._remove_source(pipe, path, source)
            pipe.execute()
            
    def _remove_source(self, client, path, source):
        node = hget(self.client, self.pathmap_key, path)
        newsources = [x for x in node['sources'] if x != source]
        self._remove_reverse_map(client, path, source)
        if len(newsources) > 0:
            node['sources'] = newsources
            hset(client, self.pathmap_key, path, node)
        else:
            client.hdel(self.pathmap_key, path)

    def _remove_reverse_map(self, client, path, source):
        sourcekey = self.sourcekey(source)
        paths = hget(self.client, path, source)
        if paths is not None:
            paths = [x for x in paths if paths != path]
            if len(paths) == 0:
                client.hdel(self.reversemap_key, sourcekey)
            else:
                hset(client, self.reversemap_key, sourcekey, paths)

def generate_config_hdf5(servername, blazeprefix, datapath, config):
    assert blazeprefix.startswith('/') and not blazeprefix.endswith('/')
    try:
        f = tables.openFile(datapath)
    except Exception as e:
        #log.exception(e)
        return
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
            obj = config.array_obj([config.source_obj(servername,
                                                      'hdf5',
                                                      serverpath=datapath,
                                                      localpath=node._v_pathname)])
            config.create_dataset(serverpath, obj)
        elif nodetype == 'array':
            obj = config.array_obj([config.source_obj(servername,
                                                      'hdf5',
                                                      serverpath=datapath,
                                                      localpath=node._v_pathname)])
            config.create_dataset(serverpath, obj)

def generate_config_numpy(servername, blazeprefix, filepath, config):
    assert blazeprefix.startswith('/') and not blazeprefix.endswith('/')
    arr = numpy.load(filepath)
    obj = config.array_obj(
            [config.source_obj(servername, 'numpy', serverpath=filepath)])
    blazeurl = blazeprefix
    config.create_dataset(blazeurl, obj)

def load_dir(datadir, blazeprefix, servername, config,
             ignore=['redis.db', 'redis.log', 'blaze.config']):
    ignore = set([os.path.join(datadir, x) for x in ignore])
    base_split_names = path_split(datadir)
    base_split_names = [x for x in base_split_names if x != '']
    for dirpath, dirnames, filenames in os.walk(datadir):
        for f in filenames:
            fpath = os.path.join(dirpath, f)
            if fpath in ignore:
                continue
            file_split_names = path_split(fpath)
            file_split_names = file_split_names[len(base_split_names):]
            blaze_url = blazepath.join(blazeprefix, *file_split_names)
            generate_config_hdf5(servername, blaze_url, fpath, config)



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
