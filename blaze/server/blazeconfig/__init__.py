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
import numpy
import posixpath as blazepath
import blaze.server.redisutils as redisutils
import redis
import redis.exceptions 
import cPickle as pickle
import glob
import urllib

def encode(str):
    return urllib.quote(str)

def decode(str):
    return urllib.unquote(str)

def serialize(obj):
    return pickle.dumps(obj)
        
def deserialize(strdata):
    if strdata is None:
        return None
    else:
        return pickle.loads(strdata)
    
def kget(client, key):
    return deserialize(client.get(key))
    
def kset(client, key, val):
    client.set(key, serialize(val))
    
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
        with self.client.pipeline() as pipe:
            rootkey = self.pathmap_key("/")            
            pipe.watch(self.pathmap_key("/"))
            pipe.multi()
            group = self.get_pathmap_obj("/")
            if not group:
                self.set_pathmap_obj("/", self.group_obj([]), client=pipe)
            pipe.execute()
        if sourceconfig is not None:
            self.load_sources(sourceconfig)
        
    def get_reversemap_paths(self, sourcekey, client=None):
        if client is None: client = self.client
        return hget(client, self.reversemap_key(), sourcekey)
    
    def set_reversemap_paths(self, sourcekey, paths, client=None):
        if client is None: client = self.client
        hset(client, self.reversemap_key(), sourcekey, paths)        

    def delete_reversemap_paths(self, sourcekey, client=None):
        if client is None: client = self.client        
        client.hdel(self.reversemap_key(), sourcekey)

    def reverse_map_exists(self, sourcekey, client=None):
        if client is None: client = self.client
        return client.hexists(self.reversemap_key(), sourcekey)
    
    def add_to_group(self, path, name):
        with self.client.pipeline() as pipe:
            key = self.pathmap_key(path)
            pipe.watch(key)
            pipe.multi()
            group = self.get_pathmap_obj(path)
            if group is None:
                self.set_pathmap_obj(path, self.group_obj([name]), client=pipe)
            elif group['type'] == 'group':
                group['children'].append(name)
                self.set_pathmap_obj(path, group, client=pipe)
            else:
                raise BlazeConfigError, "item at %s is not a group" % path
            pipe.execute()
            
    def is_group(self, path, client=None):
        if client is None: client = self.client
        return kget(client, self.pathmap_key(path))['type'] == 'group'
    
    def is_none(self, path, client=None):
        if client is None: client = self.client
        return client.type(self.pathmap_key(path)) == 'none'
            
    def set_pathmap_obj(self, path, obj, client=None):
        if client is None: client = self.client        
        kset(client, self.pathmap_key(path), obj)
    
    def delete_pathmap_obj(self, path, client=None):
        if client is None: client = self.client
        client.delete(self.pathmap_key(path))
        
    def get_pathmap_obj(self, path, client=None):
        if client is None: client = self.client
        return kget(client, self.pathmap_key(path))

    def pathmap_key(self, path):
        return 'pathmap:' + path
    
    def reversemap_key(self):
        return 'reversemap:%s' % (self.servername)
    
    def get_tree(self, path, depth=None):
        """depth None, is infinite depth
        """
        metadata = self.get_metadata(path)
        metadata['url'] = path
        if depth == 0:
            return metadata
        if metadata['type'] == 'group':
            new_children = []
            for child in metadata['children']:
                newdepth = depth - 1 if depth is not None else None
                childmetadata = self.get_tree(blazepath.join(path, child),
                                              newdepth)
                new_children.append(childmetadata)
            metadata['children'] = new_children
        return metadata
    
    def load_native(path, url):
        import pdb;pdb.set_trace()
    
    def load_sources(self, sources):
        for prefix, source in sources.iteritems():
            if source['type'] == 'native':
                for filegroup, path in source['paths'].iteritems():
                    url = blazepath.join('/', prefix, filegroup)
                    if os.path.isdir(path):
                        load_dir(self.servername, url, path, self)
                    else :
                        load_file(self.servername, url, path, self)
            if source['type'] == 'disco':
                import disco.ddfs as ddfs
                d = ddfs.DDFS(master=source['connection'])
                tags = d.list()
                for tag in tags:
                    num_files = len(list(d.pull(tag)))
                    for n in range(num_files):
                        url = blazepath.join('/', prefix, tag, str(n))
                        log.error('ADDING %s', url)
                        sourceobj = self.source_obj(
                            self.servername, 'disco',
                            tag=tag, index=str(n), conn=source['connection'])
                        obj = self.disco_obj(sources=[sourceobj])
                        self.create_dataset(url, obj)
    
    def source_obj(self, servername, type, **kwargs):
        base = {'type' : type,
                'servername' : servername}
        for k,v in kwargs.iteritems():
            base[k] = v
        return base
    
    def group_obj(self, children):
        return {'type' : 'group',
                'children' : children}
            
    def array_obj(self, sources):
        return {'type' : 'array',
                'sources' : sources}
    
    def disco_obj(self, sources):
        return {'type' : 'disco',
                'sources' : sources}
    
    def ensuregroups(self, path):
        if self.is_none(path):
            parentpath = blazepath.dirname(path)
            parentkey = self.pathmap_key(parentpath)
            name = blazepath.basename(path)
            self.ensuregroups(parentpath)
            self.add_to_group(parentpath, name)
        
    def add_reverse_map(self, path, source):
        with self.client.pipeline() as pipe:
            pipe.watch(self.reversemap_key)
            self._add_reverse_map(pipe, path, source)
            pipe.execute()
                
    def _add_reverse_map(self, writeclient, path, source):
        sourcekey = self.sourcekey(source)
        if self.reverse_map_exists(sourcekey):
            paths = self.get_reversemap_paths(sourcekey)
            if path not in paths: paths.append(path)
        else:
            paths = [path]
        self.set_reversemap_paths(sourcekey, paths, client=writeclient)
            
    def get_metadata(self, path):
        return self.get_pathmap_obj(path)        
            
    def create_dataset(self, path, obj):
        self.ensuregroups(path)
        #add reverse maps for sources
        #add current node
        with self.client.pipeline() as pipe:
            newkey = self.pathmap_key(path)
            pipe.watch(self.reversemap_key())
            pipe.watch(newkey)
            pipe.multi()
            for source in obj['sources']:
                self._add_reverse_map(pipe, path, source)
            self.set_pathmap_obj(path, obj, client=pipe)
            pipe.execute()
        
    def list_children(self, path):
        paths = self.get_pathmap_obj(path)['children']
        return [blazepath.join(path, x) for x in paths]
            
    def add_source(self, path, source):
        with self.client.pipeline() as pipe:
            pipe.watch(self.pathmap_key(path))
            pipe.watch(self.reversemap_key())                
            node = self.get_pathmap_obj(path)
            if source not in node['sources']:
                node['sources'].append(source)
            self.set_pathmap_obj(path, node, client=pipe)
            self._add_reverse_map(pipe, path, source)
            
    def sourcekey(self, sourceobj):
        keys = sourceobj.keys()
        keys.sort()
        vals = []
        for k in keys:
            vals.append(encode(k))
            vals.append(encode(sourceobj[k]))
        return ":".join(vals)
    
    def parse_sourcekey(self, sourcekey):
        data = sourcekey.split(":")
        data = [(decode(data[x]), decode(data[x+1])) \
                for x in range(0, len(data), 2)]
        return dict(data)
    
    def get_matching_reversemap_keys(self, **kwargs):
        keys = self.client.hkeys(self.reversemap_key())
        matching = set()
        for key in keys:
            sourceobj = self.parse_sourcekey(key)
            if all([(kwargs.get(k) == sourceobj.get(k)) for k in kwargs.keys()]):
                matching.add(key)
        return matching
    
    def get_dependencies(self, **kwargs):
        keys = self.get_matching_reversemap_keys(**kwargs)
        deps = set()
        for key in keys:
            deps.update(self.get_reversemap_paths(key))
        return deps
    
    def remove_sources(self, **kwargs):
        """remove all sources that match the kwargs passed in.
        remove any urls which are empty as a result
        """
        matching_keys = self.get_matching_reversemap_keys(**kwargs)
        for key in matching_keys:
            sourceobj = self.parse_sourcekey(key)
            paths = self.get_reversemap_paths(key)
            for p in paths:
                self.remove_source(p, sourceobj)
                
    def remove_source(self, path, source):
        with self.client.pipeline() as pipe:
            pipe.watch(self.pathmap_key(path))
            pipe.watch(self.reversemap_key())
            pipe.multi()
            self._remove_source(pipe, path, source)
            pipe.execute()
            
    def _remove_source(self, writeclient, path, source):
        node = self.get_pathmap_obj(path)
        newsources = [x for x in node['sources'] if x != source]
        self._remove_reverse_map(writeclient, path, source)
        if len(newsources) > 0:
            node['sources'] = newsources
            self.set_pathmap_obj(path, node, client=writeclient)
        else:
            self.delete_pathmap_obj(path, client=writeclient)

    def _remove_reverse_map(self, writeclient, path, source):
        sourcekey = self.sourcekey(source)
        paths = self.get_reversemap_paths(sourcekey)
        if paths is not None:
            paths = [x for x in paths if paths != path]
            if len(paths) == 0:
                self.delete_reversemap_paths(sourcekey, client=writeclient)
            else:
                self.set_reversemap_paths(sourcekey, paths, client=writeclient)
        else:
            raise BlazeConfigError
        
    def watch_tree(self, pipe, path):
        pipe.watch(self.pathmap_key(path))
        if self.is_group(path):
            child_paths = self.list_children(path)
            for cpath in child_paths:
                self.watch_tree(pipe, cpath)
        
    def remove_url(self, path):
        parentpath = blazepath.dirname(path)
        name = blazepath.basename(path)
        with self.client.pipeline() as pipe:
            pipe.watch(self.reversemap_key())
            pipe.watch(self.pathmap_key(parentpath))
            self.watch_tree(pipe, path)
            
            pipe.multi()
            
            parentobj = self.get_pathmap_obj(parentpath)
            parentobj['children'] = [x for x in parentobj['children'] if x != name]
            self.set_pathmap_obj(parentpath, parentobj, client=pipe)

            self._remove_url(pipe, path)
            pipe.execute()
            
    def _remove_url(self, writeclient, path):
        print 'REMOVING', path
        parentpath = blazepath.dirname(path)
        name = blazepath.basename(path)        
        metadata = self.get_metadata(path)
        if metadata['type'] == 'group':
            for childpath in metadata['children']:
                self._remove_url(writeclient, blazepath.join(path, childpath))
            self.delete_pathmap_obj(path, client=writeclient)
        else:
            for source in metadata['sources']:
                self._remove_source(writeclient, path, source)

                                

def generate_config_hdf5(servername, blazeprefix, datapath, config):
    assert blazeprefix.startswith('/') and not blazeprefix.endswith('/')
    if tables.isHDF5File(datapath):
        f = tables.openFile(datapath)
    else:
        return None
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

def generate_config_csv(servername, blazeprefix, filepath, config):
    obj = config.array_obj(
            [config.source_obj(servername, 'csv', serverpath=filepath)])
    blazeurl = blazeprefix
    config.create_dataset(blazeurl, obj)
    
def load_file(servername, blazeprefix, filepath, config):
    try:
        if tables.isHDF5File(filepath):
            generate_config_hdf5(servername, blazeprefix, filepath, config)
        elif os.path.splitext(filepath)[-1] in ['.npy', '.npz']:
            generate_config_numpy(servername, blazeprefix, filepath, config)
        else:
            generate_config_csv(servername, blazeprefix, filepath, config)
        return            
    except Exception as e:
        log.exception(e)

    
def load_dir(servername, blazeprefix, datadir, config,
             ignore=['redis.db', 'redis.log', 'blaze.config', 'blaze.pid', 'CDX.pid']):
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
            load_file(servername, blaze_url, fpath, config)
    
    

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
