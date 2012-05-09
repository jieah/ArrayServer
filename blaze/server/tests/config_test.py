import unittest
import blazeconfig
import shelve
import os
import numpy as np


class InMemoryConfigTestCase(unittest.TestCase):
    def setUp(self):
        self.config = blazeconfig.BlazeConfig(blazeconfig.InMemoryMap(),
                                              blazeconfig.InMemoryMap())
    def test_create_group(self):
        self.config.create_group("/path/here/again")
        assert "/path/here" in self.config.list_children("/path")
        assert "/path/here/again" in self.config.list_children("/path/here")

    def test_create_dataset(self):
        a = np.arange(200)
        appendable = False
        sources = []
        datasetobj = self.config.array_obj(appendable,
                                           a.dtype,
                                           a.shape,
                                           {'shardtype' : 'noshards'},
                                           sources)
        self.config.create_dataset("/path/here/myset",
                                   datasetobj)
        assert "/path/here" in self.config.list_children("/path")
        assert "/path/here/myset" in self.config.list_children("/path/here")
        node = self.config.get_node("/path/here/myset")
        assert node['dtype'] == a.dtype
        assert node['shape'] == a.shape
        sourceobj = self.config.source_obj(
            'server1', 'hdf5', '/data/bin/data',
            {'shardtype' : 'noshards'},
            '/datasets/scan')
        self.config.add_source("/path/here/myset", sourceobj)
        assert '/path/here/myset' in self.config.get_dependencies('server1')
        assert '/path/here/myset' in self.config.get_dependencies('server1',
                                                                  '/data/bin/data')
        assert '/path/here/myset' in self.config.get_dependencies('server1',
                                                                  '/data/bin/data',
                                                                  '/datasets/scan')
    def from_hdf5_test(self):
        testroot = os.path.abspath(os.path.dirname(__file__))
        hdfpath = os.path.join(testroot, 'gold.hdf5')
        
        blazeconfig.generate_config_hdf5('myserver', '/hugodata',
                                         hdfpath, self.config)
        node = self.config.get_node('/hugodata/20100217/names')
        assert node['shape'] ==  (3,)
        assert node['sources'][0]['localpath'] == '/20100217/names'
        assert '/hugodata/20100217/names' in self.config.get_dependencies('myserver')
    def test_remove(self):
        a = np.arange(200)
        appendable = False
        sources = []
        sourceobj = self.config.source_obj(
            'server1', 'hdf5', '/data/bin/data',
            {'shardtype' : 'noshards'},
            '/datasets/scan')
        datasetobj = self.config.array_obj(appendable,
                                           a.dtype,
                                           a.shape,
                                           {'shardtype' : 'noshards'},
                                           sources)
        self.config.create_dataset("/path/here/myset",
                                   datasetobj)
        self.config.add_source("/path/here/myset", sourceobj)
        self.config.remove_source("/path/here/myset", sourceobj)
        assert self.config.get_node("/path/here/myset") is None

class PersistentConfigTestCase(InMemoryConfigTestCase):
    def setUp(self):
        testroot = os.path.abspath(os.path.dirname(__file__))
        self.pathmapfname = os.path.join(testroot, 'pathmap.db')
        self.pathmap = shelve.open(self.pathmapfname, 'c')
        self.reversemapfname = os.path.join(testroot, 'reversemap.db')
        self.reversemap = shelve.open(self.reversemapfname, 'c')
        self.config = blazeconfig.BlazeConfig(self.pathmap,
                                              self.reversemap)
        
    def tearDown(self):
        os.remove(self.pathmapfname)
        os.remove(self.reversemapfname)


