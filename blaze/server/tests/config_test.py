import unittest
import blaze.server.blazeconfig as blazeconfig
import blaze.server.redisutils as redisutils
import shelve
import os
import numpy as np
import time

class ConfigTestCase(unittest.TestCase):
    def setUp(self):
        self.redisproc = redisutils.RedisProcess(9000, '/tmp', save=False)
        time.sleep(0.1)
        self.config = blazeconfig.BlazeConfig('testserver', port=9000)
        
    def tearDown(self):
        self.redisproc.close()

    def test_create_dataset(self):
        a = np.arange(200)
        appendable = False
        sources = []
        datasetobj = self.config.array_obj(sources)
        self.config.create_dataset("/path/here/myset",
                                   datasetobj)
        assert "/path/here" in self.config.list_children("/path")
        assert "/path/here/myset" in self.config.list_children("/path/here")
        node = self.config.get_metadata("/path/here/myset")
        sourceobj = self.config.source_obj(
            'testserver', 'hdf5',
            serverpath='/data/bin/data',
            localpath='/datasets/scan')
        self.config.add_source("/path/here/myset", sourceobj)
        assert '/path/here/myset' in self.config.get_dependencies(
            serverpath='/data/bin/data')
        assert '/path/here/myset' in self.config.get_dependencies(
            localpath='/datasets/scan')
        
    def test_load_from_hdf5(self):
        testroot = os.path.abspath(os.path.dirname(__file__))
        hdfpath = os.path.join(testroot, 'data', 'gold.hdf5')

        blazeconfig.generate_config_hdf5('myserver', '/data',
                                         hdfpath, self.config)
        node = self.config.get_metadata('/data/20100217/names')
        assert node['sources'][0]['localpath'] == '/20100217/names'
        assert node['sources'][0]['type'] == 'hdf5'
        assert '/data/20100217/names' in self.config.get_dependencies()
        assert '/data/20100217/names' in self.config.get_dependencies(
            localpath='/20100217/names')
        
    def test_load_from_numpy(self):
        testroot = os.path.abspath(os.path.dirname(__file__))
        path = os.path.join(testroot, 'data', 'test.npy')

        blazeconfig.generate_config_numpy('testserver', '/data/test', path,
                                          self.config)
        node = self.config.get_metadata('/data/test')
        assert node['sources'][0]['serverpath'] == path
        assert node['sources'][0]['type'] == 'numpy'
        assert '/data/test' in self.config.get_dependencies()

    def test_remove_source(self):
        a = np.arange(200)
        appendable = False
        sourceobj = self.config.source_obj('testserver', 'hdf5',
                                           serverpath='/data/bin/data',
                                           localpath='/datasets/scan')
        datasetobj = self.config.array_obj([sourceobj])
        self.config.create_dataset("/path/here/myset", datasetobj)
        self.config.add_source("/path/here/myset", sourceobj)
        self.config.remove_source("/path/here/myset", sourceobj)
        assert self.config.get_metadata("/path/here/myset") is None
        
    def test_remove_sources(self):
        a = np.arange(200)
        appendable = False
        sourceobj = self.config.source_obj('testserver', 'hdf5',
                                           serverpath='/data/bin/data',
                                           localpath='/datasets/scan')
        datasetobj = self.config.array_obj([sourceobj])
        self.config.create_dataset("/path/here/myset", datasetobj)
        sourceobj = self.config.source_obj('testserver', 'hdf5',
                                           serverpath='/data/bin/data2',
                                           localpath='/datasets/scan')
        datasetobj = self.config.array_obj([sourceobj])
        self.config.create_dataset("/path/here/myset2", datasetobj)
        assert self.config.get_metadata("/path/here/myset") is not None
        self.config.remove_sources(servername='testserver')
        assert self.config.get_metadata("/path/here/myset") is None

    def test_load_from_sources(self):
        testroot = os.path.abspath(os.path.dirname(__file__))
        hdfpath = os.path.join(testroot, 'data', 'gold.hdf5')
        sources = {'data' : {'type' : 'native',
                             'paths' : {'test' : hdfpath}}}
        self.config.load_sources(sources)
        node = self.config.get_metadata('/data/test/20100217/names')
        assert node['sources'][0]['localpath'] == '/20100217/names'
        assert node['sources'][0]['type'] == 'hdf5'
        assert '/data/test/20100217/names' in self.config.get_dependencies()
        assert '/data/test/20100217/names' in self.config.get_dependencies(
            localpath='/20100217/names')
        
    def test_remove_url(self):
        a = np.arange(200)
        appendable = False
        sourceobj = self.config.source_obj('testserver', 'hdf5',
                                           serverpath='/data/bin/data',
                                           localpath='/datasets/scan')
        datasetobj = self.config.array_obj([sourceobj])
        self.config.create_dataset("/path/here1/myset", datasetobj)
        self.config.create_dataset("/path/here1/myset2", datasetobj)
        
        sourceobj = self.config.source_obj('testserver', 'hdf5',
                                           serverpath='/data/bin/data2',
                                           localpath='/datasets/scan')
        datasetobj = self.config.array_obj([sourceobj])
        self.config.create_dataset("/path/here2/myset", datasetobj)
        
        self.config.remove_url("/path/here1")
        assert self.config.get_metadata("/path/here1/myset") is None
        assert self.config.get_metadata("/path/here1/myset2") is None
        assert self.config.get_metadata("/path/here1") is None
        assert self.config.get_metadata("/path/here2/myset") is not None


