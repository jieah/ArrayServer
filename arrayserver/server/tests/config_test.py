import unittest
import arrayserver.server.arrayserverconfig as arrayserverconfig
import arrayserver.server.redisutils as redisutils
import shelve
import os
import numpy as np
import time

class ConfigTestCase(unittest.TestCase):
    def setUp(self):
        self.redisproc = redisutils.RedisProcess(9000, '/tmp', save=False)
        testroot = os.path.abspath(os.path.dirname(__file__))        
        time.sleep(0.1)
        self.config = arrayserverconfig.ArrayServerConfig('testserver', port=9000)
        self.hdfpath = os.path.join(testroot, 'data', 'random.hdf5')
        
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
        hdfpath = os.path.join(testroot, 'data', 'random.hdf5')

        arrayserverconfig.generate_config_hdf5('myserver', '/data',
                                         hdfpath, self.config)
        node = self.config.get_metadata('/data/a')
        assert node['sources'][0]['localpath'] == '/a'
        assert node['sources'][0]['type'] == 'hdf5'
        assert '/data/a' in self.config.get_dependencies()
        assert '/data/a' in self.config.get_dependencies(
            localpath='/a')
        
    def test_load_from_numpy(self):
        testroot = os.path.abspath(os.path.dirname(__file__))
        path = os.path.join(testroot, 'data', 'test.npy')

        arrayserverconfig.generate_config_numpy('testserver', '/data/test', path,
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
        hdfpath = self.hdfpath
        source = {'prefix' : "/data/test",
                  'type' : 'native',
                  'paths' : [hdfpath]}
        self.config.load_source(**source)
        node = self.config.get_metadata('/data/test/a')
        assert node['sources'][0]['localpath'] == '/a'
        assert node['sources'][0]['type'] == 'hdf5'
        assert '/data/test/a' in self.config.get_dependencies()
        assert '/data/test/a' in self.config.get_dependencies(
            localpath='/a')
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


