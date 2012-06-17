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
        del self.redisproc
        
    def test_create_group(self):
        self.config.create_group("/path/here/again")
        assert "/path/here" in self.config.list_children("/path")
        assert "/path/here/again" in self.config.list_children("/path/here")

    def test_create_dataset(self):
        a = np.arange(200)
        appendable = False
        sources = []
        datasetobj = self.config.array_obj(sources)
        self.config.create_dataset("/path/here/myset",
                                   datasetobj)
        assert "/path/here" in self.config.list_children("/path")
        assert "/path/here/myset" in self.config.list_children("/path/here")
        node = self.config.get_node("/path/here/myset")
        sourceobj = self.config.source_obj(
            'testserver', 'hdf5',
            serverpath='/data/bin/data',
            localpath='/datasets/scan')
        self.config.add_source("/path/here/myset", sourceobj)
        assert '/path/here/myset' in self.config.get_dependencies(
            serverpath='/data/bin/data')
        assert '/path/here/myset' in self.config.get_dependencies(
            localpath='/datasets/scan')
        
    def test_from_hdf5(self):
        testroot = os.path.abspath(os.path.dirname(__file__))
        hdfpath = os.path.join(testroot, 'gold.hdf5')

        blazeconfig.generate_config_hdf5('myserver', '/data',
                                         hdfpath, self.config)
        node = self.config.get_node('/data/20100217/names')
        assert node['sources'][0]['localpath'] == '/20100217/names'
        assert node['sources'][0]['type'] == 'hdf5'
        assert '/data/20100217/names' in self.config.get_dependencies()
        assert '/data/20100217/names' in self.config.get_dependencies(
            localpath='/20100217/names')
        
    def test_from_numpy(self):
        testroot = os.path.abspath(os.path.dirname(__file__))
        path = os.path.join(testroot, 'test.npy')

        blazeconfig.generate_config_numpy('testserver', '/data/test', path,
                                          self.config)
        node = self.config.get_node('/data/test')
        assert node['sources'][0]['serverpath'] == path
        assert node['sources'][0]['type'] == 'numpy'
        assert '/data/test' in self.config.get_dependencies()

    def test_remove(self):
        a = np.arange(200)
        appendable = False
        sources = []
        sourceobj = self.config.source_obj('testserver', 'hdf5',
                                           servepath='/data/bin/data',
                                           localpath='/datasets/scan')
        datasetobj = self.config.array_obj(sources)
        self.config.create_dataset("/path/here/myset",
                                   datasetobj)
        self.config.add_source("/path/here/myset", sourceobj)
        self.config.remove_source("/path/here/myset", sourceobj)
        assert self.config.get_node("/path/here/myset") is None




