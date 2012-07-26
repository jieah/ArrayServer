import unittest
import protocol
import numpy as np

class JSONTestCase(unittest.TestCase):
    def test_serialize_numpy_array(self):
        data = np.arange(5).astype('float64')
        data = protocol.serialize_json(data)
        data = protocol.deserialize_json(data)
        #we don't convert lists to numpy arrays, we just want to make sure
        #that we can go from numpy to lists/json
        assert data == [0.,1.,2.,3.,4.]
        
    def test_serialize_numpy_array_ints(self):        
        data = np.arange(5)
        data = protocol.serialize_json(data)
        data = protocol.deserialize_json(data)
        assert data == [0, 1, 2, 3, 4]
        
    def test_numpy_2d(self):
        data = np.array([[1,2],[3,4]])
        data = protocol.serialize_json(data)
        data = protocol.deserialize_json(data)
        assert data == [[1, 2], [3, 4]]
        
    def test_numpy_nested(self):
        data = np.array([[1,2],[3,4]])
        a =  {'a' : data}
        data = protocol.serialize_json(a)
        data = protocol.deserialize_json(data)
        assert data['a'] == [[1, 2], [3, 4]]


        




