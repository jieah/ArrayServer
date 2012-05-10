
import numpy
import blaze.array_proxy.npproxy as npp
from numpy.testing.utils import *
from numpy import pi
import unittest

from blaze.array_proxy.array_proxy import ArrayProxy, ArrayNode


def func1(np):
    x = np.arange(-2.*pi, 2*pi, pi/10)
    return np.sin(x)

def func2(np):
    x = np.linspace(-2*pi, 2*pi, 10)
    s = np.sin(x)
    c = np.cos(x)
    return np.where(s > c, s, c)

class ArrayProxyTest(unittest.TestCase):

    def verify(self,func):
        npval = func(numpy)
        print "Numpy:", npval
        nppval = func(npp).eval()
        print "Proxy:", nppval
        assert_array_equal(npval, nppval)

    def test_main(self):
        self.verify(func1)
        self.verify(func2)

if __name__ == "__main__":
    grapheval.DEBUG = False
    unittest.main()

