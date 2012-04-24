
import numpy
import npproxy as npp
from numpy.testing.utils import *
from numpy import pi

from array_proxy import ArrayProxy, ArrayNode


def test1(np):
    x = np.arange(-2.*pi, 2*pi, pi/10)
    return np.sin(x)

def test2(np):
    x = np.linspace(-2*pi, 2*pi, 10)
    s = np.sin(x)
    c = np.cos(x)
    return np.where(s > c, s, c)

def verify(func):
    npval = func(numpy)
    print "Numpy:", npval
    nppval = func(npp).eval()
    print "Proxy:", nppval
    assert_array_equal(npval, nppval)

def testmain():
    verify(test1)
    verify(test2)

if __name__ == "__main__":
    import grapheval
    grapheval.DEBUG = False
    testmain()

