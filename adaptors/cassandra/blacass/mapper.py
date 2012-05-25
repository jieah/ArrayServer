import yaml
import numpy as np

class StringType(object):
    def __init__(self, python=False, length=-1, trunc_err=False):
        self.python = python
        self.length = length
        self.trunc_err = trunc_err
    def __repr__(self):
        return "%s(python=%r, length=%r, trunc_err=%r)" % (
            self.__class__.__name__, self.python, self.length, self.trunc_err)

class Bytes(StringType):
    def __init__(self, python=False, length=-1, trunc_err=False):
        StringType.__init__(self, python, length, trunc_err)
        self.dtype = np.str_

class Unicode(StringType):
    def __init__(self, python=False, length=-1, trunc_err=False):
        StringType.__init__(self, python, length, trunc_err)
        self.dtype = np.unicode_

class NumberType(object):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        self.default = default
        self.sentinel = sentinel
        self.nonnull = nonnull
        self.length = 1
    def __repr__(self):
        return "%s(default=%r, sentinel=%r, nonnull=%r)" % (
            self.__class__.__name__, self.default, self.sentinel, self.nonnull)

class Int8(NumberType):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        NumberType.__init__(self, default, sentinel, nonnull)
        self.dtype = np.int8

class UInt8(NumberType):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        NumberType.__init__(self, default, sentinel, nonnull)
        self.dtype = np.uint8

class Int16(NumberType):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        NumberType.__init__(self, default, sentinel, nonnull)
        self.dtype = np.int16

class UInt16(NumberType):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        NumberType.__init__(self, default, sentinel, nonnull)
        self.dtype = np.uint16

class Int32(NumberType):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        NumberType.__init__(self, default, sentinel, nonnull)
        self.dtype = np.int32

class UInt32(NumberType):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        NumberType.__init__(self, default, sentinel, nonnull)
        self.dtype = np.uint32

class Int64(NumberType):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        NumberType.__init__(self, default, sentinel, nonnull)
        self.dtype = np.int64

class UInt64(NumberType):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        NumberType.__init__(self, default, sentinel, nonnull)
        self.dtype = np.uint64

class Float32(NumberType):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        NumberType.__init__(self, default, sentinel, nonnull)
        self.dtype = np.float32

class Float64(NumberType):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        NumberType.__init__(self, default, sentinel, nonnull)
        self.dtype = np.float64

class DateType(object):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        self.default = default
        self.sentinel = sentinel
        self.nonnull = nonnull
        self.length = 1
    def __repr__(self):
        return "%s(default=%r, sentinel=%r, nonnull=%r)" % (
            self.__class__.__name__, self.default, self.sentinel, self.nonnull)

class Datetime64(DateType):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        DateType.__init__(self, default, sentinel, nonnull)
        self.dtype = "datetime64[us]"


# Allowed types in YAML configuration file
ktypes = ['Bytes', 'Unicode',
          'Int8', 'UInt8',
          'Int16', 'UInt16',
          'Int32', 'UInt32',
          'Int64', 'UInt64',
          'Float32', 'Float64',
          'Datetime64']


def mapper(filename, keyspace, columnfamily):
    f = open(filename, 'r')

    config = yaml.load(f)
    print config
    f.close()

    cf = config['Keyspaces'][keyspace][columnfamily]
    retcf = {}
    for name in cf:
        ks = cf[name]
        kn = ks.split('(')[0].strip()
        if kn not in ktypes:
            raise ValueError("Data type `%s` not understood" % ks)
        try:
            klass = eval(ks)
        except:
            print "Error when processing %s entry in %s ComlumnFamily"
            raise
        retcf[name] = klass

    return retcf

if __name__ == "__main__":
    retcf = mapper('get-range.yaml', 'Keyspace4', 'ColumnFamily2')
    print "retcf->", retcf
