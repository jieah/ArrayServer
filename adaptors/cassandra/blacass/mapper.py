#
# Module for reading a YAML config file that explicits how to map Cassandra data into NumPy
#
# Author: Francesc Alted
# Date: 2012-05-26

import yaml


# Allowed types in YAML configuration file and mapping to NumPy dtypes
ktypes = {'Bytes': 'str_',
          'Unicode': 'unicode_',
          'Int8': 'int8',
          'UInt8': 'uint8',
          'Int16': 'int16',
          'UInt16': 'uint16',
          'Int32': 'int32',
          'UInt32': 'uint32',
          'Int64': 'int64',
          'UInt64': 'uint64',
          'Float32': 'float32',
          'Float64': 'float64',
          'Datetime64': 'datetime64[us]',   # default is to use microseconds units
          }


class StringType(object):
    def __init__(self, python=False, length=-1, trunc_err=False):
        self.python = python
        self.length = length
        self.trunc_err = trunc_err
        self.dtype = ktypes[self.__class__.__name__]
    def __repr__(self):
        return "%s(python=%r, length=%r, trunc_err=%r)" % (
            self.__class__.__name__, self.python, self.length, self.trunc_err)

class Bytes(StringType):
    pass

class Unicode(StringType):
    pass

class NumberType(object):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        self.default = default
        self.sentinel = sentinel
        self.nonnull = nonnull
        self.length = 1
        self.dtype = ktypes[self.__class__.__name__]
    def __repr__(self):
        return "%s(default=%r, sentinel=%r, nonnull=%r)" % (
            self.__class__.__name__, self.default, self.sentinel, self.nonnull)

class Int8(NumberType):
    pass

class UInt8(NumberType):
    pass

class Int16(NumberType):
    pass

class UInt16(NumberType):
    pass

class Int32(NumberType):
    pass

class UInt32(NumberType):
    pass

class Int64(NumberType):
    pass

class UInt64(NumberType):
    pass

class Float32(NumberType):
    pass

class Float64(NumberType):
    pass

class DateType(object):
    def __init__(self, default=None, sentinel=None, nonnull=False):
        self.default = default
        self.sentinel = sentinel
        self.nonnull = nonnull
        self.length = 1
        self.dtype = ktypes[self.__class__.__name__]
    def __repr__(self):
        return "%s(default=%r, sentinel=%r, nonnull=%r)" % (
            self.__class__.__name__, self.default, self.sentinel, self.nonnull)

class Datetime64(DateType):
    pass



def mapper(filename, keyspace, columnfamily):
    """Parse config file to get info about the conversions.

    `filename` -- The configuration file (YAML format)
    `keyspace` -- The keyspace to select
    `columnfamily` -- The ColumnFamily to select
    """
    f = open(filename, 'r')

    config = yaml.load(f)
    print config
    f.close()

    cf = config['Keyspaces'][keyspace][columnfamily]
    retcf = {}
    allowed_types = ktypes.keys()
    for name in cf:
        ks = cf[name]
        kn = ks.split('(')[0].strip()
        if kn not in allowed_types:
            raise ValueError("Data type `%s` not understood" % ks)
        try:
            klass = eval(ks)
        except:
            print "Error when processing %s entry in %s ComlumnFamily"
            raise
        retcf[name] = klass

    return retcf

if __name__ == "__main__":
    retcf = mapper('demos/get-range.yaml', 'Keyspace4', 'ColumnFamily2')
    print "retcf->", retcf
