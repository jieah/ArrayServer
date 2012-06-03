# Demo that creates a Cassandra keyspace and a column family, and
# populates it with some entries.  Then the script show how to load
# the entries into a regular Python list, as well as on a NumPy
# container.
#
# Author: Francesc Alted
# Date: 2012-05-23

from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa import system_manager
import datetime
import numpy as np
from time import time
import sys

from blacass import mapper


keyspace = "Keyspace6"
columnfamily = "ColumnFamily2"
N = 10000   # number of entries



def write(cf, nentries=N):
    print "Writing...", nentries, "entries"
    t0 = time()
    for i in xrange(nentries):
        cf.insert('row_%d'%i, {
            'strcol': 'val_%d'%i,
            'intcol': i,
            'longcol': i,
            'floatcol': i,
            'doublecol': i,
            'datecol': datetime.datetime.now(),
            })
    print "Time for writing:", round(time()-t0, 3)
    return nentries


def read_cl(cf):
    print "Reading to comprehension list..."
    t0 = time()
    result = [(key, columns) for key, columns in cf.get_range()]
    print "Time for reading:", round(time()-t0, 3)
    return result


def read_np(cf, conffile):
    print "Reading to structured array..."
    t0 = time()

    # Create the compound dtype
    cfg = mapper.mapper(conffile, keyspace, columnfamily)
    dtype = []
    if '__key__' in cfg:
        key = cfg['__key__']
        dtype = [("__key__", key.dtype, key.length)]
    for name, t in cfg.iteritems():
        if name != "__key__":
            dtype.append((name, t.dtype, t.length))
    dtype = np.dtype(dtype)

    # Fill the structured array
    if '__key__' in cfg:
        sarray = np.fromiter(
            ((key,) + tuple([cols[name] for name in dtype.names if name != "__key__"])
             for key, cols in cf.get_range()),
            dtype=dtype)
    else:
        sarray = np.fromiter(
            (tuple([cols[name] for name in dtype.names])
             for key, cols in cf.get_range()),
            dtype=dtype)
    print "Time for reading:", round(time()-t0, 3)
    return sarray


def setup_keyspace(sysm):
    if keyspace in sysm.list_keyspaces():
        sysm.drop_keyspace(keyspace)
    sysm.create_keyspace(keyspace, system_manager.SIMPLE_STRATEGY,
                         {'replication_factor': '1'})
    sysm.create_column_family(keyspace, columnfamily)
    sysm.alter_column(keyspace, columnfamily, 'strcol', system_manager.ASCII_TYPE)
    sysm.alter_column(keyspace, columnfamily, 'intcol', system_manager.INT_TYPE)
    sysm.alter_column(keyspace, columnfamily, 'longcol', system_manager.LONG_TYPE)
    sysm.alter_column(keyspace, columnfamily, 'floatcol', system_manager.FLOAT_TYPE)
    sysm.alter_column(keyspace, columnfamily, 'doublecol', system_manager.DOUBLE_TYPE)
    sysm.alter_column(keyspace, columnfamily, 'datecol', system_manager.DATE_TYPE)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Error.  Pass the name of the YAML configuration file as parameter."
        sys.exit(-1)
    conffile = sys.argv[1]

    sysm = system_manager.SystemManager()
    setup_keyspace(sysm)
    pool = ConnectionPool(keyspace)
    cf = ColumnFamily(pool, columnfamily)

    # Write and read keys
    write(cf)
    clist = read_cl(cf)
    #print "First rows of clist ->", clist[:10]
    sarray = read_np(cf, conffile)
    print "First rows of sarray->", repr(sarray[:10])
