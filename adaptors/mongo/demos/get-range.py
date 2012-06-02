# Demo that creates a Cassandra keyspace and a column family, and
# populates it with some entries.  Then the script show how to load
# the entries into a regular Python list, as well as on a NumPy
# container.
#
# Author: Francesc Alted
# Date: 2012-05-23

from pymongo import Connection
import datetime
import numpy as np
from time import time
import sys

from blamon import mapper


database = "database1"
collection = "collection1"
N = 10   # number of entries


def write(coll, nentries=N):
    print "Writing...", nentries, "entries"
    t0 = time()
    for i in xrange(nentries):
        coll.insert({
            'strcol': 'val_%d'%i,
            'intcol': i,
            'longcol': i,
            'floatcol': i+.1,
            'doublecol': float(i),
            'datecol': datetime.datetime.now(),
            })
    print "Time for writing:", round(time()-t0, 3)
    return nentries


def read_cl(coll):
    print "Reading to comprehension list..."
    t0 = time()
    result = [obj for obj in coll.find()]
    print "Time for reading:", round(time()-t0, 3)
    return result


def read_np(coll, conffile):
    print "Reading to structured array..."
    t0 = time()

    # Create the compound dtype
    cfg = mapper.mapper(conffile, database, collection)
    key = cfg['_id']
    dtype = [("_id", key.dtype, key.length)]
    colmeta = coll.find_one()
    for name, value in colmeta:
        nptype = cfg[name].dtype
        length = cfg[name].length
        dtype.append((name, nptype, length))
    dtype = np.dtype(dtype)

    # Fill the structured array
    sarray = np.fromiter(
        (str(_id,) + tuple([cols[name] for name in dtype.names if name != "_id"])
         for name, vals in coll.find().iteritems()),
        dtype=dtype)
    print "Time for reading:", round(time()-t0, 3)
    return sarray



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Error.  Pass the name of the YAML configuration file as parameter."
        sys.exit(-1)
    conffile = sys.argv[1]

    conn = Connection()
    db = conn[database]
    db.drop_collection(collection)  # get rid of previous entries
    coll = db[collection]

    # Write and read keys
    write(coll)
    print "collections ->", db.collection_names()
    
    # rec = coll.find_one()
    # for name, val in rec.iteritems():
    #     print "name, val, type->", name, val, type(val)

    clist = read_cl(coll)
    print "First rows of clist ->", clist[:10]

    #sarray = read_np(coll, conffile)
    #print "First rows of sarray->", repr(sarray[:10])
