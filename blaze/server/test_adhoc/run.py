import gevent
import gevent.monkey
gevent.monkey.patch_all()
import gevent_zeromq
gevent_zeromq.monkey_patch()
import zmq

import blazeconfig
import blazenode
import arrayserver_app
import shelve
import sys
import os
import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

pathmapfile = 'pathamp.db'
reversemapfile = 'reversemap.db'
servername = 'myserver'
blazeprefix = '/hugodata'
datapath = 'tests/gold.hdf5'
frontaddr = 'tcp://127.0.0.1:5555'
backaddr = 'tcp://127.0.0.1:5556'

try:
    os.remove(pathmapfile)
    os.remove(reversemapfile)
except:
    pass

config = blazeconfig.BlazeConfig(shelve.open(pathmapfile, 'c'),
                                 shelve.open(reversemapfile, 'c'))
blazeconfig.generate_config_hdf5(servername, blazeprefix, datapath, config)
config.sync()

def run_node():
    log.debug("starting node")
    pathmap = shelve.open(pathmapfile)
    reversemap = shelve.open(reversemapfile)
    config = blazeconfig.BlazeConfig(pathmap, reversemap)
    node = blazenode.BlazeNode(backaddr, servername, config)
    node.start()

def run_broker():
    log.debug("starting broker")
    b = arrayserver_app.BlazeBroker(frontaddr, backaddr)
    b.start()
    
run_node()
run_broker()
