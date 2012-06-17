import uuid

import gevent
import gevent.monkey
gevent.monkey.patch_all()
import gevent_zeromq
gevent_zeromq.monkey_patch()
import zmq

import blaze.server.redisutils as redisutils
import blaze.server.blazeconfig as blazeconfig
import blaze.server.blazenode as blazenode
import blaze.server.blazebroker as blazebroker

import rpd.redis_hash_dict as redis_hash_dict
import rpd.redis_config as redis_config
import redis
import sys
import os
import logging
import posixpath as blazepath
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

#load a directory full of hdf5 files
def init_dir(datadir):
    # datadir will have redis.db, redis.log, and a data directory,
    # as well as a blaze.config
    config_path = os.path.join(datadir, 'blaze.config')
    if not os.path.exists(config_path):
        base_config = os.path.join(os.path.dirname(__file__), 'blaze.config')
        with open(base_config) as f:
            config = f.read()
        config = config % {'datapath' : datadir}
        with open(config_path, 'w+') as f:
            f.write(config)

import os.path
def path_split(fpath, pathm=os.path):
    paths = []
    currpath = fpath
    while currpath != pathm.dirname(currpath):
        currpath, head = pathm.split(currpath)
        paths.append(head)
        print paths
    return paths
    
def load_dir(datadir, servername, config,
             ignore=['redis.db', 'redis.log', 'blaze.config']):
    ignore = set([os.path.join(datadir, x) for x in ignore])
    base_split_names = path_split(datadir)
    base_split_names = [x for x in base_split_names if x != '']
    for dirpath, dirnames, filenames in os.walk(datadir):
        for f in filenames:
            fpath = os.path.join(dirpath, f)
            if fpath in ignore:
                continue
            file_split_names = path_split(fpath)
            file_split_names = [len(base_split_names):]
            blaze_url = blazepath.join('/', *file_split_names)
            blazeconfig.generate_config_hdf5(servername, blaze_url,
                                             datapath, config)
            
import argparse
def main():
    parser = argparse.ArgumentParser(description='Start blaze')
    parser.add_argument('datapath', nargs="?")
    parser.add_argument(
        '-s', '--server-name',
        help='name of server',
        default='myserver'
    )
    parser.add_argument(
        '-fa', '--front-address',
        help='specify the adress which communicates with the outside world',
        default='tcp://127.0.0.1:5555'
    )
    parser.add_argument(
        '-ba', '--back-address',
        help='specify the internal address for communicating with workers',
        default='tcp://127.0.0.1:5556'
    )
    parser.add_argument(
        '-n', '--namespace',
        help='namespace of node, nodes can see all nodes within this namespace'
        default='main'
    )
    parser.add_argument('-nr', '--no-redis', action='store_true')
    parser.add_argument('-rh', '--redis-host', default='localhost')
    parser.add_argument('-rp', '--redis-port', default=6379)
    parser.add_argument('-sc', '--skip-config', action='store_true')
    parser.add_argument('-rc', '--rebuild-config', action='store_true')    
    args = parser.parse_args()
    print args
    servername = args.server_name
    def run_node():
         log.debug("starting node")
         config = blazeconfig.BlazeConfig(pathmap, reversemap)
         node = blazenode.BlazeNode(backaddr, servername, config)
         node.start()
        
    def run_broker():
         b = blazebroker.BlazeBroker(frontaddr, backaddr)
         b.start()
         
    datapath = args.datapath
    if not args.no_redis:
        assert args.redis_host == 'localhost', 'cannot start redis on another host'
        proc = redisutils.RedisProcess(
            args.redis_port,
            datapath,
            data_file=os.path.join(datapath, 'redis.db'),
            log_file=os.path.join(datapath, 'redis.log'))
    redis_config.CLIENT = redis.Redis(host=args.redis_host, port=args.redis_port)
    namespace = args.namespace
    pathmap = redisutils.redis_pathmap(namespace)
    reversemap = redisutils.redis_pathmap(namespace)    
    config = blazeconfig.BlazeConfig(pathmap, reversemap)
    load_dir(datapath, servername, config)
    print args
    frontaddr = args.front_address
    backaddr = args.back_address

    run_node()
    run_broker()

if __name__ == "__main__":
    main()
    
