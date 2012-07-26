import uuid

import gevent
import gevent.monkey
gevent.monkey.patch_all()
import gevent_zeromq
gevent_zeromq.monkey_patch()
import zmq

import blaze.server.tests
import blaze.server.redisutils as redisutils
import blaze.server.blazeconfig as blazeconfig
import blaze.server.blazeconfig.orderedyaml as orderedyaml
import blaze.server.blazenode as blazenode
import blaze.server.blazebroker as blazebroker
import blaze.server.tests.test_utils as test_utils

import redis
import collections
import sys
import yaml
import os
import logging
import time
import atexit

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def start_redis(datapath, redis_port):
    proc = redisutils.RedisProcess(redis_port, datapath,
        data_file=os.path.join(datapath, 'redis.db'),
        log_file=os.path.join(datapath, 'redis.log'))
    atexit.register(proc.close)
    def redis_ping():
        log.info('checking on redis')
        conn = redis.Redis(port=redis_port)
        try:
            status =  conn.ping()
            if status:
                log.info('redis is up')
            return status
        except redis.ConnectionError as e:
            return False
    test_utils.wait_until(redis_ping, timeout=5.0, interval=0.5)
    return proc

#load a directory full of hdf5 files
def build_config(datadir, disco=None):
    # datadir will have redis.db, redis.log, and a data directory,
    # as well as a blaze.config
    config_path = os.path.join(datadir, 'blaze.config')
    base_config = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               'default_config.yaml')
    with open(base_config) as f:
        config = f.read()
    config = config % {'datapath' : datadir}
    yamlconfig =yaml.load(config, Loader=orderedyaml.OrderedDictYAMLLoader)
    if disco is not None:
        yamlconfig['disco'] = collections.OrderedDict([('type', 'disco'),
                                                       ('connection', disco)])
    with open(config_path, 'w+') as f:
        f.write(yaml.dump(yamlconfig))
            
import os.path
import argparse
def argparser():
    parser = argparse.ArgumentParser(description='Start blaze')
    datapath = os.path.abspath(os.path.dirname(blaze.server.tests.__file__))
    datapath = os.path.join(datapath, 'data')
    parser.add_argument('datapath', nargs="?", default=datapath)
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
        help='namespace of node, nodes can see all nodes within this namespace',
        default='main'
    )
    
    parser.add_argument(
        '-d', '--disco',
        help='disco host:port',
        default=None
    )
    
    parser.add_argument('-nr', '--no-redis', action='store_true')
    parser.add_argument('-rh', '--redis-host', default='localhost')
    parser.add_argument('-rp', '--redis-port', default=6379)
    parser.add_argument('-sc', '--skip-config', action='store_true')
    parser.add_argument('-rc', '--rebuild-config', action='store_true')
    return parser

def write_pid(prefix, scriptname):
    pidfile = os.path.join(prefix, scriptname + '.pid')
    if os.path.exists(pidfile):
        with open(pidfile) as f:
            pid = int(f.read())
            if pid != os.getpid():
                raise Exception, "%s already running on this at PID %s" % (scriptname, pid)
    else:
        with open(pidfile, 'w') as f:
            f.write(str(os.getpid()))
    atexit.register(os.remove, pidfile)
    return True

def start_blaze(args):
    servername = args.server_name
    if args.datapath is None:
        datapath = os.path.abspath(os.path.dirname(blaze.server.tests.__file__))
        datapath = os.path.join(datapath, 'data')
    else:
        datapath = os.path.abspath(args.datapath)
    print 'datapath', datapath
    if not args.no_redis:
        assert args.redis_host == 'localhost', 'cannot start redis on another host'
        proc = start_redis(datapath, args.redis_port)
    if not args.skip_config:
        build_config(datapath, disco=args.disco)
        data = yaml.load(open(os.path.join(datapath, 'blaze.config')).read(),
                         Loader=orderedyaml.OrderedDictYAMLLoader)
        config = blazeconfig.BlazeConfig(servername, host=args.redis_host,
                                         port=args.redis_port, sourceconfig=data)
    else:
        config = blazeconfig.BlazeConfig(servername, host=args.redis_host,
                                         port=args.redis_port)
    namespace = args.namespace
    frontaddr = args.front_address
    backaddr = args.back_address
    broker = blazebroker.BlazeBroker(frontaddr, backaddr, config)
    broker.start()
    node = blazenode.BlazeNode(backaddr, servername, config)
    node.start()
    return proc, broker, node
    
def main():
    parser = argparser()
    args = parser.parse_args()
    write_pid('blaze')
    redisproc, broker, node = start_blaze(args)
    node.join()
    
if __name__ == "__main__":
    main()
    
