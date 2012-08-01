
import gevent
import gevent.monkey
gevent.monkey.patch_all()
import gevent_zeromq
gevent_zeromq.monkey_patch()

import argparse
import logging
import shelve

from arrayserver.server.arrayserverconfig import ArrayServerConfig
from arrayserver.server.arrayservernode import ArrayServerNode

def main():

    parser = argparse.ArgumentParser(description='Start a ArrayServer Array Node process.')

    parser.add_argument(
        '-v', '--verbose', help='increase output verbosity', action='store_true'
    )
    parser.add_argument(
        '-i', '--identity', help='node identity', default='TEST'
    )
    parser.add_argument(
        '-a', '--address', help='specify the broker address', default='127.0.0.1'
    )
    parser.add_argument(
        '-p', '--port', help='specify broker port', type=int, default=5002
    )
    parser.add_argument(
        '-c', '--configdir', help='specify the config directory', default='.'
    )

    args = parser.parse_args()

    log_level = logging.INFO if not args.verbose else logging.DEBUG
    logging.basicConfig(level=log_level)
    log = logging.getLogger(__name__)

    pathmap = shelve.open('%s/pathmap.db' % args.configdir, 'c')
    reversemap = shelve.open('%s/reversemap.db' % args.configdir, 'c')
    config = ArrayServerConfig(pathmap, reversemap)

    node = ArrayServerNode('tcp://%s:%d' % (args.address, args.port), args.identity, config)
    node.run()
